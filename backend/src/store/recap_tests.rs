use super::recap_side::DigestRow;
use super::recaps::{ChunkStatus, NewChunk};
use super::*;
use crate::test_support::{recap_row, temp_store};
use crate::{MeetingKind, MeetingSource, MeetingStatus};

fn legacy(id: &str) -> Meeting {
    let mut m = recap_row(id, "x", "2026-11-08", "2026-11-08T09:00:00Z");
    m.kind = MeetingKind::Meeting;
    m.recap = None;
    m.source = MeetingSource::Manual;
    m
}

#[tokio::test]
async fn recap_queries_are_owner_scoped_and_legacy_list_hides_recaps() {
    let store = temp_store();
    store.upsert_meeting(&legacy("mtg_1")).await.unwrap();
    store
        .upsert_meeting(&recap_row(
            "rcp_a",
            "alice",
            "2026-11-07",
            "2026-11-07T10:00:00Z",
        ))
        .await
        .unwrap();
    store
        .upsert_meeting(&recap_row(
            "rcp_b",
            "alice",
            "2026-11-08",
            "2026-11-08T10:00:00Z",
        ))
        .await
        .unwrap();
    store
        .upsert_meeting(&recap_row(
            "rcp_c",
            "bob",
            "2026-11-08",
            "2026-11-08T11:00:00Z",
        ))
        .await
        .unwrap();

    let legacy_ids: Vec<String> = store
        .list_meetings()
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.id)
        .collect();
    assert_eq!(legacy_ids, vec!["mtg_1"]);

    assert!(store
        .get_owned_recap("alice", "rcp_a")
        .await
        .unwrap()
        .is_some());
    assert!(store
        .get_owned_recap("bob", "rcp_a")
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_owned_recap("alice", "mtg_1")
        .await
        .unwrap()
        .is_none());

    let alice: Vec<String> = store
        .list_owned_recaps("alice", None, None, None, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.id)
        .collect();
    assert_eq!(alice, vec!["rcp_b", "rcp_a"], "newest first, alice only");
    let one_day = store
        .list_owned_recaps("alice", Some("2026-11-08"), Some("2026-11-08"), None, 10)
        .await
        .unwrap();
    assert_eq!(one_day.len(), 1);
    let after_cursor = store
        .list_owned_recaps("alice", None, None, Some("2026-11-08T10:00:00Z"), 10)
        .await
        .unwrap();
    assert_eq!(after_cursor[0].id, "rcp_a");

    assert_eq!(
        store.kind_and_owner("rcp_c").await.unwrap(),
        Some((MeetingKind::Recap, Some("bob".into())))
    );
    let mut owners = store.recap_owners().await.unwrap();
    owners.sort();
    assert_eq!(owners, vec!["alice", "bob"]);
}

#[tokio::test]
async fn session_lookup_and_chunk_ledger_commit_with_their_segment() {
    let store = temp_store();
    let recap = recap_row("rcp_s", "alice", "2026-11-08", "2026-11-08T10:00:00Z");
    let session = recap.recap.as_ref().unwrap().client_session_id.clone();
    store.upsert_meeting(&recap).await.unwrap();
    assert_eq!(
        store
            .find_recap_by_session("alice", &session)
            .await
            .unwrap()
            .map(|m| m.id),
        Some("rcp_s".into())
    );
    assert!(store
        .find_recap_by_session("bob", &session)
        .await
        .unwrap()
        .is_none());

    let later = NewChunk {
        seq: 1,
        offset_ms: 30_000,
        duration_ms: 30_000,
        sha256: "b",
        bytes: 10,
        status: ChunkStatus::Transcribed,
        text: Some("second"),
    };
    let first = NewChunk {
        seq: 0,
        offset_ms: 0,
        duration_ms: 30_000,
        sha256: "a",
        bytes: 10,
        status: ChunkStatus::Transcribed,
        text: Some("first"),
    };
    let silent = NewChunk {
        seq: 2,
        offset_ms: 60_000,
        duration_ms: 30_000,
        sha256: "c",
        bytes: 0,
        status: ChunkStatus::SkippedClient,
        text: None,
    };
    // Out of order on purpose: segments still read back by offset.
    assert!(matches!(
        store.record_recap_chunk("rcp_s", later).await.unwrap(),
        ChunkOutcome::Recorded(Some(_))
    ));
    assert!(matches!(
        store.record_recap_chunk("rcp_s", first).await.unwrap(),
        ChunkOutcome::Recorded(Some(_))
    ));
    assert!(matches!(
        store.record_recap_chunk("rcp_s", silent).await.unwrap(),
        ChunkOutcome::Recorded(None)
    ));
    let texts: Vec<String> = store
        .recap_segments("rcp_s")
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.text)
        .collect();
    assert_eq!(texts, vec!["first", "second"]);
    let chunks = store.recap_chunks("rcp_s").await.unwrap();
    assert_eq!(
        chunks.iter().map(|c| c.seq).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(chunks[2].status, "skipped_client");
    assert!(chunks[2].segment_id.is_none());

    let dup = NewChunk {
        seq: 0,
        offset_ms: 0,
        duration_ms: 30_000,
        sha256: "a",
        bytes: 10,
        status: ChunkStatus::Transcribed,
        text: Some("again"),
    };
    // A seq recorded meanwhile is reported with its row, and nothing is written.
    match store.record_recap_chunk("rcp_s", dup).await.unwrap() {
        ChunkOutcome::Existing(row) => assert_eq!(row.sha256, "a"),
        other => panic!("expected the existing row, got {other:?}"),
    }
    assert_eq!(
        store.recap_segments("rcp_s").await.unwrap().len(),
        2,
        "nothing written"
    );

    assert!(store.delete_recap_rows("rcp_s").await.unwrap());
    assert!(store.recap_chunks("rcp_s").await.unwrap().is_empty());
    assert!(store.list_segments("rcp_s").await.unwrap().is_empty());
}

#[tokio::test]
async fn settings_digests_and_usage_round_trip_and_purge_per_owner() {
    let store = temp_store();
    store
        .put_recap_settings("alice", &serde_json::json!({ "people": true }))
        .await
        .unwrap();
    assert_eq!(
        store.recap_settings("alice").await.unwrap().unwrap()["people"],
        true
    );
    assert!(store.recap_settings("bob").await.unwrap().is_none());

    let row = DigestRow {
        owner_user_id: "alice".into(),
        day: "2026-11-01".into(),
        tz: "UTC".into(),
        json: serde_json::json!({ "count": 1 }),
        source_ids: "rcp_1".into(),
        generated_at: "now".into(),
    };
    store.put_recap_digest(&row).await.unwrap();
    store
        .put_recap_digest(&DigestRow {
            day: "2026-11-08".into(),
            ..row.clone()
        })
        .await
        .unwrap();
    assert_eq!(
        store.recap_digest("alice", "2026-11-01").await.unwrap(),
        Some(row)
    );
    assert!(store
        .recap_digest("bob", "2026-11-01")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .delete_recap_digests_before("alice", "2026-11-05")
            .await
            .unwrap(),
        1
    );

    assert_eq!(
        store
            .add_recap_usage("alice", "2026-11-08", 5)
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        store
            .add_recap_usage("alice", "2026-11-08", 7)
            .await
            .unwrap(),
        12
    );
    assert_eq!(store.recap_usage("bob", "2026-11-08").await.unwrap(), 0);

    store
        .put_recap_settings("bob", &serde_json::json!({}))
        .await
        .unwrap();
    store.purge_recap_side_rows("alice").await.unwrap();
    assert!(store.recap_settings("alice").await.unwrap().is_none());
    assert!(store
        .recap_digest("alice", "2026-11-08")
        .await
        .unwrap()
        .is_none());
    assert_eq!(store.recap_usage("alice", "2026-11-08").await.unwrap(), 0);
    assert!(
        store.recap_settings("bob").await.unwrap().is_some(),
        "bob untouched"
    );
}

#[tokio::test]
async fn recap_side_tables_are_sealed_on_disk() {
    let dir =
        std::env::temp_dir().join(format!("ryu-recap-seal-{}", uuid::Uuid::new_v4().simple()));
    let path = dir.join("meetings.db");
    use base64::Engine as _;
    let subkey = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
    let keys = PluginKeys::sealing(
        ryu_crypto::PluginCipher::from_subkey_b64(PLUGIN_ID, &subkey).unwrap(),
        crate::seal_keys::KeySource::Delivered,
    );
    let store = MeetingStore::open_with_keys(path.clone(), &keys).unwrap();
    let secret = "walrus-recap-secret";
    store
        .put_recap_settings("alice", &serde_json::json!({ "model": secret }))
        .await
        .unwrap();
    store
        .put_recap_digest(&DigestRow {
            owner_user_id: "alice".into(),
            day: "2026-11-08".into(),
            tz: "UTC".into(),
            json: serde_json::json!({ "summary": secret }),
            source_ids: String::new(),
            generated_at: "now".into(),
        })
        .await
        .unwrap();
    let mut recap = recap_row("rcp_x", "alice", "2026-11-08", "2026-11-08T10:00:00Z");
    recap.title = secret.into();
    recap.status = MeetingStatus::Done;
    store.upsert_meeting(&recap).await.unwrap();
    let mut bytes = std::fs::read(&path).unwrap_or_default();
    bytes.extend(std::fs::read(dir.join("meetings.db-wal")).unwrap_or_default());
    assert!(!bytes.windows(secret.len()).any(|w| w == secret.as_bytes()));
    assert_eq!(
        store.recap_settings("alice").await.unwrap().unwrap()["model"],
        secret
    );
    let _ = std::fs::remove_dir_all(dir);
}

fn chunk(seq: u32, text: Option<&'static str>) -> NewChunk<'static> {
    NewChunk {
        seq,
        offset_ms: i64::from(seq) * 30_000,
        duration_ms: 30_000,
        sha256: "sha",
        bytes: 10,
        status: ChunkStatus::Transcribed,
        text,
    }
}

async fn orphan_rows(store: &MeetingStore, id: &str) -> (usize, usize) {
    (
        store.list_segments(id).await.unwrap().len(),
        store.recap_chunks(id).await.unwrap().len(),
    )
}

#[tokio::test]
async fn a_chunk_for_a_recap_deleted_during_stt_leaves_no_orphan() {
    let store = temp_store();
    let recap = recap_row("rcp_gone", "alice", "2026-11-08", "2026-11-08T10:00:00Z");
    store.upsert_meeting(&recap).await.unwrap();
    // The handler saw a recording recap (`get_owned_recap` + `accepts_audio`)...
    assert!(store
        .get_owned_recap("alice", "rcp_gone")
        .await
        .unwrap()
        .is_some());
    // ...then the owner deleted it while the chunk was in STT.
    assert!(store.delete_recap_rows("rcp_gone").await.unwrap());
    assert!(matches!(
        store
            .record_recap_chunk("rcp_gone", chunk(0, Some("secret words")))
            .await
            .unwrap(),
        ChunkOutcome::RecapGone
    ));
    assert_eq!(orphan_rows(&store, "rcp_gone").await, (0, 0));
}

#[tokio::test]
async fn a_chunk_for_a_recap_finalized_or_unconfirmed_is_refused() {
    let store = temp_store();
    let mut done = recap_row("rcp_done", "alice", "2026-11-08", "2026-11-08T10:00:00Z");
    done.status = crate::MeetingStatus::Processing;
    store.upsert_meeting(&done).await.unwrap();
    assert!(matches!(
        store
            .record_recap_chunk("rcp_done", chunk(0, Some("late")))
            .await
            .unwrap(),
        ChunkOutcome::RecapClosed
    ));
    assert_eq!(orphan_rows(&store, "rcp_done").await, (0, 0));

    let mut pending = recap_row("rcp_fast", "alice", "2026-11-08", "2026-11-08T10:00:00Z");
    if let Some(meta) = pending.recap.as_mut() {
        meta.needs_confirm = true;
    }
    store.upsert_meeting(&pending).await.unwrap();
    assert!(matches!(
        store
            .record_recap_chunk("rcp_fast", chunk(0, Some("early")))
            .await
            .unwrap(),
        ChunkOutcome::RecapClosed
    ));
    assert_eq!(orphan_rows(&store, "rcp_fast").await, (0, 0));
}

#[test]
fn constraint_violations_are_found_under_a_context() {
    let raw = rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY),
        Some("UNIQUE constraint failed: recap_chunks.meeting_id, recap_chunks.seq".into()),
    );
    let wrapped = anyhow::Error::from(raw).context("recording recap chunk");
    assert_eq!(wrapped.to_string(), "recording recap chunk");
    assert!(is_constraint_violation(&wrapped));
    assert!(!is_constraint_violation(&anyhow::anyhow!(
        "UNIQUE but not sqlite"
    )));
}
