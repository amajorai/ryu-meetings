use super::*;
use crate::{Meeting, MeetingSource, MeetingStatus};

fn meeting(id: &str, created_at: &str) -> Meeting {
    Meeting {
        id: id.to_string(),
        title: "T".to_string(),
        title_custom: false,
        icon: None,
        app: None,
        source: MeetingSource::Manual,
        status: MeetingStatus::Recording,
        started_at: created_at.to_string(),
        ended_at: None,
        participants: vec![],
        notes: None,
        space_id: None,
        doc_id: None,
        kind: crate::MeetingKind::Meeting,
        recap: None,
        created_at: created_at.to_string(),
        updated_at: created_at.to_string(),
    }
}

// ── at rest ──────────────────────────────────────────────────────────────

const SECRET: &str = "zebra-meetings-marker";

fn keys(n: u8) -> PluginKeys {
    use base64::Engine as _;
    let subkey = base64::engine::general_purpose::STANDARD.encode([n; 32]);
    PluginKeys::sealing(
        ryu_crypto::PluginCipher::from_subkey_b64(PLUGIN_ID, &subkey).unwrap(),
        crate::seal_keys::KeySource::Delivered,
    )
}

fn db_path() -> PathBuf {
    std::env::temp_dir()
        .join(format!(
            "ryu-meetings-seal-{}",
            uuid::Uuid::new_v4().simple()
        ))
        .join("meetings.db")
}

fn disk_has(path: &Path, needle: &str) -> bool {
    let mut bytes = std::fs::read(path).unwrap_or_default();
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    bytes.extend(std::fs::read(PathBuf::from(wal)).unwrap_or_default());
    bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
}

async fn seed(store: &MeetingStore) -> i64 {
    let mut m = meeting("m1", "2026-01-01T00:00:00Z");
    m.title = format!("title {SECRET}");
    store.upsert_meeting(&m).await.unwrap();
    let segment = store
        .insert_segment(
            "m1",
            0,
            Some(&format!("spk {SECRET}")),
            &format!("said {SECRET}"),
        )
        .await
        .unwrap();
    store
        .set_segment_speaker(segment.id, &format!("Me {SECRET}"))
        .await
        .unwrap();
    segment.id
}

async fn assert_reads(store: &MeetingStore) {
    assert_eq!(
        store.get_meeting("m1").await.unwrap().unwrap().title,
        format!("title {SECRET}")
    );
    let segments = store.list_segments("m1").await.unwrap();
    assert_eq!(segments[0].text, format!("said {SECRET}"));
    assert_eq!(
        segments[0].speaker.as_deref(),
        Some(format!("Me {SECRET}").as_str())
    );
}

#[tokio::test]
async fn transcripts_and_meetings_are_sealed_on_disk() {
    let path = db_path();
    let store = MeetingStore::open_with_keys(path.clone(), &keys(1)).unwrap();
    seed(&store).await;
    assert_reads(&store).await;
    assert!(!disk_has(&path, SECRET));
    let conn = store.conn.lock().await;
    let cipher = keys(1).content_cipher(STORE).unwrap();
    ryu_crypto::content_sqlite::assert_all_open(&conn, &cipher, &column_plans().unwrap()).unwrap();
}

#[tokio::test]
async fn legacy_plaintext_is_migrated_once_and_still_reads() {
    let path = db_path();
    {
        let store = MeetingStore::open_with_keys(path.clone(), &PluginKeys::compat()).unwrap();
        assert_eq!(store.seal_health()["state"], "compat");
        seed(&store).await;
    }
    assert!(disk_has(&path, SECRET));
    let store = MeetingStore::open_with_keys(path.clone(), &keys(2)).unwrap();
    assert_reads(&store).await;
    assert!(!disk_has(&path, SECRET));
    let snapshot = |conn: &Connection| -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT json FROM meetings UNION ALL SELECT text || speaker FROM segments")
            .unwrap();
        let rows = stmt.query_map([], |row| row.get(0)).unwrap();
        rows.map(Result::unwrap).collect()
    };
    let first = {
        let conn = store.conn.lock().await;
        let cipher = keys(2).content_cipher(STORE).unwrap();
        ryu_crypto::content_sqlite::assert_all_open(&conn, &cipher, &column_plans().unwrap())
            .unwrap();
        snapshot(&conn)
    };
    drop(store);
    let again = MeetingStore::open_with_keys(path, &keys(2)).unwrap();
    assert_eq!(snapshot(&*again.conn.lock().await), first);
}

/// What the next start leaves: `ok` once it vacuumed, `vacuum_pending`
/// only when the disk is too full for a `VACUUM` (the freelist scrub
/// still ran).
fn expected_after_pending_vacuum(db: &std::path::Path) -> &'static str {
    let size = std::fs::metadata(db).map(|m| m.len()).unwrap_or(0);
    let needed = size
        .saturating_mul(2)
        .saturating_add(ryu_crypto::content_sqlite::VACUUM_HEADROOM_BYTES);
    match ryu_crypto::sealed_file::ensure_free_space(db.parent().unwrap(), needed) {
        Ok(()) => "ok",
        Err(_) => "vacuum_pending",
    }
}

#[tokio::test]
async fn background_migration_leaves_compaction_to_the_next_start_and_inserts_keep_working() {
    use ryu_crypto::content_sqlite::{item_state, read_store_record};
    let path = db_path();
    {
        let legacy = MeetingStore::open_with_keys(path.clone(), &PluginKeys::compat()).unwrap();
        seed(&legacy).await;
        for index in 1..300 {
            legacy
                .insert_segment("m1", index, None, &format!("said {SECRET} {index}"))
                .await
                .unwrap();
        }
    }
    // Every store counts as large: the migration runs on its own thread.
    super::TEST_SYNC_MIGRATION_MAX_ROWS.with(|rows| rows.set(Some(0)));
    let store = MeetingStore::open_with_keys(path.clone(), &keys(6)).unwrap();
    super::TEST_SYNC_MIGRATION_MAX_ROWS.with(|rows| rows.set(None));
    // Live transcript segments keep landing while it runs.
    for index in 300..340 {
        store
            .insert_segment("m1", index, None, &format!("live {index}"))
            .await
            .unwrap();
    }
    let plans = column_plans().unwrap();
    let watcher = Connection::open(&path).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while !plans.iter().all(|plan| {
        item_state(&watcher, STORE, &plan.item())
            .unwrap()
            .as_deref()
            == Some("sealed")
    }) {
        assert!(
            std::time::Instant::now() < deadline,
            "background migration stalled"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        read_store_record(&watcher, STORE).unwrap().unwrap().state,
        "vacuum_pending",
        "the background thread never scrubs or vacuums under traffic"
    );
    assert_eq!(store.list_segments("m1").await.unwrap().len(), 340);
    drop(watcher);
    drop(store);
    // The next start compacts before serving.
    let reopened = MeetingStore::open_with_keys(path.clone(), &keys(6)).unwrap();
    let state = read_store_record(&*reopened.conn.lock().await, STORE)
        .unwrap()
        .unwrap()
        .state;
    assert_eq!(state, expected_after_pending_vacuum(&path));
    assert_reads(&reopened).await;
    assert!(
        !disk_has(&path, SECRET),
        "compaction scrubbed the plaintext"
    );
}

#[tokio::test]
async fn a_segment_copied_to_another_row_does_not_open_and_is_skipped() {
    let store = MeetingStore::open_with_keys(db_path(), &keys(3)).unwrap();
    store
        .upsert_meeting(&meeting("m1", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    store.insert_segment("m1", 0, None, "first").await.unwrap();
    store.insert_segment("m1", 1, None, "second").await.unwrap();
    store
        .conn
        .lock()
        .await
        .execute(
            "UPDATE segments SET text = (SELECT text FROM segments ORDER BY id LIMIT 1)
             WHERE id = (SELECT MAX(id) FROM segments)",
            [],
        )
        .unwrap();
    let segments = store.list_segments("m1").await.unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].text, "first");
    assert_eq!(store.seal_health()["unreadable_rows"], 1);
}

#[tokio::test]
async fn another_key_or_no_key_refuses_writes_and_keeps_data() {
    let path = db_path();
    seed(&MeetingStore::open_with_keys(path.clone(), &keys(4)).unwrap()).await;
    for other in [keys(5), PluginKeys::compat()] {
        let store = MeetingStore::open_with_keys(path.clone(), &other).unwrap();
        assert_eq!(store.seal_health()["state"], "unhealthy");
        assert!(store.insert_segment("m1", 9, None, "x").await.is_err());
        assert!(store.set_title("m1", "x").await.is_err());
        assert!(!store.audio_seal().is_sealing());
    }
    let back = MeetingStore::open_with_keys(path, &keys(4)).unwrap();
    assert_reads(&back).await;
}
