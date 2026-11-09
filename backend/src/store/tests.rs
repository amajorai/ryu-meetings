use super::*;
use crate::{Meeting, MeetingNotes, MeetingSource, MeetingStatus};

fn temp_store() -> MeetingStore {
    let dir = std::env::temp_dir().join(format!(
        "ryu-meetings-store-test-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    MeetingStore::open(dir.join("meetings.db")).expect("open temp store")
}

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

#[tokio::test]
async fn upsert_get_roundtrip_and_conflict_update() {
    let store = temp_store();
    let mut m = meeting("m1", "2026-01-01T00:00:00Z");
    store.upsert_meeting(&m).await.unwrap();
    assert_eq!(store.get_meeting("m1").await.unwrap().unwrap().title, "T");

    // Same id ⇒ ON CONFLICT updates the json in place (no duplicate row).
    m.title = "Updated".into();
    store.upsert_meeting(&m).await.unwrap();
    assert_eq!(
        store.get_meeting("m1").await.unwrap().unwrap().title,
        "Updated"
    );
    assert_eq!(store.list_meetings().await.unwrap().len(), 1);
}

#[tokio::test]
async fn get_missing_meeting_is_none() {
    let store = temp_store();
    assert!(store.get_meeting("nope").await.unwrap().is_none());
}

#[tokio::test]
async fn list_orders_newest_created_first() {
    let store = temp_store();
    store
        .upsert_meeting(&meeting("old", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    store
        .upsert_meeting(&meeting("new", "2026-06-01T00:00:00Z"))
        .await
        .unwrap();
    let ids: Vec<String> = store
        .list_meetings()
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.id)
        .collect();
    assert_eq!(ids, vec!["new".to_string(), "old".to_string()]);
}

#[tokio::test]
async fn set_title_marks_custom_and_missing_is_none() {
    let store = temp_store();
    store
        .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    let updated = store.set_title("m", "Renamed").await.unwrap().unwrap();
    assert_eq!(updated.title, "Renamed");
    assert!(updated.title_custom);
    assert!(store.set_title("missing", "x").await.unwrap().is_none());
}

#[tokio::test]
async fn auto_set_title_respects_custom_guard() {
    let store = temp_store();
    store
        .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    // Not custom ⇒ auto title is written but title_custom stays false.
    let updated = store.auto_set_title("m", "Auto").await.unwrap().unwrap();
    assert_eq!(updated.title, "Auto");
    assert!(!updated.title_custom);

    // A manual rename locks it; a later auto attempt is refused.
    store.set_title("m", "Manual").await.unwrap();
    assert!(store.auto_set_title("m", "Auto2").await.unwrap().is_none());
    assert_eq!(
        store.get_meeting("m").await.unwrap().unwrap().title,
        "Manual"
    );

    // Missing id ⇒ None.
    assert!(store
        .auto_set_title("missing", "x")
        .await
        .unwrap()
        .is_none());
}

fn notes(summary: &str) -> MeetingNotes {
    MeetingNotes {
        summary: summary.to_string(),
        ..Default::default()
    }
}

/// A rename landing while notes generate must survive the finalize write:
/// only notes/status/ended_at change, so `title_custom` keeps the auto-namer
/// off the user's title too.
#[tokio::test]
async fn finish_meeting_preserves_concurrent_rename() {
    let store = temp_store();
    store
        .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    store
        .set_status("m", MeetingStatus::Processing)
        .await
        .unwrap();
    store.set_title("m", "Mine").await.unwrap();

    let done = store
        .finish_meeting("m", notes("summary"), "2026-01-01T01:00:00Z")
        .await
        .unwrap()
        .expect("meeting still exists");
    assert_eq!(done.title, "Mine");
    assert!(done.title_custom);
    assert_eq!(done.status, MeetingStatus::Done);
    assert_eq!(done.ended_at.as_deref(), Some("2026-01-01T01:00:00Z"));
    let stored = store.get_meeting("m").await.unwrap().unwrap();
    assert_eq!(stored.title, "Mine");
    assert!(stored.title_custom);
    assert_eq!(stored.notes.unwrap().summary, "summary");
}

/// Finishing (or linking) a meeting deleted mid-finalize must not re-create it.
#[tokio::test]
async fn finish_meeting_does_not_resurrect_deleted() {
    let store = temp_store();
    store
        .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    assert!(store.delete_meeting("m").await.unwrap());

    assert!(store
        .finish_meeting("m", notes("late"), "2026-01-01T01:00:00Z")
        .await
        .unwrap()
        .is_none());
    assert!(store.set_space("m", "s", "d").await.unwrap().is_none());
    assert!(store
        .set_status("m", MeetingStatus::Processing)
        .await
        .unwrap()
        .is_none());
    assert!(store.get_meeting("m").await.unwrap().is_none());
    assert!(store.list_meetings().await.unwrap().is_empty());
}

#[tokio::test]
async fn revert_processing_only_moves_a_processing_meeting() {
    let store = temp_store();
    store
        .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    // Still recording ⇒ nothing to revert.
    assert!(!store
        .revert_processing("m", MeetingStatus::Recording)
        .await
        .unwrap());

    store
        .set_status("m", MeetingStatus::Processing)
        .await
        .unwrap();
    assert!(store
        .revert_processing("m", MeetingStatus::Recording)
        .await
        .unwrap());
    assert_eq!(
        store.get_meeting("m").await.unwrap().unwrap().status,
        MeetingStatus::Recording
    );
    assert!(!store
        .revert_processing("missing", MeetingStatus::Recording)
        .await
        .unwrap());
}

#[tokio::test]
async fn delete_cascades_segments_and_reports_removed() {
    let store = temp_store();
    store
        .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    store.insert_segment("m", 0, None, "hi").await.unwrap();
    assert!(store.delete_meeting("m").await.unwrap());
    assert!(store.list_segments("m").await.unwrap().is_empty());
    // Second delete ⇒ nothing removed.
    assert!(!store.delete_meeting("m").await.unwrap());
}

#[tokio::test]
async fn segments_insert_order_and_speaker_update() {
    let store = temp_store();
    let s1 = store.insert_segment("m", 0, None, "first").await.unwrap();
    let s2 = store
        .insert_segment("m", 10, Some("Me"), "second")
        .await
        .unwrap();
    assert!(s2.id > s1.id);
    assert_eq!(s2.speaker.as_deref(), Some("Me"));

    store.set_segment_speaker(s1.id, "Speaker 1").await.unwrap();
    let listed = store.list_segments("m").await.unwrap();
    assert_eq!(listed.len(), 2);
    // Order is by id ASC.
    assert_eq!(listed[0].text, "first");
    assert_eq!(listed[0].speaker.as_deref(), Some("Speaker 1"));
    assert_eq!(listed[1].text, "second");
}

#[tokio::test]
async fn list_segments_scopes_to_meeting() {
    let store = temp_store();
    store.insert_segment("a", 0, None, "a-seg").await.unwrap();
    store.insert_segment("b", 0, None, "b-seg").await.unwrap();
    let a = store.list_segments("a").await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].text, "a-seg");
}

#[tokio::test]
async fn emit_reaches_a_live_subscriber() {
    let store = temp_store();
    let mut rx = store.subscribe();
    store.emit(MeetingEvent::Status {
        meeting_id: "m".into(),
        status: MeetingStatus::Processing,
    });
    match rx.try_recv().unwrap() {
        MeetingEvent::Status { meeting_id, status } => {
            assert_eq!(meeting_id, "m");
            assert_eq!(status, MeetingStatus::Processing);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn emit_without_subscribers_is_a_noop() {
    let store = temp_store();
    // No subscriber ⇒ send error is swallowed; must not panic.
    store.emit(MeetingEvent::Detected {
        app: "zoom".into(),
        title: "t".into(),
        detected_at: "now".into(),
    });
}
