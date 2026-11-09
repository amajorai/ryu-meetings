//! Transcript segments: per-row sealing binds each value to its own row.

use super::super::*;
use crate::seal_keys::PluginKeys;

fn sealing_store() -> MeetingStore {
    use base64::Engine as _;
    let subkey = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
    let keys = PluginKeys::sealing(
        ryu_crypto::PluginCipher::from_subkey_b64(PLUGIN_ID, &subkey).unwrap(),
        crate::seal_keys::KeySource::Delivered,
    );
    let path = std::env::temp_dir()
        .join(format!(
            "ryu-meetings-segments-{}",
            uuid::Uuid::new_v4().simple()
        ))
        .join("meetings.db");
    MeetingStore::open_with_keys(path, &keys).unwrap()
}

#[tokio::test]
async fn a_segment_round_trips_and_is_never_plaintext_on_disk() {
    let store = sealing_store();
    let first = store
        .insert_segment("m1", 5, Some("Ana"), "the budget is 42")
        .await
        .unwrap();
    let second = store.insert_segment("m1", 9, None, "ok").await.unwrap();
    assert!(second.id > first.id);
    assert_eq!(first.t_offset_ms, 5);

    let listed = store.list_segments("m1").await.unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].text, "the budget is 42");
    assert_eq!(listed[0].speaker.as_deref(), Some("Ana"));
    assert_eq!(listed[1].speaker, None);

    let conn = store.conn.lock().await;
    let (text, speaker): (String, Option<String>) = conn
        .query_row(
            "SELECT text, speaker FROM segments WHERE id = ?1",
            params![first.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(!text.contains("budget"), "text sealed: {text}");
    assert!(!speaker.unwrap_or_default().contains("Ana"));
}

#[tokio::test]
async fn a_sealed_value_moved_to_another_row_is_skipped_not_misread() {
    let store = sealing_store();
    let a = store
        .insert_segment("m1", 0, None, "secret A")
        .await
        .unwrap();
    let b = store
        .insert_segment("m1", 1, None, "plain B")
        .await
        .unwrap();
    {
        let conn = store.conn.lock().await;
        conn.execute(
            "UPDATE segments SET text = (SELECT text FROM segments WHERE id = ?1) WHERE id = ?2",
            params![a.id, b.id],
        )
        .unwrap();
    }
    let listed = store.list_segments("m1").await.unwrap();
    assert_eq!(listed.len(), 1, "the row bound to another id must not open");
    assert_eq!(listed[0].id, a.id);
    assert_eq!(listed[0].text, "secret A");
}

#[tokio::test]
async fn setting_the_speaker_of_an_unknown_segment_is_a_no_op() {
    let store = sealing_store();
    store.set_segment_speaker(9_999, "Nobody").await.unwrap();
    let seg = store.insert_segment("m2", 0, None, "hi").await.unwrap();
    store
        .set_segment_speaker(seg.id, "Speaker 1")
        .await
        .unwrap();
    let listed = store.list_segments("m2").await.unwrap();
    assert_eq!(listed[0].speaker.as_deref(), Some("Speaker 1"));
    assert!(store.list_segments("m1").await.unwrap().is_empty());
}
