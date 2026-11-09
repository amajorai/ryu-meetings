use super::{load_prefs, save_prefs, MeetingsHost, SidecarMeetingsHost};
use std::collections::HashMap;

#[tokio::test]
async fn sidecar_host_pref_get_set_persist_to_disk() {
    let dir = std::env::temp_dir().join(format!(
        "ryu-meetings-host-test-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    let path = dir.join("prefs.json");
    let host = SidecarMeetingsHost::new(path.clone());

    assert!(host.pref_get("k").await.is_none());
    host.pref_set("k", "v").await.unwrap();
    assert_eq!(host.pref_get("k").await.as_deref(), Some("v"));

    // Durable: a second host over the same file sees the value.
    let reopened = SidecarMeetingsHost::new(path);
    assert_eq!(reopened.pref_get("k").await.as_deref(), Some("v"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn generate_title_declines_short_summaries() {
    let dir = std::env::temp_dir().join(format!(
        "ryu-meetings-title-test-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    let host = SidecarMeetingsHost::new(dir.join("p.json"));
    // Under 16 chars ⇒ short-circuits to None without any Gateway call.
    assert!(host.generate_title("too short").await.is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn prefs_roundtrip_through_file() {
    let dir = std::env::temp_dir().join(format!("ryu-meetings-test-{}", std::process::id()));
    let path = dir.join("prefs.json");
    let _ = std::fs::remove_file(&path);

    // Missing file → empty map (fresh install falls back to defaults).
    assert!(load_prefs(&path).is_empty());

    let mut map = HashMap::new();
    map.insert("meeting-notes-model".to_string(), "custom".to_string());
    save_prefs(&path, &map).expect("save prefs");

    // Reloaded map survives (the restart-durability property).
    let reloaded = load_prefs(&path);
    assert_eq!(
        reloaded.get("meeting-notes-model").map(String::as_str),
        Some("custom")
    );

    let _ = std::fs::remove_dir_all(&dir);
}
