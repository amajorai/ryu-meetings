/// Core's `notify.db` is not sealed, so the notes-ready notice must carry no
/// meeting content (the title is sealed in `meetings.json`). Every notice the
/// HTTP layer raises goes through the generic helper.
#[test]
fn notes_ready_notice_carries_no_sealed_field() {
    let hint = serde_json::to_value(super::notes_ready_notify_hint()).unwrap();
    assert_eq!(hint["title"], "Meeting notes ready");
    assert_eq!(hint["body"], "Filed to your Spaces.");
    assert_eq!(hint["level"], "success");
    assert_eq!(hint["i18n"]["title"]["message"], "Meeting notes ready");
    for (file, source) in [
        ("api/finalize.rs", include_str!("api/finalize.rs")),
        ("api/mod.rs", include_str!("api/mod.rs")),
    ] {
        assert!(
            !source.contains("NotifyHint::info("),
            "{file} must raise notices through notes_ready_notify_hint, not inline text"
        );
    }
}

#[test]
fn lifecycle_activity_rows_match_the_feed_wording() {
    let detected = serde_json::to_value(super::detected_activity("zoom", "Zoom call")).unwrap();
    assert_eq!(detected["kind"], "meeting");
    assert_eq!(detected["title"], "Meeting detected: Zoom call");
    assert_eq!(detected["metadata"]["app"], "zoom");
    assert!(detected.get("level").is_none());
}
use super::*;
use crate::engine::default_title;
use crate::engine::ingest::millis_since;
use crate::engine::shadow::*;
use crate::engine::start::pretty_app;

#[test]
fn pretty_app_known_and_unknown() {
    assert_eq!(pretty_app("zoom"), "Zoom");
    assert_eq!(pretty_app("google meet"), "Google Meet");
    assert_eq!(pretty_app("acme"), "Acme");
}

#[test]
fn default_title_includes_app() {
    assert!(default_title(Some("zoom")).starts_with("Zoom meeting — "));
    assert!(default_title(None).starts_with("Meeting — "));
}

#[test]
fn millis_since_is_non_negative() {
    let future = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
    assert_eq!(millis_since(&future), 0);
    assert_eq!(millis_since("not-a-date"), 0);
}

#[test]
fn millis_since_past_is_positive() {
    let past = (chrono::Utc::now() - chrono::Duration::seconds(5)).to_rfc3339();
    assert!(millis_since(&past) >= 4_000);
}

#[test]
fn pretty_app_covers_the_slug_table() {
    assert_eq!(pretty_app("zoom.us"), "Zoom");
    assert_eq!(pretty_app("ms-teams"), "Teams");
    assert_eq!(pretty_app("meet"), "Google Meet");
    assert_eq!(pretty_app("slack"), "Slack");
    assert_eq!(pretty_app("discord"), "Discord");
    assert_eq!(pretty_app("webex"), "Webex");
    assert_eq!(pretty_app(""), "");
}

#[test]
fn default_title_without_app_has_no_app_prefix() {
    let t = default_title(Some(""));
    assert!(
        t.starts_with("Meeting — "),
        "empty app ⇒ generic title: {t}"
    );
}

#[test]
fn shadow_url_defaults_to_loopback() {
    // With RYU_SHADOW_URL unset in the test env, the default loopback is used.
    if std::env::var_os("RYU_SHADOW_URL").is_none() {
        assert_eq!(shadow_url(), "http://127.0.0.1:3030");
    }
}

/// A listener on Shadow's port that records each request's path and
/// `Authorization`. With `proof_token`, `/health` answers the peer challenge
/// like a real Shadow holding that token; without, it is an impostor.
async fn spawn_recording_shadow(
    proof_token: Option<&'static str>,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<(String, Option<String>)>>>,
) {
    use std::sync::{Arc, Mutex};
    let seen: Arc<Mutex<Vec<(String, Option<String>)>>> = Arc::default();
    let record = Arc::clone(&seen);
    let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
        let record = Arc::clone(&record);
        async move {
            let auth = req
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            record
                .lock()
                .unwrap()
                .push((req.uri().path().to_owned(), auth));
            let challenge = req
                .uri()
                .query()
                .unwrap_or_default()
                .split('&')
                .find_map(|pair| pair.strip_prefix("challenge="))
                .unwrap_or_default()
                .to_owned();
            let proof = proof_token
                .and_then(|token| ryu_sidecar_runtime::peer_proof::proof(token, &challenge));
            axum::Json(serde_json::json!({ "status": "healthy", "proof": proof }))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), seen)
}

#[test]
fn shadow_token_prefers_the_current_token_file_over_the_spawn_env() {
    let path = std::env::temp_dir().join(format!(
        "ryu-meetings-shadow-token-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(&path, "  rotated-token\n").unwrap();
    assert_eq!(
        resolve_shadow_token(Some(path.clone().into()), Some("spawn-token".into())),
        Some("rotated-token".to_owned())
    );
    std::fs::remove_file(&path).unwrap();
    assert_eq!(
        resolve_shadow_token(Some(path.into()), Some(" spawn-token ".into())),
        Some("spawn-token".to_owned())
    );
    assert_eq!(resolve_shadow_token(None, None), None);
}

#[tokio::test]
async fn shadow_capture_control_reaches_only_a_verified_shadow() {
    let prev = std::env::var("SHADOW_API_TOKEN").ok();
    std::env::set_var("SHADOW_API_TOKEN", "meetings-token");
    let client = reqwest::Client::new();

    // An impostor gets the challenge only: no bearer, no capture request.
    let (impostor, seen) = spawn_recording_shadow(None).await;
    let request = client.post(format!("{impostor}/meeting/start"));
    assert!(send_to_shadow(request).await.is_err());
    let impostor_seen = seen.lock().unwrap().clone();
    assert!(impostor_seen
        .iter()
        .all(|(path, auth)| path == "/health" && auth.is_none()));

    // The real Shadow proves the token and receives the bearer.
    let (shadow, seen) = spawn_recording_shadow(Some("meetings-token")).await;
    let request = client.post(format!("{shadow}/meeting/start"));
    assert!(send_to_shadow(request).await.is_ok());
    assert_eq!(
        seen.lock().unwrap().last(),
        Some(&(
            "/meeting/start".to_owned(),
            Some("Bearer meetings-token".to_owned())
        ))
    );

    match prev {
        Some(v) => std::env::set_var("SHADOW_API_TOKEN", v),
        None => std::env::remove_var("SHADOW_API_TOKEN"),
    }
}

#[test]
fn meeting_status_serializes_snake_case() {
    let j = serde_json::to_string(&MeetingStatus::Recording).unwrap();
    assert_eq!(j, "\"recording\"");
    let j = serde_json::to_string(&MeetingSource::Auto).unwrap();
    assert_eq!(j, "\"auto\"");
}

#[test]
fn init_and_read_data_dir_is_a_path() {
    // data_dir() never panics pre-init (falls back to temp dir).
    assert!(data_dir().is_absolute() || data_dir().as_os_str().len() > 0);
}

#[test]
fn kind_columns_and_source_wire_values_round_trip() {
    use crate::model::{MeetingKind, MeetingSource};
    for kind in [
        MeetingKind::Meeting,
        MeetingKind::Recap,
        MeetingKind::Ambient,
    ] {
        assert_eq!(MeetingKind::from_column(kind.as_str()), kind);
    }
    assert_eq!(MeetingKind::from_column("future"), MeetingKind::Meeting);
    assert_eq!(
        MeetingSource::parse("action_button"),
        MeetingSource::ActionButton
    );
    assert_eq!(MeetingSource::parse("unknown"), MeetingSource::Manual);
    assert!(MeetingSource::Control.is_fast_path());
    assert!(!MeetingSource::Phone.is_fast_path());
    // A legacy row without `kind` reads as a meeting.
    let legacy: crate::Meeting = serde_json::from_value(serde_json::json!({
        "id": "m", "title": "t", "status": "done", "started_at": "s",
        "created_at": "c", "updated_at": "u"
    }))
    .unwrap();
    assert_eq!(legacy.kind, MeetingKind::Meeting);
    assert!(legacy.recap.is_none());
}
