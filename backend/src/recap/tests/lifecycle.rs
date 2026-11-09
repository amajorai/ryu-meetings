//! Start, consent, idempotency, confirmation and the node rollout gate.

use axum::http::{Method, StatusCode};
use serde_json::json;

use super::support::*;
use crate::recap::settings::CONSENT_VERSION;
use crate::MeetingStatus;

#[tokio::test]
async fn capture_is_refused_until_the_node_enables_recaps() {
    let h = harness_with(Default::default(), true, false).await;
    let session = uuid::Uuid::new_v4().to_string();
    let (code, body) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&session, "phone")),
    )
    .await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::FORBIDDEN, Some("recaps_disabled"))
    );
    let (code, body) = call(&h.app, Method::GET, "/recaps/settings", Some("alice"), None).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["enabled"], false);
    assert_eq!(
        body["can_enable"], false,
        "a member cannot switch a managed node"
    );
    // A member (verified caller header) may not flip the node-wide switch.
    let (code, body) = call(
        &h.app,
        Method::PUT,
        "/recaps/availability",
        Some("alice"),
        Some(json!({ "enabled": true })),
    )
    .await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::FORBIDDEN, Some("admin_required"))
    );
    let (_, body) = call(&h.app, Method::GET, "/recaps/settings", Some("alice"), None).await;
    assert_eq!(body["enabled"], false, "still off");
    // The operator's node-bearer call (no member header) may.
    let (code, _) = call(
        &h.app,
        Method::PUT,
        "/recaps/availability",
        None,
        Some(json!({ "enabled": true })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let (_, body) = call(&h.app, Method::GET, "/recaps/settings", Some("alice"), None).await;
    assert_eq!(body["enabled"], true);
}

#[tokio::test]
async fn the_local_owner_turns_recaps_on_for_a_local_node() {
    let h = harness_with(Default::default(), false, false).await;
    let (_, body) = call(&h.app, Method::GET, "/recaps/settings", None, None).await;
    assert_eq!(body["can_enable"], true);
    let (code, _) = call(
        &h.app,
        Method::PUT,
        "/recaps/availability",
        None,
        Some(json!({ "enabled": true })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let (_, body) = call(&h.app, Method::GET, "/recaps/settings", None, None).await;
    assert_eq!(body["enabled"], true);
}

#[tokio::test]
async fn start_without_consent_is_refused_and_consent_names_the_processors() {
    let h = harness().await;
    let session = uuid::Uuid::new_v4().to_string();
    let (code, body) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&session, "phone")),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(body["error"], "consent_required");
    assert!(body["processors"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p.as_str().unwrap().starts_with("summary:test-model")));

    consent(&h.app, "alice").await;
    let (_, settings) = call(&h.app, Method::GET, "/recaps/settings", Some("alice"), None).await;
    assert_eq!(settings["consent_current"], true);
    assert_eq!(
        settings["settings"]["people"], false,
        "names are off by default"
    );
    let (code, _) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&session, "phone")),
    )
    .await;
    assert_eq!(code, StatusCode::CREATED);
    // Bob never consented: Alice's consent is not his.
    let other = uuid::Uuid::new_v4().to_string();
    let (code, _) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("bob"),
        Some(start_body(&other, "phone")),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
}

#[tokio::test]
async fn a_changed_summary_model_or_consent_version_asks_again() {
    let h = harness().await;
    consent(&h.app, "alice").await;
    h.ctx
        .engine
        .pref_set("meeting-notes-model", "cloud-model")
        .await
        .unwrap();
    let session = uuid::Uuid::new_v4().to_string();
    let (code, body) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&session, "phone")),
    )
    .await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("consent_required"))
    );
    let (code, _) = call(
        &h.app,
        Method::POST,
        "/recaps/consent",
        Some("alice"),
        Some(json!({ "version": CONSENT_VERSION + 1 })),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    consent(&h.app, "alice").await;
    let mut stale = start_body(&session, "phone");
    stale["consent_version"] = json!(0);
    let (code, _) = call(&h.app, Method::POST, "/recaps", Some("alice"), Some(stale)).await;
    assert_eq!(
        code,
        StatusCode::CONFLICT,
        "a client showing old consent text is refused"
    );
}

#[tokio::test]
async fn replaying_a_client_session_id_returns_the_same_recap_and_never_drives_shadow() {
    let h = harness().await;
    consent(&h.app, "alice").await;
    let session = uuid::Uuid::new_v4().to_string();
    let (code, first) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&session, "watch")),
    )
    .await;
    assert_eq!(code, StatusCode::CREATED);
    let (code, again) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&session.to_uppercase(), "watch")),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(first["recap"]["id"], again["recap"]["id"]);
    assert_eq!(first["recap"]["kind"], "recap");
    assert_eq!(first["recap"]["source"], "watch");
    assert_eq!(first["upload"]["max_seq"], 1440);
    assert_eq!(first["upload"]["max_chunk_bytes"], 2_000_000);
    assert!(!first["upload"]["accepted"].to_string().contains("opus"));
    // A recap is created without any Shadow capture control: it is not a meeting.
    assert_eq!(first["recap"]["app"], "recap");
}

#[tokio::test]
async fn start_metadata_is_validated() {
    let h = harness().await;
    consent(&h.app, "alice").await;
    for bad in [
        json!({ "client_session_id": "not-a-uuid" }),
        json!({ "tz": "Mars/Base" }),
        json!({ "local_day": "2001-01-01" }),
    ] {
        let mut body = start_body(&uuid::Uuid::new_v4().to_string(), "phone");
        for (k, v) in bad.as_object().unwrap() {
            body[k] = v.clone();
        }
        let (code, _) = call(&h.app, Method::POST, "/recaps", Some("alice"), Some(body)).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn a_fast_path_start_needs_confirmation_before_audio_and_expires_unconfirmed() {
    let h = harness().await;
    consent(&h.app, "alice").await;
    let session = uuid::Uuid::new_v4().to_string();
    let (_, body) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&session, "action_button")),
    )
    .await;
    let id = body["recap"]["id"].as_str().unwrap().to_string();
    assert_eq!(body["recap"]["recap"]["needs_confirm"], true);
    let (code, body) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("recap_unconfirmed"))
    );
    let (code, body) = call(
        &h.app,
        Method::POST,
        &format!("/recaps/{id}/confirm"),
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert!(body["recap"]["recap"]["confirmed_at"].is_string());
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(code, StatusCode::OK);

    // A second fast-path start that is never confirmed is discarded by the sweep.
    let other = uuid::Uuid::new_v4().to_string();
    let (_, body) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&other, "control")),
    )
    .await;
    let unconfirmed = body["recap"]["id"].as_str().unwrap().to_string();
    let later = chrono::Utc::now() + chrono::Duration::seconds(31);
    let report = crate::recap::retention::sweep(&h.ctx.engine, later).await;
    assert_eq!(report.unconfirmed_discarded, 1);
    assert!(h
        .ctx
        .engine
        .store
        .get_meeting(&unconfirmed)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        h.ctx
            .engine
            .store
            .get_meeting(&id)
            .await
            .unwrap()
            .map(|m| m.status),
        Some(MeetingStatus::Recording),
        "the confirmed one stays"
    );
}

#[tokio::test]
async fn withdrawing_consent_blocks_new_sessions_and_offers_delete_all() {
    let h = harness().await;
    let _id = started(&h.app, "alice").await;
    let (code, body) = call(
        &h.app,
        Method::DELETE,
        "/recaps/consent",
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["recap_count"], 1);
    assert_eq!(body["delete_all"]["path"], "/recaps");
    let session = uuid::Uuid::new_v4().to_string();
    let (code, _) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&session, "phone")),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    let (code, body) = call(&h.app, Method::DELETE, "/recaps", Some("alice"), None).await;
    assert_eq!((code, body["removed"].as_u64()), (StatusCode::OK, Some(1)));
}

#[tokio::test]
async fn settings_are_validated_and_space_filing_is_unavailable() {
    let h = harness().await;
    let (code, body) = call(
        &h.app,
        Method::PUT,
        "/recaps/settings",
        Some("alice"),
        Some(json!({ "digest_time": "07:30", "people": true })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["settings"]["digest_time"], "07:30");
    assert_eq!(body["settings"]["people"], true);
    for bad in [
        json!({ "digest_time": "7pm" }),
        json!({ "tz": "Nowhere/City" }),
        json!({ "retention": { "segments_days": 0, "notes_days": 30, "digests_days": 30, "audio_days": 7 } }),
        json!({ "file_to_space": true }),
        json!({ "consent": { "version": 1 } }),
    ] {
        let (code, _) = call(
            &h.app,
            Method::PUT,
            "/recaps/settings",
            Some("alice"),
            Some(bad.clone()),
        )
        .await;
        assert!(code.is_client_error(), "{bad}");
    }
}
