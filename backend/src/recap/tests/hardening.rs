//! Review hardening: per-caller data calls, the confirm window, consent
//! re-checks after start, the node switch, and quota-preserving delete-all.

use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

use super::support::*;
use crate::recap::caller::LOCAL_OWNER;
use crate::test_support::FakeHost;
use crate::MeetingStatus;

const REPLY: &str = r#"{"title":"Talk","summary":"We talked.","key_points":[],"action_items":[],"decisions":[],"topics":[]}"#;

type Shared<T> = Arc<Mutex<T>>;

/// A harness whose model calls are recorded, plus a handle on the host's
/// transcription route.
async fn gateway_harness() -> (Harness, Shared<Vec<Value>>, Shared<String>) {
    let (url, seen) = capture_gateway(serde_json::from_str(REPLY).unwrap()).await;
    let host = FakeHost {
        gateway_url: url,
        ..FakeHost::default()
    };
    let route = Arc::clone(&host.stt_route);
    (harness_with(host, true, true).await, seen, route)
}

async fn data_call(
    h: &Harness,
    path: &str,
    user: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    call(&h.app, Method::POST, path, user, Some(body)).await
}

async fn exists(h: &Harness, id: &str) -> bool {
    h.ctx.engine.store.get_meeting(id).await.unwrap().is_some()
}

#[tokio::test]
async fn a_member_cannot_purge_or_count_another_members_recaps() {
    let h = harness().await;
    let a = started(&h.app, "alice").await;
    let b = started(&h.app, "bob").await;
    for path in ["/data/purge", "/data/count"] {
        let (code, body) = data_call(
            &h,
            path,
            Some("alice"),
            json!({ "category": "meetings", "user_id": "bob" }),
        )
        .await;
        assert_eq!(
            (code, body["error"].as_str()),
            (StatusCode::FORBIDDEN, Some("other_member")),
            "{path}"
        );
    }
    assert!(exists(&h, &b).await, "bob's recap survives alice's purge");
    assert!(h
        .ctx
        .engine
        .store
        .recap_settings("bob")
        .await
        .unwrap()
        .is_some());
    // Naming herself (or nobody) covers only alice.
    let (_, count) = data_call(
        &h,
        "/data/count",
        Some("alice"),
        json!({ "category": "meetings", "user_id": "alice" }),
    )
    .await;
    assert_eq!(count["count"], 1);
    let (code, purged) = data_call(
        &h,
        "/data/purge",
        Some("alice"),
        json!({ "category": "meetings" }),
    )
    .await;
    assert_eq!(
        (code, purged["removed"].as_u64()),
        (StatusCode::OK, Some(1))
    );
    assert!(!exists(&h, &a).await);
    assert!(exists(&h, &b).await, "bob untouched");
}

#[tokio::test]
async fn a_members_purge_on_a_local_node_never_touches_the_local_owner() {
    let h = harness_with(FakeHost::default(), false, true).await;
    // The headerless local owner's recap (seeded directly: a caller header can
    // never name the local owner).
    let owner_recap =
        crate::test_support::recap_row("rcp_owner", LOCAL_OWNER, &today(), "2026-11-08T10:00:00Z");
    h.ctx
        .engine
        .store
        .upsert_meeting(&owner_recap)
        .await
        .unwrap();
    let mine = started(&h.app, "carol").await;
    let (code, purged) = data_call(
        &h,
        "/data/purge",
        Some("carol"),
        json!({ "category": "meetings" }),
    )
    .await;
    assert_eq!(
        (code, purged["removed"].as_u64()),
        (StatusCode::OK, Some(1))
    );
    assert!(!exists(&h, &mine).await);
    assert!(
        exists(&h, "rcp_owner").await,
        "the local owner's recap stays"
    );
    let (code, _) = data_call(
        &h,
        "/data/count",
        Some("carol"),
        json!({ "category": "meetings", "user_id": LOCAL_OWNER }),
    )
    .await;
    assert_eq!(code, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn core_s_headerless_fan_out_still_purges_a_named_member() {
    let h = harness().await;
    let b = started(&h.app, "bob").await;
    let (code, purged) = data_call(
        &h,
        "/data/purge",
        None,
        json!({ "category": "meetings", "user_id": "bob" }),
    )
    .await;
    assert_eq!(
        (code, purged["removed"].as_u64()),
        (StatusCode::OK, Some(1))
    );
    assert!(!exists(&h, &b).await);
}

async fn fast_path_start(h: &Harness) -> String {
    consent(&h.app, "alice").await;
    let session = uuid::Uuid::new_v4().to_string();
    let (_, body) = call(
        &h.app,
        Method::POST,
        "/recaps",
        Some("alice"),
        Some(start_body(&session, "control")),
    )
    .await;
    body["recap"]["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn a_confirm_after_the_window_discards_the_recap() {
    let h = harness().await;
    let id = fast_path_start(&h).await;
    let past = (chrono::Utc::now() - chrono::Duration::seconds(31)).to_rfc3339();
    h.ctx
        .engine
        .store
        .mutate_recap(&id, |m| {
            m.started_at = past;
            true
        })
        .await
        .unwrap();
    let (code, body) = call(
        &h.app,
        Method::POST,
        &format!("/recaps/{id}/confirm"),
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("confirm_expired"))
    );
    assert!(
        !exists(&h, &id).await,
        "the late session is discarded, not kept"
    );
}

#[tokio::test]
async fn a_confirm_inside_the_window_still_works() {
    let h = harness().await;
    let id = fast_path_start(&h).await;
    let (code, body) = call(
        &h.app,
        Method::POST,
        &format!("/recaps/{id}/confirm"),
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn withdrawing_consent_mid_session_closes_it_without_a_summary() {
    let (h, seen, _) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(code, StatusCode::OK);
    let (code, body) = call(
        &h.app,
        Method::DELETE,
        "/recaps/consent",
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["closed_recordings"], 1);
    let (code, body) = put_chunk(
        &h.app,
        "alice",
        &id,
        1,
        1_000,
        1_000,
        speech_wav(1_000),
        &[],
    )
    .await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("recap_closed"))
    );
    let later = chrono::Utc::now() + chrono::Duration::hours(1);
    crate::recap::tick::run_once(&h.ctx.engine, later).await;
    assert!(
        seen.lock().unwrap().is_empty(),
        "no model call after withdrawal"
    );
    let recap = h.ctx.engine.store.get_meeting(&id).await.unwrap().unwrap();
    assert_eq!(recap.status, MeetingStatus::Done);
    assert!(recap.recap.unwrap().unsummarized);
    assert_eq!(
        h.ctx.engine.store.recap_segments(&id).await.unwrap().len(),
        1,
        "the transcript is kept for the owner"
    );
}

#[tokio::test]
async fn a_model_change_after_start_blocks_chunks_and_the_summary_until_consent() {
    let (h, seen, _) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(code, StatusCode::OK);
    h.ctx
        .engine
        .pref_set("meeting-notes-model", "cloud-model")
        .await
        .unwrap();
    let (code, body) = put_chunk(
        &h.app,
        "alice",
        &id,
        1,
        1_000,
        1_000,
        speech_wav(1_000),
        &[],
    )
    .await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("consent_required"))
    );
    let finalize = json!({ "expected_chunks": 1 });
    let path = format!("/recaps/{id}/finalize");
    let (code, body) = call(
        &h.app,
        Method::POST,
        &path,
        Some("alice"),
        Some(finalize.clone()),
    )
    .await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("consent_required"))
    );
    assert!(seen.lock().unwrap().is_empty());
    consent(&h.app, "alice").await;
    let (code, _) = call(&h.app, Method::POST, &path, Some("alice"), Some(finalize)).await;
    assert_eq!(code, StatusCode::ACCEPTED);
    wait_status(&h.ctx, &id, MeetingStatus::Done).await;
    let sent = seen.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0]["model"], "cloud-model",
        "only the newly consented model"
    );
}

#[tokio::test]
async fn the_stale_tick_never_summarizes_without_current_consent() {
    let (h, seen, _) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(code, StatusCode::OK);
    h.ctx
        .engine
        .pref_set("meeting-notes-model", "cloud-model")
        .await
        .unwrap();
    let later = chrono::Utc::now() + chrono::Duration::minutes(31);
    assert_eq!(
        crate::recap::tick::finalize_stale(&h.ctx.engine, later).await,
        1
    );
    assert!(seen.lock().unwrap().is_empty());
    let recap = h.ctx.engine.store.get_meeting(&id).await.unwrap().unwrap();
    assert_eq!(recap.status, MeetingStatus::Done);
    assert!(recap.recap.unwrap().unsummarized);
}

#[tokio::test]
async fn a_transcription_route_change_asks_for_consent_again() {
    let (h, _, route) = gateway_harness().await;
    consent(&h.app, "alice").await;
    let (_, view) = call(&h.app, Method::GET, "/recaps/settings", Some("alice"), None).await;
    assert_eq!(view["consent_current"], true);
    assert_eq!(
        view["settings"]["consent"]["transcription"],
        "Parakeet (on this Ryu node)"
    );
    assert!(view["processors"][0]
        .as_str()
        .unwrap()
        .starts_with("transcription:Parakeet"));
    *route.lock().unwrap() = "Ryu Gateway (cloud: groq, whisper-large-v3)".into();
    let (_, view) = call(&h.app, Method::GET, "/recaps/settings", Some("alice"), None).await;
    assert_eq!(view["consent_current"], false);
    assert_eq!(
        view["processors"][0],
        "transcription:Ryu Gateway (cloud: groq, whisper-large-v3)"
    );
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
}

#[tokio::test]
async fn deleting_all_recaps_keeps_the_days_upload_usage() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(code, StatusCode::OK);
    let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let used = h.ctx.engine.store.recap_usage("alice", &day).await.unwrap();
    assert!(used > 0);
    let (code, body) = call(&h.app, Method::DELETE, "/recaps", Some("alice"), None).await;
    assert_eq!((code, body["removed"].as_u64()), (StatusCode::OK, Some(1)));
    assert_eq!(
        h.ctx.engine.store.recap_usage("alice", &day).await.unwrap(),
        used
    );
    let (_, view) = call(&h.app, Method::GET, "/recaps/settings", Some("alice"), None).await;
    assert_eq!(view["consent_current"], true, "consent survives delete-all");
    // Only the account/member purge clears usage.
    let (code, _) = call(
        &h.app,
        Method::POST,
        "/data/purge",
        None,
        Some(json!({ "category": "meetings", "user_id": "alice" })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(
        h.ctx.engine.store.recap_usage("alice", &day).await.unwrap(),
        0
    );
}

#[test]
fn a_replayed_seq_is_a_duplicate_only_with_the_same_bytes() {
    let row = crate::store::ChunkRow {
        seq: 3,
        offset_ms: 0,
        duration_ms: 1_000,
        sha256: "abc".into(),
        bytes: 1,
        status: "transcribed".into(),
        segment_id: None,
        received_at: "2026-11-08T10:00:00Z".into(),
    };
    let same = crate::recap::ingest::replayed(&row, "abc", 3).unwrap();
    assert_eq!(same.0["duplicate"], true);
    match crate::recap::ingest::replayed(&row, "other", 3) {
        Err(crate::recap::RecapError::Conflict(code, _)) => assert_eq!(code, "seq_conflict"),
        other => panic!("expected seq_conflict, got {other:?}"),
    }
}
