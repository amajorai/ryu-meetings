//! Opt-in interim summaries: owner scope, the opt-in, the speech step, redaction
//! before the model, the rate limit, and clearing on finalize and delete. Also
//! pins that reminder settings (and their coordinates) never reach the server.

use std::sync::{Arc, Mutex};

use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

use super::support::*;
use crate::recap::interim::{serve, INTERIM_SPEECH_STEP_MS, MAX_INTERIM_REQUESTS_PER_MINUTE};
use crate::recap::RecapError;
use crate::store::{ChunkStatus, NewChunk};
use crate::test_support::FakeHost;

const REPLY: &str =
    r#"{"summary":"So far: the budget review moves.","key_points":["review on Friday"]}"#;
const CHUNK_MS: i64 = 30_000;

async fn gateway_harness() -> (Harness, Arc<Mutex<Vec<Value>>>) {
    let (url, seen) = capture_gateway(serde_json::from_str(REPLY).unwrap()).await;
    let host = FakeHost {
        gateway_url: url,
        ..FakeHost::default()
    };
    (harness_with(host, true, true).await, seen)
}

/// Record `count` transcribed chunks after `from` directly in the ledger.
async fn speak(h: &Harness, id: &str, from: u32, count: u32, text: &str) {
    for seq in from..from + count {
        let sha = format!("{seq:064}");
        h.ctx
            .engine
            .store
            .record_recap_chunk(
                id,
                NewChunk {
                    seq,
                    offset_ms: i64::from(seq) * CHUNK_MS,
                    duration_ms: CHUNK_MS,
                    sha256: &sha,
                    bytes: 10,
                    status: ChunkStatus::Transcribed,
                    text: Some(text),
                },
            )
            .await
            .unwrap();
    }
}

fn step_chunks() -> u32 {
    u32::try_from(INTERIM_SPEECH_STEP_MS / CHUNK_MS).unwrap()
}

async fn set_interim(h: &Harness, user: &str, on: bool) {
    let (code, body) = call(
        &h.app,
        Method::PUT,
        "/recaps/settings",
        Some(user),
        Some(json!({ "interim": on })),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
}

async fn interim(h: &Harness, user: &str, id: &str) -> (StatusCode, Value) {
    call(
        &h.app,
        Method::GET,
        &format!("/recaps/{id}/interim"),
        Some(user),
        None,
    )
    .await
}

#[tokio::test]
async fn interim_is_opt_in_owner_scoped_and_waits_for_speech() {
    let (h, seen) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    let (code, body) = interim(&h, "alice", &id).await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::FORBIDDEN, Some("interim_disabled"))
    );
    set_interim(&h, "alice", true).await;
    let (code, _) = interim(&h, "bob", &id).await;
    assert_eq!(code, StatusCode::NOT_FOUND, "another member's recap is 404");
    speak(&h, &id, 0, step_chunks() - 1, "not enough yet").await;
    let (code, body) = interim(&h, "alice", &id).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["interim"], Value::Null);
    assert_eq!(body["next_at_speech_ms"], INTERIM_SPEECH_STEP_MS);
    assert!(
        seen.lock().unwrap().is_empty(),
        "no model call before a step"
    );
}

#[tokio::test]
async fn interim_redacts_before_the_model_and_refreshes_once_per_step() {
    let (h, seen) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    set_interim(&h, "alice", true).await;
    speak(&h, &id, 0, step_chunks(), "mail me at alice@example.com").await;
    let (code, body) = interim(&h, "alice", &id).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["refreshed"], true);
    assert_eq!(
        body["interim"]["summary"],
        "So far: the budget review moves."
    );
    assert_eq!(body["interim"]["speech_ms"], INTERIM_SPEECH_STEP_MS);
    let sent = seen.lock().unwrap()[0].to_string();
    assert!(
        !sent.contains("alice@example.com"),
        "redacted before the model"
    );
    assert!(sent.contains("[email]"));
    // No new speech: the cached summary, no second call.
    let (_, again) = interim(&h, "alice", &id).await;
    assert_eq!(again["refreshed"], false);
    assert_eq!(again["interim"], body["interim"]);
    assert_eq!(seen.lock().unwrap().len(), 1);
    // Another full step of speech refreshes it.
    speak(&h, &id, step_chunks(), step_chunks(), "more talk").await;
    let (_, third) = interim(&h, "alice", &id).await;
    assert_eq!(third["refreshed"], true);
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn interim_is_dropped_on_finalize_and_on_delete() {
    let (h, _) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    set_interim(&h, "alice", true).await;
    speak(&h, &id, 0, step_chunks(), "talk").await;
    assert_eq!(interim(&h, "alice", &id).await.0, StatusCode::OK);
    assert_eq!(h.ctx.engine.interim.len(), 1);
    let (code, body) = call(
        &h.app,
        Method::POST,
        &format!("/recaps/{id}/finalize"),
        Some("alice"),
        Some(json!({ "expected_chunks": step_chunks() })),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED, "{body}");
    assert!(h.ctx.engine.interim.is_empty(), "never kept past recording");
    let (code, body) = interim(&h, "alice", &id).await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::CONFLICT, Some("recap_not_recording"))
    );

    let other = started(&h.app, "alice").await;
    speak(&h, &other, 0, step_chunks(), "talk").await;
    assert_eq!(interim(&h, "alice", &other).await.0, StatusCode::OK);
    assert_eq!(h.ctx.engine.interim.len(), 1);
    let path = format!("/recaps/{other}/detail");
    let (code, _) = call(&h.app, Method::DELETE, &path, Some("alice"), None).await;
    assert_eq!(code, StatusCode::OK);
    assert!(h.ctx.engine.interim.is_empty());
}

#[tokio::test]
async fn interim_requests_are_rate_limited_per_member() {
    let (h, _) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    set_interim(&h, "alice", true).await;
    for _ in 0..MAX_INTERIM_REQUESTS_PER_MINUTE {
        assert_eq!(interim(&h, "alice", &id).await.0, StatusCode::OK);
    }
    let (code, body) = interim(&h, "alice", &id).await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("rate_limited"))
    );
}

#[tokio::test]
async fn reminder_settings_and_coordinates_are_refused_server_side() {
    let h = harness().await;
    let (code, _) = call(
        &h.app,
        Method::PUT,
        "/recaps/settings",
        Some("alice"),
        Some(json!({
            "reminders": [{ "kind": "location", "latitude": 52.52, "longitude": 13.4 }]
        })),
    )
    .await;
    assert!(code.is_client_error(), "{code}");
    let stored = h.ctx.engine.store.recap_settings("alice").await.unwrap();
    assert!(stored.is_none(), "nothing was saved");
    let (_, view) = call(&h.app, Method::GET, "/recaps/settings", Some("alice"), None).await;
    assert!(view["settings"].get("reminders").is_none());
}

#[tokio::test]
async fn a_recording_closed_before_the_claim_gets_no_summary() {
    let (h, seen) = gateway_harness().await;
    let engine = &h.ctx.engine;
    let id = started(&h.app, "alice").await;
    set_interim(&h, "alice", true).await;
    speak(&h, &id, 0, step_chunks(), "talk").await;
    let member = crate::recap::settings::load(engine, "alice").await.unwrap();
    // The route saw the recap recording; it then closes before the claim.
    crate::recap::finalize::mark_processing(engine, &id, step_chunks(), false)
        .await
        .unwrap();
    let outcome = serve(engine, "alice", &id, &member, INTERIM_SPEECH_STEP_MS).await;
    assert!(
        matches!(outcome, Err(RecapError::Conflict("recap_not_recording", _))),
        "{outcome:?}"
    );
    assert!(engine.interim.is_empty(), "no entry outlives the recording");
    assert!(seen.lock().unwrap().is_empty(), "no model call after close");
}

#[tokio::test]
async fn switching_interim_off_or_withdrawing_consent_drops_the_summary() {
    let (h, seen) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    set_interim(&h, "alice", true).await;
    speak(&h, &id, 0, step_chunks(), "talk").await;
    assert_eq!(interim(&h, "alice", &id).await.1["refreshed"], true);
    assert_eq!(h.ctx.engine.interim.len(), 1);
    set_interim(&h, "alice", false).await;
    assert!(
        h.ctx.engine.interim.is_empty(),
        "turning it off discards it"
    );
    set_interim(&h, "alice", true).await;
    // Nothing cached survived: the same speech needs a fresh model call.
    let (code, body) = interim(&h, "alice", &id).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["refreshed"], true);
    assert_eq!(seen.lock().unwrap().len(), 2);
    assert_eq!(h.ctx.engine.interim.len(), 1);
    let (code, _) = call(
        &h.app,
        Method::DELETE,
        "/recaps/consent",
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert!(h.ctx.engine.interim.is_empty(), "withdrawal discards it");
}
