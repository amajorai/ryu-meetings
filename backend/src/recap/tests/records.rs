//! Finalize, notes shaping, records, digests, search, export and retention.

use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

use super::support::*;
use crate::test_support::FakeHost;
use crate::MeetingStatus;

const MODEL_REPLY: &str = r#"{"title":"Budget sync","summary":"We moved the budget review. Card 4111 1111 1111 1111 was mentioned.","key_points":["review moves to Friday"],"action_items":["send deck"],"action_items_detail":[{"text":"send the deck","owner":"Dana","due":"2026-11-20"}],"decisions":[],"topics":["budget","planning"],"people":["Dana"]}"#;

async fn gateway_harness() -> (Harness, std::sync::Arc<std::sync::Mutex<Vec<Value>>>) {
    let (url, seen) = capture_gateway(serde_json::from_str(MODEL_REPLY).unwrap()).await;
    let host = FakeHost {
        transcribe_text: "my card is 4111 1111 1111 1111 and mail me at jo@example.com".into(),
        gateway_url: url,
        ..FakeHost::default()
    };
    (harness_with(host, true, true).await, seen)
}

async fn finalized(h: &Harness, user: &str) -> String {
    let id = started(&h.app, user).await;
    let (code, _) = put_chunk(&h.app, user, &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(code, StatusCode::OK);
    let (code, body) = call(
        &h.app,
        Method::POST,
        &format!("/recaps/{id}/finalize"),
        Some(user),
        Some(json!({ "expected_chunks": 1 })),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED, "{body}");
    wait_status(&h.ctx, &id, MeetingStatus::Done).await;
    id
}

#[tokio::test]
async fn finalize_redacts_before_the_model_and_shapes_the_notes() {
    let (h, seen) = gateway_harness().await;
    let id = finalized(&h, "alice").await;
    let sent = seen.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    let prompt = sent[0].to_string();
    assert!(!prompt.contains("4111"), "no card number reached the model");
    assert!(!prompt.contains("jo@example.com"));
    assert!(prompt.contains("[card number]") && prompt.contains("[email]"));
    assert!(prompt.contains("do not name people"));

    let (_, body) = call(
        &h.app,
        Method::GET,
        &format!("/recaps/{id}/detail"),
        Some("alice"),
        None,
    )
    .await;
    let recap = &body["recap"];
    assert_eq!(recap["title"], "Budget sync");
    let notes = &recap["notes"];
    assert!(
        notes["summary"].as_str().unwrap().contains("[card number]"),
        "notes are redacted too"
    );
    assert!(
        notes.get("people").is_none(),
        "names are dropped without the opt-in"
    );
    assert!(notes["action_items_detail"][0].get("owner").is_none());
    assert_eq!(notes["topics"], json!(["budget", "planning"]));
    // The owner's sealed transcript keeps the original words.
    assert!(body["transcript"][0]["text"]
        .as_str()
        .unwrap()
        .contains("4111"));
}

#[tokio::test]
async fn finalize_reports_missing_chunks_and_force_marks_partial() {
    let (h, _) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    put_chunk(
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
    let (code, body) = call(
        &h.app,
        Method::POST,
        &format!("/recaps/{id}/finalize"),
        Some("alice"),
        Some(json!({ "expected_chunks": 3 })),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(body["missing"], json!([0, 2]));
    let (code, _) = call(
        &h.app,
        Method::POST,
        &format!("/recaps/{id}/finalize"),
        Some("alice"),
        Some(json!({ "expected_chunks": 3, "force": true })),
    )
    .await;
    assert_eq!(code, StatusCode::ACCEPTED);
    wait_status(&h.ctx, &id, MeetingStatus::Done).await;
    let recap = h.ctx.engine.store.get_meeting(&id).await.unwrap().unwrap();
    assert!(recap.recap.unwrap().partial);
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(
        code,
        StatusCode::CONFLICT,
        "a finished recap takes no audio"
    );
}

#[tokio::test]
async fn patch_delete_and_the_digest_never_keep_deleted_text() {
    let (h, _) = gateway_harness().await;
    let first = finalized(&h, "alice").await;
    let second = finalized(&h, "alice").await;
    let day = today();
    let (code, body) = call(
        &h.app,
        Method::GET,
        &format!("/recaps/daily?date={day}&tz=UTC"),
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let digest = &body["digest"];
    assert_eq!(digest["count"], 2);
    assert_eq!(digest["recap_ids"].as_array().unwrap().len(), 2);
    assert!(!digest.to_string().contains("Dana"), "no names in a digest");

    let (code, body) = call(
        &h.app,
        Method::PATCH,
        &format!("/recaps/{first}/detail"),
        Some("alice"),
        Some(json!({ "title": "Mine", "pinned": true })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["recap"]["title"], "Mine");
    assert_eq!(body["recap"]["recap"]["pinned"], true);

    let (code, _) = call(
        &h.app,
        Method::DELETE,
        &format!("/recaps/{second}/detail"),
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let stored = h
        .ctx
        .engine
        .store
        .recap_digest("alice", &day)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.json["count"], 1);
    assert!(
        !stored.json.to_string().contains(&second),
        "the deleted recap left the digest"
    );
    let (_, body) = call(
        &h.app,
        Method::GET,
        &format!("/recaps/daily?date={day}"),
        Some("bob"),
        None,
    )
    .await;
    assert!(body["digest"].is_null(), "bob has no digest of alice's day");
}

#[tokio::test]
async fn search_is_owner_scoped_and_export_returns_only_the_callers_recaps() {
    let (h, _) = gateway_harness().await;
    let id = finalized(&h, "alice").await;
    let (code, body) = call(
        &h.app,
        Method::POST,
        "/recaps/search",
        Some("alice"),
        Some(json!({ "q": "budget" })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["results"][0]["recap_id"], id.as_str());
    let (_, body) = call(
        &h.app,
        Method::POST,
        "/recaps/search",
        Some("bob"),
        Some(json!({ "q": "budget" })),
    )
    .await;
    assert_eq!(body["results"], json!([]));
    let (_, body) = call(
        &h.app,
        Method::POST,
        "/recaps/search",
        Some("alice"),
        Some(json!({ "q": "dana" })),
    )
    .await;
    assert_eq!(body["results"], json!([]), "people are not searchable");
    let (code, _) = call(
        &h.app,
        Method::POST,
        "/recaps/search",
        Some("alice"),
        Some(json!({ "q": "x", "scope": "everything" })),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);

    let (code, body) = call(&h.app, Method::GET, "/recaps/export", Some("alice"), None).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["recaps"].as_array().unwrap().len(), 1);
    let (_, md) = call(
        &h.app,
        Method::GET,
        "/recaps/export?format=md",
        Some("alice"),
        None,
    )
    .await;
    assert!(md.as_str().unwrap().starts_with("# Ryu recaps export"));
    let (_, body) = call(&h.app, Method::GET, "/recaps/export", Some("bob"), None).await;
    assert_eq!(body["recaps"], json!([]));
}

#[tokio::test]
async fn retention_expires_transcripts_then_unpinned_recaps() {
    let (h, _) = gateway_harness().await;
    let kept = finalized(&h, "alice").await;
    let gone = finalized(&h, "alice").await;
    call(
        &h.app,
        Method::PATCH,
        &format!("/recaps/{kept}/detail"),
        Some("alice"),
        Some(json!({ "pinned": true })),
    )
    .await;
    let engine = &h.ctx.engine;
    let in_8_days = chrono::Utc::now() + chrono::Duration::days(8);
    let report = crate::recap::retention::sweep(engine, in_8_days).await;
    assert_eq!(report.transcripts_expired, 2);
    assert!(engine.store.recap_segments(&kept).await.unwrap().is_empty());
    assert!(engine
        .store
        .get_meeting(&gone)
        .await
        .unwrap()
        .unwrap()
        .notes
        .is_some());
    let in_31_days = chrono::Utc::now() + chrono::Duration::days(31);
    let report = crate::recap::retention::sweep(engine, in_31_days).await;
    assert_eq!(report.recaps_expired, 1);
    assert!(engine.store.get_meeting(&gone).await.unwrap().is_none());
    assert!(
        engine.store.get_meeting(&kept).await.unwrap().is_some(),
        "pinned is kept"
    );
}

#[tokio::test]
async fn the_evening_digest_is_sent_once_per_day_after_digest_time() {
    let (h, _) = gateway_harness().await;
    finalized(&h, "alice").await;
    let engine = &h.ctx.engine;
    let now = chrono::Utc::now();
    let day = now.format("%Y-%m-%d").to_string();
    // Not yet due: the digest time is still ahead (unless it is the last minute).
    call(
        &h.app,
        Method::PUT,
        "/recaps/settings",
        Some("alice"),
        Some(json!({ "digest_time": "23:59", "tz": "UTC" })),
    )
    .await;
    if now.format("%H:%M").to_string() != "23:59" {
        assert_eq!(crate::recap::tick::send_due_digests(engine, now).await, 0);
    }
    call(
        &h.app,
        Method::PUT,
        "/recaps/settings",
        Some("alice"),
        Some(json!({ "digest_time": "00:00" })),
    )
    .await;
    assert_eq!(crate::recap::tick::send_due_digests(engine, now).await, 1);
    assert_eq!(
        crate::recap::tick::send_due_digests(engine, now).await,
        0,
        "once per day"
    );
    let stored = engine
        .store
        .recap_digest("alice", &day)
        .await
        .unwrap()
        .unwrap();
    assert!(stored.json["notified_at"].is_string());
}

#[tokio::test]
async fn a_stale_session_is_finalized_as_partial_by_the_tick() {
    let (h, _) = gateway_harness().await;
    let id = started(&h.app, "alice").await;
    put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    let later = chrono::Utc::now() + chrono::Duration::minutes(31);
    assert_eq!(
        crate::recap::tick::finalize_stale(&h.ctx.engine, later).await,
        1
    );
    let recap = h.ctx.engine.store.get_meeting(&id).await.unwrap().unwrap();
    assert_eq!(recap.status, MeetingStatus::Done);
    assert!(recap.recap.unwrap().partial);
}

#[test]
fn notify_hints_are_generic_and_addressed_to_the_owner() {
    use crate::recap::events::{digest_hint, ready_hint};
    let alice =
        crate::test_support::recap_row("rcp_1", "alice", "2026-11-08", "2026-11-08T10:00:00Z");
    let value = serde_json::to_value(ready_hint(&alice).unwrap()).unwrap();
    assert_eq!(value["title"], "Your recap is ready");
    assert_eq!(value["target_user_id"], "alice");
    let value = serde_json::to_value(digest_hint("alice", 3, true).unwrap()).unwrap();
    assert_eq!(
        value["body"],
        "Open Ryu to read today's recaps. Conversations recapped: 3."
    );
    assert_eq!(value["target_user_id"], "alice");
    assert!(
        digest_hint("alice", 3, false).is_none(),
        "the owner turned it off"
    );
    // The headerless local owner has no concrete id: no notice rather than one
    // Core would hand to whichever account is active on the node.
    let local = crate::test_support::recap_row(
        "rcp_2",
        crate::recap::caller::LOCAL_OWNER,
        "2026-11-08",
        "2026-11-08T10:00:00Z",
    );
    assert!(ready_hint(&local).is_none());
    assert!(digest_hint(crate::recap::caller::LOCAL_OWNER, 3, true).is_none());
    let ready = crate::recap::events::ready_payload(&alice);
    assert!(
        ready.get("title").is_none(),
        "event payloads carry no recap text"
    );
}
