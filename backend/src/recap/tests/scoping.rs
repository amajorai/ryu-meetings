//! Owner scoping across every surface: recap routes, legacy routes, capability
//! routes, the shared stream, the data fan-out and the OpenAPI tool document.

use axum::http::{Method, StatusCode};
use serde_json::json;

use super::support::*;

#[tokio::test]
async fn another_member_gets_404_on_every_recap_route() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    let routes = [
        (Method::GET, format!("/recaps/{id}/detail"), None),
        (
            Method::PATCH,
            format!("/recaps/{id}/detail"),
            Some(json!({ "pinned": true })),
        ),
        (Method::DELETE, format!("/recaps/{id}/detail"), None),
        (Method::POST, format!("/recaps/{id}/confirm"), None),
        (Method::GET, format!("/recaps/{id}/chunks"), None),
        (
            Method::POST,
            format!("/recaps/{id}/finalize"),
            Some(json!({ "expected_chunks": 0 })),
        ),
        (Method::GET, format!("/recaps/{id}/events"), None),
        (Method::GET, format!("/recaps/{id}/interim"), None),
    ];
    consent(&h.app, "bob").await;
    for (method, path, body) in routes {
        let (code, _) = call(&h.app, method.clone(), &path, Some("bob"), body).await;
        assert_eq!(code, StatusCode::NOT_FOUND, "{method} {path}");
    }
    let (code, _) = put_chunk(&h.app, "bob", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    let (_, list) = call(&h.app, Method::GET, "/recaps", Some("bob"), None).await;
    assert_eq!(list["recaps"], json!([]));
    let (_, list) = call(&h.app, Method::GET, "/recaps", Some("alice"), None).await;
    assert_eq!(list["recaps"][0]["id"], id.as_str());
    assert!(
        h.ctx.engine.store.get_meeting(&id).await.unwrap().is_some(),
        "still there"
    );
}

#[tokio::test]
async fn legacy_and_capability_routes_never_expose_a_recap() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    for (method, path, body) in [
        (Method::GET, format!("/{id}"), None),
        (Method::DELETE, format!("/{id}"), None),
        (Method::GET, format!("/{id}/transcript"), None),
        (
            Method::POST,
            format!("/{id}/title"),
            Some(json!({ "title": "x" })),
        ),
        (
            Method::POST,
            format!("/{id}/icon"),
            Some(json!({ "icon": null })),
        ),
        (Method::POST, format!("/{id}/finalize"), None),
        (
            Method::POST,
            "/resolve".to_string(),
            Some(json!({ "id": id })),
        ),
        (
            Method::POST,
            "/ambient/chunk".to_string(),
            Some(json!({ "id": id, "audio_base64": "AAAA" })),
        ),
    ] {
        let (code, _) = call(&h.app, method.clone(), &path, Some("alice"), body).await;
        assert_eq!(code, StatusCode::NOT_FOUND, "{method} {path}");
    }
    let (_, legacy) = call(&h.app, Method::GET, "/", Some("alice"), None).await;
    assert_eq!(legacy["meetings"], json!([]));
    let recap = h.ctx.engine.store.get_meeting(&id).await.unwrap().unwrap();
    assert_eq!(recap.title, "Recap", "the legacy rename never landed");
}

#[tokio::test]
async fn a_headerless_request_is_refused_on_a_managed_node_and_is_the_owner_locally() {
    let managed = harness().await;
    let (code, body) = call(&managed.app, Method::GET, "/recaps", None, None).await;
    assert_eq!(
        (code, body["error"].as_str()),
        (StatusCode::UNAUTHORIZED, Some("tenant_required"))
    );

    let local = harness_with(Default::default(), false, true).await;
    let (code, _) = call(
        &local.app,
        Method::POST,
        "/recaps/consent",
        None,
        Some(json!({ "version": 1 })),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let session = uuid::Uuid::new_v4().to_string();
    let (code, body) = call(
        &local.app,
        Method::POST,
        "/recaps",
        None,
        Some(start_body(&session, "phone")),
    )
    .await;
    assert_eq!(code, StatusCode::CREATED);
    assert_eq!(body["recap"]["recap"]["owner_user_id"], "local");
}

#[tokio::test]
async fn the_shared_stream_carries_no_recap_events() {
    let h = harness().await;
    let mut shared = h.ctx.engine.store.subscribe();
    let mut recap_rx = h.ctx.engine.store.subscribe_recaps();
    let id = started(&h.app, "alice").await;
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(code, StatusCode::OK);
    assert!(
        shared.try_recv().is_err(),
        "nothing about a recap reached /stream"
    );
    let (tagged, _) = recap_rx.try_recv().unwrap();
    assert_eq!(tagged, id);
}

#[tokio::test]
async fn data_count_and_purge_are_per_caller() {
    let h = harness().await;
    let a = started(&h.app, "alice").await;
    let b = started(&h.app, "bob").await;
    h.ctx
        .engine
        .start("Shared".into(), None, crate::MeetingSource::Manual)
        .await
        .unwrap();
    let (_, count) = call(
        &h.app,
        Method::POST,
        "/data/count",
        None,
        Some(json!({ "category": "meetings", "user_id": "alice" })),
    )
    .await;
    assert_eq!(count["count"], 1);
    // A managed node's Danger Zone purge (no user id) never touches members' recaps.
    let (_, purged) = call(
        &h.app,
        Method::POST,
        "/data/purge",
        None,
        Some(json!({ "category": "meetings" })),
    )
    .await;
    assert_eq!(purged["removed"], 1, "only the shared meeting");
    assert!(h.ctx.engine.store.get_meeting(&a).await.unwrap().is_some());
    let (_, purged) = call(
        &h.app,
        Method::POST,
        "/data/purge",
        None,
        Some(json!({ "category": "meetings", "user_id": "alice" })),
    )
    .await;
    assert_eq!(purged["removed"], 1);
    assert!(h.ctx.engine.store.get_meeting(&a).await.unwrap().is_none());
    assert!(h
        .ctx
        .engine
        .store
        .recap_settings("alice")
        .await
        .unwrap()
        .is_none());
    assert!(
        h.ctx.engine.store.get_meeting(&b).await.unwrap().is_some(),
        "bob untouched"
    );
}

#[test]
fn the_openapi_document_exposes_no_recap_route() {
    let doc = crate::api::openapi();
    assert!(doc.paths.paths.keys().all(|p| !p.contains("/recaps")));
}

#[tokio::test]
async fn reading_and_deleting_still_work_when_capture_is_turned_off() {
    let h = harness().await;
    let id = started(&h.app, "alice").await;
    h.ctx
        .engine
        .pref_set(crate::recap::settings::NODE_ENABLED_PREF, "false")
        .await
        .unwrap();
    let (code, _) = call(
        &h.app,
        Method::GET,
        &format!("/recaps/{id}/detail"),
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let (code, _) = put_chunk(&h.app, "alice", &id, 0, 0, 1_000, speech_wav(1_000), &[]).await;
    assert_eq!(code, StatusCode::FORBIDDEN);
    let (code, _) = call(
        &h.app,
        Method::DELETE,
        &format!("/recaps/{id}/detail"),
        Some("alice"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert!(h.ctx.engine.store.get_meeting(&id).await.unwrap().is_none());
}
