//! Shared recap test harness: the real router over a temp store and a fake host,
//! plus a loopback "Gateway" that records what the model was sent.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    routing::post,
    Json, Router,
};
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::api::MeetingsCtx;
use crate::recap::caller::CALLER_USER_HEADER;
use crate::recap::settings::{CONSENT_VERSION, NODE_ENABLED_PREF};
use crate::test_support::{engine_with, FakeHost};
use crate::{audio, MeetingStatus};

pub(crate) struct Harness {
    pub ctx: MeetingsCtx,
    pub app: Router,
}

/// A harness with recaps enabled on the node (unless `enabled` is false).
pub(crate) async fn harness_with(host: FakeHost, managed: bool, enabled: bool) -> Harness {
    let ctx = MeetingsCtx::with_managed(engine_with(host), managed);
    if enabled {
        ctx.engine
            .pref_set(NODE_ENABLED_PREF, "true")
            .await
            .unwrap();
    }
    let app = crate::api::routes(ctx.clone());
    Harness { ctx, app }
}

pub(crate) async fn harness() -> Harness {
    harness_with(FakeHost::default(), true, true).await
}

pub(crate) fn today() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

/// Send one request; `user` becomes the verified caller header.
pub(crate) async fn call(
    app: &Router,
    method: Method,
    path: &str,
    user: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(path);
    if let Some(user) = user {
        req = req.header(CALLER_USER_HEADER, user);
    }
    let body = match body {
        Some(value) => {
            req = req.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    send(app, req.body(body).unwrap()).await
}

pub(crate) async fn send(app: &Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, value)
}

/// Upload one chunk.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn put_chunk(
    app: &Router,
    user: &str,
    id: &str,
    seq: u32,
    offset_ms: i64,
    duration_ms: i64,
    bytes: Vec<u8>,
    extra: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(Method::PUT)
        .uri(format!("/recaps/{id}/chunks/{seq}"))
        .header(CALLER_USER_HEADER, user)
        .header("x-recap-offset-ms", offset_ms.to_string())
        .header("x-recap-duration-ms", duration_ms.to_string());
    if !extra.iter().any(|(name, _)| *name == "content-type") {
        req = req.header("content-type", "audio/wav");
    }
    for (name, value) in extra {
        req = req.header(*name, *value);
    }
    send(app, req.body(Body::from(bytes)).unwrap()).await
}

/// A loud 16 kHz mono tone the energy VAD reads as speech.
pub(crate) fn speech_wav(ms: usize) -> Vec<u8> {
    let samples: Vec<i16> = (0..ms * 16)
        .map(|i| ((i as f32 * 0.2).sin() * 9_000.0) as i16)
        .collect();
    audio::encode_wav(1, audio::TARGET_RATE, &samples).unwrap()
}

/// Digital silence.
pub(crate) fn silent_wav(ms: usize) -> Vec<u8> {
    audio::encode_wav(1, audio::TARGET_RATE, &vec![0i16; ms * 16]).unwrap()
}

pub(crate) async fn consent(app: &Router, user: &str) {
    let (code, body) = call(
        app,
        Method::POST,
        "/recaps/consent",
        Some(user),
        Some(json!({ "version": CONSENT_VERSION })),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
}

pub(crate) fn start_body(session: &str, source: &str) -> Value {
    json!({
        "client_session_id": session,
        "source": source,
        "tz": "UTC",
        "local_day": today(),
        "consent_version": CONSENT_VERSION,
    })
}

/// Consent and start a recap for `user`; returns its id.
pub(crate) async fn started(app: &Router, user: &str) -> String {
    consent(app, user).await;
    let session = uuid::Uuid::new_v4().to_string();
    let (code, body) = call(
        app,
        Method::POST,
        "/recaps",
        Some(user),
        Some(start_body(&session, "phone")),
    )
    .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    body["recap"]["id"].as_str().unwrap().to_string()
}

/// Poll until the recap reaches `status` (the finalize tail runs in the
/// background).
pub(crate) async fn wait_status(ctx: &MeetingsCtx, id: &str, status: MeetingStatus) {
    for _ in 0..200 {
        let current = ctx
            .engine
            .store
            .get_meeting(id)
            .await
            .unwrap()
            .map(|m| m.status);
        if current == Some(status) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("recap {id} never reached {status:?}");
}

/// A loopback chat-completions endpoint answering `reply` and recording every
/// request body it receives.
pub(crate) async fn capture_gateway(reply: Value) -> (String, Arc<Mutex<Vec<Value>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let state = (Arc::clone(&seen), reply);
    let app = Router::new()
        .route(
            "/v1/chat/completions",
            post(
                |axum::extract::State((seen, reply)): axum::extract::State<(
                    Arc<Mutex<Vec<Value>>>,
                    Value,
                )>,
                 Json(body): Json<Value>| async move {
                    seen.lock().unwrap().push(body);
                    Json(json!({
                        "choices": [{ "message": { "content": reply.to_string() } }]
                    }))
                },
            ),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), seen)
}
