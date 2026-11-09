//! Router-level checks for the split handler modules (multipart, SSE, capability).

use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

use super::*;
use crate::api::{capability, chunk, detect, import, meetings, stream};

fn app() -> (MeetingsCtx, axum::Router) {
    let ctx = ctx_with(FakeHost::default());
    let app = crate::api::routes(ctx.clone());
    (ctx, app)
}

fn multipart(field: &str, bytes: &[u8]) -> (String, Vec<u8>) {
    let boundary = "ryu-test-boundary";
    let mut body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"{field}\"; filename=\"a.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

async fn post_multipart(app: &axum::Router, path: &str, field: &str, bytes: &[u8]) -> StatusCode {
    let (content_type, body) = multipart(field, bytes);
    let req = Request::post(path)
        .header("content-type", content_type)
        .body(Body::from(body))
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn chunk_upload_without_a_file_field_is_a_bad_request() {
    let (ctx, app) = app();
    let m = ctx
        .engine
        .start(String::new(), None, MeetingSource::Manual)
        .await
        .unwrap();
    let path = format!("/{}/chunk", m.id);
    assert_eq!(
        post_multipart(&app, &path, "other", b"x").await,
        StatusCode::BAD_REQUEST
    );
    let query: chunk::ChunkQuery = serde_json::from_value(json!({ "offset_ms": 1200 })).unwrap();
    assert_eq!(query.offset_ms, Some(1200));
}

#[tokio::test]
async fn importing_a_non_wav_file_is_unsupported_media() {
    let (ctx, app) = app();
    assert_eq!(
        post_multipart(&app, "/import", "file", b"not a wav").await,
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert!(
        ctx.engine.list().await.unwrap().is_empty(),
        "nothing was created"
    );
    let _ = import::import_meeting;
}

#[tokio::test]
async fn the_stream_primes_with_a_comment_frame() {
    let (_, app) = app();
    let resp = app
        .clone()
        .oneshot(Request::get("/stream").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body().into_data_stream();
    use futures_util::StreamExt as _;
    let first = body.next().await.unwrap().unwrap();
    assert_eq!(std::str::from_utf8(&first).unwrap(), ": ready\n\n");
    let _ = stream::meetings_stream;
}

#[tokio::test]
async fn split_handlers_keep_their_behaviour() {
    let (ctx, _) = app();
    let Json(listed) = meetings::list_meetings(State(ctx.clone())).await;
    assert_eq!(listed["meetings"], json!([]));
    let (code, Json(count)) = capability::capability_data_count(
        State(ctx.clone()),
        axum::http::HeaderMap::new(),
        Json(DataCategoryBody {
            category: "meetings".into(),
            user_id: None,
        }),
    )
    .await;
    assert_eq!((code, count["count"].as_u64()), (StatusCode::OK, Some(0)));
    let Json(config) = detect::get_detection_config(State(ctx)).await;
    assert_eq!(config["enabled"], true);
}
