//! Meetings CRUD and the full-transcript read.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::json;

use super::MeetingsCtx;
use crate::MeetingSource;

// ---- meetings CRUD --------------------------------------------------------

/// `GET /api/meetings` — list all meetings, newest first.
#[utoipa::path(
    get,
    path = "/api/meetings",
    tag = "Meetings",
    summary = "list all meetings, newest first.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_meetings(State(ctx): State<MeetingsCtx>) -> Json<serde_json::Value> {
    match ctx.engine.list().await {
        Ok(meetings) => Json(json!({ "meetings": meetings })),
        Err(e) => Json(json!({ "meetings": [], "error": e })),
    }
}

/// Request body for starting a meeting.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct StartBody {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub app: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
}

/// `POST /api/meetings` — start a meeting (and best-effort begin Shadow capture).
#[utoipa::path(
    post,
    path = "/api/meetings",
    tag = "Meetings",
    summary = "start a meeting (and best-effort begin Shadow capture).",
    request_body = StartBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn create_meeting(
    State(ctx): State<MeetingsCtx>,
    Json(body): Json<StartBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let source = match body.source.as_deref() {
        Some("auto") => MeetingSource::Auto,
        _ => MeetingSource::Manual,
    };
    match ctx.engine.start(body.title, body.app, source).await {
        Ok(meeting) => (StatusCode::OK, Json(json!({ "meeting": meeting }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e })),
        ),
    }
}

/// `GET /api/meetings/:id` — one meeting (without the transcript body).
#[utoipa::path(
    get,
    path = "/api/meetings/{id}",
    tag = "Meetings",
    summary = "one meeting (without the transcript body).",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_meeting(
    State(ctx): State<MeetingsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(refused) = super::refuse_recap(&ctx, &id).await {
        return refused;
    }
    match ctx.engine.get(&id).await {
        Ok(Some(m)) => (StatusCode::OK, Json(json!({ "meeting": m }))),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e })),
        ),
    }
}

/// Request body for renaming a meeting.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct RenameBody {
    pub title: String,
}

/// `POST /api/meetings/:id/title` — manually rename a meeting. Marks the title
/// user-chosen so the transcript auto-namer leaves it alone.
#[utoipa::path(
    post,
    path = "/api/meetings/{id}/title",
    tag = "Meetings",
    summary = "manually rename a meeting. Marks the title",
    params(("id" = String, Path)),
    request_body = RenameBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn rename_meeting(
    State(ctx): State<MeetingsCtx>,
    Path(id): Path<String>,
    Json(body): Json<RenameBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(refused) = super::refuse_recap(&ctx, &id).await {
        return refused;
    }
    let title = body.title.trim();
    if title.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "title must not be empty" })),
        );
    }
    match ctx.engine.store.set_title(&id, title).await {
        Ok(Some(m)) => (StatusCode::OK, Json(json!({ "meeting": m }))),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct SetIconBody {
    /// Notion-style glyph JSON, or `null` to clear.
    pub icon: Option<serde_json::Value>,
}

/// `POST /api/meetings/:id/icon` — set or clear a meeting's glyph.
#[utoipa::path(
    post,
    path = "/api/meetings/{id}/icon",
    tag = "Meetings",
    summary = "set or clear a meeting glyph",
    params(("id" = String, Path)),
    request_body = SetIconBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn set_meeting_icon(
    State(ctx): State<MeetingsCtx>,
    Path(id): Path<String>,
    Json(body): Json<SetIconBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(refused) = super::refuse_recap(&ctx, &id).await {
        return refused;
    }
    match ctx.engine.store.set_icon(&id, body.icon).await {
        Ok(Some(m)) => (StatusCode::OK, Json(json!({ "meeting": m }))),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// `DELETE /api/meetings/:id` — remove a meeting and its transcript.
#[utoipa::path(
    delete,
    path = "/api/meetings/{id}",
    tag = "Meetings",
    summary = "remove a meeting and its transcript.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn delete_meeting(
    State(ctx): State<MeetingsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(refused) = super::refuse_recap(&ctx, &id).await {
        return refused;
    }
    match ctx.engine.delete(&id).await {
        Ok(true) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(false) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e })),
        ),
    }
}

/// `GET /api/meetings/:id/transcript` — the full transcript (segments + text).
#[utoipa::path(
    get,
    path = "/api/meetings/{id}/transcript",
    tag = "Meetings",
    summary = "the full transcript (segments + text).",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_transcript(
    State(ctx): State<MeetingsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(refused) = super::refuse_recap(&ctx, &id).await {
        return refused;
    }
    let segments = match ctx.engine.store.list_segments(&id).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };
    let text = segments
        .iter()
        .map(|s| s.text.clone())
        .collect::<Vec<_>>()
        .join("\n");
    (
        StatusCode::OK,
        Json(json!({ "segments": segments, "text": text })),
    )
}
