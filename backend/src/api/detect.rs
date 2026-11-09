//! Shadow's mic-in-use detection hook and the detection config.

use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use serde_json::json;

use super::{MeetingsCtx, DEFAULT_MEETING_APPS, DETECTION_APPS_PREF, DETECTION_ENABLED_PREF};

/// Request body Shadow posts when it detects a process using the microphone.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct DetectBody {
    /// The owning process / app slug (e.g. `zoom`).
    pub app: String,
    #[serde(default)]
    pub title: Option<String>,
}

/// `POST /api/meetings/detect` — Shadow's mic-in-use detection hook. Shadow
/// reports the *raw* process currently using the microphone; Core is the brain
/// that decides whether it's a meeting: it filters against the configured
/// meeting-app list, debounces, then broadcasts a `detected` event so the island
/// can prompt to start notes.
#[utoipa::path(
    post,
    path = "/api/meetings/detect",
    tag = "Meetings",
    summary = "Shadow's mic-in-use detection hook. Shadow",
    request_body = DetectBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn detect(
    State(ctx): State<MeetingsCtx>,
    Json(body): Json<DetectBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Respect the master toggle.
    if let Some(v) = ctx.engine.pref_get(DETECTION_ENABLED_PREF).await {
        if v.trim() == "false" {
            return (
                StatusCode::OK,
                Json(json!({ "broadcast": false, "reason": "detection disabled" })),
            );
        }
    }

    // Only meeting apps trigger a prompt — a process using the mic for dictation
    // or a voice note shouldn't pop "start meeting notes?". An empty list means
    // "match nothing extra"; we always fall back to the built-in defaults so the
    // feature works before the user customizes anything.
    let apps = ctx
        .engine
        .pref_get(DETECTION_APPS_PREF)
        .await
        .and_then(|v| serde_json::from_str::<Vec<String>>(&v).ok())
        .unwrap_or_else(|| DEFAULT_MEETING_APPS.iter().map(|s| s.to_string()).collect());
    let app_lower = body.app.to_lowercase();
    let matched = apps
        .iter()
        .find(|slug| !slug.trim().is_empty() && app_lower.contains(&slug.to_lowercase()))
        .cloned();
    let Some(slug) = matched else {
        return (
            StatusCode::OK,
            Json(json!({ "broadcast": false, "reason": "not a known meeting app" })),
        );
    };

    let broadcast = ctx
        .engine
        .record_detection(&slug, body.title.as_deref())
        .await;
    (StatusCode::OK, Json(json!({ "broadcast": broadcast })))
}

/// `GET /api/meetings/detection-config` — the detection toggle + meeting-app list.
#[utoipa::path(
    get,
    path = "/api/meetings/detection-config",
    tag = "Meetings",
    summary = "the detection toggle + meeting-app list.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_detection_config(State(ctx): State<MeetingsCtx>) -> Json<serde_json::Value> {
    let enabled = ctx
        .engine
        .pref_get(DETECTION_ENABLED_PREF)
        .await
        .map(|v| v.trim() != "false")
        .unwrap_or(true);
    let apps = ctx
        .engine
        .pref_get(DETECTION_APPS_PREF)
        .await
        .and_then(|v| serde_json::from_str::<Vec<String>>(&v).ok())
        .unwrap_or_else(|| DEFAULT_MEETING_APPS.iter().map(|s| s.to_string()).collect());
    Json(json!({ "enabled": enabled, "apps": apps }))
}

/// Request body for updating the detection config.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct DetectionConfigBody {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub apps: Option<Vec<String>>,
}

/// `PUT /api/meetings/detection-config` — update the toggle and/or app list.
#[utoipa::path(
    put,
    path = "/api/meetings/detection-config",
    tag = "Meetings",
    summary = "update the toggle and/or app list.",
    request_body = DetectionConfigBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn put_detection_config(
    State(ctx): State<MeetingsCtx>,
    Json(body): Json<DetectionConfigBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Some(enabled) = body.enabled {
        let _ = ctx
            .engine
            .pref_set(
                DETECTION_ENABLED_PREF,
                if enabled { "true" } else { "false" },
            )
            .await;
    }
    if let Some(apps) = body.apps {
        let json = serde_json::to_string(&apps).unwrap_or_else(|_| "[]".to_string());
        let _ = ctx.engine.pref_set(DETECTION_APPS_PREF, &json).await;
    }
    (StatusCode::OK, get_detection_config(State(ctx)).await)
}
