//! Notes templates: the Settings picker and the Store tab.

use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use serde_json::json;

use super::{MeetingsCtx, NOTES_TEMPLATE_PREF};
use crate::templates;

/// `GET /api/meetings/templates` — the built-in notes templates.
///
/// Serves both the Settings picker and the app's registered **Store tab**
/// (`contributes.store_tabs` in the manifest), which browses the same list as cards.
/// Each entry therefore carries browse metadata (description / category / icon /
/// tags) plus `active`, the flag the Store card reads to render "Added" — which is
/// why this needs state: `active` is derived from the `meeting-notes-template`
/// preference, resolved through the same fallback the notes pipeline uses.
#[utoipa::path(
    get,
    path = "/api/meetings/templates",
    tag = "Meetings",
    summary = "the built-in notes templates for the picker and the Store tab.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_templates(State(ctx): State<MeetingsCtx>) -> Json<serde_json::Value> {
    let selected = ctx
        .engine
        .pref_get(NOTES_TEMPLATE_PREF)
        .await
        .unwrap_or_default();
    Json(templates::catalog_json(&selected))
}

/// Request body for selecting a notes template.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct SelectTemplateBody {
    pub template_id: String,
}

/// `POST /api/meetings/templates/select` — make a template the active one.
///
/// This is what "install" means for a meeting-notes template, and it is deliberately
/// the whole of it: a template is a prompt preset, so adopting one is a preference
/// write to `meeting-notes-template` — the exact key `resolve_notes_prompt` already
/// reads. No parallel store of user templates, and no second source of truth for
/// which template is in force.
///
/// An unknown id is rejected rather than stored: `prompt_for` would silently fall
/// back to the default, so accepting it would leave the UI showing a selection that
/// does nothing. A user's fully custom `meeting-notes-prompt` still overrides
/// whatever is selected here — that precedence is unchanged.
#[utoipa::path(
    post,
    path = "/api/meetings/templates/select",
    tag = "Meetings",
    summary = "make a notes template the active one.",
    request_body = SelectTemplateBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn select_template(
    State(ctx): State<MeetingsCtx>,
    Json(body): Json<SelectTemplateBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let Some(template) = templates::by_id(&body.template_id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "success": false,
                "error": format!("unknown template '{}'", body.template_id),
            })),
        );
    };
    match ctx.engine.pref_set(NOTES_TEMPLATE_PREF, template.id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "success": true, "template_id": template.id })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "success": false, "error": e })),
        ),
    }
}
