//! HTTP API for meeting notes (`/api/meetings/*`).
//!
//! CRUD over meetings, multipart chunk ingest (transcribe → append → broadcast),
//! finalize (gateway note generation), a full-transcript read, an SSE event
//! stream, the Shadow detection hook, and the detection-config KV.
//!
//! Per the Core-vs-Gateway rule this is **Core** — it decides *what runs* (start
//! a recording, transcribe a chunk, ask a model for notes). Audio capture is a
//! device-bound sensor and lives in Shadow; this surface only ingests the chunks
//! Shadow streams up.

mod capability;
mod chunk;
mod detect;
mod finalize;
mod import;
mod meetings;
mod stream;
mod templates;
#[cfg(test)]
mod tests;

use axum::{
    routing::{get, post},
    Router,
};

pub use capability::*;
pub use chunk::*;
pub use detect::*;
pub use finalize::*;
pub use import::*;
pub use meetings::*;
pub use stream::*;
pub use templates::*;

use axum::{http::StatusCode, Json};
use serde_json::json;

use crate::{MeetingEngine, MeetingKind};

/// Router state for the meetings HTTP surface: the [`MeetingEngine`] (which owns
/// the store and the inverted [`crate::MeetingsHost`]), whether this is a managed
/// node (a headerless recap request is then refused), and the recap runtime.
#[derive(Clone)]
pub struct MeetingsCtx {
    pub engine: MeetingEngine,
    pub managed_node: bool,
    pub recaps: crate::recap::RecapRuntime,
}

impl MeetingsCtx {
    /// Build the state, reading `RYU_MANAGED_NODE` from the environment.
    pub fn new(engine: MeetingEngine) -> Self {
        Self::with_managed(engine, crate::recap::caller::is_managed_node())
    }

    /// Build the state with an explicit managed-node flag.
    pub fn with_managed(engine: MeetingEngine, managed_node: bool) -> Self {
        Self {
            engine,
            managed_node,
            recaps: crate::recap::RecapRuntime::default(),
        }
    }
}

/// The legacy meeting routes, the capability routes and the shared stream never
/// touch a recap: a recap id answers exactly like an unknown id (404).
pub(crate) async fn refuse_recap(
    ctx: &MeetingsCtx,
    id: &str,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    match ctx.engine.store.kind_and_owner(id).await {
        Ok(Some((MeetingKind::Recap, _))) => {
            Err((StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))))
        }
        _ => Ok(()),
    }
}

/// Build the `/api/meetings/*` router with its own state baked in, returning a
/// state-less `Router<()>` the host nests at `/api/meetings` behind the
/// Meetings-App gate. Static segments (`stream`, `detect`, `detection-config`,
/// `templates`, `import`) are registered before `:id` so they match first —
/// byte-identical to the old direct mount.
pub fn routes(ctx: MeetingsCtx) -> Router<()> {
    Router::new()
        // Owner-scoped recap routes (none of them is in the OpenAPI document).
        .merge(crate::recap::routes())
        .route("/stream", get(meetings_stream))
        .route("/detect", post(detect))
        .route(
            "/detection-config",
            get(get_detection_config).put(put_detection_config),
        )
        // `/templates/select` is registered before `/templates` for the same reason
        // the whole static block precedes `/:id`: longest-static-first, so a new
        // sub-path can never be swallowed by a shorter sibling.
        .route("/templates/select", post(select_template))
        .route("/templates", get(list_templates))
        .route("/import", post(import_meeting))
        // Capability-only routes (see `CAPABILITY_ONLY_ROUTES`).
        .route("/resolve", post(capability_resolve))
        .route("/ambient/chunk", post(capability_append_audio))
        .route("/data/count", post(capability_data_count))
        .route("/data/purge", post(capability_data_purge))
        .route("/", get(list_meetings).post(create_meeting))
        .route("/:id", get(get_meeting).delete(delete_meeting))
        .route("/:id/title", post(rename_meeting))
        .route("/:id/icon", post(set_meeting_icon))
        .route("/:id/chunk", post(ingest_chunk))
        .route("/:id/finalize", post(finalize_meeting))
        .route("/:id/transcript", get(get_transcript))
        .with_state(ctx)
}

/// Routes declared ONLY so a manifest `provides` row can point at them: Core's
/// capability consumer is their caller (`meetings.get`, `meetings.appendAudio`, and
/// the `data.count` / `data.purge` fan-out behind the Danger Zone row;
/// `meetings.start` reuses `POST /`). They stay out of the OpenAPI document so they
/// never become agent tools.
pub const CAPABILITY_ONLY_ROUTES: &[&str] =
    &["/resolve", "/ambient/chunk", "/data/count", "/data/purge"];

/// The Danger Zone category this app declares in `contributes.data_categories`.
const DATA_CATEGORY: &str = "meetings";

/// The OpenAPI sub-document for the meetings surface.
///
/// Nothing merges this into Core's own spec (it says so nowhere in `apps/core`).
/// `main.rs` serves it at this sidecar's server root, and Core fetches
/// `http://127.0.0.1:<port>/openapi.json` on the first Healthy edge and lowers
/// every operation it finds into an LLM tool — so the `request_body` types on the
/// annotations below are the arguments a model gets to see.
pub fn openapi() -> utoipa::openapi::OpenApi {
    <MeetingsApiDoc as utoipa::OpenApi>::openapi()
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
    create_meeting,
    delete_meeting,
    detect,
    finalize_meeting,
    get_detection_config,
    get_meeting,
    get_transcript,
    import_meeting,
    ingest_chunk,
    list_meetings,
    list_templates,
    meetings_stream,
    put_detection_config,
    rename_meeting,
    select_template,
    set_meeting_icon,
    ),
    // Every write body, listed explicitly. utoipa 5 also auto-collects whatever is
    // reachable from `paths(...)`, so this is belt-and-braces — but a bare list is
    // greppable and survives an edit to the annotations above, and a body type that
    // silently stops being registered yields a `$ref` Core cannot resolve, which
    // means an LLM tool with ZERO visible arguments.
    components(schemas(
        ChunkUpload,
        DetectBody,
        DetectionConfigBody,
        ImportUpload,
        RenameBody,
        SelectTemplateBody,
        SetIconBody,
        StartBody,
    ))
)]
struct MeetingsApiDoc;

pub(crate) const NOTES_MODEL_PREF: &str = "meeting-notes-model";
pub(crate) const NOTES_EFFORT_PREF: &str = "meeting-notes-effort";
pub(crate) const NOTES_PROMPT_PREF: &str = "meeting-notes-prompt";
pub(crate) const NOTES_TEMPLATE_PREF: &str = "meeting-notes-template";
pub(crate) const DETECTION_APPS_PREF: &str = "meeting-detection-apps";
pub(crate) const DETECTION_ENABLED_PREF: &str = "meeting-detection-enabled";
pub(crate) const DIARIZATION_ENABLED_PREF: &str = "meeting-diarization-enabled";
pub(crate) const DIARIZATION_MODEL_PREF: &str = "meeting-diarization-model";

/// Default processes whose mic use is treated as "you're in a meeting". The
/// detector (Shadow) matches a foreground/mic-owning process against this list;
/// it is a *swappable default*, editable via the detection-config endpoint.
pub(crate) const DEFAULT_MEETING_APPS: &[&str] = &[
    "zoom", "teams", "meet", "slack", "discord", "webex", "skype", "facetime", "whereby", "around",
    "gather", "huddle",
];

// ---- model / prompt resolution (nothing hardcoded) ------------------------

pub(crate) async fn resolve_notes_model(engine: &MeetingEngine) -> String {
    if let Some(pref) = engine.pref_get(NOTES_MODEL_PREF).await {
        let trimmed = pref.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    for var in ["RYU_MEETING_NOTES_MODEL", "RYU_DEFAULT_LLM_MODEL"] {
        if let Ok(val) = std::env::var(var) {
            if !val.is_empty() {
                return val;
            }
        }
    }
    engine.default_notes_model()
}

pub(crate) async fn resolve_notes_effort(engine: &MeetingEngine) -> String {
    if let Some(pref) = engine.pref_get(NOTES_EFFORT_PREF).await {
        let trimmed = pref.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    std::env::var("RYU_MEETING_NOTES_EFFORT")
        .ok()
        .unwrap_or_default()
}

/// Resolve the notes system prompt. A user's fully custom prompt wins; otherwise
/// the selected template's prompt is used; otherwise the default template.
pub(crate) async fn resolve_notes_prompt(engine: &MeetingEngine) -> String {
    if let Some(pref) = engine.pref_get(NOTES_PROMPT_PREF).await {
        let trimmed = pref.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    let template_id = engine
        .pref_get(NOTES_TEMPLATE_PREF)
        .await
        .unwrap_or_default();
    crate::templates::prompt_for(&template_id)
}
