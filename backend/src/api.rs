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

use axum::{
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use ryu_diarization::{DiarizationModel, DiarizationRequest};
use serde::Deserialize;
use serde_json::json;

use crate::{
    audio, diarize, notes::MeetingNotes, templates, Meeting, MeetingEngine, MeetingSource,
    EVENT_MEETING_ENDED, EVENT_NOTES_READY,
};

/// Router state for the meetings HTTP surface: the [`MeetingEngine`] (which owns
/// the store and the inverted [`crate::MeetingsHost`]).
#[derive(Clone)]
pub struct MeetingsCtx {
    pub engine: MeetingEngine,
}

impl MeetingsCtx {
    pub fn new(engine: MeetingEngine) -> Self {
        Self { engine }
    }
}

/// Build the `/api/meetings/*` router with its own state baked in, returning a
/// state-less `Router<()>` the host nests at `/api/meetings` behind the
/// Meetings-App gate. Static segments (`stream`, `detect`, `detection-config`,
/// `templates`, `import`) are registered before `:id` so they match first —
/// byte-identical to the old direct mount.
pub fn routes(ctx: MeetingsCtx) -> Router<()> {
    Router::new()
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

/// Body of `POST /api/meetings/resolve` (`meetings.get`).
#[derive(Debug, Deserialize)]
pub struct CapabilityResolveBody {
    pub id: String,
}

/// `POST /api/meetings/resolve` (`meetings.get`) — the body-addressed twin of
/// `GET /api/meetings/:id`: `{ meeting }`, or 404 when it is gone.
pub async fn capability_resolve(
    state: State<MeetingsCtx>,
    Json(body): Json<CapabilityResolveBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    get_meeting(state, Path(body.id)).await
}

/// Body of `POST /api/meetings/ambient/chunk` (`meetings.appendAudio`): one
/// captured WAV segment, base64-encoded because a capability call is JSON.
#[derive(Debug, Deserialize)]
pub struct CapabilityAppendAudioBody {
    pub id: String,
    #[serde(default)]
    pub filename: Option<String>,
    pub audio_base64: String,
}

/// `POST /api/meetings/ambient/chunk` (`meetings.appendAudio`) — append one WAV
/// segment to a meeting's live transcript, with the same reply shapes as
/// `POST /api/meetings/:id/chunk` (`{ segment }`, or 200 `{ segment: null,
/// skipped }` for silence).
pub async fn capability_append_audio(
    State(ctx): State<MeetingsCtx>,
    Json(body): Json<CapabilityAppendAudioBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    use base64::Engine as _;
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(body.audio_base64.trim())
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "audio_base64 is not valid base64" })),
        );
    };
    let filename = body.filename.unwrap_or_else(|| "chunk.wav".to_string());
    match ctx
        .engine
        .ingest_chunk(&body.id, bytes, filename, None, None)
        .await
    {
        Ok(segment) => (StatusCode::OK, Json(json!({ "segment": segment }))),
        Err(e) if e.contains("silence") => (
            StatusCode::OK,
            Json(json!({ "segment": null, "skipped": e })),
        ),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({ "error": e }))),
    }
}

/// Body of the `data.count` / `data.purge` capabilities.
#[derive(Debug, Deserialize)]
pub struct DataCategoryBody {
    pub category: String,
}

fn unknown_category(category: &str) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("unknown data category '{category}'") })),
    )
}

/// `POST /api/meetings/data/count` (`data.count`) — `{ count }` for the
/// `meetings` category.
pub async fn capability_data_count(
    State(ctx): State<MeetingsCtx>,
    Json(body): Json<DataCategoryBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if body.category != DATA_CATEGORY {
        return unknown_category(&body.category);
    }
    match ctx.engine.list().await {
        Ok(meetings) => (StatusCode::OK, Json(json!({ "count": meetings.len() }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e })),
        ),
    }
}

/// `POST /api/meetings/data/purge` (`data.purge`) — delete every meeting through
/// the same per-item path as `DELETE /api/meetings/:id` (so each delete is
/// broadcast on the meetings stream) and answer `{ removed }`.
pub async fn capability_data_purge(
    State(ctx): State<MeetingsCtx>,
    Json(body): Json<DataCategoryBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if body.category != DATA_CATEGORY {
        return unknown_category(&body.category);
    }
    let meetings = match ctx.engine.list().await {
        Ok(meetings) => meetings,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e })),
            );
        }
    };
    let mut removed = 0u64;
    for meeting in meetings {
        match ctx.engine.delete(&meeting.id).await {
            Ok(true) => removed += 1,
            Ok(false) => {}
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": e, "removed": removed })),
                );
            }
        }
    }
    (StatusCode::OK, Json(json!({ "removed": removed })))
}

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

const NOTES_MODEL_PREF: &str = "meeting-notes-model";
const NOTES_EFFORT_PREF: &str = "meeting-notes-effort";
const NOTES_PROMPT_PREF: &str = "meeting-notes-prompt";
const NOTES_TEMPLATE_PREF: &str = "meeting-notes-template";
const DETECTION_APPS_PREF: &str = "meeting-detection-apps";
const DETECTION_ENABLED_PREF: &str = "meeting-detection-enabled";
const DIARIZATION_ENABLED_PREF: &str = "meeting-diarization-enabled";
const DIARIZATION_MODEL_PREF: &str = "meeting-diarization-model";

/// Default processes whose mic use is treated as "you're in a meeting". The
/// detector (Shadow) matches a foreground/mic-owning process against this list;
/// it is a *swappable default*, editable via the detection-config endpoint.
const DEFAULT_MEETING_APPS: &[&str] = &[
    "zoom", "teams", "meet", "slack", "discord", "webex", "skype", "facetime", "whereby", "around",
    "gather", "huddle",
];

// ---- model / prompt resolution (nothing hardcoded) ------------------------

async fn resolve_notes_model(engine: &MeetingEngine) -> String {
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

async fn resolve_notes_effort(engine: &MeetingEngine) -> String {
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
async fn resolve_notes_prompt(engine: &MeetingEngine) -> String {
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
    templates::prompt_for(&template_id)
}

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

/// Run diarization on a finalized meeting's persisted audio when the toggle is on,
/// writing speaker labels onto the transcript segments. Best-effort throughout: a
/// disabled toggle, a missing sidecar, or no persisted audio all just no-op.
async fn resolve_diarization_model(engine: &MeetingEngine) -> DiarizationModel {
    let model_value = engine
        .pref_get(DIARIZATION_MODEL_PREF)
        .await
        .unwrap_or_default();
    match model_value.trim() {
        "" => DiarizationModel::Pyannote,
        value => match DiarizationModel::try_from(value) {
            Ok(model) => model,
            Err(error) => {
                tracing::warn!(
                    "meetings: invalid diarization model preference; using pyannote: {error}"
                );
                DiarizationModel::Pyannote
            }
        },
    }
}

async fn diarize_if_enabled(engine: &MeetingEngine, id: &str) {
    let enabled = engine
        .pref_get(DIARIZATION_ENABLED_PREF)
        .await
        .map(|v| v.trim() == "true")
        .unwrap_or(false);
    if !enabled {
        return;
    }
    let model = resolve_diarization_model(engine).await;
    let records = engine.store.pcm_records(id).await.unwrap_or(0);
    let seal = engine.store.audio_seal();
    // One read of the (possibly sealed) log serves both the WAV the sidecar
    // diarizes and the PCM the speaker assignment aligns against.
    let pcm = match audio::read_pcm(seal, id, records) {
        Ok(Some(pcm)) => pcm,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!("meetings: diarization skipped for {id}: {e:#}");
            return;
        }
    };
    let wav = match audio::read_pcm_as_wav(seal, id, records) {
        Ok(Some(w)) => w,
        _ => return,
    };
    let turns = match diarize::diarize_wav(wav, DiarizationRequest::new(model)).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!("meetings: diarization skipped for {id}: {e}");
            return;
        }
    };
    let segments = match engine.store.list_segments(id).await {
        Ok(s) => s,
        Err(_) => return,
    };
    for (seg_id, speaker) in diarize::assign(&segments, &turns, &pcm) {
        let _ = engine.store.set_segment_speaker(seg_id, &speaker).await;
    }
}

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
    match ctx.engine.delete(&id).await {
        Ok(true) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(false) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e })),
        ),
    }
}

/// Optional `?engine=` selector (mirroring the voice transcribe route) and
/// `?offset_ms=` — the chunk's sample-accurate position from the recorder, used
/// to time the transcript segment instead of wall-clock.
#[derive(Debug, Deserialize)]
pub struct ChunkQuery {
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub offset_ms: Option<i64>,
}

/// The multipart form this route reads. Nothing deserialises into it — the
/// handler walks `Multipart` field by field — it exists so the document says what
/// the form actually is.
//
// Declared `multipart/form-data` rather than typed as JSON, because that is the
// truth: the old `request_body = serde_json::Value` claimed a JSON body that
// would 400 here. Core's tool importer only reads
// `content/application~1json/schema`, so this route contributes no LLM arguments
// either way — which is the right outcome for a binary upload no model can send.
#[derive(utoipa::ToSchema)]
pub struct ChunkUpload {
    /// The captured WAV chunk. The only field read; anything else is ignored.
    #[schema(format = Binary)]
    pub file: String,
}

/// `POST /api/meetings/:id/chunk` — ingest one captured WAV chunk (multipart
/// `file` field), transcribe it, and append it to the live transcript.
#[utoipa::path(
    post,
    path = "/api/meetings/{id}/chunk",
    tag = "Meetings",
    summary = "ingest one captured WAV chunk (multipart",
    params(("id" = String, Path)),
    request_body(content = ChunkUpload, content_type = "multipart/form-data"),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn ingest_chunk(
    State(ctx): State<MeetingsCtx>,
    Path(id): Path<String>,
    Query(query): Query<ChunkQuery>,
    mut multipart: Multipart,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut audio: Option<(String, Vec<u8>)> = None;
    while let Ok(Some(field)) = multipart.next_field().await {
        if field.name() == Some("file") {
            let filename = field
                .file_name()
                .map(str::to_string)
                .unwrap_or_else(|| "chunk.wav".to_string());
            match field.bytes().await {
                Ok(bytes) => audio = Some((filename, bytes.to_vec())),
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "error": format!("could not read audio field: {e}") })),
                    );
                }
            }
        }
    }
    let Some((filename, bytes)) = audio else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "missing `file` field (the audio chunk)" })),
        );
    };

    match ctx
        .engine
        .ingest_chunk(
            &id,
            bytes,
            filename,
            query.engine.as_deref(),
            query.offset_ms,
        )
        .await
    {
        Ok(segment) => (StatusCode::OK, Json(json!({ "segment": segment }))),
        // A silent chunk is not an error worth a 5xx — report it softly.
        Err(e) if e.contains("silence") => (
            StatusCode::OK,
            Json(json!({ "segment": null, "skipped": e })),
        ),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({ "error": e }))),
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

/// `POST /api/meetings/:id/finalize` — stop capture, generate notes, mark done,
/// and save the notes into the "Meetings" Space so they're editable + searchable
/// through the existing Spaces UI (best-effort; a Space failure doesn't fail the
/// finalize — the notes still live on the meeting record). A second request for
/// the same meeting while one is still running gets 409 instead of generating
/// (and filing) the notes twice.
#[utoipa::path(
    post,
    path = "/api/meetings/{id}/finalize",
    tag = "Meetings",
    summary = "stop capture, generate notes, mark done,",
    params(("id" = String, Path)),
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 409, description = "This meeting is already being finalized", body = serde_json::Value),
        (status = 502, description = "Note generation failed or the meeting does not exist", body = serde_json::Value)
    )
)]
pub async fn finalize_meeting(
    State(ctx): State<MeetingsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    finalize_and_save(&ctx.engine, &id).await
}

/// Shared finalize tail: generate notes (model/effort/prompt from prefs), run
/// diarization if enabled, auto-title, and save into the Meetings Space. Used by
/// both the live finalize and the import path.
async fn finalize_and_save(
    engine: &MeetingEngine,
    id: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    // Held until this function returns, so the auto-title and Space filing below
    // are covered too: an overlapping request cannot file a second document.
    let Some(claim) = engine.begin_finalize(id) else {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "meeting is already being finalized" })),
        );
    };
    let model = resolve_notes_model(engine).await;
    let effort = resolve_notes_effort(engine).await;
    let prompt = resolve_notes_prompt(engine).await;
    let mut meeting = match engine.finalize(&claim, &model, &effort, &prompt).await {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_GATEWAY, Json(json!({ "error": e }))),
    };

    // Speaker diarization (opt-in) — label the transcript's segments before the
    // notes are rendered into the Space. Best-effort: a missing sidecar or a
    // disabled toggle just leaves speakers unlabeled.
    diarize_if_enabled(engine, id).await;

    // Auto-name the meeting from its summary with the default local model, unless
    // the user already chose a title. Best-effort; on success update the local
    // copy so the Space document below uses the new title.
    if !meeting.title_custom {
        if let Some(summary) = meeting.notes.as_ref().map(|n| n.summary.clone()) {
            if let Some(new_title) = engine.auto_title(id, &summary).await {
                meeting.title = new_title;
            }
        }
    }

    let filed = save_notes_to_space(engine, &meeting).await;
    let final_meeting = match &filed {
        Some((space_id, doc_id)) => engine
            .attach_space(id, space_id, doc_id)
            .await
            .unwrap_or(meeting),
        None => meeting,
    };

    // The app events fire here rather than inside `MeetingEngine::finalize` because
    // only at this point are the two things a consumer actually needs settled: the
    // auto-generated title (the one the user will see) and whether the notes reached
    // the Space. Both emits are best-effort and cannot fail the finalize.
    //
    // The transcript is deliberately not in the payload — it is unbounded and app-event
    // payloads are size-capped; a consumer reads it from `/api/meetings/{id}/transcript`.
    let notes = final_meeting.notes.as_ref();
    engine
        .events
        .emit_with_activity(
            EVENT_MEETING_ENDED,
            json!({
                "meeting_id": final_meeting.id,
                "title": final_meeting.title,
                "app": final_meeting.app,
                "started_at": final_meeting.started_at,
                "ended_at": final_meeting.ended_at,
                "summary": notes.map(|n| n.summary.as_str()),
                "action_items": notes.map(|n| n.action_items.as_slice()).unwrap_or_default(),
            }),
            crate::notes_ready_activity(&final_meeting),
        )
        .await;
    // Only when the notes really landed in the Space: `notes.ready` promises a document
    // that exists, so emitting it on a failed filing would hand consumers a dead doc id.
    if let Some((space_id, doc_id)) = &filed {
        engine
            .events
            .emit_with_notify(
                EVENT_NOTES_READY,
                json!({
                    "meeting_id": final_meeting.id,
                    "title": final_meeting.title,
                    "space_id": space_id,
                    "doc_id": doc_id,
                }),
                // Notes filed to a Space is exactly the "your long job is done" moment.
                // Core's notification inbox is not sealed, so the notice is generic:
                // the meeting title is sealed content and stays in the event payload.
                Some(crate::notes_ready_notify_hint()),
            )
            .await;
    }

    (StatusCode::OK, Json(json!({ "meeting": final_meeting })))
}

/// Multipart field parse for import; everything but `file` is optional.
//
// Same reasoning as [`ChunkUpload`]: the wire form is `multipart/form-data`, so
// saying so is the only honest schema. Nothing deserialises into this struct.
#[derive(utoipa::ToSchema)]
pub struct ImportUpload {
    /// The audio file to import (WAV v1).
    #[schema(format = Binary)]
    pub file: String,
    /// STT engine override; the configured default is used when absent.
    pub engine: Option<String>,
    /// Meeting title; auto-titled from the transcript when absent or blank.
    pub title: Option<String>,
}

/// `POST /api/meetings/import` — create a meeting from an uploaded audio file
/// (WAV v1), transcribe it window-by-window through the same pipeline as a live
/// recording, then finalize (notes + optional diarization + Space save).
#[utoipa::path(
    post,
    path = "/api/meetings/import",
    tag = "Meetings",
    summary = "create a meeting from an uploaded audio file",
    request_body(content = ImportUpload, content_type = "multipart/form-data"),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn import_meeting(
    State(ctx): State<MeetingsCtx>,
    mut multipart: Multipart,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut audio: Option<Vec<u8>> = None;
    let mut engine: Option<String> = None;
    let mut title = String::new();
    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("file") => {
                if let Ok(bytes) = field.bytes().await {
                    audio = Some(bytes.to_vec());
                }
            }
            Some("engine") => engine = field.text().await.ok().filter(|s| !s.is_empty()),
            Some("title") => title = field.text().await.unwrap_or_default(),
            _ => {}
        }
    }
    let Some(bytes) = audio else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "missing `file` field (the audio to import)" })),
        );
    };

    // WAV-only in v1. Real-world files (mp3/m4a/mov) need an ffmpeg decode step,
    // which is gated/optional — reject clearly rather than mis-transcribing.
    let decoded = match audio::decode_wav(&bytes) {
        Ok(d) => audio::resample_to_16k(&d),
        Err(_) => {
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                Json(json!({
                    "error": "import currently accepts WAV files only; convert mp3/m4a to WAV first"
                })),
            )
        }
    };

    let meeting = match ctx.engine.start_import(title).await {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e })),
            )
        }
    };
    let id = meeting.id.clone();

    // Feed the file through the live-chunk pipeline (transcribe + persist stereo),
    // one 30 s window at a time, with real offsets.
    for (offset_ms, wav) in audio::window_wavs(&decoded, 30) {
        let _ = ctx
            .engine
            .ingest_chunk(
                &id,
                wav,
                "import.wav".to_string(),
                engine.as_deref(),
                Some(offset_ms),
            )
            .await;
    }

    finalize_and_save(&ctx.engine, &id).await
}

/// Write a finalized meeting's notes (+ transcript) into the Meetings Space as a
/// markdown document. Returns `(space_id, doc_id)` on success, `None` on any
/// failure (logged) so finalize stays best-effort.
async fn save_notes_to_space(
    engine: &MeetingEngine,
    meeting: &Meeting,
) -> Option<(String, String)> {
    let notes = meeting.notes.as_ref()?;
    let transcript = engine.transcript(&meeting.id).await.unwrap_or_default();
    let markdown = build_notes_markdown(meeting, notes, &transcript);
    // Finding/creating the Meetings Space, the background owner/tenancy, and the
    // Spaces ingest all stay Core-side behind the host — this crate only produces
    // the document title + markdown.
    engine.save_notes_to_space(&meeting.title, &markdown).await
}

/// Render a meeting's notes + transcript as a markdown document for the Space.
fn build_notes_markdown(meeting: &Meeting, notes: &MeetingNotes, transcript: &str) -> String {
    fn bullets(items: &[String]) -> String {
        if items.is_empty() {
            return "_None_".to_string();
        }
        items
            .iter()
            .map(|i| format!("- {i}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    let subtitle = match &meeting.app {
        Some(app) if !app.is_empty() => format!("{app} · {}", meeting.started_at),
        _ => meeting.started_at.clone(),
    };
    let mut md = format!("# {}\n\n_{subtitle}_\n\n", meeting.title);
    md.push_str(&format!("## Summary\n\n{}\n\n", notes.summary));
    md.push_str(&format!(
        "## Key points\n\n{}\n\n",
        bullets(&notes.key_points)
    ));
    md.push_str(&format!(
        "## Action items\n\n{}\n\n",
        bullets(&notes.action_items)
    ));
    md.push_str(&format!(
        "## Decisions\n\n{}\n\n",
        bullets(&notes.decisions)
    ));
    if !transcript.trim().is_empty() {
        md.push_str(&format!("## Transcript\n\n{transcript}\n"));
    }
    md
}

/// `GET /api/meetings/stream` — SSE feed of meeting events (detected / started /
/// segment / status / finalized).
#[utoipa::path(
    get,
    path = "/api/meetings/stream",
    tag = "Meetings",
    summary = "SSE feed of meeting events (detected / started /",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn meetings_stream(
    State(ctx): State<MeetingsCtx>,
) -> axum::response::sse::Sse<
    impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use tokio::sync::broadcast::error::RecvError;

    let rx = ctx.engine.store.subscribe();
    // Seed the stream with an immediate SSE comment so the FIRST body byte lands at
    // connect, not only when the first meeting event (or the 15s keep-alive) arrives.
    // Meetings is frequently idle for long stretches (no active meeting), so without this
    // seed the stream stays byte-silent until the keep-alive — and any intermediary that
    // withholds the response head behind the first upstream body byte (the ext-proxy's
    // pre-streaming failure mode) reads that as a "no headers for ~15s" hang. A comment
    // line is ignored by `EventSource`, so this is invisible to real consumers. The `true`
    // in the unfold seed is the "emit the priming comment on first poll" flag.
    let stream = futures_util::stream::unfold((rx, true), |(mut rx, first)| async move {
        if first {
            return Some((Ok(Event::default().comment("ready")), (rx, false)));
        }
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let data = serde_json::to_string(&event).unwrap_or_default();
                    return Some((Ok(Event::default().data(data)), (rx, false)));
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{engine_with, FakeHost};
    use crate::{MeetingSource, MeetingStatus};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn ctx_with(host: FakeHost) -> MeetingsCtx {
        MeetingsCtx::new(engine_with(host))
    }

    // ── Capability routes ──────────────────────────────────────────────────

    fn audio_body(id: &str, wav: &[u8]) -> Json<CapabilityAppendAudioBody> {
        use base64::Engine as _;
        Json(CapabilityAppendAudioBody {
            id: id.to_owned(),
            filename: Some("seg.wav".into()),
            audio_base64: base64::engine::general_purpose::STANDARD.encode(wav),
        })
    }

    #[tokio::test]
    async fn capability_routes_resolve_append_count_and_purge() {
        let ctx = ctx_with(FakeHost::default());
        let m = ctx
            .engine
            .start(
                "Ambient".into(),
                Some("ryu-hardware".into()),
                MeetingSource::Auto,
            )
            .await
            .unwrap();

        let (code, Json(found)) = capability_resolve(
            State(ctx.clone()),
            Json(CapabilityResolveBody { id: m.id.clone() }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(found["meeting"]["id"], m.id.as_str());
        let (code, _) = capability_resolve(
            State(ctx.clone()),
            Json(CapabilityResolveBody {
                id: "mtg_gone".into(),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::NOT_FOUND);

        let wav = crate::test_support::stereo_wav_16k(10);
        let (code, Json(appended)) =
            capability_append_audio(State(ctx.clone()), audio_body(&m.id, &wav)).await;
        assert_eq!(code, StatusCode::OK, "{appended}");
        assert!(appended["segment"]["id"].is_number(), "{appended}");
        let (code, _) = capability_append_audio(
            State(ctx.clone()),
            Json(CapabilityAppendAudioBody {
                id: m.id.clone(),
                filename: None,
                audio_base64: "not base64!".into(),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);

        let category = |c: &str| {
            Json(DataCategoryBody {
                category: c.to_owned(),
            })
        };
        let (_, Json(count)) =
            capability_data_count(State(ctx.clone()), category("meetings")).await;
        assert_eq!(count["count"], 1);
        let (code, _) = capability_data_purge(State(ctx.clone()), category("monitors")).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        let (code, Json(purged)) =
            capability_data_purge(State(ctx.clone()), category("meetings")).await;
        assert_eq!(
            (code, purged["removed"].as_u64()),
            (StatusCode::OK, Some(1))
        );
        let (_, Json(count)) = capability_data_count(State(ctx), category("meetings")).await;
        assert_eq!(count["count"], 0);
    }

    /// A silent segment keeps the soft 200 `{ segment: null, skipped }` shape the
    /// hardware path maps to an ambient skip.
    #[tokio::test]
    async fn capability_append_reports_silence_softly() {
        let host = FakeHost {
            transcribe_text: "   ".into(),
            ..FakeHost::default()
        };
        let ctx = ctx_with(host);
        let m = ctx
            .engine
            .start(String::new(), None, MeetingSource::Auto)
            .await
            .unwrap();
        let wav = crate::test_support::stereo_wav_16k(10);
        let (code, Json(reply)) =
            capability_append_audio(State(ctx), audio_body(&m.id, &wav)).await;
        assert_eq!(code, StatusCode::OK);
        assert!(reply["segment"].is_null());
        assert!(reply["skipped"].as_str().unwrap().contains("silence"));
        crate::audio::remove_pcm(&m.id);
    }

    #[tokio::test]
    async fn list_templates_returns_catalog() {
        let ctx = ctx_with(FakeHost::default());
        let Json(v) = list_templates(State(ctx)).await;
        assert!(v.get("templates").and_then(|t| t.as_array()).is_some());
    }

    /// The Store tab renders these as cards, so the listing must carry what a card
    /// draws — a bare id/name pair would render as a blank tile.
    #[tokio::test]
    async fn list_templates_carries_browse_metadata() {
        let ctx = ctx_with(FakeHost::default());
        let Json(v) = list_templates(State(ctx)).await;
        let first = &v["templates"][0];
        for key in ["id", "name", "description", "category", "icon", "tags"] {
            assert!(!first[key].is_null(), "listing is missing '{key}'");
        }
    }

    /// Selecting a template writes the SAME preference the notes pipeline reads, so
    /// the Store's install and the resolved prompt cannot disagree.
    #[tokio::test]
    async fn select_template_sets_the_notes_prompt() {
        let ctx = ctx_with(FakeHost::default());
        let (code, Json(v)) = select_template(
            State(ctx.clone()),
            Json(SelectTemplateBody {
                template_id: "customer_call".into(),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["success"], true);
        assert_eq!(
            ctx.engine.pref_get(NOTES_TEMPLATE_PREF).await.as_deref(),
            Some("customer_call")
        );
        assert_eq!(
            resolve_notes_prompt(&ctx.engine).await,
            templates::prompt_for("customer_call")
        );
    }

    /// The active flag is what the Store card reads for "Added", so it must follow
    /// the preference rather than a client-side guess.
    #[tokio::test]
    async fn selected_template_is_marked_active_in_the_listing() {
        let ctx = ctx_with(FakeHost::default());
        select_template(
            State(ctx.clone()),
            Json(SelectTemplateBody {
                template_id: "retro".into(),
            }),
        )
        .await;
        let Json(v) = list_templates(State(ctx)).await;
        let active: Vec<&str> = v["templates"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["active"] == true)
            .map(|t| t["id"].as_str().unwrap())
            .collect();
        assert_eq!(active, vec!["retro"]);
    }

    /// An unknown id would silently fall back to the default prompt, leaving the UI
    /// claiming a selection that does nothing — so it is refused, not stored.
    #[tokio::test]
    async fn select_template_rejects_an_unknown_id() {
        let ctx = ctx_with(FakeHost::default());
        let (code, Json(v)) = select_template(
            State(ctx.clone()),
            Json(SelectTemplateBody {
                template_id: "not-a-template".into(),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert_eq!(v["success"], false);
        assert!(ctx.engine.pref_get(NOTES_TEMPLATE_PREF).await.is_none());
    }

    #[tokio::test]
    async fn create_and_get_meeting_roundtrip() {
        let ctx = ctx_with(FakeHost::default());
        let body = StartBody {
            title: "Kickoff".into(),
            app: Some("zoom".into()),
            source: Some("auto".into()),
        };
        let (code, Json(v)) = create_meeting(State(ctx.clone()), Json(body)).await;
        assert_eq!(code, StatusCode::OK);
        let id = v["meeting"]["id"].as_str().unwrap().to_string();
        assert_eq!(v["meeting"]["source"], "auto");

        let (code, Json(got)) = get_meeting(State(ctx), Path(id)).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(got["meeting"]["title"], "Kickoff");
    }

    #[tokio::test]
    async fn get_missing_meeting_is_404() {
        let ctx = ctx_with(FakeHost::default());
        let (code, Json(v)) = get_meeting(State(ctx), Path("nope".into())).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        assert_eq!(v["error"], "not found");
    }

    #[tokio::test]
    async fn create_meeting_defaults_source_to_manual() {
        let ctx = ctx_with(FakeHost::default());
        let body = StartBody {
            title: String::new(),
            app: None,
            source: None,
        };
        let (_, Json(v)) = create_meeting(State(ctx), Json(body)).await;
        assert_eq!(v["meeting"]["source"], "manual");
    }

    #[tokio::test]
    async fn rename_rejects_empty_title() {
        let ctx = ctx_with(FakeHost::default());
        let (code, _) = rename_meeting(
            State(ctx),
            Path("x".into()),
            Json(RenameBody {
                title: "   ".into(),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rename_missing_is_404_and_valid_is_200() {
        let ctx = ctx_with(FakeHost::default());
        let (code, _) = rename_meeting(
            State(ctx.clone()),
            Path("missing".into()),
            Json(RenameBody {
                title: "New".into(),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::NOT_FOUND);

        let m = ctx
            .engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let (code, Json(v)) = rename_meeting(
            State(ctx),
            Path(m.id),
            Json(RenameBody {
                title: "Renamed".into(),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["meeting"]["title"], "Renamed");
        assert_eq!(v["meeting"]["title_custom"], true);
    }

    #[tokio::test]
    async fn delete_meeting_ok_then_404() {
        let ctx = ctx_with(FakeHost::default());
        let m = ctx
            .engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let (code, _) = delete_meeting(State(ctx.clone()), Path(m.id.clone())).await;
        assert_eq!(code, StatusCode::OK);
        let (code, _) = delete_meeting(State(ctx), Path(m.id)).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn list_meetings_reports_created() {
        let ctx = ctx_with(FakeHost::default());
        ctx.engine
            .start("m".into(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let Json(v) = list_meetings(State(ctx)).await;
        assert_eq!(v["meetings"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn transcript_endpoint_returns_segments_and_text() {
        let ctx = ctx_with(FakeHost::default());
        let m = ctx
            .engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        ctx.engine
            .store
            .insert_segment(&m.id, 0, None, "hi")
            .await
            .unwrap();
        ctx.engine
            .store
            .insert_segment(&m.id, 5, None, "there")
            .await
            .unwrap();
        let (code, Json(v)) = get_transcript(State(ctx), Path(m.id)).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["text"], "hi\nthere");
        assert_eq!(v["segments"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn detect_respects_master_toggle() {
        let ctx = ctx_with(FakeHost::default());
        ctx.engine
            .pref_set("meeting-detection-enabled", "false")
            .await
            .unwrap();
        let (code, Json(v)) = detect(
            State(ctx),
            Json(DetectBody {
                app: "zoom".into(),
                title: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["broadcast"], false);
        assert_eq!(v["reason"], "detection disabled");
    }

    #[tokio::test]
    async fn detect_ignores_non_meeting_app() {
        let ctx = ctx_with(FakeHost::default());
        let (_, Json(v)) = detect(
            State(ctx),
            Json(DetectBody {
                app: "notepad".into(),
                title: None,
            }),
        )
        .await;
        assert_eq!(v["broadcast"], false);
        assert_eq!(v["reason"], "not a known meeting app");
    }

    #[tokio::test]
    async fn detect_known_app_broadcasts_once_then_debounces() {
        let ctx = ctx_with(FakeHost::default());
        // Substring match against the default app list ("zoom").
        let (_, Json(first)) = detect(
            State(ctx.clone()),
            Json(DetectBody {
                app: "ZoomMtg".into(),
                title: Some("Standup".into()),
            }),
        )
        .await;
        assert_eq!(first["broadcast"], true);
        let (_, Json(second)) = detect(
            State(ctx),
            Json(DetectBody {
                app: "ZoomMtg".into(),
                title: None,
            }),
        )
        .await;
        assert_eq!(second["broadcast"], false);
    }

    #[tokio::test]
    async fn detection_config_defaults_and_updates() {
        let ctx = ctx_with(FakeHost::default());
        // Default: enabled true, non-empty app list.
        let Json(def) = get_detection_config(State(ctx.clone())).await;
        assert_eq!(def["enabled"], true);
        assert!(!def["apps"].as_array().unwrap().is_empty());

        // Update both fields.
        let (code, Json(after)) = put_detection_config(
            State(ctx.clone()),
            Json(DetectionConfigBody {
                enabled: Some(false),
                apps: Some(vec!["custom".into()]),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(after["enabled"], false);
        assert_eq!(after["apps"], json!(["custom"]));

        // Persisted: a fresh read reflects the update.
        let Json(reread) = get_detection_config(State(ctx)).await;
        assert_eq!(reread["enabled"], false);
        assert_eq!(reread["apps"], json!(["custom"]));
    }

    #[tokio::test]
    async fn finalize_empty_transcript_saves_space_when_host_files_it() {
        let mut host = FakeHost::default();
        host.space = Some(("space-1".into(), "doc-1".into()));
        let ctx = ctx_with(host);
        let m = ctx
            .engine
            .start("Fixed title".into(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let (code, Json(v)) = finalize_meeting(State(ctx), Path(m.id)).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["meeting"]["status"], "done");
        assert_eq!(v["meeting"]["space_id"], "space-1");
        assert_eq!(v["meeting"]["doc_id"], "doc-1");
    }

    /// A finalize arriving while another is still running for the same meeting
    /// (double-click, client retry) is refused with 409: no second note generation,
    /// no second Space document, and the meeting is left to the first request.
    #[tokio::test]
    async fn overlapping_finalize_is_conflict_and_files_space_once() {
        let host = FakeHost {
            space: Some(("space-1".into(), "doc-1".into())),
            ..FakeHost::default()
        };
        let space_calls = Arc::clone(&host.space_calls);
        let ctx = ctx_with(host);
        let m = ctx
            .engine
            .start("Fixed title".into(), None, MeetingSource::Manual)
            .await
            .unwrap();

        let in_flight = ctx.engine.begin_finalize(&m.id).expect("first claim");
        let (code, Json(v)) = finalize_meeting(State(ctx.clone()), Path(m.id.clone())).await;
        assert_eq!(code, StatusCode::CONFLICT);
        assert_eq!(v["error"], "meeting is already being finalized");
        assert_eq!(space_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            ctx.engine.get(&m.id).await.unwrap().unwrap().status,
            MeetingStatus::Recording,
            "the refused request must not touch the meeting"
        );

        drop(in_flight);
        let (code, Json(v)) = finalize_meeting(State(ctx.clone()), Path(m.id.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["meeting"]["doc_id"], "doc-1");
        assert_eq!(space_calls.load(Ordering::SeqCst), 1);
        // The request released its claim on the way out.
        assert!(ctx.engine.begin_finalize(&m.id).is_some());
    }

    /// A loopback Gateway whose chat-completions call parks until `release`
    /// fires, so a test can act while a real finalize is mid note generation.
    #[derive(Clone)]
    struct HeldGateway {
        url: String,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        hits: Arc<AtomicUsize>,
    }

    async fn held_completion(State(gateway): State<HeldGateway>) -> Json<serde_json::Value> {
        gateway.hits.fetch_add(1, Ordering::SeqCst);
        gateway.entered.notify_one();
        gateway.release.notified().await;
        Json(json!({
            "choices": [{ "message": { "content": "{\"summary\":\"We shipped the release.\"}" } }]
        }))
    }

    async fn held_gateway() -> HeldGateway {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway = HeldGateway {
            url: format!("http://{}", listener.local_addr().unwrap()),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            hits: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/v1/chat/completions", post(held_completion))
            .with_state(gateway.clone());
        tokio::spawn(async move { axum::serve(listener, app).await });
        gateway
    }

    /// The race end to end, against a real (held) note generation: a second
    /// finalize meanwhile is refused, and a rename made meanwhile survives the
    /// finalize write and keeps the auto-namer off. Notes are generated and filed
    /// exactly once.
    #[tokio::test]
    async fn rename_during_note_generation_survives_finalize() {
        let gateway = held_gateway().await;
        let host = FakeHost {
            gateway_url: gateway.url.clone(),
            title: Some("Auto title".into()),
            space: Some(("space-1".into(), "doc-1".into())),
            ..FakeHost::default()
        };
        let space_calls = Arc::clone(&host.space_calls);
        let ctx = ctx_with(host);
        let m = ctx
            .engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        assert!(!m.title_custom);
        ctx.engine
            .store
            .insert_segment(&m.id, 0, None, "we shipped the release")
            .await
            .unwrap();

        let first = tokio::spawn(finalize_meeting(State(ctx.clone()), Path(m.id.clone())));
        gateway.entered.notified().await;
        let (code, _) = finalize_meeting(State(ctx.clone()), Path(m.id.clone())).await;
        assert_eq!(code, StatusCode::CONFLICT);
        ctx.engine.store.set_title(&m.id, "Mine").await.unwrap();
        gateway.release.notify_one();

        let (code, Json(v)) = first.await.unwrap();
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["meeting"]["title"], "Mine");
        assert_eq!(v["meeting"]["doc_id"], "doc-1");
        let stored = ctx.engine.get(&m.id).await.unwrap().unwrap();
        assert_eq!(stored.title, "Mine");
        assert!(stored.title_custom);
        assert_eq!(stored.status, MeetingStatus::Done);
        assert_eq!(stored.notes.unwrap().summary, "We shipped the release.");
        assert_eq!(gateway.hits.load(Ordering::SeqCst), 1);
        assert_eq!(space_calls.load(Ordering::SeqCst), 1);
    }

    /// A meeting deleted while its notes generate stays deleted: the finalize
    /// fails instead of re-inserting the row, and nothing is filed to the Space.
    #[tokio::test]
    async fn delete_during_note_generation_is_not_undone() {
        let gateway = held_gateway().await;
        let host = FakeHost {
            gateway_url: gateway.url.clone(),
            space: Some(("space-1".into(), "doc-1".into())),
            ..FakeHost::default()
        };
        let space_calls = Arc::clone(&host.space_calls);
        let ctx = ctx_with(host);
        let m = ctx
            .engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        ctx.engine
            .store
            .insert_segment(&m.id, 0, None, "we shipped the release")
            .await
            .unwrap();

        let first = tokio::spawn(finalize_meeting(State(ctx.clone()), Path(m.id.clone())));
        gateway.entered.notified().await;
        assert!(ctx.engine.delete(&m.id).await.unwrap());
        gateway.release.notify_one();

        let (code, Json(v)) = first.await.unwrap();
        assert_eq!(code, StatusCode::BAD_GATEWAY);
        assert!(
            v["error"].as_str().is_some_and(|e| e.contains("deleted")),
            "got: {v}"
        );
        assert!(ctx.engine.get(&m.id).await.unwrap().is_none());
        assert!(ctx.engine.list().await.unwrap().is_empty());
        assert_eq!(space_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn finalize_missing_meeting_is_bad_gateway() {
        let ctx = ctx_with(FakeHost::default());
        let (code, _) = finalize_meeting(State(ctx), Path("nope".into())).await;
        assert_eq!(code, StatusCode::BAD_GATEWAY);
    }

    // ---- private helpers -------------------------------------------------

    #[tokio::test]
    async fn resolve_model_prefers_pref() {
        let ctx = ctx_with(FakeHost::default());
        ctx.engine
            .pref_set("meeting-notes-model", "  pref-model  ")
            .await
            .unwrap();
        assert_eq!(resolve_notes_model(&ctx.engine).await, "pref-model");
    }

    #[tokio::test]
    async fn resolve_prompt_uses_custom_then_template() {
        let ctx = ctx_with(FakeHost::default());
        // A custom prompt wins outright.
        ctx.engine
            .pref_set("meeting-notes-prompt", "custom sys prompt")
            .await
            .unwrap();
        assert_eq!(resolve_notes_prompt(&ctx.engine).await, "custom sys prompt");

        // Cleared prompt ⇒ falls to the selected template.
        ctx.engine
            .pref_set("meeting-notes-prompt", "  ")
            .await
            .unwrap();
        ctx.engine
            .pref_set("meeting-notes-template", "sales")
            .await
            .unwrap();
        let p = resolve_notes_prompt(&ctx.engine).await;
        assert!(p.contains("sales/customer call"));
    }

    #[tokio::test]
    async fn resolve_effort_reads_pref() {
        let ctx = ctx_with(FakeHost::default());
        ctx.engine
            .pref_set("meeting-notes-effort", " high ")
            .await
            .unwrap();
        assert_eq!(resolve_notes_effort(&ctx.engine).await, "high");
    }

    #[tokio::test]
    async fn diarize_if_enabled_noops_when_disabled() {
        let ctx = ctx_with(FakeHost::default());
        // Toggle unset ⇒ returns immediately, no sidecar call, no panic.
        diarize_if_enabled(&ctx.engine, "any-id").await;
    }

    #[tokio::test]
    async fn diarization_model_preference_defaults_and_selects_adapters() {
        let ctx = ctx_with(FakeHost::default());
        assert_eq!(
            resolve_diarization_model(&ctx.engine).await,
            DiarizationModel::Pyannote
        );
        ctx.engine
            .pref_set(DIARIZATION_MODEL_PREF, "nemotron")
            .await
            .unwrap();
        assert_eq!(
            resolve_diarization_model(&ctx.engine).await,
            DiarizationModel::Nemotron
        );
        ctx.engine
            .pref_set(DIARIZATION_MODEL_PREF, "unknown")
            .await
            .unwrap();
        assert_eq!(
            resolve_diarization_model(&ctx.engine).await,
            DiarizationModel::Pyannote
        );
    }

    #[test]
    fn build_notes_markdown_renders_sections_and_empty_bullets() {
        let meeting = Meeting {
            id: "m1".into(),
            title: "Weekly".into(),
            title_custom: false,
            icon: None,
            app: Some("zoom".into()),
            source: MeetingSource::Manual,
            status: crate::MeetingStatus::Done,
            started_at: "2026-01-01T00:00:00Z".into(),
            ended_at: None,
            participants: vec![],
            notes: None,
            space_id: None,
            doc_id: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };
        let notes = MeetingNotes {
            summary: "We shipped.".into(),
            key_points: vec!["point a".into()],
            action_items: vec![],
            decisions: vec!["ship it".into()],
            generated_at: String::new(),
            model: String::new(),
        };
        let md = build_notes_markdown(&meeting, &notes, "line one\nline two");
        assert!(md.starts_with("# Weekly\n"));
        assert!(md.contains("_zoom · 2026-01-01T00:00:00Z_"));
        assert!(md.contains("## Summary\n\nWe shipped."));
        assert!(md.contains("- point a"));
        assert!(md.contains("_None_"), "empty action items render _None_");
        assert!(md.contains("- ship it"));
        assert!(md.contains("## Transcript\n\nline one\nline two"));
    }

    #[test]
    fn build_notes_markdown_omits_empty_transcript_and_app() {
        let meeting = Meeting {
            id: "m2".into(),
            title: "No app".into(),
            title_custom: false,
            icon: None,
            app: None,
            source: MeetingSource::Manual,
            status: crate::MeetingStatus::Done,
            started_at: "2026-02-02T00:00:00Z".into(),
            ended_at: None,
            participants: vec![],
            notes: None,
            space_id: None,
            doc_id: None,
            created_at: "2026-02-02T00:00:00Z".into(),
            updated_at: "2026-02-02T00:00:00Z".into(),
        };
        let notes = MeetingNotes::default();
        let md = build_notes_markdown(&meeting, &notes, "   ");
        assert!(md.contains("_2026-02-02T00:00:00Z_"));
        assert!(!md.contains("## Transcript"));
    }

    // ── OpenAPI document ───────────────────────────────────────────────────────

    /// This app's own manifest, read at compile time. The route contract lives there,
    /// so the invariants below compare the document against the real declaration
    /// rather than against a second list that could drift from it.
    fn openapi_manifest() -> serde_json::Value {
        serde_json::from_str(include_str!("../../manifest.json")).expect("valid JSON")
    }

    /// The manifest sidecar whose HTTP surface this router serves: the one that
    /// declares an `http.mount`. Selected BY mount rather than by index because an app
    /// may declare a second, mountless sidecar (finetune already does), and
    /// `sidecars[0]` would then quietly start asserting against the wrong process.
    fn mounted_sidecar() -> serde_json::Value {
        openapi_manifest()["sidecars"]
            .as_array()
            .expect("sidecars must be an array")
            .iter()
            .find(|s| s["http"]["mount"].is_string())
            .expect("one sidecar must declare an http.mount")
            .clone()
    }

    /// A manifest route (relative to the mount, in axum's `:param` form) rewritten
    /// into the form the OpenAPI document uses (absolute, in `{param}` form).
    ///
    /// The two forms differ ON PURPOSE — the router registers paths relative to the
    /// mount because Core nests it there, while the `#[utoipa::path]` annotations carry
    /// the absolute EXTERNAL path a caller actually hits. Normalise here; do not
    /// "align" either side.
    fn doc_path_for(mount: &str, route: &str) -> String {
        let joined = if route == "/" {
            mount.to_owned()
        } else {
            format!("{mount}{route}")
        };
        joined
            .split('/')
            .map(|seg| match seg.strip_prefix(':') {
                Some(name) => format!("{{{name}}}"),
                None => seg.to_owned(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    #[test]
    fn openapi_doc_is_served_and_non_empty() {
        // The doc is no longer dead code: Core fetches it to derive tools.
        assert!(!super::openapi().paths.paths.is_empty());
    }

    #[test]
    fn every_declared_route_appears_in_the_openapi_doc() {
        // The direction that decides tool yield. Core's `ext_api::lower` keeps only the
        // document operations the manifest ALSO declares, so a declared route with no
        // `#[utoipa::path]` annotation is a tool that silently never exists — nothing
        // errors, the agent simply cannot call it. (The other direction is harmless: an
        // annotated path the manifest does not declare is dropped by the same filter.)
        let sidecar = mounted_sidecar();
        let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
        let doc = super::openapi();
        for route in sidecar["http"]["routes"]
            .as_array()
            .expect("routes must be an array")
        {
            let path = route["path"].as_str().expect("a route path");
            let expected = doc_path_for(mount, path);
            if super::CAPABILITY_ONLY_ROUTES.contains(&path) {
                continue;
            }
            assert!(
                doc.paths.paths.contains_key(&expected),
                "'{path}' is declared in manifest.json but the OpenAPI document has no \
                 '{expected}' operation — Core derives no tool for it"
            );
        }
    }

    /// Every `provides` row points at a declared POST route, every capability-only
    /// route backs a row and stays out of the OpenAPI document, and the set is the
    /// one Core consumes.
    #[test]
    fn provided_capabilities_point_at_declared_post_routes() {
        let manifest = openapi_manifest();
        let sidecar = mounted_sidecar();
        let declared = sidecar["http"]["routes"].as_array().expect("routes");
        let mut names: Vec<&str> = Vec::new();
        let mut provided_routes: Vec<&str> = Vec::new();
        for entry in manifest["provides"].as_array().expect("provides") {
            names.push(entry["capability"].as_str().expect("a capability"));
            assert_eq!(entry["sidecar"], sidecar["name"], "{entry}");
            let route = entry["route"].as_str().expect("a route");
            provided_routes.push(route);
            assert!(
                declared
                    .iter()
                    .any(|r| r["path"] == route && r["method"] == "POST"),
                "'{route}' is not a declared POST route"
            );
            assert!(entry["grant"]
                .as_str()
                .expect("a grant")
                .starts_with("meetings:"));
        }
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "data.count",
                "data.purge",
                "meetings.appendAudio",
                "meetings.get",
                "meetings.start",
            ]
        );
        let doc = super::openapi();
        let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
        for route in super::CAPABILITY_ONLY_ROUTES {
            assert!(
                provided_routes.contains(route),
                "'{route}' backs no capability"
            );
            assert!(
                !doc.paths.paths.contains_key(&doc_path_for(mount, route)),
                "'{route}' is capability plumbing and must not become an agent tool"
            );
        }
        let categories = manifest["contributes"]["data_categories"]
            .as_array()
            .expect("data categories");
        assert!(categories.iter().any(|c| c["id"] == super::DATA_CATEGORY));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // OpenAPI → LLM tool derivation
    //
    // Core derives an LLM tool per route from this document (fetched over
    // loopback at `/openapi.json`), and the tool's ARGUMENTS come from the
    // operation's `requestBody` schema. Every annotation here used to say
    // `request_body = serde_json::Value`, which serialises to `{}` — so every
    // write route reached the model as a tool it could see and could not call.
    // These tests are the guard: they fail if a body type stops being described.
    // ─────────────────────────────────────────────────────────────────────────

    /// The JSON body schema documented at `route`/`method`, by pointer (`/` is
    /// `~1` inside a pointer segment).
    fn body_schema(doc: &serde_json::Value, route: &str, method: &str) -> serde_json::Value {
        let pointer = format!(
            "/paths/{}/{method}/requestBody/content/application~1json/schema",
            route.replace('~', "~0").replace('/', "~1")
        );
        doc.pointer(&pointer)
            .unwrap_or_else(|| panic!("no JSON request body documented at {route} {method}"))
            .clone()
    }

    #[test]
    fn post_routes_document_their_request_body() {
        let doc = serde_json::to_value(openapi()).unwrap();
        for (route, method) in [
            ("/api/meetings", "post"),
            ("/api/meetings/{id}/title", "post"),
            ("/api/meetings/{id}/icon", "post"),
            ("/api/meetings/detect", "post"),
            ("/api/meetings/templates/select", "post"),
            ("/api/meetings/detection-config", "put"),
        ] {
            let schema = body_schema(&doc, route, method);
            // A `$ref` is correct and expected — Core resolves it against
            // `components.schemas` on import.
            assert!(
                schema.get("$ref").is_some() || schema.get("properties").is_some(),
                "a derived write tool for {route} {method} would have no arguments: {schema}"
            );
        }
    }

    /// The assertion above is necessary but not sufficient: a `$ref` to a type
    /// that was never registered looks identical in the operation and still
    /// yields zero arguments once Core tries to resolve it. Walks every content
    /// type, so the multipart uploads are held to the same bar.
    #[test]
    fn every_request_body_ref_resolves_against_components() {
        let doc = serde_json::to_value(openapi()).unwrap();
        let schemas = &doc["components"]["schemas"];
        for (route, methods) in doc["paths"].as_object().expect("paths") {
            for (method, op) in methods.as_object().expect("operations") {
                let Some(content) = op.pointer("/requestBody/content") else {
                    continue;
                };
                for (media, entry) in content.as_object().expect("content map") {
                    let schema = &entry["schema"];
                    let Some(reference) = schema.get("$ref").and_then(serde_json::Value::as_str)
                    else {
                        assert!(
                            schema.get("properties").is_some(),
                            "{route} {method} ({media}) documents a body with neither a \
                             $ref nor properties — the model sees no arguments: {schema}"
                        );
                        continue;
                    };
                    let name = reference
                        .strip_prefix("#/components/schemas/")
                        .unwrap_or_else(|| panic!("{route} {method} points outside this document"));
                    assert!(
                        schemas[name].get("properties").is_some(),
                        "{route} {method} refs '{name}', which is missing from \
                         components(schemas(...)) or carries no properties"
                    );
                }
            }
        }
    }

    /// The two upload routes take `multipart/form-data`, not JSON. Saying
    /// `application/json` there was a lie in both directions: the request would
    /// 400, and Core (which reads only the JSON content) would derive a tool with
    /// no arguments anyway. Documented honestly, the route contributes no LLM
    /// arguments — the right answer for a binary upload no model can send.
    #[test]
    fn upload_routes_declare_multipart_not_json() {
        let doc = serde_json::to_value(openapi()).unwrap();
        for (route, schema_name) in [
            ("/api/meetings/{id}/chunk", "ChunkUpload"),
            ("/api/meetings/import", "ImportUpload"),
        ] {
            let content = &doc["paths"][route]["post"]["requestBody"]["content"];
            assert!(
                content.get("application/json").is_none(),
                "{route} still claims a JSON body it would reject"
            );
            assert_eq!(
                content["multipart/form-data"]["schema"]["$ref"],
                serde_json::json!(format!("#/components/schemas/{schema_name}")),
                "{route} lost its multipart form description"
            );
            assert_eq!(
                doc["components"]["schemas"][schema_name]["properties"]["file"]["format"], "binary",
                "{schema_name} no longer describes its file field as binary"
            );
        }
    }

    /// The payoff of typing the bodies: a `///` on a field becomes the argument
    /// description the model actually reads.
    #[test]
    fn body_field_docs_reach_the_schema_as_argument_descriptions() {
        let doc = serde_json::to_value(openapi()).unwrap();
        let app = &doc["components"]["schemas"]["DetectBody"]["properties"]["app"];
        assert_eq!(
            app["description"],
            "The owning process / app slug (e.g. `zoom`)."
        );
        let icon = &doc["components"]["schemas"]["SetIconBody"]["properties"]["icon"];
        assert!(
            icon["description"]
                .as_str()
                .is_some_and(|d| d.contains("null` to clear")),
            "SetIconBody.icon lost its description: {icon}"
        );
    }

    /// A handler with no body extractor must declare no body at all — a
    /// `request_body` there is a lie that makes the model send one. Its `id`
    /// argument still has to survive, and that comes from `params(...)`.
    #[test]
    fn body_less_routes_declare_no_request_body() {
        let doc = serde_json::to_value(openapi()).unwrap();
        let op = &doc["paths"]["/api/meetings/{id}/finalize"]["post"];
        assert!(
            op.is_object(),
            "the finalize route left the document entirely"
        );
        assert!(
            op.get("requestBody").is_none(),
            "finalize documents a body its handler never reads"
        );
        assert_eq!(
            op["parameters"][0]["name"], "id",
            "finalize lost its id argument: {op}"
        );
    }
}
