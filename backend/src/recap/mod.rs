//! Recaps: user-started, visibly indicated conversation sessions captured by a
//! phone or watch client, uploaded as audio chunks, transcribed here, redacted,
//! summarized, and kept owner scoped.
//!
//! A recap is a `meetings` row with `kind = recap` and a [`crate::RecapMeta`].
//! It never appears on the legacy meeting routes, the shared `/stream`, the
//! capability routes, or the OpenAPI tool surface ([`TOOL_EXCLUDED_PREFIX`]).
//! Every route resolves the caller from Core's verified headers
//! ([`caller::resolve_caller`]) and answers 404 for another member's recap.
//!
//! Capture is gated twice: the node-wide rollout preference
//! [`settings::NODE_ENABLED_PREF`] (default off) and the caller's current consent
//! record. Reading, exporting and deleting stay available when capture is off.

pub mod caller;
pub mod decode;
pub mod digest;
pub mod events;
pub mod finalize;
pub mod ingest;
pub mod interim;
pub mod limits;
pub mod processors;
pub mod prompts;
pub mod records;
pub mod redact;
pub mod retention;
pub mod search;
pub mod settings;
pub mod start;
pub mod tick;

#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::{
    extract::DefaultBodyLimit,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use serde_json::{json, Value};

pub use caller::Caller;

use crate::api::MeetingsCtx;

/// Every recap route lives under this prefix; none of them is annotated for the
/// OpenAPI document, so Core derives no agent tool from them until the caller's
/// principal reaches derived tool calls.
pub const TOOL_EXCLUDED_PREFIX: &str = "/recaps";

/// The axum body limit on the chunk route: a little above [`limits::MAX_CHUNK_BYTES`]
/// so an oversized chunk gets the typed 413 from the handler. Route-local, because
/// the sidecar-wide proxy body limit also caps file import.
pub const CHUNK_BODY_LIMIT: usize = 2_621_440;

/// The `/recaps` router (paths relative to the meetings mount).
pub fn routes() -> Router<MeetingsCtx> {
    Router::new()
        .route(
            "/recaps",
            get(records::list)
                .post(start::start)
                .delete(records::delete_all),
        )
        .route(
            "/recaps/settings",
            get(settings::get_settings).put(settings::put_settings),
        )
        .route("/recaps/availability", put(settings::put_availability))
        .route(
            "/recaps/consent",
            post(settings::record_consent).delete(settings::withdraw_consent),
        )
        .route("/recaps/daily", get(digest::get_daily))
        .route("/recaps/daily/regenerate", post(digest::regenerate))
        .route("/recaps/search", post(search::search))
        .route("/recaps/export", get(records::export))
        .route(
            "/recaps/:id/detail",
            get(records::get_one)
                .patch(records::patch)
                .delete(records::delete_one),
        )
        .route("/recaps/:id/confirm", post(start::confirm))
        .route("/recaps/:id/chunks", get(ingest::chunk_status))
        .route(
            "/recaps/:id/chunks/:seq",
            put(ingest::put_chunk).layer(DefaultBodyLimit::max(CHUNK_BODY_LIMIT)),
        )
        .route("/recaps/:id/finalize", post(finalize::finalize))
        .route("/recaps/:id/events", get(events::recap_events))
        .route("/recaps/:id/interim", get(interim::get_interim))
}

/// A typed recap error. Bodies carry a stable machine `error` code plus details;
/// they never echo recap content.
#[derive(Debug)]
pub enum RecapError {
    /// No verified caller on a managed node.
    TenantRequired,
    /// The node has not enabled recaps (rollout preference off).
    Disabled,
    /// The caller may not do this (a stable machine code, e.g. `admin_required`).
    Forbidden(&'static str),
    NotFound,
    BadRequest(String),
    Conflict(&'static str, Value),
    PayloadTooLarge,
    Unsupported(String),
    TooMany(&'static str),
    Upstream(String),
    Internal(String),
}

impl RecapError {
    /// A bare conflict with no extra fields.
    pub fn conflict(code: &'static str) -> Self {
        Self::Conflict(code, json!({}))
    }
}

impl From<anyhow::Error> for RecapError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error.to_string())
    }
}

impl IntoResponse for RecapError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            Self::TenantRequired => (
                StatusCode::UNAUTHORIZED,
                json!({ "error": "tenant_required" }),
            ),
            Self::Disabled => (StatusCode::FORBIDDEN, json!({ "error": "recaps_disabled" })),
            Self::Forbidden(code) => (StatusCode::FORBIDDEN, json!({ "error": code })),
            Self::NotFound => (StatusCode::NOT_FOUND, json!({ "error": "not_found" })),
            Self::BadRequest(detail) => (
                StatusCode::BAD_REQUEST,
                json!({ "error": "bad_request", "detail": detail }),
            ),
            Self::Conflict(code, extra) => {
                let mut body = json!({ "error": code });
                if let (Some(target), Value::Object(extra)) = (body.as_object_mut(), extra) {
                    target.extend(extra);
                }
                (StatusCode::CONFLICT, body)
            }
            Self::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({ "error": "chunk_too_large", "max_chunk_bytes": limits::MAX_CHUNK_BYTES }),
            ),
            Self::Unsupported(detail) => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                json!({ "error": "unsupported_media", "detail": detail }),
            ),
            Self::TooMany(code) => (StatusCode::TOO_MANY_REQUESTS, json!({ "error": code })),
            Self::Upstream(detail) => {
                tracing::warn!("recaps: upstream failure: {detail}");
                (
                    StatusCode::BAD_GATEWAY,
                    json!({ "error": "upstream_failed" }),
                )
            }
            Self::Internal(detail) => {
                tracing::warn!("recaps: internal failure: {detail}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({ "error": "internal" }),
                )
            }
        };
        (status, Json(body)).into_response()
    }
}

pub type RecapResult<T> = Result<T, RecapError>;

/// Per-process recap state shared by every handler: the upload and interim rate
/// limiters and the request-entry sweep throttle. Cheap to clone.
#[derive(Clone, Default)]
pub struct RecapRuntime {
    pub(crate) limiter: limits::RateLimiter,
    pub(crate) interim_limiter: limits::RateLimiter,
    last_sweep: Arc<Mutex<Option<Instant>>>,
}

impl RecapRuntime {
    /// Whether a request-entry sweep is due (at most one a minute), claiming it.
    fn claim_sweep(&self) -> bool {
        let mut last = self
            .last_sweep
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let due = last.is_none_or(|at| at.elapsed() >= Duration::from_secs(60));
        if due {
            *last = Some(Instant::now());
        }
        due
    }
}

/// Resolve the caller for any recap route and run the throttled background
/// retention sweep. Read, export and delete routes call only this.
pub async fn enter(ctx: &MeetingsCtx, headers: &HeaderMap) -> RecapResult<Caller> {
    let caller = caller::resolve_caller(headers, ctx.managed_node)
        .map_err(|_| RecapError::TenantRequired)?;
    if ctx.recaps.claim_sweep() {
        let engine = ctx.engine.clone();
        tokio::spawn(async move {
            retention::sweep(&engine, chrono::Utc::now()).await;
        });
    }
    Ok(caller)
}

/// [`enter`] plus the node rollout gate, for routes that capture or process audio.
pub async fn enter_capture(ctx: &MeetingsCtx, headers: &HeaderMap) -> RecapResult<Caller> {
    let caller = enter(ctx, headers).await?;
    if !settings::node_enabled(&ctx.engine).await {
        return Err(RecapError::Disabled);
    }
    Ok(caller)
}
