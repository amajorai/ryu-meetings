//! Capability-only routes (`meetings.get`, `meetings.appendAudio`, `data.*`).

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::Deserialize;
use serde_json::json;

use super::{get_meeting, MeetingsCtx, DATA_CATEGORY};
use crate::recap::caller;

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
    if let Err(refused) = super::refuse_recap(&ctx, &body.id).await {
        return refused;
    }
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

/// Body of the `data.count` / `data.purge` capabilities. `user_id` scopes the
/// call to one member's recaps (account deletion, member removal). Without it the
/// call covers the shared (legacy) meetings plus the caller's own recaps — never
/// another member's.
#[derive(Debug, Deserialize)]
pub struct DataCategoryBody {
    pub category: String,
    #[serde(default)]
    pub user_id: Option<String>,
}

/// What a data call covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataScope {
    /// Include the shared (legacy, ownerless) meetings.
    pub legacy: bool,
    /// The recap owner covered, if any.
    pub recap_owner: Option<String>,
}

/// Why a data call was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataScopeError {
    /// Malformed caller headers.
    TenantRequired,
    /// A member named someone else's `user_id`.
    OtherMember,
}

/// Resolve what a data call may touch.
///
/// - A verified member (Core stamped `x-ryu-caller-user-id`: a member reaching
///   the route through the ext-proxy) covers only themself. A body `user_id` that
///   names anyone else is refused; naming themself drops the shared meetings.
/// - A headerless call is Core itself (its capability fan-out for account
///   deletion or member removal, or the node owner's node-bearer request), so a
///   body `user_id` is honoured. Without one it covers the shared meetings plus,
///   on a local node, the local owner's recaps.
pub fn data_scope(
    headers: &HeaderMap,
    managed_node: bool,
    body: &DataCategoryBody,
) -> Result<DataScope, DataScopeError> {
    let named = body
        .user_id
        .as_deref()
        .map(str::trim)
        .filter(|user| !user.is_empty());
    let member = match caller::header_present(headers) {
        true => Some(
            caller::resolve_caller(headers, managed_node)
                .map_err(|_| DataScopeError::TenantRequired)?
                .user_id,
        ),
        false => None,
    };
    match (member, named) {
        (Some(member), Some(named)) if named != member => Err(DataScopeError::OtherMember),
        (Some(member), Some(_)) => Ok(DataScope {
            legacy: false,
            recap_owner: Some(member),
        }),
        (Some(member), None) => Ok(DataScope {
            legacy: true,
            recap_owner: Some(member),
        }),
        (None, Some(named)) => Ok(DataScope {
            legacy: false,
            recap_owner: Some(named.to_string()),
        }),
        (None, None) => Ok(DataScope {
            legacy: true,
            recap_owner: (!managed_node).then(|| caller::LOCAL_OWNER.to_string()),
        }),
    }
}

fn refused(error: DataScopeError) -> (StatusCode, Json<serde_json::Value>) {
    match error {
        DataScopeError::TenantRequired => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "tenant_required" })),
        ),
        DataScopeError::OtherMember => (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "other_member" })),
        ),
    }
}

fn internal(e: impl ToString) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": e.to_string() })),
    )
}

fn unknown_category(category: &str) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("unknown data category '{category}'") })),
    )
}

/// `POST /api/meetings/data/count` (`data.count`) — `{ count }` for the
/// `meetings` category, within the caller's [`data_scope`].
pub async fn capability_data_count(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Json(body): Json<DataCategoryBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if body.category != DATA_CATEGORY {
        return unknown_category(&body.category);
    }
    let scope = match data_scope(&headers, ctx.managed_node, &body) {
        Ok(scope) => scope,
        Err(error) => return refused(error),
    };
    let legacy = if scope.legacy {
        match ctx.engine.list().await {
            Ok(meetings) => meetings.len(),
            Err(e) => return internal(e),
        }
    } else {
        0
    };
    let recaps = match scope.recap_owner {
        Some(owner) => match ctx
            .engine
            .store
            .list_owned_recaps(&owner, None, None, None, usize::MAX >> 1)
            .await
        {
            Ok(recaps) => recaps.len(),
            Err(e) => return internal(e),
        },
        None => 0,
    };
    (StatusCode::OK, Json(json!({ "count": legacy + recaps })))
}

/// `POST /api/meetings/data/purge` (`data.purge`) — delete the covered meetings
/// through the same per-item path as `DELETE /api/meetings/:id` (so each delete is
/// broadcast on the meetings stream), then the covered member's recaps and recap
/// data, and answer `{ removed }`. Scoped by [`data_scope`].
pub async fn capability_data_purge(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Json(body): Json<DataCategoryBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if body.category != DATA_CATEGORY {
        return unknown_category(&body.category);
    }
    let scope = match data_scope(&headers, ctx.managed_node, &body) {
        Ok(scope) => scope,
        Err(error) => return refused(error),
    };
    let mut removed = 0u64;
    if scope.legacy {
        let meetings = match ctx.engine.list().await {
            Ok(meetings) => meetings,
            Err(e) => return internal(e),
        };
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
    }
    if let Some(owner) = scope.recap_owner {
        match crate::recap::records::purge_owner(&ctx.engine, &owner).await {
            Ok(n) => removed += n,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "recap purge failed", "removed": removed })),
                );
            }
        }
    }
    (StatusCode::OK, Json(json!({ "removed": removed })))
}
