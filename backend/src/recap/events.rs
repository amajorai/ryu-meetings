//! Recap app events (`recap.started`, `recap.ready`, `recap.digest.ready`) and the
//! owner-scoped SSE stream `GET /recaps/:id/events`.
//!
//! Event payloads carry ids, the local day and counts only: hook consumers and
//! workflows run node-wide, so recap titles and notes never ride in a payload.
//! No activity-feed row is raised for a recap, for the same reason.

use std::convert::Infallible;

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::Stream;
use serde_json::json;
use tokio::sync::broadcast::error::RecvError;

use super::{enter, Caller, RecapError, RecapResult};
use crate::api::MeetingsCtx;
use crate::fanout_messages::{digest_ready_notify_hint, recap_ready_notify_hint};
use crate::{Meeting, MeetingEngine};

pub const EVENT_RECAP_STARTED: &str = "@ryu/meetings#recap.started";
pub const EVENT_RECAP_READY: &str = "@ryu/meetings#recap.ready";
pub const EVENT_DIGEST_READY: &str = "@ryu/meetings#recap.digest.ready";

fn owner_of(recap: &Meeting) -> Option<&str> {
    recap.recap.as_ref().map(|m| m.owner_user_id.as_str())
}

/// The `recap.started` payload.
pub fn started_payload(recap: &Meeting) -> serde_json::Value {
    let meta = recap.recap.as_ref();
    json!({
        "recap_id": recap.id,
        "source": recap.source,
        "local_day": meta.map(|m| m.local_day.as_str()),
        "started_at": recap.started_at,
    })
}

/// The `recap.ready` payload.
pub fn ready_payload(recap: &Meeting) -> serde_json::Value {
    let meta = recap.recap.as_ref();
    json!({
        "recap_id": recap.id,
        "local_day": meta.map(|m| m.local_day.as_str()),
        "duration_ms": meta.map_or(0, |m| m.duration_ms),
        "partial": meta.is_some_and(|m| m.partial),
        "ended_at": recap.ended_at,
    })
}

pub async fn emit_started(engine: &MeetingEngine, recap: &Meeting) {
    engine
        .events
        .emit(EVENT_RECAP_STARTED, started_payload(recap))
        .await;
}

/// The `recap.ready` notice: addressed to the owner, or none at all when the
/// owner has no concrete user id (never the node's active account).
pub fn ready_hint(recap: &Meeting) -> Option<ryu_app_events::NotifyHint> {
    owner_of(recap)
        .and_then(Caller::notify_target)
        .map(recap_ready_notify_hint)
}

/// The digest notice, when the owner wants it and has a concrete user id.
pub fn digest_hint(owner: &str, count: i64, notify: bool) -> Option<ryu_app_events::NotifyHint> {
    if !notify {
        return None;
    }
    Caller::notify_target(owner).map(|target| digest_ready_notify_hint(target, count))
}

/// `recap.ready` with the owner-addressed generic notice.
pub async fn emit_ready(engine: &MeetingEngine, recap: &Meeting) {
    engine
        .events
        .emit_with_notify(EVENT_RECAP_READY, ready_payload(recap), ready_hint(recap))
        .await;
}

/// `recap.digest.ready`; the notice is sent only when the owner wants it.
pub async fn emit_digest_ready(
    engine: &MeetingEngine,
    owner: &str,
    day: &str,
    count: i64,
    notify: bool,
) {
    engine
        .events
        .emit_with_notify(
            EVENT_DIGEST_READY,
            json!({ "day": day, "count": count }),
            digest_hint(owner, count, notify),
        )
        .await;
}

/// `GET /recaps/:id/events` — status and segment events for ONE recap, after the
/// owner check. The shared `/stream` never carries these.
pub async fn recap_events(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> RecapResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let caller = enter(&ctx, &headers).await?;
    ctx.engine
        .store
        .get_owned_recap(&caller.user_id, &id)
        .await?
        .ok_or(RecapError::NotFound)?;
    let rx = ctx.engine.store.subscribe_recaps();
    let stream = futures_util::stream::unfold((rx, id, true), |(mut rx, id, first)| async move {
        if first {
            return Some((Ok(Event::default().comment("ready")), (rx, id, false)));
        }
        loop {
            match rx.recv().await {
                Ok((recap_id, event)) if recap_id == id => {
                    let data = serde_json::to_string(&event).unwrap_or_default();
                    return Some((Ok(Event::default().data(data)), (rx, id, false)));
                }
                Ok(_) | Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
