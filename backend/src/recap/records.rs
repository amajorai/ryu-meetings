//! Owner-scoped recap records: list, read, edit, delete, delete-all and export.

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{digest, enter, RecapError, RecapResult};
use crate::api::MeetingsCtx;
use crate::{Meeting, MeetingEngine, MeetingNotes};

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 100;
/// Rows an export may hold.
const MAX_EXPORT: usize = 10_000;

/// `GET /recaps` query.
#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub format: Option<String>,
}

fn valid_day(day: Option<&str>) -> RecapResult<()> {
    match day {
        Some(day) if chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d").is_err() => {
            Err(RecapError::BadRequest("dates must be YYYY-MM-DD".into()))
        }
        _ => Ok(()),
    }
}

/// `GET /recaps` — the caller's recaps, newest first.
pub async fn list(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    valid_day(query.from.as_deref())?;
    valid_day(query.to.as_deref())?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let recaps = ctx
        .engine
        .store
        .list_owned_recaps(
            &caller.user_id,
            query.from.as_deref(),
            query.to.as_deref(),
            query.cursor.as_deref(),
            limit,
        )
        .await?;
    let next_cursor = (recaps.len() == limit)
        .then(|| recaps.last().map(|r| r.created_at.clone()))
        .flatten();
    Ok(Json(
        json!({ "recaps": recaps, "next_cursor": next_cursor }),
    ))
}

async fn owned(ctx: &MeetingsCtx, owner: &str, id: &str) -> RecapResult<Meeting> {
    ctx.engine
        .store
        .get_owned_recap(owner, id)
        .await?
        .ok_or(RecapError::NotFound)
}

/// `GET /recaps/:id/detail` — the recap with its transcript (while kept).
pub async fn get_one(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let recap = owned(&ctx, &caller.user_id, &id).await?;
    let transcript: Vec<Value> = ctx
        .engine
        .store
        .recap_segments(&id)
        .await?
        .into_iter()
        .map(|s| json!({ "t_offset_ms": s.t_offset_ms, "text": s.text }))
        .collect();
    Ok(Json(json!({ "recap": recap, "transcript": transcript })))
}

/// `PATCH /recaps/:id/detail` body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchBody {
    pub title: Option<String>,
    pub pinned: Option<bool>,
    pub notes: Option<MeetingNotes>,
}

/// `PATCH /recaps/:id/detail` — rename, keep (pin), or edit notes. A day digest
/// that exists is rebuilt so it never shows the old text.
pub async fn patch(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<PatchBody>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let recap = owned(&ctx, &caller.user_id, &id).await?;
    let title = match body.title.as_deref().map(str::trim) {
        Some("") => return Err(RecapError::BadRequest("title must not be empty".into())),
        Some(title) if title.chars().count() > 200 => {
            return Err(RecapError::BadRequest("title is too long".into()))
        }
        other => other.map(str::to_string),
    };
    let now = chrono::Utc::now().to_rfc3339();
    let updated = ctx
        .engine
        .store
        .mutate_recap(&id, |m| {
            if let Some(title) = title {
                m.title = title;
                m.title_custom = true;
            }
            if let (Some(pinned), Some(meta)) = (body.pinned, m.recap.as_mut()) {
                meta.pinned = pinned;
            }
            if let Some(notes) = body.notes {
                m.notes = Some(notes);
            }
            m.updated_at = now;
            true
        })
        .await?
        .ok_or(RecapError::NotFound)?;
    if let Some(meta) = &recap.recap {
        digest::refresh_if_present(&ctx.engine, &caller.user_id, &meta.local_day).await;
    }
    Ok(Json(json!({ "recap": updated })))
}

/// Delete one recap with everything derived from it, then rebuild (or drop) the
/// day's digest so deleted text cannot linger there.
pub async fn delete_recap(engine: &MeetingEngine, recap: &Meeting) -> RecapResult<bool> {
    let removed = engine.store.delete_recap_rows(&recap.id).await?;
    engine.interim.clear(&recap.id);
    if let Some(meta) = &recap.recap {
        digest::refresh_if_present(engine, &meta.owner_user_id, &meta.local_day).await;
    }
    Ok(removed)
}

/// `DELETE /recaps/:id/detail`.
pub async fn delete_one(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let recap = owned(&ctx, &caller.user_id, &id).await?;
    delete_recap(&ctx.engine, &recap).await?;
    Ok(Json(json!({ "ok": true })))
}

/// Delete every recap row the owner has (with segments and chunk ledgers).
async fn delete_owned_recaps(engine: &MeetingEngine, owner: &str) -> RecapResult<u64> {
    let recaps = engine
        .store
        .list_owned_recaps(owner, None, None, None, usize::MAX >> 1)
        .await?;
    let mut removed = 0;
    for recap in recaps {
        engine.interim.clear(&recap.id);
        if engine.store.delete_recap_rows(&recap.id).await? {
            removed += 1;
        }
    }
    Ok(removed)
}

/// Delete every recap the owner has, plus their digests, settings, consent and
/// usage: the `data.purge` path (account deletion, member removal).
pub async fn purge_owner(engine: &MeetingEngine, owner: &str) -> RecapResult<u64> {
    let removed = delete_owned_recaps(engine, owner).await?;
    engine.interim.clear_owner(owner);
    engine.store.purge_recap_side_rows(owner).await?;
    Ok(removed)
}

/// `DELETE /recaps` — delete all of the caller's recaps and digests. The consent
/// record, preferences and the day's upload usage stay, so deleting everything
/// does not reset the daily quota.
pub async fn delete_all(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let removed = delete_owned_recaps(&ctx.engine, &caller.user_id).await?;
    ctx.engine
        .store
        .purge_recap_digests(&caller.user_id)
        .await?;
    Ok(Json(json!({ "removed": removed })))
}

/// One recap as portable Markdown.
pub fn recap_markdown(recap: &Meeting, transcript: &[String]) -> String {
    fn list(title: &str, items: &[String]) -> String {
        if items.is_empty() {
            return String::new();
        }
        let body: Vec<String> = items.iter().map(|i| format!("- {i}")).collect();
        format!("### {title}\n\n{}\n\n", body.join("\n"))
    }
    let day = recap.recap.as_ref().map_or("", |m| m.local_day.as_str());
    let mut md = format!("## {}\n\n_{day} · {}_\n\n", recap.title, recap.started_at);
    if let Some(notes) = &recap.notes {
        md.push_str(&format!("{}\n\n", notes.summary));
        md.push_str(&list("Key points", &notes.key_points));
        md.push_str(&list("Action items", &notes.action_items));
        md.push_str(&list("Decisions", &notes.decisions));
        md.push_str(&list("Topics", &notes.topics));
    }
    if !transcript.is_empty() {
        md.push_str(&format!("### Transcript\n\n{}\n\n", transcript.join("\n")));
    }
    md
}

/// `GET /recaps/export?from=&to=&format=json|md` — the caller's recaps, with
/// notes and kept transcripts (data portability).
pub async fn export(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> RecapResult<Response> {
    let caller = enter(&ctx, &headers).await?;
    valid_day(query.from.as_deref())?;
    valid_day(query.to.as_deref())?;
    let recaps = ctx
        .engine
        .store
        .list_owned_recaps(
            &caller.user_id,
            query.from.as_deref(),
            query.to.as_deref(),
            None,
            MAX_EXPORT,
        )
        .await?;
    let mut items = Vec::with_capacity(recaps.len());
    for recap in recaps {
        let transcript: Vec<String> = ctx
            .engine
            .store
            .recap_segments(&recap.id)
            .await?
            .into_iter()
            .map(|s| s.text)
            .collect();
        items.push((recap, transcript));
    }
    if query.format.as_deref() == Some("md") {
        let mut md = String::from("# Ryu recaps export\n\n");
        for (recap, transcript) in &items {
            md.push_str(&recap_markdown(recap, transcript));
        }
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/markdown; charset=utf-8")],
            md,
        )
            .into_response());
    }
    let recaps: Vec<Value> = items
        .into_iter()
        .map(|(recap, transcript)| json!({ "recap": recap, "transcript": transcript }))
        .collect();
    Ok(Json(json!({
        "exported_at": chrono::Utc::now().to_rfc3339(),
        "recaps": recaps,
    }))
    .into_response())
}
