//! The daily digest: one card per `(owner, local day)` built from that day's
//! finished recaps. The structured parts (highlights, action items, topics, ids,
//! duration) are assembled deterministically; only the title and summary come
//! from a model, with a plain fallback. Names never appear: action-item owners
//! and `people` are dropped.

use axum::{
    extract::{Query, State},
    http::HeaderMap,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{enter, prompts, settings, RecapError, RecapResult};
use crate::api::MeetingsCtx;
use crate::store::DigestRow;
use crate::{notes, Meeting, MeetingEngine, MeetingStatus};

/// Most highlights / action items / topics a digest keeps.
const MAX_ITEMS: usize = 12;
/// The digest field recording when the evening notice went out.
pub const NOTIFIED_AT: &str = "notified_at";

/// The owner's finished recaps for `day`, oldest first.
pub async fn day_recaps(
    engine: &MeetingEngine,
    owner: &str,
    day: &str,
) -> RecapResult<Vec<Meeting>> {
    let mut recaps: Vec<Meeting> = engine
        .store
        .list_owned_recaps(owner, Some(day), Some(day), None, 1_000)
        .await?
        .into_iter()
        .filter(|r| r.status == MeetingStatus::Done && r.notes.is_some())
        .collect();
    recaps.sort_by(|a, b| a.started_at.cmp(&b.started_at));
    Ok(recaps)
}

/// The comma-joined sorted ids a digest was built from.
pub fn source_ids(recaps: &[Meeting]) -> String {
    let mut ids: Vec<&str> = recaps.iter().map(|r| r.id.as_str()).collect();
    ids.sort_unstable();
    ids.join(",")
}

fn first_sentence(text: &str) -> String {
    let end = text
        .char_indices()
        .find(|(_, c)| matches!(c, '.' | '!' | '?'))
        .map_or(text.len(), |(i, c)| i + c.len_utf8());
    text[..end].trim().to_string()
}

/// The deterministic digest body (title and summary are fallbacks a model may
/// replace).
pub fn assemble(day: &str, tz: &str, recaps: &[Meeting]) -> Value {
    let mut highlights = Vec::new();
    let mut action_items = Vec::new();
    let mut topics: Vec<String> = Vec::new();
    let mut total_duration_ms = 0i64;
    for recap in recaps {
        total_duration_ms += recap.recap.as_ref().map_or(0, |m| m.duration_ms);
        let Some(notes) = &recap.notes else { continue };
        let line = first_sentence(&notes.summary);
        if !line.is_empty() && highlights.len() < MAX_ITEMS {
            highlights.push(json!({ "recap_id": recap.id, "text": line }));
        }
        let detailed: Vec<(String, Option<String>)> = if notes.action_items_detail.is_empty() {
            notes
                .action_items
                .iter()
                .map(|t| (t.clone(), None))
                .collect()
        } else {
            notes
                .action_items_detail
                .iter()
                .map(|a| (a.text.clone(), a.due.clone()))
                .collect()
        };
        for (text, due) in detailed {
            if action_items.len() < MAX_ITEMS {
                action_items.push(json!({ "text": text, "due": due, "recap_id": recap.id }));
            }
        }
        for topic in &notes.topics {
            if topics.len() < MAX_ITEMS && !topics.iter().any(|t| t.eq_ignore_ascii_case(topic)) {
                topics.push(topic.clone());
            }
        }
    }
    let summary = highlights
        .iter()
        .filter_map(|h| h["text"].as_str())
        .collect::<Vec<_>>()
        .join(" ");
    json!({
        "date": day,
        "tz": tz,
        "title": format!("Conversations: {}", recaps.len()),
        "summary": summary,
        "highlights": highlights,
        "action_items_detail": action_items,
        "topics": topics,
        "recap_ids": recaps.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        "count": recaps.len(),
        "total_duration_ms": total_duration_ms,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "model": Value::Null,
    })
}

/// Ask the model for a title and summary over the day's (already redacted)
/// recap summaries; keep the fallback on any failure.
async fn polish(engine: &MeetingEngine, owner: &str, body: &mut Value, recaps: &[Meeting]) {
    let input = recaps
        .iter()
        .filter_map(|r| r.notes.as_ref().map(|n| format!("- {}", n.summary)))
        .collect::<Vec<_>>()
        .join("\n");
    if input.trim().is_empty() {
        return;
    }
    let Ok(member) = settings::load(engine, owner).await else {
        return;
    };
    // The day's summaries reach a model only under current consent; otherwise
    // the deterministic fallback stays.
    if !settings::consent_current(engine, &member).await {
        return;
    }
    let model = settings::resolved_model(engine, &member).await;
    let generated = notes::generate_notes(
        &engine.http,
        &engine.host.gateway_url(),
        engine.host.gateway_token().as_deref(),
        &model,
        "",
        prompts::DAILY_DIGEST_PROMPT,
        &input,
    )
    .await;
    if let Ok(generated) = generated {
        let mut polished = generated;
        super::redact::redact_notes(&mut polished);
        if !polished.summary.trim().is_empty() {
            body["summary"] = json!(polished.summary);
        }
        if let Some(title) = polished.title.filter(|t| !t.trim().is_empty()) {
            body["title"] = json!(title.chars().take(60).collect::<String>());
        }
        body["model"] = json!(model);
    }
}

/// Build and store the digest for `(owner, day)`. With no finished recaps the
/// stored digest (if any) is removed and `None` returned.
pub async fn build(
    engine: &MeetingEngine,
    owner: &str,
    day: &str,
    tz: &str,
) -> RecapResult<Option<DigestRow>> {
    let recaps = day_recaps(engine, owner, day).await?;
    if recaps.is_empty() {
        engine.store.delete_recap_digest(owner, day).await?;
        return Ok(None);
    }
    let mut body = assemble(day, tz, &recaps);
    polish(engine, owner, &mut body, &recaps).await;
    // The evening notice is sent once per day; a rebuild keeps that marker.
    if let Some(previous) = engine.store.recap_digest(owner, day).await? {
        if let Some(notified) = previous.json.get(NOTIFIED_AT) {
            body[NOTIFIED_AT] = notified.clone();
        }
    }
    let row = DigestRow {
        owner_user_id: owner.to_string(),
        day: day.to_string(),
        tz: tz.to_string(),
        json: body,
        source_ids: source_ids(&recaps),
        generated_at: chrono::Utc::now().to_rfc3339(),
    };
    engine.store.put_recap_digest(&row).await?;
    Ok(Some(row))
}

/// Rebuild the day's digest only if one was already built (after an edit or a
/// delete), so stale or deleted text never lingers in it.
pub async fn refresh_if_present(engine: &MeetingEngine, owner: &str, day: &str) {
    let existing = match engine.store.recap_digest(owner, day).await {
        Ok(Some(existing)) => existing,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!("recaps: reading a digest failed: {error:#}");
            return;
        }
    };
    if let Err(error) = build(engine, owner, day, &existing.tz).await {
        tracing::warn!("recaps: rebuilding a digest failed: {error:?}");
    }
}

/// The stored digest when its sources are unchanged, otherwise a fresh build.
pub async fn get_or_build(
    engine: &MeetingEngine,
    owner: &str,
    day: &str,
    tz: &str,
) -> RecapResult<Option<DigestRow>> {
    if let Some(existing) = engine.store.recap_digest(owner, day).await? {
        let current = source_ids(&day_recaps(engine, owner, day).await?);
        if current == existing.source_ids {
            return Ok(Some(existing));
        }
    }
    build(engine, owner, day, tz).await
}

/// `GET /recaps/daily` query / `POST /recaps/daily/regenerate` body.
#[derive(Debug, Deserialize)]
pub struct DayQuery {
    pub date: String,
    #[serde(default)]
    pub tz: Option<String>,
}

fn validate_day(query: &DayQuery) -> RecapResult<String> {
    chrono::NaiveDate::parse_from_str(&query.date, "%Y-%m-%d")
        .map_err(|_| RecapError::BadRequest("date must be YYYY-MM-DD".into()))?;
    let tz = query.tz.clone().unwrap_or_else(|| "UTC".into());
    tz.parse::<chrono_tz::Tz>()
        .map_err(|_| RecapError::BadRequest("tz must be an IANA zone".into()))?;
    Ok(tz)
}

fn digest_view(row: Option<DigestRow>) -> Json<Value> {
    Json(json!({ "digest": row.map(|r| r.json) }))
}

/// `GET /recaps/daily?date=&tz=` — the caller's digest (`null` with no recaps).
pub async fn get_daily(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Query(query): Query<DayQuery>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let tz = validate_day(&query)?;
    let row = get_or_build(&ctx.engine, &caller.user_id, &query.date, &tz).await?;
    Ok(digest_view(row))
}

/// `POST /recaps/daily/regenerate` — force a rebuild.
pub async fn regenerate(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Json(query): Json<DayQuery>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let tz = validate_day(&query)?;
    let row = build(&ctx.engine, &caller.user_id, &query.date, &tz).await?;
    Ok(digest_view(row))
}
