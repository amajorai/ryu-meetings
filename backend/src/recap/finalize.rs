//! Finalize: check the chunk ledger, then (in the background) redact the
//! transcript, summarize it with the recap prompt, redact the notes, title the
//! recap, announce `recap.ready`, and refresh that day's digest.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use super::ingest::missing_seqs;
use super::{digest, enter_capture, events, limits, prompts, redact, settings, start};
use super::{RecapError, RecapResult};
use crate::api::MeetingsCtx;
use crate::{notes, MeetingEngine, MeetingEvent, MeetingNotes, MeetingStatus};

/// The summary stored on a recap closed without consent for a model summary.
pub const UNSUMMARIZED_SUMMARY: &str =
    "Not summarized: recap consent was withdrawn or needs to be given again. The transcript is kept.";

/// Longest kept title.
const MAX_TITLE_CHARS: usize = 80;
/// Most topics kept.
const MAX_TOPICS: usize = 8;

/// `POST /recaps/:id/finalize` body.
#[derive(Debug, Deserialize)]
pub struct FinalizeBody {
    pub expected_chunks: u32,
    #[serde(default)]
    pub ended_at: Option<String>,
    #[serde(default)]
    pub force: bool,
}

/// `POST /recaps/:id/finalize` — 409 `{missing}` while chunks are missing (unless
/// `force`), otherwise 202 with the recap in `processing`.
pub async fn finalize(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<FinalizeBody>,
) -> RecapResult<(StatusCode, Json<Value>)> {
    let caller = enter_capture(&ctx, &headers).await?;
    let store = &ctx.engine.store;
    let recap = store
        .get_owned_recap(&caller.user_id, &id)
        .await?
        .ok_or(RecapError::NotFound)?;
    match recap.status {
        MeetingStatus::Processing => {
            return Ok((StatusCode::ACCEPTED, Json(json!({ "recap": recap }))))
        }
        MeetingStatus::Recording => start::accepts_audio(&recap)?,
        _ => return Err(RecapError::conflict("recap_closed")),
    }
    // The summary must not go to a model the member has not consented to (a
    // route changed since the start, or consent was withdrawn): the client asks
    // again, and the stale-session tick closes it unsummarized if they never do.
    settings::require_consent(&ctx.engine, &caller.user_id).await?;
    if body.expected_chunks > limits::MAX_SEQ + 1 {
        return Err(RecapError::BadRequest(
            "expected_chunks is too large".into(),
        ));
    }
    let received: Vec<u32> = store
        .recap_chunks(&id)
        .await?
        .iter()
        .map(|c| c.seq)
        .collect();
    let missing = missing_seqs(&received, body.expected_chunks);
    if !missing.is_empty() && !body.force {
        return Err(RecapError::Conflict(
            "missing_chunks",
            json!({ "missing": missing }),
        ));
    }
    let recap = mark_processing(&ctx.engine, &id, body.expected_chunks, !missing.is_empty())
        .await?
        .ok_or(RecapError::NotFound)?;
    let engine = ctx.engine.clone();
    tokio::spawn(async move { run_tail(&engine, &id).await });
    Ok((StatusCode::ACCEPTED, Json(json!({ "recap": recap }))))
}

/// Record the declared chunk count and move the recap to `processing`.
pub async fn mark_processing(
    engine: &MeetingEngine,
    id: &str,
    expected: u32,
    partial: bool,
) -> RecapResult<Option<crate::Meeting>> {
    let now = chrono::Utc::now().to_rfc3339();
    let updated = engine
        .store
        .mutate_recap(id, |m| {
            if m.status != MeetingStatus::Recording {
                return false;
            }
            if let Some(meta) = m.recap.as_mut() {
                meta.expected_chunks = Some(expected);
                meta.partial = meta.partial || partial;
            }
            m.status = MeetingStatus::Processing;
            m.updated_at = now.clone();
            true
        })
        .await?;
    if let Some(recap) = &updated {
        // An interim summary never outlives the recording.
        engine.interim.clear(id);
        engine.store.emit_recap(
            id,
            MeetingEvent::Status {
                meeting_id: id.to_string(),
                status: MeetingStatus::Processing,
            },
        );
        return Ok(Some(recap.clone()));
    }
    Ok(None)
}

/// Shape the model's notes for storage: redact, honour the people opt-in, cap
/// the topic list, and drop an unusable title.
pub fn shape_notes(mut notes: MeetingNotes, people: bool) -> MeetingNotes {
    redact::redact_notes(&mut notes);
    if !people {
        notes.people.clear();
        for item in &mut notes.action_items_detail {
            item.owner = None;
        }
    }
    notes.topics.truncate(MAX_TOPICS);
    notes.title = notes
        .title
        .map(|t| {
            t.trim()
                .trim_matches('"')
                .chars()
                .take(MAX_TITLE_CHARS)
                .collect::<String>()
        })
        .filter(|t| !t.is_empty());
    notes
}

/// The background tail of a finalize. Safe to call more than once: a second
/// concurrent run is refused by the finalize guard, and a recap that is no longer
/// `processing` is left alone.
pub async fn run_tail(engine: &MeetingEngine, id: &str) {
    let Some(_claim) = engine.begin_finalize(id) else {
        return;
    };
    if let Err(error) = tail(engine, id).await {
        tracing::warn!("recaps: finalize of {id} failed; it stays retryable: {error:?}");
        if matches!(
            engine
                .store
                .revert_processing(id, MeetingStatus::Recording)
                .await,
            Ok(true)
        ) {
            engine.store.emit_recap(
                id,
                MeetingEvent::Status {
                    meeting_id: id.to_string(),
                    status: MeetingStatus::Recording,
                },
            );
        }
    }
}

async fn tail(engine: &MeetingEngine, id: &str) -> RecapResult<()> {
    let Some(recap) = engine.store.get_meeting(id).await? else {
        return Ok(());
    };
    let Some(meta) = recap.recap.clone() else {
        return Ok(());
    };
    if recap.status != MeetingStatus::Processing {
        return Ok(());
    }
    let member = settings::load(engine, &meta.owner_user_id).await?;
    if !settings::consent_current(engine, &member).await {
        // No model call without current consent; keep the transcript.
        return finish(engine, id, &meta, unsummarized_notes(), true).await;
    }
    let segments = engine.store.recap_segments(id).await?;
    let transcript = segments
        .iter()
        .map(|s| s.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let notes = if transcript.trim().is_empty() {
        MeetingNotes {
            summary: "No speech was captured in this recap.".into(),
            ..MeetingNotes::default()
        }
    } else {
        // Redaction happens BEFORE the transcript leaves for the model.
        let redacted = redact::redact_text(&transcript);
        let model = settings::resolved_model(engine, &member).await;
        let effort = crate::api::resolve_notes_effort(engine).await;
        notes::generate_notes(
            &engine.http,
            &engine.host.gateway_url(),
            engine.host.gateway_token().as_deref(),
            &model,
            &effort,
            &prompts::recap_prompt(member.people),
            &redacted,
        )
        .await
        .map_err(RecapError::Upstream)?
    };
    let notes = shape_notes(notes, member.people);
    finish(engine, id, &meta, notes, false).await
}

fn unsummarized_notes() -> MeetingNotes {
    MeetingNotes {
        summary: UNSUMMARIZED_SUMMARY.into(),
        ..MeetingNotes::default()
    }
}

/// Store the notes, mark the recap done, announce it and refresh the day digest.
async fn finish(
    engine: &MeetingEngine,
    id: &str,
    meta: &crate::RecapMeta,
    notes: MeetingNotes,
    unsummarized: bool,
) -> RecapResult<()> {
    let duration_ms: i64 = engine
        .store
        .recap_chunks(id)
        .await?
        .iter()
        .map(|c| c.duration_ms)
        .sum();
    let title = notes.title.clone();
    let now = chrono::Utc::now().to_rfc3339();
    let Some(_) = engine.store.finish_meeting(id, notes, &now).await? else {
        return Ok(());
    };
    let finished = engine
        .store
        .mutate_recap(id, |m| {
            if let (Some(title), false) = (&title, m.title_custom) {
                m.title = title.clone();
            }
            if let Some(meta) = m.recap.as_mut() {
                meta.duration_ms = duration_ms;
                meta.unsummarized = unsummarized;
            }
            true
        })
        .await?
        .ok_or(RecapError::NotFound)?;
    engine.store.emit_recap(
        id,
        MeetingEvent::Finalized {
            meeting: finished.clone(),
        },
    );
    events::emit_ready(engine, &finished).await;
    digest::refresh_if_present(engine, &meta.owner_user_id, &meta.local_day).await;
    Ok(())
}

/// Close every session the owner still has recording, with no model call: an
/// unconfirmed fast-path start is discarded (as its window would), any other is
/// finalized from the chunks received so far and closed unsummarized (the tail
/// finds no current consent). Called when consent is withdrawn. Returns how many
/// sessions were closed.
pub async fn close_owner_recordings_unsummarized(
    engine: &MeetingEngine,
    owner: &str,
) -> RecapResult<usize> {
    let recaps = engine
        .store
        .list_owned_recaps(owner, None, None, None, usize::MAX >> 1)
        .await?;
    let mut closed = 0;
    for recap in recaps {
        if recap.status != MeetingStatus::Recording {
            continue;
        }
        if start::accepts_audio(&recap).is_err() {
            super::records::delete_recap(engine, &recap).await?;
            closed += 1;
            continue;
        }
        let received = engine.store.recap_chunks(&recap.id).await?;
        let expected = received.iter().map(|c| c.seq + 1).max().unwrap_or(0);
        if mark_processing(engine, &recap.id, expected, false)
            .await?
            .is_some()
        {
            run_tail(engine, &recap.id).await;
            closed += 1;
        }
    }
    Ok(closed)
}
