//! Opt-in interim summaries: `GET /recaps/:id/interim`.
//!
//! While the owner is still recording, a client may ask for a rolling summary of
//! what was said so far. It is off by default (`settings.interim`), owner scoped,
//! needs current consent, and is redacted before the transcript leaves for the
//! model and again on the way back.
//!
//! An interim summary is never stored: it lives only in this process's
//! [`InterimCache`], keyed by recap id, and is dropped when the recap is
//! finalized, discarded, deleted or purged. A new model call is made at most once
//! per [`INTERIM_SPEECH_STEP_MS`] of new speech; in between the cached summary is
//! returned. Requests are also capped per member per minute.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use serde::Serialize;
use serde_json::{json, Value};

use super::{enter_capture, finalize, prompts, redact, settings, start};
use super::{RecapError, RecapResult};
use crate::api::MeetingsCtx;
use crate::store::ChunkRow;
use crate::{notes, MeetingEngine};

/// New speech needed before another model call (and before the first one).
pub const INTERIM_SPEECH_STEP_MS: i64 = 5 * 60 * 1000;
/// Interim requests one member may make per minute (polling included).
pub const MAX_INTERIM_REQUESTS_PER_MINUTE: usize = 6;
/// The transcript tail sent to the model, in characters.
pub const MAX_INTERIM_INPUT_CHARS: usize = 24_000;
/// Most key points kept on an interim summary.
const MAX_INTERIM_POINTS: usize = 6;

/// A rolling summary of a recap that is still recording.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InterimSummary {
    pub summary: String,
    pub key_points: Vec<String>,
    /// The speech covered, in milliseconds of transcribed audio.
    pub speech_ms: i64,
    pub generated_at: String,
}

#[derive(Debug, Default)]
struct Entry {
    owner: String,
    summary: Option<InterimSummary>,
    in_flight: bool,
}

/// In-memory interim summaries for recaps still recording. Cheap to clone.
#[derive(Clone, Default)]
pub struct InterimCache {
    entries: Arc<Mutex<HashMap<String, Entry>>>,
}

/// What [`InterimCache::claim`] decided.
#[derive(Debug, Clone, PartialEq)]
pub enum Claim {
    /// A model call is due; the caller must [`InterimCache::complete`] or
    /// [`InterimCache::release`] it.
    Refresh,
    /// Not due (or another call is running): serve what is cached.
    Cached(Option<InterimSummary>),
}

impl InterimCache {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Decide whether `speech_ms` of speech warrants a new summary for `id`.
    pub fn claim(&self, id: &str, owner: &str, speech_ms: i64) -> Claim {
        let mut entries = self.lock();
        let entry = entries.entry(id.to_string()).or_insert_with(|| Entry {
            owner: owner.to_string(),
            ..Entry::default()
        });
        if entry.owner != owner {
            return Claim::Cached(None);
        }
        let covered = entry.summary.as_ref().map_or(0, |s| s.speech_ms);
        if entry.in_flight || speech_ms - covered < INTERIM_SPEECH_STEP_MS {
            return Claim::Cached(entry.summary.clone());
        }
        entry.in_flight = true;
        Claim::Refresh
    }

    /// Store a fresh summary. Dropped when the recap was cleared meanwhile
    /// (finalized or deleted while the model call ran).
    pub fn complete(&self, id: &str, summary: InterimSummary) -> bool {
        match self.lock().get_mut(id) {
            Some(entry) if entry.in_flight => {
                entry.in_flight = false;
                entry.summary = Some(summary);
                true
            }
            _ => false,
        }
    }

    /// Give up a claimed refresh (the model call failed).
    pub fn release(&self, id: &str) {
        if let Some(entry) = self.lock().get_mut(id) {
            entry.in_flight = false;
        }
    }

    /// Whether a refresh of `id` is claimed and not yet completed or cleared.
    pub fn is_claimed(&self, id: &str) -> bool {
        self.lock().get(id).is_some_and(|entry| entry.in_flight)
    }

    /// The cached summary for `id`, when `owner` owns it.
    pub fn get(&self, id: &str, owner: &str) -> Option<InterimSummary> {
        self.lock()
            .get(id)
            .filter(|entry| entry.owner == owner)
            .and_then(|entry| entry.summary.clone())
    }

    /// Forget `id` (finalize, discard, delete).
    pub fn clear(&self, id: &str) {
        self.lock().remove(id);
    }

    /// Forget every recap `owner` has (purge).
    pub fn clear_owner(&self, owner: &str) {
        self.lock().retain(|_, entry| entry.owner != owner);
    }

    /// How many recaps have an entry (tests and diagnostics).
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether nothing is cached.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }
}

/// Milliseconds of transcribed speech in a chunk ledger (silent chunks excluded).
pub fn speech_ms(chunks: &[ChunkRow]) -> i64 {
    chunks
        .iter()
        .filter(|c| c.status == "transcribed")
        .map(|c| c.duration_ms)
        .sum()
}

/// The last `max_chars` characters of `text`, cut at a line start when possible.
pub fn transcript_tail(text: &str, max_chars: usize) -> &str {
    let count = text.chars().count();
    if count <= max_chars {
        return text;
    }
    let start = text
        .char_indices()
        .nth(count - max_chars)
        .map_or(0, |(i, _)| i);
    let tail = &text[start..];
    match tail.find('\n') {
        Some(newline) if newline + 1 < tail.len() => &tail[newline + 1..],
        _ => tail,
    }
}

fn view(summary: Option<InterimSummary>, speech_ms: i64, refreshed: bool) -> Value {
    let covered = summary.as_ref().map_or(0, |s| s.speech_ms);
    json!({
        "interim": summary,
        "speech_ms": speech_ms,
        "next_at_speech_ms": covered + INTERIM_SPEECH_STEP_MS,
        "refreshed": refreshed,
    })
}

/// `GET /recaps/:id/interim`.
pub async fn get_interim(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> RecapResult<Json<Value>> {
    let caller = enter_capture(&ctx, &headers).await?;
    let engine = &ctx.engine;
    let recap = engine
        .store
        .get_owned_recap(&caller.user_id, &id)
        .await?
        .ok_or(RecapError::NotFound)?;
    if start::accepts_audio(&recap).is_err() {
        engine.interim.clear(&id);
        return Err(RecapError::conflict("recap_not_recording"));
    }
    let member = settings::load(engine, &caller.user_id).await?;
    if !member.interim {
        return Err(RecapError::Forbidden("interim_disabled"));
    }
    if !settings::consent_current(engine, &member).await {
        return Err(settings::consent_required(engine, &member).await);
    }
    if !ctx
        .recaps
        .interim_limiter
        .allow(&caller.user_id, MAX_INTERIM_REQUESTS_PER_MINUTE)
    {
        return Err(RecapError::TooMany("rate_limited"));
    }
    let speech = speech_ms(&engine.store.recap_chunks(&id).await?);
    serve(engine, &caller.user_id, &id, &member, speech)
        .await
        .map(Json)
}

/// Whether `owner`'s recap `id` is still recording, read fresh from the store.
async fn still_recording(engine: &MeetingEngine, owner: &str, id: &str) -> RecapResult<bool> {
    let recap = engine.store.get_owned_recap(owner, id).await?;
    Ok(recap.is_some_and(|recap| start::accepts_audio(&recap).is_ok()))
}

/// Claim, and when due make, a summary for a recap the route found recording.
///
/// The recording can close between the route's status check and the claim; the
/// close's `clear` would then run before `claim` re-creates the entry. Finalize
/// writes the new status before clearing, so re-reading the recap after a
/// `Refresh` claim catches that ordering, and a `clear` after the claim makes
/// `complete` drop the result. Either way no summary outlives the recording.
pub(crate) async fn serve(
    engine: &MeetingEngine,
    owner: &str,
    id: &str,
    member: &settings::RecapSettings,
    speech: i64,
) -> RecapResult<Value> {
    match engine.interim.claim(id, owner, speech) {
        Claim::Cached(summary) => Ok(view(summary, speech, false)),
        Claim::Refresh => {
            let outcome = match still_recording(engine, owner, id).await {
                Ok(true) => summarize(engine, id, member, speech).await,
                Ok(false) => Err(RecapError::conflict("recap_not_recording")),
                Err(error) => Err(error),
            };
            match outcome {
                Ok(summary) => {
                    let kept = engine.interim.complete(id, summary.clone());
                    Ok(view(kept.then_some(summary), speech, kept))
                }
                Err(error) => {
                    engine.interim.release(id);
                    if matches!(still_recording(engine, owner, id).await, Ok(false)) {
                        engine.interim.clear(id);
                    }
                    Err(error)
                }
            }
        }
    }
}

/// Redact the transcript so far, ask the model, and shape the reply.
async fn summarize(
    engine: &MeetingEngine,
    id: &str,
    member: &settings::RecapSettings,
    speech: i64,
) -> RecapResult<InterimSummary> {
    let segments = engine.store.recap_segments(id).await?;
    let transcript = segments
        .iter()
        .map(|s| s.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    // Redaction happens BEFORE the transcript leaves for the model.
    let redacted = redact::redact_text(transcript_tail(&transcript, MAX_INTERIM_INPUT_CHARS));
    // Cleared while the transcript was read (the recording closed, or consent
    // was withdrawn): nothing leaves for the model.
    if !engine.interim.is_claimed(id) {
        return Err(RecapError::conflict("recap_not_recording"));
    }
    let model = settings::resolved_model(engine, member).await;
    let effort = crate::api::resolve_notes_effort(engine).await;
    let reply = notes::generate_notes(
        &engine.http,
        &engine.host.gateway_url(),
        engine.host.gateway_token().as_deref(),
        &model,
        &effort,
        &prompts::interim_prompt(member.people),
        &redacted,
    )
    .await
    .map_err(RecapError::Upstream)?;
    let mut shaped = finalize::shape_notes(reply, member.people);
    shaped.key_points.truncate(MAX_INTERIM_POINTS);
    Ok(InterimSummary {
        summary: shaped.summary,
        key_points: shaped.key_points,
        speech_ms: speech,
        generated_at: chrono::Utc::now().to_rfc3339(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(speech_ms: i64) -> InterimSummary {
        InterimSummary {
            summary: "so far".into(),
            key_points: vec![],
            speech_ms,
            generated_at: "now".into(),
        }
    }

    #[test]
    fn a_refresh_needs_a_full_step_of_new_speech() {
        let cache = InterimCache::default();
        let step = INTERIM_SPEECH_STEP_MS;
        assert_eq!(cache.claim("r", "a", step - 1), Claim::Cached(None));
        assert_eq!(cache.claim("r", "a", step), Claim::Refresh);
        // A second caller while the first call runs gets what is cached.
        assert_eq!(cache.claim("r", "a", step * 3), Claim::Cached(None));
        assert!(cache.complete("r", summary(step)));
        assert_eq!(
            cache.claim("r", "a", step * 2 - 1),
            Claim::Cached(Some(summary(step)))
        );
        assert_eq!(cache.claim("r", "a", step * 2), Claim::Refresh);
        cache.release("r");
        assert_eq!(cache.claim("r", "a", step * 2), Claim::Refresh);
    }

    #[test]
    fn entries_are_owner_bound_and_cleared() {
        let cache = InterimCache::default();
        assert_eq!(
            cache.claim("r", "a", INTERIM_SPEECH_STEP_MS),
            Claim::Refresh
        );
        assert!(cache.complete("r", summary(INTERIM_SPEECH_STEP_MS)));
        assert_eq!(cache.get("r", "b"), None);
        assert_eq!(
            cache.claim("r", "b", INTERIM_SPEECH_STEP_MS * 9),
            Claim::Cached(None)
        );
        assert!(cache.get("r", "a").is_some());
        cache.clear("r");
        assert!(cache.is_empty());
        // A model call that finishes after the recap was cleared is dropped.
        assert_eq!(
            cache.claim("s", "a", INTERIM_SPEECH_STEP_MS),
            Claim::Refresh
        );
        assert!(cache.is_claimed("s"));
        cache.clear("s");
        assert!(!cache.is_claimed("s"));
        assert!(!cache.complete("s", summary(1)));
        assert_eq!(cache.claim("t", "a", 0), Claim::Cached(None));
        assert_eq!(cache.claim("u", "b", 0), Claim::Cached(None));
        cache.clear_owner("a");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn the_tail_keeps_the_latest_whole_lines() {
        assert_eq!(transcript_tail("short", 10), "short");
        assert_eq!(transcript_tail("first line\nsecond", 8), "second");
        assert_eq!(transcript_tail("ééééé", 3), "ééé");
    }

    #[test]
    fn only_transcribed_chunks_count_as_speech() {
        let row = |status: &str, duration_ms| ChunkRow {
            seq: 0,
            offset_ms: 0,
            duration_ms,
            sha256: String::new(),
            bytes: 0,
            status: status.into(),
            segment_id: None,
            received_at: String::new(),
        };
        let chunks = [
            row("transcribed", 30_000),
            row("skipped_silent", 30_000),
            row("skipped_client", 30_000),
            row("transcribed", 10_000),
        ];
        assert_eq!(speech_ms(&chunks), 40_000);
    }
}
