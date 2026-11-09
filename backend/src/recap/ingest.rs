//! Chunk upload (`PUT /recaps/:id/chunks/:seq`) and resume status.
//!
//! Each chunk is idempotent on `(recap, seq)`: the server computes the sha256,
//! a replay with the same digest answers `duplicate: true`, a different digest is
//! a 409. The audio is decoded in memory, silence is skipped before STT, and no
//! audio is persisted. The ledger row and the transcript segment commit together.

use std::time::Duration;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header::CONTENT_TYPE, HeaderMap},
    Json,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::decode::{self, ChunkFormat, DecodeError};
use super::limits::{self, ChunkMeta};
use super::{enter, enter_capture, settings, start, RecapError, RecapResult};
use crate::api::MeetingsCtx;
use crate::store::{is_constraint_violation, ChunkOutcome, ChunkRow, ChunkStatus, NewChunk};
use crate::{MeetingEngine, MeetingEvent};

/// The decode time budget for one chunk.
const DECODE_BUDGET: Duration = Duration::from_secs(10);

/// The daily upload quota (`RYU_RECAP_DAILY_QUOTA_BYTES` overrides the default).
fn daily_quota() -> i64 {
    std::env::var("RYU_RECAP_DAILY_QUOTA_BYTES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|v: &i64| *v > 0)
        .unwrap_or(limits::DEFAULT_DAILY_QUOTA_BYTES)
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Decode and transcribe one chunk. `Ok(None)` means silence (nothing reached STT
/// or STT returned no words).
async fn transcribe_chunk(
    engine: &MeetingEngine,
    bytes: Bytes,
    format: ChunkFormat,
    meta: &ChunkMeta,
) -> RecapResult<Option<String>> {
    let declared = meta.duration_ms;
    let decoded = tokio::time::timeout(
        DECODE_BUDGET,
        tokio::task::spawn_blocking(move || decode::decode_chunk(&bytes, format, declared)),
    )
    .await
    .map_err(|_| RecapError::BadRequest("decode exceeded its time budget".into()))?
    .map_err(|e| RecapError::Internal(e.to_string()))?;
    let pcm = match decoded {
        Ok(pcm) => pcm,
        Err(DecodeError::Unsupported(what)) => return Err(RecapError::Unsupported(what)),
        Err(DecodeError::Overflow) => {
            return Err(RecapError::BadRequest(
                "decoded audio is longer than the declared duration".into(),
            ))
        }
        Err(DecodeError::Invalid(why)) => return Err(RecapError::BadRequest(why)),
    };
    if !decode::has_speech(&pcm) {
        return Ok(None);
    }
    let wav = decode::to_wav(&pcm).map_err(|e| RecapError::Internal(format!("{e:?}")))?;
    let text = engine
        .host
        .transcribe(wav, "recap.wav".into(), None)
        .await
        .map_err(RecapError::Upstream)?;
    let text = text.trim();
    Ok((!text.is_empty()).then(|| text.to_string()))
}

/// The answer for a seq that already has a ledger row: the same bytes are a
/// harmless replay (`duplicate: true`), different bytes a 409.
pub fn replayed(existing: &ChunkRow, sha: &str, seq: u32) -> RecapResult<Json<Value>> {
    if existing.sha256 == sha {
        return Ok(Json(json!({
            "seq": seq,
            "status": existing.status,
            "duplicate": true,
        })));
    }
    Err(RecapError::Conflict("seq_conflict", json!({ "seq": seq })))
}

/// `PUT /recaps/:id/chunks/:seq` — ingest one raw audio chunk.
pub async fn put_chunk(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Path((id, seq)): Path<(String, String)>,
    body: Bytes,
) -> RecapResult<Json<Value>> {
    let caller = enter_capture(&ctx, &headers).await?;
    let meta = limits::parse_chunk_meta(&seq, &headers, body.len())?;
    let store = &ctx.engine.store;
    let recap = store
        .get_owned_recap(&caller.user_id, &id)
        .await?
        .ok_or(RecapError::NotFound)?;
    let sha = sha256_hex(&body);
    if meta
        .client_sha256
        .as_deref()
        .is_some_and(|client| client != sha)
    {
        return Err(RecapError::Conflict(
            "sha_mismatch",
            json!({ "seq": meta.seq }),
        ));
    }
    if let Some(existing) = store.recap_chunk(&id, meta.seq).await? {
        return replayed(&existing, &sha, meta.seq);
    }
    start::accepts_audio(&recap)?;
    // Consent is re-checked on every chunk: a withdrawal, or a transcription or
    // summary route that changed since the start, stops capture until the member
    // consents again.
    settings::require_consent(&ctx.engine, &caller.user_id).await?;
    let ranges: Vec<(i64, i64)> = store
        .recap_chunks(&id)
        .await?
        .iter()
        .map(|c| (c.offset_ms, c.duration_ms))
        .collect();
    if limits::overlaps(&ranges, meta.offset_ms, meta.duration_ms) {
        return Err(RecapError::Conflict(
            "offset_overlap",
            json!({ "seq": meta.seq }),
        ));
    }
    if !ctx
        .recaps
        .limiter
        .allow(&caller.user_id, limits::MAX_CHUNKS_PER_MINUTE)
    {
        return Err(RecapError::TooMany("rate_limited"));
    }
    let usage_day = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let bytes = body.len() as i64;
    if store.recap_usage(&caller.user_id, &usage_day).await? + bytes > daily_quota() {
        return Err(RecapError::TooMany("quota_exceeded"));
    }

    let (status, text) = if meta.silent {
        (ChunkStatus::SkippedClient, None)
    } else {
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("audio/mp4");
        let format = ChunkFormat::from_content_type(content_type).map_err(|e| match e {
            DecodeError::Unsupported(what) => RecapError::Unsupported(what),
            other => RecapError::BadRequest(format!("{other:?}")),
        })?;
        match transcribe_chunk(&ctx.engine, body, format, &meta).await? {
            Some(text) => (ChunkStatus::Transcribed, Some(text)),
            None => (ChunkStatus::SkippedSilent, None),
        }
    };
    store
        .add_recap_usage(&caller.user_id, &usage_day, bytes)
        .await?;
    let outcome = store
        .record_recap_chunk(
            &id,
            NewChunk {
                seq: meta.seq,
                offset_ms: meta.offset_ms,
                duration_ms: meta.duration_ms,
                sha256: &sha,
                bytes,
                status,
                text: text.as_deref(),
            },
        )
        .await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        // Belt and braces: the store re-checks the seq inside its transaction,
        // but a constraint violation still means a concurrent upload won.
        Err(error) if is_constraint_violation(&error) => {
            match store.recap_chunk(&id, meta.seq).await? {
                Some(existing) => ChunkOutcome::Existing(existing),
                None => {
                    return Err(RecapError::Conflict(
                        "seq_conflict",
                        json!({ "seq": meta.seq }),
                    ))
                }
            }
        }
        Err(error) => return Err(error.into()),
    };
    let segment = match outcome {
        ChunkOutcome::Recorded(segment) => segment,
        ChunkOutcome::Existing(existing) => return replayed(&existing, &sha, meta.seq),
        // Deleted, purged or discarded while the chunk was in STT: nothing kept.
        ChunkOutcome::RecapGone => return Err(RecapError::NotFound),
        ChunkOutcome::RecapClosed => return Err(RecapError::conflict("recap_closed")),
    };
    if let Some(segment) = &segment {
        store.emit_recap(
            &id,
            MeetingEvent::Segment {
                segment: segment.clone(),
            },
        );
    }
    Ok(Json(json!({
        "seq": meta.seq,
        "status": status.as_str(),
        "duplicate": false,
        "segment_id": segment.map(|s| s.id),
    })))
}

/// The seqs missing below `upper` (exclusive) given the received set.
pub fn missing_seqs(received: &[u32], upper: u32) -> Vec<u32> {
    (0..upper).filter(|seq| !received.contains(seq)).collect()
}

/// `GET /recaps/:id/chunks` — `{received, missing}` so a client can resume.
pub async fn chunk_status(
    State(ctx): State<MeetingsCtx>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> RecapResult<Json<Value>> {
    let caller = enter(&ctx, &headers).await?;
    let recap = ctx
        .engine
        .store
        .get_owned_recap(&caller.user_id, &id)
        .await?
        .ok_or(RecapError::NotFound)?;
    let received: Vec<u32> = ctx
        .engine
        .store
        .recap_chunks(&id)
        .await?
        .iter()
        .map(|c| c.seq)
        .collect();
    let upper = recap
        .recap
        .as_ref()
        .and_then(|m| m.expected_chunks)
        .unwrap_or_else(|| received.iter().max().map_or(0, |max| max + 1));
    Ok(Json(json!({
        "received": received,
        "missing": missing_seqs(&received, upper),
    })))
}
