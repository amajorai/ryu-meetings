//! Upload limits for recap chunks: sequence and offset windows, chunk size, the
//! per-member daily byte quota, and a per-member request rate limit.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use serde_json::{json, Value};

use super::{RecapError, RecapResult};

/// Highest accepted chunk sequence number (12 h of 30 s chunks).
pub const MAX_SEQ: u32 = 1440;
/// Largest accepted chunk body.
pub const MAX_CHUNK_BYTES: usize = 2_000_000;
/// Longest accepted chunk.
pub const MAX_CHUNK_MS: i64 = 60_000;
/// The chunk length clients should record.
pub const RECOMMENDED_CHUNK_MS: i64 = 30_000;
/// The longest a session may run.
pub const MAX_SESSION_MS: i64 = 12 * 60 * 60 * 1000;
/// Overlap between two chunks' declared ranges tolerated as clock jitter.
pub const OVERLAP_TOLERANCE_MS: i64 = 250;
/// Default per-member daily upload quota.
pub const DEFAULT_DAILY_QUOTA_BYTES: i64 = 1_000_000_000;
/// Chunk uploads one member may make per minute.
pub const MAX_CHUNKS_PER_MINUTE: usize = 120;

pub const HDR_OFFSET: &str = "x-recap-offset-ms";
pub const HDR_DURATION: &str = "x-recap-duration-ms";
pub const HDR_SHA256: &str = "x-recap-sha256";
pub const HDR_SILENT: &str = "x-recap-silent";

/// The upload contract returned by `POST /recaps`.
pub fn upload_contract() -> Value {
    json!({
        "accepted": super::decode::ACCEPTED_TYPES,
        "max_chunk_bytes": MAX_CHUNK_BYTES,
        "recommended_chunk_ms": RECOMMENDED_CHUNK_MS,
        "max_chunk_ms": MAX_CHUNK_MS,
        "max_seq": MAX_SEQ,
    })
}

/// The validated metadata of one chunk upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMeta {
    pub seq: u32,
    pub offset_ms: i64,
    pub duration_ms: i64,
    pub client_sha256: Option<String>,
    pub silent: bool,
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

fn header_i64(headers: &HeaderMap, name: &str) -> RecapResult<i64> {
    header_str(headers, name)
        .ok_or_else(|| RecapError::BadRequest(format!("missing {name}")))?
        .parse::<i64>()
        .map_err(|_| RecapError::BadRequest(format!("{name} must be an integer")))
}

/// Parse and range-check the chunk path seq and headers.
pub fn parse_chunk_meta(seq: &str, headers: &HeaderMap, body_len: usize) -> RecapResult<ChunkMeta> {
    let seq: u32 = seq
        .parse()
        .map_err(|_| RecapError::BadRequest("seq must be an integer".into()))?;
    if seq > MAX_SEQ {
        return Err(RecapError::BadRequest(format!(
            "seq must be at most {MAX_SEQ}"
        )));
    }
    let offset_ms = header_i64(headers, HDR_OFFSET)?;
    let duration_ms = header_i64(headers, HDR_DURATION)?;
    if !(0..MAX_SESSION_MS).contains(&offset_ms) {
        return Err(RecapError::BadRequest(
            "offset outside the session window".into(),
        ));
    }
    if !(1..=MAX_CHUNK_MS).contains(&duration_ms) || offset_ms + duration_ms > MAX_SESSION_MS {
        return Err(RecapError::BadRequest(
            "duration outside the allowed range".into(),
        ));
    }
    if body_len > MAX_CHUNK_BYTES {
        return Err(RecapError::PayloadTooLarge);
    }
    let silent = header_str(headers, HDR_SILENT) == Some("1");
    if silent && body_len > 0 {
        return Err(RecapError::BadRequest(
            "a silent chunk carries no audio".into(),
        ));
    }
    if !silent && body_len == 0 {
        return Err(RecapError::BadRequest("empty chunk body".into()));
    }
    let client_sha256 = header_str(headers, HDR_SHA256).map(str::to_ascii_lowercase);
    if client_sha256
        .as_deref()
        .is_some_and(|sha| sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return Err(RecapError::BadRequest(
            "x-recap-sha256 must be 64 hex digits".into(),
        ));
    }
    Ok(ChunkMeta {
        seq,
        offset_ms,
        duration_ms,
        client_sha256,
        silent,
    })
}

/// Whether `[offset, offset + duration)` overlaps any existing range by more
/// than [`OVERLAP_TOLERANCE_MS`].
pub fn overlaps(existing: &[(i64, i64)], offset_ms: i64, duration_ms: i64) -> bool {
    let end = offset_ms + duration_ms;
    existing.iter().any(|&(other_offset, other_duration)| {
        let other_end = other_offset + other_duration;
        let overlap = end.min(other_end) - offset_ms.max(other_offset);
        overlap > OVERLAP_TOLERANCE_MS
    })
}

/// A sliding one-minute window of chunk uploads per member. Cheap to clone.
#[derive(Clone, Default)]
pub struct RateLimiter {
    windows: Arc<Mutex<HashMap<String, Vec<Instant>>>>,
}

impl RateLimiter {
    /// Record one upload for `owner`; `false` when it would exceed `limit` in the
    /// last minute (the upload is then not recorded).
    pub fn allow(&self, owner: &str, limit: usize) -> bool {
        let now = Instant::now();
        let mut windows = self.windows.lock().unwrap_or_else(PoisonError::into_inner);
        let window = windows.entry(owner.to_string()).or_default();
        window.retain(|at| now.duration_since(*at) < Duration::from_secs(60));
        if window.len() >= limit {
            return false;
        }
        window.push(now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(offset: &str, duration: &str) -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert(HDR_OFFSET, HeaderValue::from_str(offset).unwrap());
        map.insert(HDR_DURATION, HeaderValue::from_str(duration).unwrap());
        map
    }

    #[test]
    fn seq_offset_duration_and_size_are_range_checked() {
        let ok = parse_chunk_meta("3", &headers("90000", "30000"), 10).unwrap();
        assert_eq!((ok.seq, ok.offset_ms, ok.duration_ms), (3, 90_000, 30_000));
        assert!(parse_chunk_meta("1441", &headers("0", "30000"), 10).is_err());
        assert!(parse_chunk_meta("x", &headers("0", "30000"), 10).is_err());
        assert!(parse_chunk_meta("0", &headers("-1", "30000"), 10).is_err());
        assert!(parse_chunk_meta("0", &headers(&MAX_SESSION_MS.to_string(), "1000"), 10).is_err());
        assert!(parse_chunk_meta("0", &headers("0", "60001"), 10).is_err());
        assert!(matches!(
            parse_chunk_meta("0", &headers("0", "30000"), MAX_CHUNK_BYTES + 1),
            Err(RecapError::PayloadTooLarge)
        ));
        assert!(parse_chunk_meta("0", &headers("0", "30000"), 0).is_err());
    }

    #[test]
    fn silent_marker_requires_an_empty_body_and_sha_must_be_hex() {
        let mut silent = headers("0", "30000");
        silent.insert(HDR_SILENT, HeaderValue::from_static("1"));
        assert!(parse_chunk_meta("0", &silent, 0).unwrap().silent);
        assert!(parse_chunk_meta("0", &silent, 5).is_err());
        let mut bad_sha = headers("0", "30000");
        bad_sha.insert(HDR_SHA256, HeaderValue::from_static("abc"));
        assert!(parse_chunk_meta("0", &bad_sha, 5).is_err());
    }

    #[test]
    fn overlap_tolerates_jitter_only() {
        let existing = [(0, 30_000), (30_000, 30_000)];
        assert!(!overlaps(&existing, 60_000, 30_000));
        assert!(!overlaps(&existing, 59_900, 30_000));
        assert!(overlaps(&existing, 45_000, 30_000));
    }

    #[test]
    fn rate_limiter_caps_each_member_separately() {
        let limiter = RateLimiter::default();
        assert!(limiter.allow("a", 2));
        assert!(limiter.allow("a", 2));
        assert!(!limiter.allow("a", 2));
        assert!(limiter.allow("b", 2));
    }
}
