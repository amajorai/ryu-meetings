//! The five-minute recap tick (the sidecar is `lazy: false`, so it always runs):
//! retention, stale-session finalize, and the evening digest per member once
//! their local `digest_time` has passed. Idempotent; DST-safe through chrono-tz.

use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, NaiveTime, Utc};

use super::{digest, events, finalize, retention, settings};
use crate::{Meeting, MeetingEngine, MeetingStatus};

/// How often the tick runs.
pub const TICK_EVERY: StdDuration = StdDuration::from_secs(300);
/// A recording recap with no chunk for this long is finalized as partial.
pub const STALE_AFTER_MINUTES: i64 = 30;
/// A recap still processing after this long is retried (a crash mid-tail).
const STUCK_AFTER_MINUTES: i64 = 30;

fn parse(ts: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// Run the sweep now and then every [`TICK_EVERY`], forever.
pub fn spawn(engine: MeetingEngine) {
    tokio::spawn(async move {
        loop {
            run_once(&engine, Utc::now()).await;
            tokio::time::sleep(TICK_EVERY).await;
        }
    });
}

/// One tick at `now`.
pub async fn run_once(engine: &MeetingEngine, now: DateTime<Utc>) {
    retention::sweep(engine, now).await;
    finalize_stale(engine, now).await;
    send_due_digests(engine, now).await;
}

/// Whether a recording recap is stale at `now` given its last chunk time.
pub fn is_stale(recap: &Meeting, last_chunk_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    let Some(started) = parse(&recap.started_at) else {
        return false;
    };
    let last = last_chunk_at.unwrap_or(started);
    now - last >= Duration::minutes(STALE_AFTER_MINUTES)
        || now - started >= Duration::milliseconds(super::limits::MAX_SESSION_MS)
}

/// Force-finalize stale sessions (partial) and retry stuck ones.
pub async fn finalize_stale(engine: &MeetingEngine, now: DateTime<Utc>) -> usize {
    let Ok(recaps) = engine.store.all_recaps().await else {
        return 0;
    };
    let mut finalized = 0;
    for recap in recaps {
        match recap.status {
            MeetingStatus::Recording => {
                let pending_confirm = recap
                    .recap
                    .as_ref()
                    .is_some_and(|m| m.needs_confirm && m.confirmed_at.is_none());
                if pending_confirm {
                    continue;
                }
                let chunks = engine
                    .store
                    .recap_chunks(&recap.id)
                    .await
                    .unwrap_or_default();
                let last = chunks.iter().filter_map(|c| parse(&c.received_at)).max();
                if !is_stale(&recap, last, now) {
                    continue;
                }
                let expected = chunks.iter().map(|c| c.seq + 1).max().unwrap_or(0);
                let marked = finalize::mark_processing(engine, &recap.id, expected, true).await;
                if matches!(marked, Ok(Some(_))) {
                    finalize::run_tail(engine, &recap.id).await;
                    finalized += 1;
                }
            }
            MeetingStatus::Processing => {
                let stuck = parse(&recap.updated_at)
                    .is_some_and(|t| now - t >= Duration::minutes(STUCK_AFTER_MINUTES));
                if stuck {
                    finalize::run_tail(engine, &recap.id).await;
                }
            }
            _ => {}
        }
    }
    finalized
}

/// The member's digest zone: their setting, else their newest recap's zone.
async fn owner_tz(
    engine: &MeetingEngine,
    owner: &str,
    member: &settings::RecapSettings,
) -> chrono_tz::Tz {
    if let Some(tz) = member.tz.as_deref().and_then(|t| t.parse().ok()) {
        return tz;
    }
    engine
        .store
        .list_owned_recaps(owner, None, None, None, 1)
        .await
        .ok()
        .and_then(|r| r.into_iter().next())
        .and_then(|r| r.recap.and_then(|m| m.tz.parse().ok()))
        .unwrap_or(chrono_tz::Tz::UTC)
}

/// Build and announce each member's digest for their local today once their
/// `digest_time` has passed. Announced once per day (`notified_at`), and only
/// when that day has at least one finished recap.
pub async fn send_due_digests(engine: &MeetingEngine, now: DateTime<Utc>) -> usize {
    let owners = engine.store.recap_owners().await.unwrap_or_default();
    let mut sent = 0;
    for owner in owners {
        let Ok(member) = settings::load(engine, &owner).await else {
            continue;
        };
        let tz = owner_tz(engine, &owner, &member).await;
        let local = now.with_timezone(&tz);
        let due_at = NaiveTime::parse_from_str(&member.digest_time, "%H:%M")
            .unwrap_or_else(|_| NaiveTime::from_hms_opt(21, 0, 0).unwrap_or_default());
        if local.time() < due_at {
            continue;
        }
        let day = local.date_naive().format("%Y-%m-%d").to_string();
        let existing = engine.store.recap_digest(&owner, &day).await.ok().flatten();
        if existing
            .as_ref()
            .is_some_and(|d| d.json.get(digest::NOTIFIED_AT).is_some())
        {
            continue;
        }
        let Ok(Some(mut row)) = digest::get_or_build(engine, &owner, &day, tz.name()).await else {
            continue;
        };
        row.json[digest::NOTIFIED_AT] = serde_json::json!(now.to_rfc3339());
        if engine.store.put_recap_digest(&row).await.is_err() {
            continue;
        }
        let count = row.json["count"].as_i64().unwrap_or(0);
        events::emit_digest_ready(engine, &owner, &day, count, member.digest_notify).await;
        sent += 1;
    }
    sent
}
