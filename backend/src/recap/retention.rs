//! The retention sweep. Cheap and idempotent: it runs on sidecar boot, on recap
//! request entry (throttled), and in the five-minute tick.
//!
//! - unconfirmed fast-path recaps are discarded after the confirm window;
//! - transcript segments are deleted `segments_days` after finalize;
//! - recaps (notes included) are deleted `notes_days` after finalize unless pinned;
//! - digests older than `digests_days` are deleted.
//!
//! No recap audio is ever written, so there is no audio to expire.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use super::settings::{self, RecapSettings};
use super::{records, start};
use crate::{Meeting, MeetingEngine, MeetingStatus};

/// What one sweep removed (for logs and tests).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepReport {
    pub unconfirmed_discarded: usize,
    pub transcripts_expired: usize,
    pub recaps_expired: usize,
    pub digests_expired: usize,
}

fn ended_before(recap: &Meeting, days: u32, now: DateTime<Utc>) -> bool {
    let Some(ended) = recap
        .ended_at
        .as_deref()
        .and_then(|e| DateTime::parse_from_rfc3339(e).ok())
    else {
        return false;
    };
    now - ended.with_timezone(&Utc) >= Duration::days(i64::from(days))
}

async fn member_settings<'a>(
    engine: &MeetingEngine,
    cache: &'a mut HashMap<String, RecapSettings>,
    owner: &str,
) -> &'a RecapSettings {
    if !cache.contains_key(owner) {
        let loaded = settings::load(engine, owner).await.unwrap_or_default();
        cache.insert(owner.to_string(), loaded);
    }
    &cache[owner]
}

/// Run one sweep at `now`. Errors are logged; the sweep never fails a caller.
pub async fn sweep(engine: &MeetingEngine, now: DateTime<Utc>) -> SweepReport {
    let mut report = SweepReport::default();
    let recaps = match engine.store.all_recaps().await {
        Ok(recaps) => recaps,
        Err(error) => {
            tracing::warn!("recaps: retention sweep could not list recaps: {error:#}");
            return report;
        }
    };
    let mut cache = HashMap::new();
    for recap in recaps {
        let Some(owner) = recap.recap.as_ref().map(|m| m.owner_user_id.clone()) else {
            continue;
        };
        if recap.status == MeetingStatus::Recording && start::unconfirmed_expired(&recap, now) {
            engine.interim.clear(&recap.id);
            if matches!(engine.store.delete_recap_rows(&recap.id).await, Ok(true)) {
                report.unconfirmed_discarded += 1;
            }
            continue;
        }
        if recap.status != MeetingStatus::Done {
            continue;
        }
        let retention = member_settings(engine, &mut cache, &owner)
            .await
            .retention
            .clone();
        let pinned = recap.recap.as_ref().is_some_and(|m| m.pinned);
        if !pinned && ended_before(&recap, retention.notes_days, now) {
            if matches!(records::delete_recap(engine, &recap).await, Ok(true)) {
                report.recaps_expired += 1;
            }
            continue;
        }
        if ended_before(&recap, retention.segments_days, now) {
            if let Ok(n) = engine.store.delete_recap_segments(&recap.id).await {
                if n > 0 {
                    report.transcripts_expired += 1;
                }
            }
        }
    }
    let owners = engine.store.recap_owners().await.unwrap_or_default();
    for owner in owners {
        let days = member_settings(engine, &mut cache, &owner)
            .await
            .retention
            .digests_days;
        let cutoff = (now - Duration::days(i64::from(days)))
            .format("%Y-%m-%d")
            .to_string();
        if let Ok(n) = engine
            .store
            .delete_recap_digests_before(&owner, &cutoff)
            .await
        {
            report.digests_expired += n;
        }
    }
    if report != SweepReport::default() {
        tracing::info!(?report, "recaps: retention sweep removed expired data");
    }
    report
}
