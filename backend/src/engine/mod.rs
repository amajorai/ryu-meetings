//! The meeting runtime ([`MeetingEngine`]): session lifecycle, chunk ingest,
//! finalize, detection debounce and the best-effort Shadow capture control.

mod finalize;
pub(crate) mod ingest;
pub(crate) mod shadow;
pub(crate) mod start;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

pub(crate) use start::default_title;

use crate::{
    audio, detected_activity, started_activity, Meeting, MeetingEvent, MeetingStore, MeetingsHost,
    APP_ID, EVENT_MEETING_DETECTED, EVENT_MEETING_STARTED,
};

#[derive(Clone)]
pub struct MeetingEngine {
    pub store: MeetingStore,
    /// Cross-cutting Core capabilities (prefs, STT, gateway, spaces, auto-title),
    /// inverted so this crate never depends on `apps/core`.
    pub(crate) host: Arc<dyn MeetingsHost>,
    pub(crate) http: reqwest::Client,
    /// Raises this app's declared `contributes.hook_events` to Core. Built from the
    /// environment rather than taken as a parameter so the constructor's signature —
    /// and every caller — stays unchanged; outside Core it is simply an emitter whose
    /// every emit no-ops.
    pub(crate) events: ryu_app_events::EventEmitter,
    /// Last detection (app, when) for debouncing repeated mic-in-use signals.
    last_detect: Arc<Mutex<Option<(String, Instant)>>>,
    /// Meetings with a finalize in flight on this process — see
    /// [`MeetingEngine::begin_finalize`].
    finalizing: Arc<std::sync::Mutex<HashSet<String>>>,
    /// Opt-in interim recap summaries, held in memory only while a recap records.
    pub(crate) interim: crate::recap::interim::InterimCache,
}

/// Proof that the holder is the only finalize running for one meeting, taken with
/// [`MeetingEngine::begin_finalize`]. Dropping it — on success, error, or a panic
/// unwinding through the request — releases the meeting for the next finalize.
pub struct FinalizeGuard {
    finalizing: Arc<std::sync::Mutex<HashSet<String>>>,
    meeting_id: String,
}

impl FinalizeGuard {
    /// The meeting this guard claims.
    pub fn meeting_id(&self) -> &str {
        &self.meeting_id
    }
}

impl Drop for FinalizeGuard {
    fn drop(&mut self) {
        self.finalizing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.meeting_id);
    }
}

impl MeetingEngine {
    pub fn new(store: MeetingStore, host: Arc<dyn MeetingsHost>, http: reqwest::Client) -> Self {
        Self {
            store,
            host,
            // Reuse the caller's client (it is an `Arc` internally) instead of building
            // a second connection pool for a handful of loopback POSTs.
            events: ryu_app_events::EventEmitter::with_client(APP_ID, http.clone()),
            http,
            last_detect: Arc::new(Mutex::new(None)),
            finalizing: Arc::new(std::sync::Mutex::new(HashSet::new())),
            interim: crate::recap::interim::InterimCache::default(),
        }
    }

    /// Claim the right to finalize `meeting_id`. Returns `None` while another
    /// finalize of the same meeting is still running (a double-click, or a client
    /// retrying a slow request), so two note generations — and two Space
    /// documents — cannot race. Hold the guard across the whole finalize,
    /// including the auto-title and Space filing that follow
    /// [`Self::finalize`]. Finalizing again *after* the first one finished stays
    /// allowed: it regenerates the notes.
    pub fn begin_finalize(&self, meeting_id: &str) -> Option<FinalizeGuard> {
        let inserted = self
            .finalizing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(meeting_id.to_string());
        inserted.then(|| FinalizeGuard {
            finalizing: Arc::clone(&self.finalizing),
            meeting_id: meeting_id.to_string(),
        })
    }

    /// Announce a started meeting to the rest of the node.
    ///
    /// This is deliberately *next to* — not instead of — the `MeetingEvent::Started`
    /// SSE broadcast at each call site. They travel to different places: the SSE
    /// stream drives this node's own UI (desktop + island), while the app event
    /// crosses the plugin boundary to hooks and event-triggered workflows that this
    /// crate knows nothing about. Best-effort, like every emit.
    async fn emit_meeting_started(&self, meeting: &Meeting) {
        self.events
            .emit_with_activity(
                EVENT_MEETING_STARTED,
                serde_json::json!({
                    "meeting_id": meeting.id,
                    "title": meeting.title,
                    "app": meeting.app,
                    "source": meeting.source,
                    "started_at": meeting.started_at,
                }),
                started_activity(meeting),
            )
            .await;
    }

    /// Read a preference through the host (used by the config endpoints).
    pub async fn pref_get(&self, key: &str) -> Option<String> {
        self.host.pref_get(key).await
    }

    /// Write a preference through the host (used by the config endpoints).
    pub async fn pref_set(&self, key: &str, value: &str) -> Result<(), String> {
        self.host.pref_set(key, value).await
    }

    /// The fallback notes model when nothing is configured.
    pub fn default_notes_model(&self) -> String {
        self.host.default_notes_model()
    }

    /// Auto-title a finalized meeting from its summary (best-effort): ask the host
    /// for a candidate title, then write it to the store — which leaves a
    /// user-chosen title alone (`auto_set_title` returns `None` in that case).
    /// Returns the new title when one was applied.
    pub async fn auto_title(&self, meeting_id: &str, summary: &str) -> Option<String> {
        let title = self.host.generate_title(summary).await?;
        match self.store.auto_set_title(meeting_id, &title).await {
            Ok(Some(_)) => Some(title),
            _ => None,
        }
    }

    /// Save a finalized meeting's notes markdown into the Meetings Space via the
    /// host. Returns `(space_id, doc_id)` on success.
    pub async fn save_notes_to_space(
        &self,
        title: &str,
        markdown: &str,
    ) -> Option<(String, String)> {
        self.host.save_notes_to_space(title, markdown).await
    }

    /// Record a mic-in-use detection from Shadow and broadcast it — debounced so a
    /// continuously-running call doesn't spam the prompt. Returns true when the
    /// event was broadcast (i.e. it was a fresh detection).
    pub async fn record_detection(&self, app: &str, title: Option<&str>) -> bool {
        const DEBOUNCE: Duration = Duration::from_secs(120);
        {
            let mut last = self.last_detect.lock().await;
            if let Some((prev_app, when)) = last.as_ref() {
                if prev_app == app && when.elapsed() < DEBOUNCE {
                    return false;
                }
            }
            *last = Some((app.to_string(), Instant::now()));
        }
        let title = title
            .map(str::to_string)
            .unwrap_or_else(|| default_title(Some(app)));
        let detected_at = chrono::Utc::now().to_rfc3339();
        self.store.emit(MeetingEvent::Detected {
            app: app.to_string(),
            title: title.clone(),
            detected_at: detected_at.clone(),
        });
        // Only fresh detections reach here (the debounce returned above), so the event
        // and its activity row fire once per call, not once per mic-in-use signal.
        self.events
            .emit_with_activity(
                EVENT_MEETING_DETECTED,
                serde_json::json!({ "app": app, "title": title, "detected_at": detected_at }),
                detected_activity(app, &title),
            )
            .await;
        true
    }

    pub async fn list(&self) -> Result<Vec<Meeting>, String> {
        self.store.list_meetings().await.map_err(|e| e.to_string())
    }

    pub async fn get(&self, id: &str) -> Result<Option<Meeting>, String> {
        self.store.get_meeting(id).await.map_err(|e| e.to_string())
    }

    pub async fn delete(&self, id: &str) -> Result<bool, String> {
        // Best-effort: drop the persisted diarization audio alongside the row.
        audio::remove_pcm(id);
        self.store
            .delete_meeting(id)
            .await
            .map_err(|e| e.to_string())
    }
}
