//! Meeting notes (Granola / Notion-AI style): record a call, transcribe it live,
//! and generate AI notes when it ends — with automatic detection of an in-progress
//! meeting so notes can start without opening any app.
//!
//! ## Where the pieces live (Core vs sensor)
//! - **Core (this module)** is the brain: it owns the meeting session lifecycle,
//!   the chunked-transcription pipeline (reusing the existing whisper/parakeet
//!   voice path), transcript accumulation, AI note generation (via the gateway),
//!   persistence, and the live SSE event stream. It decides *what runs*.
//! - **Audio capture is a device-bound sensor**, not Core: Core can run on a
//!   remote node, so it must not grab the local machine's audio. Capture (mic +
//!   system loopback) lives in **Shadow**, which streams raw WAV chunks up to
//!   `POST /api/meetings/:id/chunk`. Core only ingests + transcribes them.
//!
//! ## The detection mechanic
//! Granola/Notion do not trigger on "the meeting app is focused" — they watch the
//! OS for *a process actively using the microphone* (Windows
//! `CapabilityAccessManager`; macOS `kAudioDevicePropertyDeviceIsRunningSomewhere`).
//! That OS-level signal is device-local, so Shadow detects it and POSTs to
//! `POST /api/meetings/detect`; Core debounces it and broadcasts a `detected`
//! event, which the island surfaces as a "start notes?" prompt.

pub mod api;
pub mod audio;
pub mod diarize;
mod engine;
mod fanout_messages;
mod model;
pub mod notes;
pub mod recap;
pub mod seal_keys;
pub mod store;
pub mod templates;
#[cfg(test)]
pub(crate) mod test_support;

use std::path::PathBuf;

use async_trait::async_trait;

pub use api::{routes, MeetingsCtx};
pub use engine::{FinalizeGuard, MeetingEngine};
pub use model::{
    Meeting, MeetingEvent, MeetingKind, MeetingSource, MeetingStatus, RecapMeta, Segment,
};
pub use notes::MeetingNotes;
pub use store::MeetingStore;

/// The host contract: the narrow set of Core capabilities the moved meeting code
/// depends on, inverted so this crate never imports `apps/core`. Core implements
/// this with its existing machinery (preferences store, the extracted STT path,
/// the Gateway loopback, the Spaces store, and the shared auto-title model call)
/// and injects `Arc<dyn MeetingsHost>` into the [`MeetingEngine`].
#[async_trait]
pub trait MeetingsHost: Send + Sync {
    /// Read a preference value (`None` when unset or on error).
    async fn pref_get(&self, key: &str) -> Option<String>;
    /// Write a preference value.
    async fn pref_set(&self, key: &str, value: &str) -> Result<(), String>;
    /// The local Gateway base URL for the note-generation completion.
    fn gateway_url(&self) -> String;
    /// The Gateway bearer token, if one is configured.
    fn gateway_token(&self) -> Option<String>;
    /// The fallback notes model when no pref/env is set.
    fn default_notes_model(&self) -> String;
    /// Transcribe one WAV chunk via the extracted STT/media path. Core forwards
    /// to `ryu_stt` through its `CoreSttHost`; the engine here only supplies the
    /// bytes and the optional engine selector.
    async fn transcribe(
        &self,
        wav: Vec<u8>,
        filename: String,
        engine: Option<String>,
    ) -> Result<String, String>;
    /// The route [`MeetingsHost::transcribe`] takes with no engine selector, by
    /// name (recap consent records it, and asks again when it changes).
    fn transcription_route(&self) -> String {
        recap::processors::resolved_transcription_route()
    }
    /// Generate a candidate auto-title from a finalized meeting's summary (the
    /// shared, Core-owned title model call). Returns `None` when unavailable or
    /// the summary is too short. The engine writes the accepted title to the
    /// store itself — this only produces the string.
    async fn generate_title(&self, summary: &str) -> Option<String>;
    /// Save a finalized meeting's notes markdown into the "Meetings" Space (find
    /// or create it, then ingest the document). Returns `(space_id, doc_id)` on
    /// success, `None` on any failure. All Spaces coupling (the store, the
    /// background owner/tenancy) stays Core-side behind this one call.
    async fn save_notes_to_space(&self, title: &str, markdown: &str) -> Option<(String, String)>;
}

/// The crate's data directory (the db + PCM audio live under it): the app's plugin
/// data dir, `<data dir>/plugin-data/@ryu+meetings`. Set once at startup;
/// [`data_dir`] falls back to the system temp dir so unit tests and any pre-init
/// handler never panic.
static DATA_DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Publish the meetings data directory. Idempotent: a second call is ignored.
pub fn init_data_dir(dir: PathBuf) {
    let _ = DATA_DIR.set(dir);
}

/// The meetings data directory, or the system temp dir when uninitialized.
pub(crate) fn data_dir() -> PathBuf {
    DATA_DIR.get().cloned().unwrap_or_else(std::env::temp_dir)
}

/// Process-global meeting engine, set once at startup from `main.rs`.
///
/// Mirrors [`crate::monitors`]: off-`ServerState` callers (e.g. a future
/// scheduled summarizer, or Shadow control proxying) read the engine here.
static ENGINE: std::sync::OnceLock<MeetingEngine> = std::sync::OnceLock::new();

/// Publish the global engine. Idempotent: a second call is ignored.
pub fn set_global_engine(engine: MeetingEngine) {
    let _ = ENGINE.set(engine);
}

/// The global engine, if it has been published.
pub fn global_engine() -> Option<&'static MeetingEngine> {
    ENGINE.get()
}

/// This app's plugin id, and the namespace half of every event id below. It MUST
/// equal `manifest.json`'s `id`: Core rejects an emit whose namespace is not the
/// calling plugin, and rejects any event the manifest does not declare.
const APP_ID: &str = "@ryu/meetings";

/// A meeting record was created and audio ingest began — raised by [`MeetingEngine::start`]
/// and [`MeetingEngine::start_import`].
const EVENT_MEETING_STARTED: &str = "@ryu/meetings#meeting.started";
/// A meeting was finalized (capture stopped, notes generated, auto-title applied) —
/// raised from the finalize tail in [`api`], not from [`MeetingEngine::finalize`],
/// because the title a consumer should show only settles after auto-naming.
pub(crate) const EVENT_MEETING_ENDED: &str = "@ryu/meetings#meeting.ended";
/// The finalized notes were filed into the Meetings Space as an editable document.
pub(crate) const EVENT_NOTES_READY: &str = "@ryu/meetings#notes.ready";
/// A meeting app started using the microphone (a fresh, debounced detection) —
/// raised by [`MeetingEngine::record_detection`].
const EVENT_MEETING_DETECTED: &str = "@ryu/meetings#meeting.detected";

// ── Activity-feed rows ─────────────────────────────────────────────────────────
//
// The app owns its activity wording: each lifecycle event carries an `activity` row
// that Core records on the node's activity feed, attributed to this app. (Core used
// to subscribe to `/api/meetings/stream` and map frames itself.)

/// The activity row for a fresh meeting detection.
pub(crate) fn detected_activity(app: &str, title: &str) -> ryu_app_events::ActivityHint {
    ryu_app_events::ActivityHint::new("meeting", format!("Meeting detected: {title}"))
        .with_metadata(serde_json::json!({ "app": app }))
}

/// The activity row for a started meeting.
pub(crate) fn started_activity(meeting: &Meeting) -> ryu_app_events::ActivityHint {
    ryu_app_events::ActivityHint::new("meeting", format!("Meeting started: {}", meeting.title))
        .with_metadata(serde_json::json!({ "meeting_id": meeting.id }))
}

/// The activity row for a finalized meeting whose notes were generated.
pub(crate) fn notes_ready_activity(meeting: &Meeting) -> ryu_app_events::ActivityHint {
    ryu_app_events::ActivityHint::new("meeting", format!("Meeting notes ready: {}", meeting.title))
        .with_level("success")
        .with_metadata(
            serde_json::json!({ "meeting_id": meeting.id, "space_id": meeting.space_id }),
        )
}

pub(crate) use fanout_messages::notes_ready_notify_hint;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
