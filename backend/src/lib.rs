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
pub mod notes;
pub mod store;
pub mod templates;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

pub use api::{routes, MeetingsCtx};
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

/// The crate's data directory (defaults + PCM audio live under it). Set once at
/// startup from Core (`ryu_dir()`); [`data_dir`] falls back to the system temp
/// dir so unit tests and any pre-init handler never panic.
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

/// Default Shadow base URL Core uses to drive device-local capture. Overridable
/// via `RYU_SHADOW_URL` (the "nothing hardcoded" knob); Shadow is the local
/// sensor, so this stays loopback by default.
fn shadow_url() -> String {
    std::env::var("RYU_SHADOW_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "http://127.0.0.1:3030".to_string())
}

/// The shared-secret bearer Shadow's HTTP surface requires (everything except
/// `/health` — see `apps/shadow/src/server.rs`). Read from `SHADOW_API_TOKEN`,
/// the SAME env var Shadow itself resolves: Core injects it at sidecar spawn
/// (`manifest_sidecar::inject_ext_env`), and an operator export works for a
/// standalone run. `None` = send no bearer; Shadow then 401s and the
/// best-effort capture-control calls degrade exactly like an absent Shadow.
fn shadow_token() -> Option<String> {
    std::env::var("SHADOW_API_TOKEN")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

/// A meeting's lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeetingStatus {
    /// Auto-detected but the user has not started notes yet (transient; only used
    /// when we choose to persist a detection).
    Detected,
    /// Actively recording + transcribing.
    Recording,
    /// Recording stopped; notes are being generated.
    Processing,
    /// Finished, notes available.
    Done,
}

/// How a meeting came to exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeetingSource {
    /// The user pressed record.
    #[default]
    Manual,
    /// Started from an auto-detection prompt.
    Auto,
}

/// A recorded meeting (the unit the REST API and GUIs deal with).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meeting {
    pub id: String,
    pub title: String,
    /// Whether the title was chosen by the user (manual rename) rather than the
    /// default/auto-generated one. Gates the background auto-namer the same way
    /// `title_custom` does for conversations: once the user renames a meeting,
    /// auto-naming from the transcript leaves it alone. Defaults false so older
    /// rows (and freshly-started meetings with a default title) stay eligible.
    #[serde(default)]
    pub title_custom: bool,
    /// The detected meeting app (e.g. `zoom`, `teams`), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app: Option<String>,
    #[serde(default)]
    pub source: MeetingSource,
    pub status: MeetingStatus,
    pub started_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    /// Free-form participant labels (optional; diarization is future work).
    #[serde(default)]
    pub participants: Vec<String>,
    /// Generated notes, present once finalized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<MeetingNotes>,
    /// The Space the finalized notes were saved into (reuses the Spaces feature
    /// for storage + editing). Set on finalize; `None` until then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
    /// The Space document holding the editable notes markdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// One transcribed audio chunk in a meeting's live transcript.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub id: i64,
    pub meeting_id: String,
    /// Milliseconds from the meeting start (best-effort ordering hint).
    pub t_offset_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaker: Option<String>,
    pub text: String,
    pub created_at: String,
}

/// Live events broadcast over SSE to the desktop + island.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MeetingEvent {
    /// A meeting was detected as starting (a process is using the mic). Carries
    /// no meeting id — the client decides whether to start notes.
    Detected {
        app: String,
        title: String,
        detected_at: String,
    },
    /// A meeting started recording.
    Started { meeting: Meeting },
    /// A new transcript segment was appended.
    Segment { segment: Segment },
    /// A meeting's status changed (e.g. recording → processing).
    Status {
        meeting_id: String,
        status: MeetingStatus,
    },
    /// A meeting was finalized with notes.
    Finalized { meeting: Meeting },
}

/// The meeting runtime: holds the store, an HTTP client (for transcription proxy,
/// note-gen, and driving Shadow capture), and a small in-memory detection
/// debounce. Cheap to clone.
#[derive(Clone)]
pub struct MeetingEngine {
    pub store: MeetingStore,
    /// Cross-cutting Core capabilities (prefs, STT, gateway, spaces, auto-title),
    /// inverted so this crate never depends on `apps/core`.
    pub(crate) host: Arc<dyn MeetingsHost>,
    http: reqwest::Client,
    /// Last detection (app, when) for debouncing repeated mic-in-use signals.
    last_detect: Arc<Mutex<Option<(String, Instant)>>>,
}

impl MeetingEngine {
    pub fn new(store: MeetingStore, host: Arc<dyn MeetingsHost>, http: reqwest::Client) -> Self {
        Self {
            store,
            host,
            http,
            last_detect: Arc::new(Mutex::new(None)),
        }
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

    /// Start a new meeting: persist it as `recording` and best-effort tell Shadow
    /// to begin device-local capture (mic + system loopback) streaming chunks
    /// back to `POST /api/meetings/:id/chunk`. Returns the created meeting.
    pub async fn start(
        &self,
        title: String,
        app: Option<String>,
        source: MeetingSource,
    ) -> Result<Meeting, String> {
        let now = chrono::Utc::now().to_rfc3339();
        // A user-supplied start title counts as custom (no auto-rename); an empty
        // one falls back to the app default and stays eligible for auto-naming
        // once the transcript gives the model something to work with.
        let user_titled = !title.trim().is_empty();
        let meeting = Meeting {
            id: format!("mtg_{}", uuid::Uuid::new_v4().simple()),
            title: if user_titled {
                title
            } else {
                default_title(app.as_deref())
            },
            title_custom: user_titled,
            app,
            source,
            status: MeetingStatus::Recording,
            started_at: now.clone(),
            ended_at: None,
            participants: Vec::new(),
            notes: None,
            space_id: None,
            doc_id: None,
            created_at: now.clone(),
            updated_at: now,
        };
        self.store
            .upsert_meeting(&meeting)
            .await
            .map_err(|e| e.to_string())?;
        self.store.emit(MeetingEvent::Started {
            meeting: meeting.clone(),
        });
        // Best-effort: drive Shadow capture. A missing/absent Shadow must not fail
        // the meeting — the user can also feed chunks from the GUI mic.
        self.notify_shadow_start(&meeting.id).await;
        Ok(meeting)
    }

    /// Create a meeting for an **imported** audio file. Like [`Self::start`] but it
    /// does not drive Shadow capture — the audio comes from the uploaded file,
    /// which the caller feeds through [`Self::ingest_chunk`] window by window.
    pub async fn start_import(&self, title: String) -> Result<Meeting, String> {
        let now = chrono::Utc::now().to_rfc3339();
        let user_titled = !title.trim().is_empty();
        let meeting = Meeting {
            id: format!("mtg_{}", uuid::Uuid::new_v4().simple()),
            title: if user_titled {
                title
            } else {
                default_title(None)
            },
            title_custom: user_titled,
            app: Some("import".to_string()),
            source: MeetingSource::Manual,
            status: MeetingStatus::Recording,
            started_at: now.clone(),
            ended_at: None,
            participants: Vec::new(),
            notes: None,
            space_id: None,
            doc_id: None,
            created_at: now.clone(),
            updated_at: now,
        };
        self.store
            .upsert_meeting(&meeting)
            .await
            .map_err(|e| e.to_string())?;
        self.store.emit(MeetingEvent::Started {
            meeting: meeting.clone(),
        });
        Ok(meeting)
    }

    /// Ingest one captured audio chunk: downmix it to mono for transcription
    /// (whisper default, or the requested engine), persist the stereo audio for
    /// later diarization, append the text as a transcript segment, and broadcast
    /// it. `offset_ms` is the chunk's sample-accurate position from Shadow; when
    /// absent (e.g. a GUI-mic feed) we fall back to wall-clock since start.
    pub async fn ingest_chunk(
        &self,
        meeting_id: &str,
        wav: Vec<u8>,
        filename: String,
        engine: Option<&str>,
        offset_ms: Option<i64>,
    ) -> Result<Segment, String> {
        let meeting = self
            .store
            .get_meeting(meeting_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("meeting '{meeting_id}' not found"))?;

        // Decode once, then fan out: mono for whisper, stereo (16 kHz) persisted
        // for diarization. A decode failure falls back to sending the raw bytes so
        // an odd WAV still transcribes.
        let mono_wav = match audio::decode_wav(&wav) {
            Ok(decoded) => {
                if decoded.rate == audio::TARGET_RATE {
                    let stereo = audio::to_stereo_i16(&decoded);
                    if let Err(e) = audio::append_pcm(meeting_id, &stereo) {
                        tracing::debug!("meetings: persisting chunk audio failed: {e:#}");
                    }
                }
                audio::to_mono_wav(&decoded).unwrap_or(wav)
            }
            Err(_) => wav,
        };

        let text = self
            .host
            .transcribe(mono_wav, filename, engine.map(str::to_string))
            .await?;
        let text = text.trim().to_string();
        // Whisper emits blank/placeholder text for silence; skip empty segments so
        // the transcript stays clean.
        if text.is_empty() {
            return Err("empty transcription (silence)".to_string());
        }

        let t_offset_ms = offset_ms.unwrap_or_else(|| millis_since(&meeting.started_at));
        let segment = self
            .store
            .insert_segment(meeting_id, t_offset_ms, None, &text)
            .await
            .map_err(|e| e.to_string())?;
        self.store.emit(MeetingEvent::Segment {
            segment: segment.clone(),
        });
        Ok(segment)
    }

    /// The full transcript as one newline-joined string.
    pub async fn transcript(&self, meeting_id: &str) -> Result<String, String> {
        let segments = self
            .store
            .list_segments(meeting_id)
            .await
            .map_err(|e| e.to_string())?;
        Ok(segments
            .into_iter()
            .map(|s| s.text)
            .collect::<Vec<_>>()
            .join("\n"))
    }

    /// Finalize a meeting: stop Shadow capture, generate notes from the transcript
    /// via the gateway (`model`/`effort`/`prompt` resolved by the caller), persist
    /// them, and mark the meeting done. Returns the updated meeting.
    pub async fn finalize(
        &self,
        meeting_id: &str,
        model: &str,
        effort: &str,
        prompt: &str,
    ) -> Result<Meeting, String> {
        let mut meeting = self
            .store
            .get_meeting(meeting_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("meeting '{meeting_id}' not found"))?;

        // Stop device-local capture first (best-effort).
        self.notify_shadow_stop(meeting_id).await;

        meeting.status = MeetingStatus::Processing;
        meeting.updated_at = chrono::Utc::now().to_rfc3339();
        let _ = self.store.upsert_meeting(&meeting).await;
        self.store.emit(MeetingEvent::Status {
            meeting_id: meeting_id.to_string(),
            status: MeetingStatus::Processing,
        });

        let transcript = self.transcript(meeting_id).await?;
        let notes = if transcript.trim().is_empty() {
            // Nothing was captured — record empty notes rather than calling a model
            // on an empty transcript.
            MeetingNotes {
                summary: "No speech was captured for this meeting.".to_string(),
                ..Default::default()
            }
        } else {
            let gateway_url = self.host.gateway_url();
            let gateway_token = self.host.gateway_token();
            notes::generate_notes(
                &self.http,
                &gateway_url,
                gateway_token.as_deref(),
                model,
                effort,
                prompt,
                &transcript,
            )
            .await?
        };

        let now = chrono::Utc::now().to_rfc3339();
        meeting.notes = Some(notes);
        meeting.status = MeetingStatus::Done;
        meeting.ended_at = Some(now.clone());
        meeting.updated_at = now;
        self.store
            .upsert_meeting(&meeting)
            .await
            .map_err(|e| e.to_string())?;
        self.store.emit(MeetingEvent::Finalized {
            meeting: meeting.clone(),
        });
        Ok(meeting)
    }

    /// Link a finalized meeting to the Space document its notes were saved into,
    /// so the GUI can open the editable notes in the existing Spaces editor.
    /// Persists the linkage and re-broadcasts the meeting. Returns the updated
    /// meeting (or the unchanged one if it no longer exists).
    pub async fn attach_space(
        &self,
        meeting_id: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Meeting, String> {
        let mut meeting = self
            .store
            .get_meeting(meeting_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("meeting '{meeting_id}' not found"))?;
        meeting.space_id = Some(space_id.to_string());
        meeting.doc_id = Some(doc_id.to_string());
        meeting.updated_at = chrono::Utc::now().to_rfc3339();
        self.store
            .upsert_meeting(&meeting)
            .await
            .map_err(|e| e.to_string())?;
        self.store.emit(MeetingEvent::Finalized {
            meeting: meeting.clone(),
        });
        Ok(meeting)
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
        self.store.emit(MeetingEvent::Detected {
            app: app.to_string(),
            title: title
                .map(str::to_string)
                .unwrap_or_else(|| default_title(Some(app))),
            detected_at: chrono::Utc::now().to_rfc3339(),
        });
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

    // ---- Shadow capture control (best-effort) -----------------------------

    async fn notify_shadow_start(&self, meeting_id: &str) {
        let url = format!("{}/meeting/start", shadow_url().trim_end_matches('/'));
        let body = serde_json::json!({ "meeting_id": meeting_id });
        let mut req = self.http.post(&url);
        if let Some(token) = shadow_token() {
            req = req.bearer_auth(token);
        }
        if let Err(e) = req
            .timeout(Duration::from_secs(5))
            .json(&body)
            .send()
            .await
        {
            tracing::debug!(
                "meetings: shadow capture start not available ({e}); GUI mic can still feed chunks"
            );
        }
    }

    async fn notify_shadow_stop(&self, meeting_id: &str) {
        let url = format!("{}/meeting/stop", shadow_url().trim_end_matches('/'));
        let body = serde_json::json!({ "meeting_id": meeting_id });
        let mut req = self.http.post(&url);
        if let Some(token) = shadow_token() {
            req = req.bearer_auth(token);
        }
        let _ = req
            .timeout(Duration::from_secs(5))
            .json(&body)
            .send()
            .await;
    }
}

/// A friendly default title from the detected app + today's date.
fn default_title(app: Option<&str>) -> String {
    let date = chrono::Local::now().format("%b %-d, %-I:%M %p");
    match app {
        Some(a) if !a.is_empty() => format!("{} meeting — {date}", pretty_app(a)),
        _ => format!("Meeting — {date}"),
    }
}

/// Title-case a known app slug for display.
fn pretty_app(app: &str) -> String {
    match app.to_lowercase().as_str() {
        "zoom" | "zoom.us" => "Zoom".to_string(),
        "teams" | "ms-teams" | "microsoft teams" => "Teams".to_string(),
        "meet" | "google meet" => "Google Meet".to_string(),
        "slack" => "Slack".to_string(),
        "discord" => "Discord".to_string(),
        "webex" => "Webex".to_string(),
        other => {
            let mut c = other.chars();
            match c.next() {
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                None => other.to_string(),
            }
        }
    }
}

/// Milliseconds between an RFC3339 start time and now (clamped at 0).
fn millis_since(started_at: &str) -> i64 {
    match chrono::DateTime::parse_from_rfc3339(started_at) {
        Ok(start) => {
            let elapsed =
                chrono::Utc::now().signed_duration_since(start.with_timezone(&chrono::Utc));
            elapsed.num_milliseconds().max(0)
        }
        Err(_) => 0,
    }
}

/// Shared test scaffolding (a fake [`MeetingsHost`] + a temp-backed engine) reused
/// by the engine tests here and the handler tests in [`crate::api`].
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// An in-memory, deterministic [`MeetingsHost`]: prefs live in a map, the host
    /// callbacks return whatever the fields say. No network, no env, no Core.
    pub(crate) struct FakeHost {
        pub prefs: StdMutex<HashMap<String, String>>,
        /// What `transcribe` returns for every chunk (whitespace ⇒ silence path).
        pub transcribe_text: String,
        /// What `generate_title` returns (`None` ⇒ auto-title no-op).
        pub title: Option<String>,
        /// What `save_notes_to_space` returns (`None` ⇒ no Space linkage).
        pub space: Option<(String, String)>,
        pub default_model: String,
        /// Points at an unused loopback port so any real note-gen call fails fast.
        pub gateway_url: String,
        pub gateway_token: Option<String>,
        /// Force `pref_set` to error (to exercise the write-failure path).
        pub pref_set_fails: bool,
    }

    impl Default for FakeHost {
        fn default() -> Self {
            Self {
                prefs: StdMutex::new(HashMap::new()),
                transcribe_text: "hello world".to_string(),
                title: None,
                space: None,
                default_model: "test-model".to_string(),
                gateway_url: "http://127.0.0.1:59991".to_string(),
                gateway_token: None,
                pref_set_fails: false,
            }
        }
    }

    #[async_trait]
    impl MeetingsHost for FakeHost {
        async fn pref_get(&self, key: &str) -> Option<String> {
            self.prefs.lock().unwrap().get(key).cloned()
        }
        async fn pref_set(&self, key: &str, value: &str) -> Result<(), String> {
            if self.pref_set_fails {
                return Err("pref write failed".to_string());
            }
            self.prefs
                .lock()
                .unwrap()
                .insert(key.to_string(), value.to_string());
            Ok(())
        }
        fn gateway_url(&self) -> String {
            self.gateway_url.clone()
        }
        fn gateway_token(&self) -> Option<String> {
            self.gateway_token.clone()
        }
        fn default_notes_model(&self) -> String {
            self.default_model.clone()
        }
        async fn transcribe(
            &self,
            _wav: Vec<u8>,
            _filename: String,
            _engine: Option<String>,
        ) -> Result<String, String> {
            Ok(self.transcribe_text.clone())
        }
        async fn generate_title(&self, _summary: &str) -> Option<String> {
            self.title.clone()
        }
        async fn save_notes_to_space(
            &self,
            _title: &str,
            _markdown: &str,
        ) -> Option<(String, String)> {
            self.space.clone()
        }
    }

    /// A fresh SQLite store under a unique temp path (WAL files land in the OS temp
    /// dir; harmless leftovers for a test run).
    pub(crate) fn temp_store() -> MeetingStore {
        let dir = std::env::temp_dir().join(format!(
            "ryu-meetings-test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        MeetingStore::open(dir.join("meetings.db")).expect("open temp meetings store")
    }

    /// Build an engine over a temp store and the given fake host.
    pub(crate) fn engine_with(host: FakeHost) -> MeetingEngine {
        MeetingEngine::new(temp_store(), Arc::new(host), reqwest::Client::new())
    }

    /// A valid interleaved-stereo 16 kHz WAV of `frames` L/R pairs.
    pub(crate) fn stereo_wav_16k(frames: usize) -> Vec<u8> {
        let samples: Vec<i16> = (0..frames).flat_map(|i| [i as i16, -(i as i16)]).collect();
        crate::audio::encode_wav(2, crate::audio::TARGET_RATE, &samples).unwrap()
    }
}

#[cfg(test)]
mod engine_tests {
    use super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn start_defaults_title_and_stays_auto_nameable() {
        let engine = engine_with(FakeHost::default());
        let mut rx = engine.store.subscribe();
        let m = engine
            .start(String::new(), Some("zoom".into()), MeetingSource::Manual)
            .await
            .unwrap();
        assert!(m.id.starts_with("mtg_"));
        assert_eq!(m.status, MeetingStatus::Recording);
        assert!(!m.title_custom, "empty title stays eligible for auto-naming");
        assert!(m.title.starts_with("Zoom meeting — "));
        // Persisted and broadcast.
        assert!(engine.get(&m.id).await.unwrap().is_some());
        match rx.try_recv() {
            Ok(MeetingEvent::Started { meeting }) => assert_eq!(meeting.id, m.id),
            other => panic!("expected Started event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn start_with_user_title_marks_custom() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start("Board sync".into(), None, MeetingSource::Auto)
            .await
            .unwrap();
        assert_eq!(m.title, "Board sync");
        assert!(m.title_custom);
        assert_eq!(m.source, MeetingSource::Auto);
    }

    #[tokio::test]
    async fn start_import_tags_app_and_defaults_title() {
        let engine = engine_with(FakeHost::default());
        let m = engine.start_import(String::new()).await.unwrap();
        assert_eq!(m.app.as_deref(), Some("import"));
        assert!(!m.title_custom);
        assert!(m.title.starts_with("Meeting — "));
    }

    #[tokio::test]
    async fn ingest_chunk_appends_transcribed_segment() {
        let mut host = FakeHost::default();
        host.transcribe_text = "  captured speech  ".into();
        let engine = engine_with(host);
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let wav = stereo_wav_16k(16_000);
        let seg = engine
            .ingest_chunk(&m.id, wav, "chunk.wav".into(), None, Some(4200))
            .await
            .unwrap();
        assert_eq!(seg.text, "captured speech", "text is trimmed");
        assert_eq!(seg.t_offset_ms, 4200, "explicit offset wins over wall-clock");
        // Persisted stereo PCM is readable back as a WAV (diarization audio).
        let pcm = audio::read_pcm_as_wav(&m.id).unwrap();
        assert!(pcm.is_some());
        audio::remove_pcm(&m.id);
    }

    #[tokio::test]
    async fn ingest_chunk_wallclock_offset_when_absent() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let seg = engine
            .ingest_chunk(&m.id, stereo_wav_16k(10), "c.wav".into(), None, None)
            .await
            .unwrap();
        assert!(seg.t_offset_ms >= 0);
    }

    #[tokio::test]
    async fn ingest_chunk_silence_is_an_error() {
        let mut host = FakeHost::default();
        host.transcribe_text = "   ".into();
        let engine = engine_with(host);
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let err = engine
            .ingest_chunk(&m.id, stereo_wav_16k(10), "c.wav".into(), None, Some(0))
            .await
            .unwrap_err();
        assert!(err.contains("silence"), "got: {err}");
    }

    #[tokio::test]
    async fn ingest_chunk_unknown_meeting_errors() {
        let engine = engine_with(FakeHost::default());
        let err = engine
            .ingest_chunk("mtg_missing", stereo_wav_16k(10), "c.wav".into(), None, None)
            .await
            .unwrap_err();
        assert!(err.contains("not found"), "got: {err}");
    }

    #[tokio::test]
    async fn transcript_joins_segments_in_order() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        engine.store.insert_segment(&m.id, 0, None, "one").await.unwrap();
        engine.store.insert_segment(&m.id, 10, None, "two").await.unwrap();
        assert_eq!(engine.transcript(&m.id).await.unwrap(), "one\ntwo");
    }

    #[tokio::test]
    async fn finalize_empty_transcript_writes_placeholder() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let done = engine.finalize(&m.id, "model", "", "prompt").await.unwrap();
        assert_eq!(done.status, MeetingStatus::Done);
        assert!(done.ended_at.is_some());
        let notes = done.notes.expect("notes present");
        assert!(notes.summary.contains("No speech"));
    }

    #[tokio::test]
    async fn finalize_with_transcript_but_unreachable_gateway_errors() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        engine
            .store
            .insert_segment(&m.id, 0, None, "we shipped the release")
            .await
            .unwrap();
        // gateway_url points at an unused loopback port ⇒ generate_notes errors.
        let err = engine.finalize(&m.id, "model", "low", "prompt").await.unwrap_err();
        assert!(err.contains("gateway"), "got: {err}");
    }

    #[tokio::test]
    async fn finalize_unknown_meeting_errors() {
        let engine = engine_with(FakeHost::default());
        let err = engine.finalize("nope", "m", "", "p").await.unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn record_detection_debounces_same_app() {
        let engine = engine_with(FakeHost::default());
        assert!(engine.record_detection("zoom", Some("Standup")).await);
        // Same app within the debounce window ⇒ suppressed.
        assert!(!engine.record_detection("zoom", None).await);
        // A different app is a fresh detection.
        assert!(engine.record_detection("teams", None).await);
    }

    #[tokio::test]
    async fn auto_title_applies_only_when_not_custom() {
        let mut host = FakeHost::default();
        host.title = Some("Q3 planning".into());
        let engine = engine_with(host);
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let applied = engine.auto_title(&m.id, "long enough summary text").await;
        assert_eq!(applied.as_deref(), Some("Q3 planning"));
        assert_eq!(engine.get(&m.id).await.unwrap().unwrap().title, "Q3 planning");
    }

    #[tokio::test]
    async fn auto_title_leaves_user_titled_alone() {
        let mut host = FakeHost::default();
        host.title = Some("Model pick".into());
        let engine = engine_with(host);
        let m = engine
            .start("My title".into(), None, MeetingSource::Manual)
            .await
            .unwrap();
        assert!(engine.auto_title(&m.id, "summary").await.is_none());
        assert_eq!(engine.get(&m.id).await.unwrap().unwrap().title, "My title");
    }

    #[tokio::test]
    async fn auto_title_none_when_host_declines() {
        let engine = engine_with(FakeHost::default()); // title = None
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        assert!(engine.auto_title(&m.id, "summary").await.is_none());
    }

    #[tokio::test]
    async fn attach_space_persists_linkage() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let updated = engine.attach_space(&m.id, "space1", "doc1").await.unwrap();
        assert_eq!(updated.space_id.as_deref(), Some("space1"));
        assert_eq!(updated.doc_id.as_deref(), Some("doc1"));
        let reloaded = engine.get(&m.id).await.unwrap().unwrap();
        assert_eq!(reloaded.doc_id.as_deref(), Some("doc1"));
    }

    #[tokio::test]
    async fn attach_space_unknown_meeting_errors() {
        let engine = engine_with(FakeHost::default());
        assert!(engine.attach_space("nope", "s", "d").await.is_err());
    }

    #[tokio::test]
    async fn delete_removes_meeting() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        assert!(engine.delete(&m.id).await.unwrap());
        assert!(engine.get(&m.id).await.unwrap().is_none());
        // Deleting again reports "not removed".
        assert!(!engine.delete(&m.id).await.unwrap());
    }

    #[tokio::test]
    async fn list_returns_created_meetings() {
        let engine = engine_with(FakeHost::default());
        engine.start("a".into(), None, MeetingSource::Manual).await.unwrap();
        engine.start("b".into(), None, MeetingSource::Manual).await.unwrap();
        assert_eq!(engine.list().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn pref_and_model_pass_through_host() {
        let engine = engine_with(FakeHost::default());
        engine.pref_set("k", "v").await.unwrap();
        assert_eq!(engine.pref_get("k").await.as_deref(), Some("v"));
        assert_eq!(engine.default_notes_model(), "test-model");
    }

    #[tokio::test]
    async fn pref_set_surfaces_host_error() {
        let mut host = FakeHost::default();
        host.pref_set_fails = true;
        let engine = engine_with(host);
        assert!(engine.pref_set("k", "v").await.is_err());
    }

    #[tokio::test]
    async fn save_notes_to_space_passes_through() {
        let mut host = FakeHost::default();
        host.space = Some(("s".into(), "d".into()));
        let engine = engine_with(host);
        assert_eq!(
            engine.save_notes_to_space("t", "# md").await,
            Some(("s".to_string(), "d".to_string()))
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pretty_app_known_and_unknown() {
        assert_eq!(pretty_app("zoom"), "Zoom");
        assert_eq!(pretty_app("google meet"), "Google Meet");
        assert_eq!(pretty_app("acme"), "Acme");
    }

    #[test]
    fn default_title_includes_app() {
        assert!(default_title(Some("zoom")).starts_with("Zoom meeting — "));
        assert!(default_title(None).starts_with("Meeting — "));
    }

    #[test]
    fn millis_since_is_non_negative() {
        let future = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
        assert_eq!(millis_since(&future), 0);
        assert_eq!(millis_since("not-a-date"), 0);
    }

    #[test]
    fn millis_since_past_is_positive() {
        let past = (chrono::Utc::now() - chrono::Duration::seconds(5)).to_rfc3339();
        assert!(millis_since(&past) >= 4_000);
    }

    #[test]
    fn pretty_app_covers_the_slug_table() {
        assert_eq!(pretty_app("zoom.us"), "Zoom");
        assert_eq!(pretty_app("ms-teams"), "Teams");
        assert_eq!(pretty_app("meet"), "Google Meet");
        assert_eq!(pretty_app("slack"), "Slack");
        assert_eq!(pretty_app("discord"), "Discord");
        assert_eq!(pretty_app("webex"), "Webex");
        assert_eq!(pretty_app(""), "");
    }

    #[test]
    fn default_title_without_app_has_no_app_prefix() {
        let t = default_title(Some(""));
        assert!(t.starts_with("Meeting — "), "empty app ⇒ generic title: {t}");
    }

    #[test]
    fn shadow_url_defaults_to_loopback() {
        // With RYU_SHADOW_URL unset in the test env, the default loopback is used.
        if std::env::var_os("RYU_SHADOW_URL").is_none() {
            assert_eq!(shadow_url(), "http://127.0.0.1:3030");
        }
    }

    #[test]
    fn meeting_status_serializes_snake_case() {
        let j = serde_json::to_string(&MeetingStatus::Recording).unwrap();
        assert_eq!(j, "\"recording\"");
        let j = serde_json::to_string(&MeetingSource::Auto).unwrap();
        assert_eq!(j, "\"auto\"");
    }

    #[test]
    fn init_and_read_data_dir_is_a_path() {
        // data_dir() never panics pre-init (falls back to temp dir).
        assert!(data_dir().is_absolute() || data_dir().as_os_str().len() > 0);
    }
}
