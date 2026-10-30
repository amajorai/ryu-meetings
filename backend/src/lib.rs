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
pub mod seal_keys;
pub mod store;
pub mod templates;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, PoisonError};
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

/// The Inbox notice raised when a meeting's notes are filed to a Space. Core keeps
/// notifications in plaintext (`notify.db`) and the meeting title is sealed, so the
/// text names no meeting: the Companion opens the sealed meeting by id.
pub(crate) fn notes_ready_notify_hint() -> ryu_app_events::NotifyHint {
    ryu_app_events::NotifyHint::info(
        "Meeting notes ready",
        Some("Filed to your Spaces.".to_string()),
    )
    .with_level("success")
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
///
/// Core also passes the token file's path (`SHADOW_API_TOKEN_FILE`). The file
/// is read on every call and wins over the env value, because Core can rotate
/// the token after this sidecar was spawned.
fn shadow_token() -> Option<String> {
    resolve_shadow_token(
        std::env::var_os("SHADOW_API_TOKEN_FILE"),
        std::env::var("SHADOW_API_TOKEN").ok(),
    )
}

/// The token from the file at `file` when it is readable and non-empty, else
/// `env_token`. Either value is trimmed; an empty one counts as absent.
fn resolve_shadow_token(
    file: Option<std::ffi::OsString>,
    env_token: Option<String>,
) -> Option<String> {
    let non_empty = |value: String| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    };
    file.and_then(|path| std::fs::read_to_string(path).ok())
        .and_then(non_empty)
        .or_else(|| env_token.and_then(non_empty))
}

/// Recent successful Shadow peer proofs (see [`send_to_shadow`]).
static SHADOW_PEER_PROOF: ryu_sidecar_runtime::peer_proof::ProofCache =
    ryu_sidecar_runtime::peer_proof::ProofCache::new(Duration::from_secs(2));

/// Send a capture-control request to Shadow with its bearer — but only after
/// the listener proved it holds the token (`ryu_sidecar_runtime::peer_proof`),
/// so a process squatting on Shadow's port gets neither the token nor the
/// request. Without a token nothing is sent (Shadow would 401 it anyway).
async fn send_to_shadow(request: reqwest::RequestBuilder) -> Result<reqwest::Response, String> {
    let token = shadow_token().ok_or_else(|| "no Shadow API token is configured".to_owned())?;
    ryu_sidecar_runtime::peer_proof::send_with_verified_bearer(request, &token, &SHADOW_PEER_PROOF)
        .await
        .map_err(|error| error.to_string())
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
    /// Optional Notion-style glyph (`GlyphValue` JSON from `@ryu/ui`). Null =
    /// use the Meetings section fallback icon.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<serde_json::Value>,
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
            icon: None,
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
        self.emit_meeting_started(&meeting).await;
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
            icon: None,
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
        // An import is still a meeting starting — a consumer that reacts to
        // `meeting.started` should see file-imported meetings too, and `app: "import"`
        // is how it tells them apart. `meeting.ended` follows within the same request.
        self.emit_meeting_started(&meeting).await;
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
                    match audio::append_pcm(self.store.audio_seal(), meeting_id, &stereo) {
                        Ok(0) => {}
                        Ok(records) => {
                            if let Err(e) = self.store.set_pcm_records(meeting_id, records).await {
                                tracing::debug!("meetings: recording the audio length failed: {e:#}");
                            }
                        }
                        Err(e) => tracing::debug!("meetings: persisting chunk audio failed: {e:#}"),
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

    /// Finalize the meeting `claim` holds: stop Shadow capture, generate notes
    /// from the transcript via the gateway (`model`/`effort`/`prompt` resolved by
    /// the caller), persist them, and mark the meeting done. Returns the updated
    /// meeting.
    ///
    /// Taking the [`FinalizeGuard`] (from [`Self::begin_finalize`]) means two
    /// finalizes of one meeting never overlap. Every write is field-scoped, so a
    /// rename made while the notes generate is kept, and a meeting deleted
    /// meanwhile is not re-created. If note generation fails the meeting goes back
    /// to the status it had, so the caller can retry.
    pub async fn finalize(
        &self,
        claim: &FinalizeGuard,
        model: &str,
        effort: &str,
        prompt: &str,
    ) -> Result<Meeting, String> {
        debug_assert!(
            Arc::ptr_eq(&claim.finalizing, &self.finalizing),
            "finalize guard taken from a different engine"
        );
        let meeting_id = claim.meeting_id();
        let prior_status = self
            .store
            .get_meeting(meeting_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("meeting '{meeting_id}' not found"))?
            .status;

        // Stop device-local capture first (best-effort).
        self.notify_shadow_stop(meeting_id).await;

        let _ = self
            .store
            .set_status(meeting_id, MeetingStatus::Processing)
            .await;
        self.store.emit(MeetingEvent::Status {
            meeting_id: meeting_id.to_string(),
            status: MeetingStatus::Processing,
        });

        let notes = match self
            .generate_meeting_notes(meeting_id, model, effort, prompt)
            .await
        {
            Ok(notes) => notes,
            Err(e) => {
                // Hand the meeting back rather than leaving it stuck in Processing:
                // the GUI only offers "Stop & summarize" on a recording meeting.
                // A stale Processing (an earlier crash mid-finalize) reverts to
                // Recording for the same reason.
                let restored = match prior_status {
                    MeetingStatus::Processing => MeetingStatus::Recording,
                    other => other,
                };
                if matches!(
                    self.store.revert_processing(meeting_id, restored).await,
                    Ok(true)
                ) {
                    self.store.emit(MeetingEvent::Status {
                        meeting_id: meeting_id.to_string(),
                        status: restored,
                    });
                }
                return Err(e);
            }
        };

        let now = chrono::Utc::now().to_rfc3339();
        let meeting = self
            .store
            .finish_meeting(meeting_id, notes, &now)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("meeting '{meeting_id}' was deleted during finalize"))?;
        self.store.emit(MeetingEvent::Finalized {
            meeting: meeting.clone(),
        });
        Ok(meeting)
    }

    /// The notes for a meeting's current transcript: a placeholder when nothing
    /// was captured, otherwise a gateway note-generation call.
    async fn generate_meeting_notes(
        &self,
        meeting_id: &str,
        model: &str,
        effort: &str,
        prompt: &str,
    ) -> Result<MeetingNotes, String> {
        let transcript = self.transcript(meeting_id).await?;
        if transcript.trim().is_empty() {
            // Nothing was captured — record empty notes rather than calling a model
            // on an empty transcript.
            return Ok(MeetingNotes {
                summary: "No speech was captured for this meeting.".to_string(),
                ..Default::default()
            });
        }
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
        .await
    }

    /// Link a finalized meeting to the Space document its notes were saved into,
    /// so the GUI can open the editable notes in the existing Spaces editor.
    /// Persists the linkage (field-scoped, so it never re-creates a meeting
    /// deleted meanwhile) and re-broadcasts the meeting. Returns the updated
    /// meeting, or an error if it no longer exists.
    pub async fn attach_space(
        &self,
        meeting_id: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Meeting, String> {
        let meeting = self
            .store
            .set_space(meeting_id, space_id, doc_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("meeting '{meeting_id}' not found"))?;
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

    // ---- Shadow capture control (best-effort) -----------------------------

    async fn notify_shadow_start(&self, meeting_id: &str) {
        let url = format!("{}/meeting/start", shadow_url().trim_end_matches('/'));
        let body = serde_json::json!({ "meeting_id": meeting_id });
        let req = self
            .http
            .post(&url)
            .timeout(Duration::from_secs(5))
            .json(&body);
        if let Err(e) = send_to_shadow(req).await {
            tracing::debug!(
                "meetings: shadow capture start not available ({e}); GUI mic can still feed chunks"
            );
        }
    }

    async fn notify_shadow_stop(&self, meeting_id: &str) {
        let url = format!("{}/meeting/stop", shadow_url().trim_end_matches('/'));
        let body = serde_json::json!({ "meeting_id": meeting_id });
        let req = self
            .http
            .post(&url)
            .timeout(Duration::from_secs(5))
            .json(&body);
        let _ = send_to_shadow(req).await;
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
    use std::sync::atomic::{AtomicUsize, Ordering};
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
        /// How many times `save_notes_to_space` ran (shared, so a test can keep a
        /// handle after the host moves into the engine).
        pub space_calls: Arc<AtomicUsize>,
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
                space_calls: Arc::new(AtomicUsize::new(0)),
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
            self.space_calls.fetch_add(1, Ordering::SeqCst);
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
        assert!(
            !m.title_custom,
            "empty title stays eligible for auto-naming"
        );
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
        assert_eq!(
            seg.t_offset_ms, 4200,
            "explicit offset wins over wall-clock"
        );
        // Persisted stereo PCM is readable back as a WAV (diarization audio).
        let records = engine.store.pcm_records(&m.id).await.unwrap();
        let pcm = audio::read_pcm_as_wav(engine.store.audio_seal(), &m.id, records).unwrap();
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
            .ingest_chunk(
                "mtg_missing",
                stereo_wav_16k(10),
                "c.wav".into(),
                None,
                None,
            )
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
        engine
            .store
            .insert_segment(&m.id, 0, None, "one")
            .await
            .unwrap();
        engine
            .store
            .insert_segment(&m.id, 10, None, "two")
            .await
            .unwrap();
        assert_eq!(engine.transcript(&m.id).await.unwrap(), "one\ntwo");
    }

    /// Claim and finalize in one step (the guard drops on return).
    async fn finalize_now(
        engine: &MeetingEngine,
        id: &str,
        effort: &str,
    ) -> Result<Meeting, String> {
        let claim = engine.begin_finalize(id).expect("no finalize in flight");
        engine.finalize(&claim, "model", effort, "prompt").await
    }

    #[tokio::test]
    async fn finalize_empty_transcript_writes_placeholder() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        let done = finalize_now(&engine, &m.id, "").await.unwrap();
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
        let err = finalize_now(&engine, &m.id, "low").await.unwrap_err();
        assert!(err.contains("gateway"), "got: {err}");
    }

    /// A failed note generation must not strand the meeting in Processing: the
    /// GUI only offers "Stop & summarize" (the retry) on a recording meeting.
    #[tokio::test]
    async fn finalize_gateway_error_reverts_to_recording() {
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
        let mut rx = engine.store.subscribe();
        assert!(finalize_now(&engine, &m.id, "low").await.is_err());

        let stored = engine.get(&m.id).await.unwrap().unwrap();
        assert_eq!(stored.status, MeetingStatus::Recording);
        assert!(stored.notes.is_none());
        let mut statuses = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let MeetingEvent::Status { status, .. } = event {
                statuses.push(status);
            }
        }
        assert_eq!(
            statuses,
            vec![MeetingStatus::Processing, MeetingStatus::Recording]
        );
        // The claim was released, so the user can retry.
        assert!(engine.begin_finalize(&m.id).is_some());
    }

    #[tokio::test]
    async fn finalize_unknown_meeting_errors() {
        let engine = engine_with(FakeHost::default());
        let err = finalize_now(&engine, "nope", "").await.unwrap_err();
        assert!(err.contains("not found"));
    }

    /// One finalize per meeting at a time: a second claim is refused until the
    /// first guard drops, and claims on other meetings are independent.
    #[tokio::test]
    async fn begin_finalize_refuses_an_overlapping_claim() {
        let engine = engine_with(FakeHost::default());
        let first = engine.begin_finalize("mtg_a").expect("first claim");
        assert_eq!(first.meeting_id(), "mtg_a");
        assert!(engine.begin_finalize("mtg_a").is_none());
        // Clones share the in-flight set (handlers hold clones of one engine).
        assert!(engine.clone().begin_finalize("mtg_a").is_none());
        assert!(engine.begin_finalize("mtg_b").is_some());
        drop(first);
        assert!(engine.begin_finalize("mtg_a").is_some());
    }

    /// Finalizing a finished meeting again regenerates its notes (documented:
    /// finalizing is not one-shot) without touching the user's title.
    #[tokio::test]
    async fn refinalize_keeps_user_title() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        finalize_now(&engine, &m.id, "").await.unwrap();
        engine.store.set_title(&m.id, "Mine").await.unwrap();
        let again = finalize_now(&engine, &m.id, "").await.unwrap();
        assert_eq!(again.status, MeetingStatus::Done);
        assert_eq!(again.title, "Mine");
        assert!(again.title_custom);
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
        assert_eq!(
            engine.get(&m.id).await.unwrap().unwrap().title,
            "Q3 planning"
        );
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

    /// Filing finishes after the meeting row was deleted ⇒ the link fails rather
    /// than re-creating the deleted meeting.
    #[tokio::test]
    async fn attach_space_does_not_resurrect_a_deleted_meeting() {
        let engine = engine_with(FakeHost::default());
        let m = engine
            .start(String::new(), None, MeetingSource::Manual)
            .await
            .unwrap();
        assert!(engine.delete(&m.id).await.unwrap());
        assert!(engine.attach_space(&m.id, "s", "d").await.is_err());
        assert!(engine.get(&m.id).await.unwrap().is_none());
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
        engine
            .start("a".into(), None, MeetingSource::Manual)
            .await
            .unwrap();
        engine
            .start("b".into(), None, MeetingSource::Manual)
            .await
            .unwrap();
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
    /// Core's `notify.db` is not sealed, so the notes-ready notice must carry no
    /// meeting content (the title is sealed in `meetings.json`). Every notice the
    /// HTTP layer raises goes through the generic helper.
    #[test]
    fn notes_ready_notice_carries_no_sealed_field() {
        let hint = serde_json::to_value(super::notes_ready_notify_hint()).unwrap();
        assert_eq!(hint["title"], "Meeting notes ready");
        assert_eq!(hint["body"], "Filed to your Spaces.");
        assert_eq!(hint["level"], "success");
        let api = include_str!("api.rs");
        assert!(
            !api.contains("NotifyHint::info("),
            "api.rs must raise notices through notes_ready_notify_hint, not inline text"
        );
    }

    #[test]
    fn lifecycle_activity_rows_match_the_feed_wording() {
        let detected = serde_json::to_value(super::detected_activity("zoom", "Zoom call")).unwrap();
        assert_eq!(detected["kind"], "meeting");
        assert_eq!(detected["title"], "Meeting detected: Zoom call");
        assert_eq!(detected["metadata"]["app"], "zoom");
        assert!(detected.get("level").is_none());
    }
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
        assert!(
            t.starts_with("Meeting — "),
            "empty app ⇒ generic title: {t}"
        );
    }

    #[test]
    fn shadow_url_defaults_to_loopback() {
        // With RYU_SHADOW_URL unset in the test env, the default loopback is used.
        if std::env::var_os("RYU_SHADOW_URL").is_none() {
            assert_eq!(shadow_url(), "http://127.0.0.1:3030");
        }
    }

    /// A listener on Shadow's port that records each request's path and
    /// `Authorization`. With `proof_token`, `/health` answers the peer challenge
    /// like a real Shadow holding that token; without, it is an impostor.
    async fn spawn_recording_shadow(
        proof_token: Option<&'static str>,
    ) -> (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<(String, Option<String>)>>>,
    ) {
        use std::sync::{Arc, Mutex};
        let seen: Arc<Mutex<Vec<(String, Option<String>)>>> = Arc::default();
        let record = Arc::clone(&seen);
        let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
            let record = Arc::clone(&record);
            async move {
                let auth = req
                    .headers()
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                record
                    .lock()
                    .unwrap()
                    .push((req.uri().path().to_owned(), auth));
                let challenge = req
                    .uri()
                    .query()
                    .unwrap_or_default()
                    .split('&')
                    .find_map(|pair| pair.strip_prefix("challenge="))
                    .unwrap_or_default()
                    .to_owned();
                let proof = proof_token
                    .and_then(|token| ryu_sidecar_runtime::peer_proof::proof(token, &challenge));
                axum::Json(serde_json::json!({ "status": "healthy", "proof": proof }))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), seen)
    }

    #[test]
    fn shadow_token_prefers_the_current_token_file_over_the_spawn_env() {
        let path = std::env::temp_dir().join(format!(
            "ryu-meetings-shadow-token-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::write(&path, "  rotated-token\n").unwrap();
        assert_eq!(
            resolve_shadow_token(Some(path.clone().into()), Some("spawn-token".into())),
            Some("rotated-token".to_owned())
        );
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            resolve_shadow_token(Some(path.into()), Some(" spawn-token ".into())),
            Some("spawn-token".to_owned())
        );
        assert_eq!(resolve_shadow_token(None, None), None);
    }

    #[tokio::test]
    async fn shadow_capture_control_reaches_only_a_verified_shadow() {
        let prev = std::env::var("SHADOW_API_TOKEN").ok();
        std::env::set_var("SHADOW_API_TOKEN", "meetings-token");
        let client = reqwest::Client::new();

        // An impostor gets the challenge only: no bearer, no capture request.
        let (impostor, seen) = spawn_recording_shadow(None).await;
        let request = client.post(format!("{impostor}/meeting/start"));
        assert!(send_to_shadow(request).await.is_err());
        let impostor_seen = seen.lock().unwrap().clone();
        assert!(impostor_seen
            .iter()
            .all(|(path, auth)| path == "/health" && auth.is_none()));

        // The real Shadow proves the token and receives the bearer.
        let (shadow, seen) = spawn_recording_shadow(Some("meetings-token")).await;
        let request = client.post(format!("{shadow}/meeting/start"));
        assert!(send_to_shadow(request).await.is_ok());
        assert_eq!(
            seen.lock().unwrap().last(),
            Some(&(
                "/meeting/start".to_owned(),
                Some("Bearer meetings-token".to_owned())
            ))
        );

        match prev {
            Some(v) => std::env::set_var("SHADOW_API_TOKEN", v),
            None => std::env::remove_var("SHADOW_API_TOKEN"),
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
