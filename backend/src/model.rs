//! The meeting records the REST API, the store and the SSE stream share.

use serde::{Deserialize, Serialize};

use crate::MeetingNotes;

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
    /// A recap started in the phone app.
    Phone,
    /// A recap recorded on a watch.
    Watch,
    /// A recap started from the iPhone Action Button (needs confirmation).
    ActionButton,
    /// A recap started from an App Shortcut or Siri.
    Shortcut,
    /// A recap started from a Control Center / Quick Settings control (needs
    /// confirmation).
    Control,
    /// A recap recorded on a Wear OS watch.
    Wear,
    /// A recap started from a reminder notification's Start action.
    Reminder,
    /// A capture started by a hardware device.
    Hardware,
}

impl MeetingSource {
    /// Parse a wire value; anything unknown falls back to [`MeetingSource::Manual`].
    pub fn parse(value: &str) -> Self {
        match value.trim() {
            "auto" => Self::Auto,
            "phone" => Self::Phone,
            "watch" => Self::Watch,
            "action_button" => Self::ActionButton,
            "shortcut" => Self::Shortcut,
            "control" => Self::Control,
            "wear" => Self::Wear,
            "reminder" => Self::Reminder,
            "hardware" => Self::Hardware,
            _ => Self::Manual,
        }
    }

    /// A start that bypasses the in-app acknowledgement (a hardware button or a
    /// system control) and must be confirmed within the confirm window.
    pub fn is_fast_path(self) -> bool {
        matches!(self, Self::ActionButton | Self::Control)
    }
}

/// What a row records. Legacy rows (no `kind`) are meetings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeetingKind {
    #[default]
    Meeting,
    /// A user-started recap session captured by a phone or watch client. Owner
    /// scoped: never visible through the legacy meeting routes or the shared SSE
    /// stream.
    Recap,
    /// Ambient hardware capture.
    Ambient,
}

impl MeetingKind {
    /// The plaintext `meetings.kind` column value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Meeting => "meeting",
            Self::Recap => "recap",
            Self::Ambient => "ambient",
        }
    }

    /// Parse the `meetings.kind` column (unknown values read as a meeting).
    pub fn from_column(value: &str) -> Self {
        match value {
            "recap" => Self::Recap,
            "ambient" => Self::Ambient,
            _ => Self::Meeting,
        }
    }
}

/// Recap-only session metadata. Present exactly when `kind == recap`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RecapMeta {
    /// The member who owns the recap (`local` for the owner of a local node).
    pub owner_user_id: String,
    /// The client's idempotency key for the start call.
    pub client_session_id: String,
    /// The IANA zone the session was recorded in.
    pub tz: String,
    /// The owner's local calendar day (`YYYY-MM-DD`) the session started on.
    pub local_day: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,
    /// Kept by the owner past the notes retention window.
    #[serde(default)]
    pub pinned: bool,
    /// The fast-path start must be confirmed before audio is accepted.
    #[serde(default)]
    pub needs_confirm: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_at: Option<String>,
    /// The chunk count the client declared at finalize.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_chunks: Option<u32>,
    /// Finalized with chunks missing (forced, stale or expired on the device).
    #[serde(default)]
    pub partial: bool,
    /// The summed duration of the received chunks.
    #[serde(default)]
    pub duration_ms: i64,
    /// Closed without a model summary because the owner's consent was withdrawn
    /// or no longer covered the transcription or summary route. The transcript
    /// is kept for the owner; no text reached a model.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unsummarized: bool,
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
    /// What the row records; legacy rows read as a meeting.
    #[serde(default)]
    pub kind: MeetingKind,
    /// Recap session metadata (`kind == recap` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recap: Option<RecapMeta>,
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
