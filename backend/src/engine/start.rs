//! Creating meetings: a live recording (drives Shadow) or a file import.

use crate::{Meeting, MeetingEngine, MeetingEvent, MeetingSource, MeetingStatus};

impl MeetingEngine {
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
            kind: crate::MeetingKind::Meeting,
            recap: None,
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
            kind: crate::MeetingKind::Meeting,
            recap: None,
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
}

/// A friendly default title from the detected app + today's date.
pub(crate) fn default_title(app: Option<&str>) -> String {
    let date = chrono::Local::now().format("%b %-d, %-I:%M %p");
    match app {
        Some(a) if !a.is_empty() => format!("{} meeting — {date}", pretty_app(a)),
        _ => format!("Meeting — {date}"),
    }
}

/// Title-case a known app slug for display.
pub(crate) fn pretty_app(app: &str) -> String {
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
