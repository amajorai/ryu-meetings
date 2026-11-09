//! Finalize: stop capture, generate notes through the Gateway, persist them.

use std::sync::Arc;

use crate::{
    notes, FinalizeGuard, Meeting, MeetingEngine, MeetingEvent, MeetingNotes, MeetingStatus,
};

impl MeetingEngine {
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
}
