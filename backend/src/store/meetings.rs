//! Meeting rows: sealed JSON, field-scoped mutations.

use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};

use super::MeetingStore;
use crate::{Meeting, MeetingNotes, MeetingStatus};

impl MeetingStore {
    /// Insert or replace a meeting definition.
    pub async fn upsert_meeting(&self, meeting: &Meeting) -> Result<()> {
        let json = self.seal_meeting(meeting)?;
        let conn = self.conn.lock().await;
        // `kind` and the recap owner/day/session columns are written on insert only:
        // they never change for a row, and owner-scoped queries rely on them.
        let recap = meeting.recap.as_ref();
        conn.execute(
            "INSERT INTO meetings
                 (id, json, created_at, updated_at, kind, owner_user_id, local_day, client_session_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(id) DO UPDATE SET json = ?2, updated_at = ?4",
            params![
                meeting.id,
                json,
                meeting.created_at,
                meeting.updated_at,
                meeting.kind.as_str(),
                recap.map(|r| r.owner_user_id.as_str()),
                recap.map(|r| r.local_day.as_str()),
                recap.map(|r| r.client_session_id.as_str()),
            ],
        )
        .context("upserting meeting")?;
        Ok(())
    }

    /// Fetch a meeting by id.
    pub async fn get_meeting(&self, id: &str) -> Result<Option<Meeting>> {
        let conn = self.conn.lock().await;
        let json = conn
            .query_row(
                "SELECT json FROM meetings WHERE id = ?1",
                params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading meeting")?;
        match json {
            Some(j) => Ok(Some(self.open_meeting(id, &j)?)),
            None => Ok(None),
        }
    }

    /// List all meetings, newest first.
    ///
    /// Recap rows are never listed here: they are owner scoped and served only by
    /// the `/recaps` routes.
    pub async fn list_meetings(&self) -> Result<Vec<Meeting>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, json FROM meetings WHERE kind != 'recap' ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, json) = row?;
            match self.open_meeting(&id, &json) {
                Ok(meeting) => out.push(meeting),
                Err(error) => self.skip_unreadable("meeting", &error),
            }
        }
        Ok(out)
    }

    /// Read-modify-write one meeting under a single connection lock, so no other
    /// writer can land between the read and the write. `apply` edits the stored
    /// meeting and returns whether it changed anything; only then is the row
    /// rewritten. The write is an `UPDATE`, never an insert, so a meeting deleted
    /// in the meantime stays deleted.
    ///
    /// Every field-scoped mutation goes through here instead of a
    /// `get_meeting` + `upsert_meeting` pair: that pair writes back a whole
    /// snapshot, so a rename (or delete) made between the two calls was silently
    /// undone. Returns the meeting as it now stands plus whether it was written,
    /// or `None` when no such meeting exists.
    pub(super) async fn mutate_meeting<F>(
        &self,
        id: &str,
        apply: F,
    ) -> Result<Option<(Meeting, bool)>>
    where
        F: FnOnce(&mut Meeting) -> bool,
    {
        let conn = self.conn.lock().await;
        let json = conn
            .query_row(
                "SELECT json FROM meetings WHERE id = ?1",
                params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading meeting")?;
        let Some(json) = json else {
            return Ok(None);
        };
        let mut meeting = self.open_meeting(id, &json)?;
        if !apply(&mut meeting) {
            return Ok(Some((meeting, false)));
        }
        let json = self.seal_meeting(&meeting)?;
        conn.execute(
            "UPDATE meetings SET json = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, json, meeting.updated_at],
        )
        .context("updating meeting")?;
        Ok(Some((meeting, true)))
    }

    /// Rename a meeting (manual). Marks the title user-chosen so the background
    /// auto-namer leaves it alone. Returns the updated meeting, or `None` when no
    /// such meeting exists.
    pub async fn set_title(&self, id: &str, title: &str) -> Result<Option<Meeting>> {
        let updated = self
            .mutate_meeting(id, |meeting| {
                meeting.title = title.to_string();
                meeting.title_custom = true;
                meeting.updated_at = chrono::Utc::now().to_rfc3339();
                true
            })
            .await?;
        Ok(updated.map(|(meeting, _)| meeting))
    }

    /// Set (or clear) a meeting's Notion-style glyph. `None` clears it.
    pub async fn set_icon(
        &self,
        id: &str,
        icon: Option<serde_json::Value>,
    ) -> Result<Option<Meeting>> {
        let updated = self
            .mutate_meeting(id, |meeting| {
                meeting.icon = icon;
                meeting.updated_at = chrono::Utc::now().to_rfc3339();
                true
            })
            .await?;
        Ok(updated.map(|(meeting, _)| meeting))
    }

    /// Apply an auto-generated title from the transcript, but only when the user
    /// hasn't chosen one (`title_custom == false`). Does not set `title_custom`,
    /// so an auto title stays replaceable and a later manual rename still locks
    /// it. Returns the updated meeting when it wrote, else `None`.
    pub async fn auto_set_title(&self, id: &str, title: &str) -> Result<Option<Meeting>> {
        let updated = self
            .mutate_meeting(id, |meeting| {
                if meeting.title_custom {
                    return false;
                }
                meeting.title = title.to_string();
                meeting.updated_at = chrono::Utc::now().to_rfc3339();
                true
            })
            .await?;
        Ok(updated.and_then(|(meeting, written)| written.then_some(meeting)))
    }

    /// Set a meeting's lifecycle status, leaving every other stored field as it
    /// is. Returns the updated meeting, or `None` when it no longer exists.
    pub async fn set_status(&self, id: &str, status: MeetingStatus) -> Result<Option<Meeting>> {
        let updated = self
            .mutate_meeting(id, |meeting| {
                meeting.status = status;
                meeting.updated_at = chrono::Utc::now().to_rfc3339();
                true
            })
            .await?;
        Ok(updated.map(|(meeting, _)| meeting))
    }

    /// Undo a finalize's `Processing` mark after note generation failed: move the
    /// meeting back to `status`, but only while it is still `Processing`, so a
    /// writer that already moved it on is not overridden. Returns true when it
    /// wrote.
    pub async fn revert_processing(&self, id: &str, status: MeetingStatus) -> Result<bool> {
        let updated = self
            .mutate_meeting(id, |meeting| {
                if meeting.status != MeetingStatus::Processing {
                    return false;
                }
                meeting.status = status;
                meeting.updated_at = chrono::Utc::now().to_rfc3339();
                true
            })
            .await?;
        Ok(updated.is_some_and(|(_, written)| written))
    }

    /// Record a finalize's result: the generated notes, `Done`, and the end time.
    /// Only those fields (and `updated_at`) change, so a rename, icon, or Space
    /// link written while the notes were being generated is kept. Returns the
    /// finished meeting, or `None` when it was deleted in the meantime (it is not
    /// re-created).
    pub async fn finish_meeting(
        &self,
        id: &str,
        notes: MeetingNotes,
        ended_at: &str,
    ) -> Result<Option<Meeting>> {
        let updated = self
            .mutate_meeting(id, |meeting| {
                meeting.notes = Some(notes);
                meeting.status = MeetingStatus::Done;
                meeting.ended_at = Some(ended_at.to_string());
                meeting.updated_at = ended_at.to_string();
                true
            })
            .await?;
        Ok(updated.map(|(meeting, _)| meeting))
    }

    /// Link a meeting to the Space document its notes were filed into, leaving
    /// every other stored field as it is. Returns the updated meeting, or `None`
    /// when it no longer exists (it is not re-created).
    pub async fn set_space(
        &self,
        id: &str,
        space_id: &str,
        doc_id: &str,
    ) -> Result<Option<Meeting>> {
        let updated = self
            .mutate_meeting(id, |meeting| {
                meeting.space_id = Some(space_id.to_string());
                meeting.doc_id = Some(doc_id.to_string());
                meeting.updated_at = chrono::Utc::now().to_rfc3339();
                true
            })
            .await?;
        Ok(updated.map(|(meeting, _)| meeting))
    }

    /// Delete a meeting and its transcript segments. Returns true when removed.
    pub async fn delete_meeting(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute("DELETE FROM meetings WHERE id = ?1", params![id])?;
        conn.execute("DELETE FROM segments WHERE meeting_id = ?1", params![id])?;
        Ok(n > 0)
    }
}
