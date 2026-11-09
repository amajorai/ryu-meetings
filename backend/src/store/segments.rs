//! Transcript segments: sealed text and speaker per row.

use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};

use super::{segment_ctx, MeetingStore};
use crate::Segment;

impl MeetingStore {
    /// Append a transcript segment, returning it with its generated id.
    pub async fn insert_segment(
        &self,
        meeting_id: &str,
        t_offset_ms: i64,
        speaker: Option<&str>,
        text: &str,
    ) -> Result<Segment> {
        self.seal.ensure_writable()?;
        let now = chrono::Utc::now().to_rfc3339();
        let id = {
            let mut conn = self.conn.lock().await;
            // The AUTOINCREMENT id is bound into the sealed values, so insert a
            // placeholder and write the sealed values under the new id in the
            // same transaction (no plaintext ever lands).
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO segments (meeting_id, t_offset_ms, speaker, text, created_at)
                 VALUES (?1, ?2, NULL, '', ?3)",
                params![meeting_id, t_offset_ms, now],
            )
            .context("inserting segment")?;
            let id = tx.last_insert_rowid();
            let sealed_text = self
                .seal
                .seal_text(text, &segment_ctx("text", meeting_id, id))?;
            let sealed_speaker = self
                .seal
                .seal_opt(speaker, &segment_ctx("speaker", meeting_id, id))?;
            tx.execute(
                "UPDATE segments SET text = ?2, speaker = ?3 WHERE id = ?1",
                params![id, sealed_text, sealed_speaker],
            )
            .context("sealing segment")?;
            tx.commit()?;
            id
        };
        Ok(Segment {
            id,
            meeting_id: meeting_id.to_string(),
            t_offset_ms,
            speaker: speaker.map(str::to_string),
            text: text.to_string(),
            created_at: now,
        })
    }

    /// Set (or overwrite) a segment's speaker label — used by diarization once the
    /// finished recording has been split into speaker turns.
    pub async fn set_segment_speaker(&self, segment_id: i64, speaker: &str) -> Result<()> {
        let conn = self.conn.lock().await;
        let Some(meeting_id) = conn
            .query_row(
                "SELECT meeting_id FROM segments WHERE id = ?1",
                params![segment_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        else {
            return Ok(());
        };
        let speaker = self
            .seal
            .seal_text(speaker, &segment_ctx("speaker", &meeting_id, segment_id))?;
        conn.execute(
            "UPDATE segments SET speaker = ?2 WHERE id = ?1",
            params![segment_id, speaker],
        )
        .context("updating segment speaker")?;
        Ok(())
    }

    /// All transcript segments for a meeting, in capture order (oldest first).
    pub async fn list_segments(&self, meeting_id: &str) -> Result<Vec<Segment>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, meeting_id, t_offset_ms, speaker, text, created_at
             FROM segments WHERE meeting_id = ?1 ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![meeting_id], |row| {
            Ok(Segment {
                id: row.get(0)?,
                meeting_id: row.get(1)?,
                t_offset_ms: row.get(2)?,
                speaker: row.get(3)?,
                text: row.get(4)?,
                created_at: row.get(5)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            let mut segment = row?;
            let opened = (|| -> Result<()> {
                segment.text = self.seal.open_text(
                    &segment.text,
                    &segment_ctx("text", &segment.meeting_id, segment.id),
                )?;
                segment.speaker = self.seal.open_opt(
                    segment.speaker.as_deref(),
                    &segment_ctx("speaker", &segment.meeting_id, segment.id),
                )?;
                Ok(())
            })();
            match opened {
                Ok(()) => out.push(segment),
                Err(error) => self.skip_unreadable("segment", &error),
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "segments_tests.rs"]
mod tests;
