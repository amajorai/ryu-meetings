//! Recap rows and their chunk ledger. Every query is keyed by the plaintext
//! `kind` / `owner_user_id` columns, so a member's request never opens another
//! member's sealed row.

use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};

use super::{segment_ctx, MeetingStore};
use crate::{Meeting, MeetingKind, MeetingStatus, Segment};

/// How a received chunk was handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkStatus {
    /// Transcribed into a segment (or transcribed to empty text).
    Transcribed,
    /// The server's VAD found no speech; nothing reached STT.
    SkippedSilent,
    /// The client marked the chunk silent and sent no audio.
    SkippedClient,
}

impl ChunkStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Transcribed => "transcribed",
            Self::SkippedSilent => "skipped_silent",
            Self::SkippedClient => "skipped_client",
        }
    }
}

/// What [`MeetingStore::record_recap_chunk`] did.
#[derive(Debug, Clone)]
pub enum ChunkOutcome {
    /// The ledger row (and the segment, when the chunk carried text) committed.
    Recorded(Option<Segment>),
    /// The seq already has a ledger row (a concurrent upload won); nothing written.
    Existing(ChunkRow),
    /// The recap row is gone (deleted, purged, discarded or expired meanwhile).
    RecapGone,
    /// The recap no longer accepts audio (finalized, or an unconfirmed fast path).
    RecapClosed,
}

/// One row of the chunk ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRow {
    pub seq: u32,
    pub offset_ms: i64,
    pub duration_ms: i64,
    pub sha256: String,
    pub bytes: i64,
    pub status: String,
    pub segment_id: Option<i64>,
    pub received_at: String,
}

/// A chunk about to be recorded (with its transcript text, when there is one).
pub struct NewChunk<'a> {
    pub seq: u32,
    pub offset_ms: i64,
    pub duration_ms: i64,
    pub sha256: &'a str,
    pub bytes: i64,
    pub status: ChunkStatus,
    pub text: Option<&'a str>,
}

impl MeetingStore {
    /// The kind and recap owner of a row, from plaintext columns only.
    pub async fn kind_and_owner(&self, id: &str) -> Result<Option<(MeetingKind, Option<String>)>> {
        let conn = self.conn.lock().await;
        let row = conn
            .query_row(
                "SELECT kind, owner_user_id FROM meetings WHERE id = ?1",
                params![id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .context("reading meeting kind")?;
        Ok(row.map(|(kind, owner)| (MeetingKind::from_column(&kind), owner)))
    }

    /// The caller's recap with `id`, or `None` when it does not exist, is not a
    /// recap, or belongs to someone else (callers answer all three with 404).
    pub async fn get_owned_recap(&self, owner: &str, id: &str) -> Result<Option<Meeting>> {
        let conn = self.conn.lock().await;
        let json = conn
            .query_row(
                "SELECT json FROM meetings WHERE id = ?1 AND kind = 'recap' AND owner_user_id = ?2",
                params![id, owner],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading recap")?;
        json.map(|json| self.open_meeting(id, &json)).transpose()
    }

    /// The caller's recap started with `client_session_id` (start idempotency).
    pub async fn find_recap_by_session(
        &self,
        owner: &str,
        client_session_id: &str,
    ) -> Result<Option<Meeting>> {
        let conn = self.conn.lock().await;
        let row = conn
            .query_row(
                "SELECT id, json FROM meetings
                 WHERE kind = 'recap' AND owner_user_id = ?1 AND client_session_id = ?2",
                params![owner, client_session_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .context("reading recap by session")?;
        row.map(|(id, json)| self.open_meeting(&id, &json))
            .transpose()
    }

    /// The owner's recaps whose local day is within `[from, to]` (inclusive,
    /// `YYYY-MM-DD`; either bound optional), newest first. `before` is an exclusive
    /// `created_at` cursor.
    pub async fn list_owned_recaps(
        &self,
        owner: &str,
        from: Option<&str>,
        to: Option<&str>,
        before: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Meeting>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, json FROM meetings
             WHERE kind = 'recap' AND owner_user_id = ?1
               AND (?2 IS NULL OR local_day >= ?2)
               AND (?3 IS NULL OR local_day <= ?3)
               AND (?4 IS NULL OR created_at < ?4)
             ORDER BY created_at DESC LIMIT ?5",
        )?;
        let rows = stmt.query_map(params![owner, from, to, before, limit as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, json) = row?;
            match self.open_meeting(&id, &json) {
                Ok(meeting) => out.push(meeting),
                Err(error) => self.skip_unreadable("recap", &error),
            }
        }
        Ok(out)
    }

    /// Every recap row on the node (the retention and tick sweeps).
    pub async fn all_recaps(&self) -> Result<Vec<Meeting>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT id, json FROM meetings WHERE kind = 'recap'")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, json) = row?;
            match self.open_meeting(&id, &json) {
                Ok(meeting) => out.push(meeting),
                Err(error) => self.skip_unreadable("recap", &error),
            }
        }
        Ok(out)
    }

    /// Read-modify-write one recap row (see `mutate_meeting`).
    pub async fn mutate_recap<F>(&self, id: &str, apply: F) -> Result<Option<Meeting>>
    where
        F: FnOnce(&mut Meeting) -> bool,
    {
        Ok(self.mutate_meeting(id, apply).await?.map(|(m, _)| m))
    }

    /// Delete a recap row with its segments and chunk ledger.
    pub async fn delete_recap_rows(&self, id: &str) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        let n = tx.execute(
            "DELETE FROM meetings WHERE id = ?1 AND kind = 'recap'",
            params![id],
        )?;
        tx.execute("DELETE FROM segments WHERE meeting_id = ?1", params![id])?;
        tx.execute(
            "DELETE FROM recap_chunks WHERE meeting_id = ?1",
            params![id],
        )?;
        tx.commit()?;
        Ok(n > 0)
    }

    /// Delete only a recap's transcript segments (segment retention); the notes
    /// and the chunk ledger stay.
    pub async fn delete_recap_segments(&self, id: &str) -> Result<usize> {
        let conn = self.conn.lock().await;
        Ok(conn.execute("DELETE FROM segments WHERE meeting_id = ?1", params![id])?)
    }

    // ---- chunk ledger -------------------------------------------------------

    /// The ledger row for `(id, seq)`.
    pub async fn recap_chunk(&self, id: &str, seq: u32) -> Result<Option<ChunkRow>> {
        let conn = self.conn.lock().await;
        conn.query_row(
            "SELECT seq, offset_ms, duration_ms, sha256, bytes, status, segment_id, received_at
             FROM recap_chunks WHERE meeting_id = ?1 AND seq = ?2",
            params![id, seq],
            chunk_row,
        )
        .optional()
        .context("reading recap chunk")
    }

    /// Every ledger row for a recap, by seq.
    pub async fn recap_chunks(&self, id: &str) -> Result<Vec<ChunkRow>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT seq, offset_ms, duration_ms, sha256, bytes, status, segment_id, received_at
             FROM recap_chunks WHERE meeting_id = ?1 ORDER BY seq ASC",
        )?;
        let rows = stmt.query_map(params![id], chunk_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Record a chunk and, when it carries text, its transcript segment in ONE
    /// transaction, so a crash never leaves a segment without its ledger row (which
    /// would double the text on the client's retry) or the reverse.
    ///
    /// Decode and STT take seconds, so the recap is re-checked INSIDE the
    /// transaction: a recap deleted, finalized or discarded meanwhile gets no
    /// orphan segment (no delete or retention path would ever reach one), and a
    /// seq another upload recorded meanwhile is reported, not duplicated.
    pub async fn record_recap_chunk(&self, id: &str, chunk: NewChunk<'_>) -> Result<ChunkOutcome> {
        self.seal.ensure_writable()?;
        let now = chrono::Utc::now().to_rfc3339();
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        let stored = tx
            .query_row(
                "SELECT json FROM meetings WHERE id = ?1 AND kind = 'recap'",
                params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("re-reading recap")?;
        let Some(stored) = stored else {
            return Ok(ChunkOutcome::RecapGone);
        };
        if !accepts_audio(&self.open_meeting(id, &stored)?) {
            return Ok(ChunkOutcome::RecapClosed);
        }
        if let Some(existing) = tx
            .query_row(
                "SELECT seq, offset_ms, duration_ms, sha256, bytes, status, segment_id, received_at
                 FROM recap_chunks WHERE meeting_id = ?1 AND seq = ?2",
                params![id, chunk.seq],
                chunk_row,
            )
            .optional()
            .context("reading recap chunk")?
        {
            return Ok(ChunkOutcome::Existing(existing));
        }
        let mut segment = None;
        if let Some(text) = chunk.text {
            tx.execute(
                "INSERT INTO segments (meeting_id, t_offset_ms, speaker, text, created_at)
                 VALUES (?1, ?2, NULL, '', ?3)",
                params![id, chunk.offset_ms, now],
            )
            .context("inserting recap segment")?;
            let segment_id = tx.last_insert_rowid();
            let sealed = self
                .seal
                .seal_text(text, &segment_ctx("text", id, segment_id))?;
            tx.execute(
                "UPDATE segments SET text = ?2 WHERE id = ?1",
                params![segment_id, sealed],
            )
            .context("sealing recap segment")?;
            segment = Some(Segment {
                id: segment_id,
                meeting_id: id.to_string(),
                t_offset_ms: chunk.offset_ms,
                speaker: None,
                text: text.to_string(),
                created_at: now.clone(),
            });
        }
        tx.execute(
            "INSERT INTO recap_chunks
                 (meeting_id, seq, offset_ms, duration_ms, sha256, bytes, status, segment_id, received_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                id,
                chunk.seq,
                chunk.offset_ms,
                chunk.duration_ms,
                chunk.sha256,
                chunk.bytes,
                chunk.status.as_str(),
                segment.as_ref().map(|s| s.id),
                now,
            ],
        )
        .context("recording recap chunk")?;
        tx.commit()?;
        Ok(ChunkOutcome::Recorded(segment))
    }

    /// A recap's transcript segments ordered by audio offset (chunks may arrive
    /// out of order, so insertion order is not capture order).
    pub async fn recap_segments(&self, id: &str) -> Result<Vec<Segment>> {
        let mut segments = self.list_segments(id).await?;
        segments.sort_by_key(|s| (s.t_offset_ms, s.id));
        Ok(segments)
    }
}

/// Whether a stored recap still takes audio: recording, and confirmed when its
/// fast-path start requires it.
fn accepts_audio(recap: &Meeting) -> bool {
    recap.status == MeetingStatus::Recording
        && recap
            .recap
            .as_ref()
            .is_some_and(|meta| !meta.needs_confirm || meta.confirmed_at.is_some())
}

/// Whether an error is (or wraps) a SQLite constraint violation, e.g. the ledger's
/// `(meeting_id, seq)` primary key. Walks the whole chain: `Display` of an
/// `anyhow` error shows only its outermost context.
pub fn is_constraint_violation(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(failure, _))
                if failure.code == rusqlite::ErrorCode::ConstraintViolation
        )
    })
}

fn chunk_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChunkRow> {
    Ok(ChunkRow {
        seq: row.get(0)?,
        offset_ms: row.get(1)?,
        duration_ms: row.get(2)?,
        sha256: row.get(3)?,
        bytes: row.get(4)?,
        status: row.get(5)?,
        segment_id: row.get(6)?,
        received_at: row.get(7)?,
    })
}
