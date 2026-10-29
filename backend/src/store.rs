//! SQLite-backed persistence for meeting notes.
//!
//! Two tables live in `<data dir>/plugin-data/@ryu+meetings/meetings.db`:
//!   - `meetings` — one row per recorded/detected meeting (stored as JSON, the
//!     same shape as the REST surface), including the generated notes once
//!     finalized.
//!   - `segments` — the live transcript: one row per transcribed audio chunk,
//!     time-ordered, that accumulates while a meeting records.
//!
//! A broadcast channel fans meeting events (detection, a new transcript segment,
//! status changes, finalized notes) out to SSE subscribers — the desktop
//! Meetings page and the island "start notes?" prompt — mirroring
//! [`crate::monitors::store`].
//!
//! Placement note (Core vs Gateway): this stores *what was said and the notes we
//! derived* — it decides what runs, not what is allowed — so it is Core. Audio
//! *capture* is a device-bound sensor and lives in Shadow; Core only ingests the
//! resulting chunks.
//!
//! At rest (see [`crate::seal_keys`]): `meetings.json` (title, participants,
//! app, generated notes) and `segments.text` / `segments.speaker` are sealed
//! per value (`encb:v1:`, store `meetings.db`; a meeting binds its id, a
//! segment binds its meeting id and row id). Ids, offsets and timestamps stay
//! plaintext. The diarization PCM is one `RYU-SL01` log per meeting (store
//! `meetings-audio`, see [`crate::audio`]); the count of committed records is
//! kept in `meetings.pcm_records`. Legacy plaintext rows are sealed on open (on
//! a background connection for large stores); every connection runs
//! `secure_delete = ON`.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use ryu_crypto::content_sqlite::{
    enable_secure_delete, finish_store, migrate_columns, run_pending_vacuum, ColumnPlan, KeyKind,
};
use ryu_crypto::ContentContext;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

use super::{Meeting, MeetingEvent, MeetingNotes, MeetingStatus, Segment};
use crate::seal_keys::{sqlite_store_seal, PluginKeys, StoreSeal};

/// The registered content-store name of the database (never rename).
pub const STORE: &str = "meetings.db";
/// The registered content-store name of the PCM logs (never rename).
pub const AUDIO_STORE: &str = "meetings-audio";
/// This app's canonical plugin id (the sealing key's scope).
pub const PLUGIN_ID: &str = "@ryu/meetings";
/// Stores with more segments than this migrate on a background connection.
const SYNC_MIGRATION_MAX_ROWS: i64 = 20_000;

fn meeting_ctx(id: &str) -> ContentContext {
    ContentContext::for_id("meetings.json", id)
}

fn segment_ctx(column: &str, meeting_id: &str, id: i64) -> ContentContext {
    ContentContext::new(format!("segments.{column}"))
        .with("meeting_id", meeting_id)
        .with("id", id.to_string())
}

/// The sealed columns, for the open-time migration.
pub fn column_plans() -> Result<Vec<ColumnPlan>> {
    let segment = |column: &'static str| -> Result<ColumnPlan> {
        ColumnPlan::text("segments", column, "id", KeyKind::Integer)?.with_context(
            &["meeting_id"],
            move |row| {
                segment_ctx(
                    column,
                    &row.text("meeting_id"),
                    row.key().parse().unwrap_or(0),
                )
            },
        )
    };
    Ok(vec![
        ColumnPlan::text("meetings", "json", "id", KeyKind::Text)?,
        segment("text")?,
        segment("speaker")?,
    ])
}

/// Below this many segments the migration runs inline at open (tests lower it).
fn sync_migration_max_rows() -> i64 {
    #[cfg(test)]
    if let Some(rows) = TEST_SYNC_MIGRATION_MAX_ROWS.with(std::cell::Cell::get) {
        return rows;
    }
    SYNC_MIGRATION_MAX_ROWS
}

#[cfg(test)]
thread_local! {
    static TEST_SYNC_MIGRATION_MAX_ROWS: std::cell::Cell<Option<i64>> =
        const { std::cell::Cell::new(None) };
}

/// How a migration ends once its values are sealed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Finish {
    /// Inline at open, before the store serves anything: scrub and vacuum now.
    Now,
    /// On the background thread while live transcripts are written: never
    /// scrub or vacuum concurrently. `migrate_columns` already marked the store
    /// `vacuum_pending`; `run_pending_vacuum` compacts it at the next start,
    /// before the store serves requests.
    NextStart,
}

fn migrate_content(
    conn: &mut Connection,
    path: &Path,
    seal: &StoreSeal,
    finish: Finish,
) -> Result<u64> {
    let Some(cipher) = seal.cipher() else {
        return Ok(0);
    };
    let report = migrate_columns(conn, cipher, &column_plans()?)
        .context("sealing legacy meetings.db rows")?;
    if report.unreadable > 0 {
        tracing::warn!(
            count = report.unreadable,
            "meetings.db: values that do not open were left untouched"
        );
    }
    if report.sealed > 0 {
        tracing::info!(count = report.sealed, "meetings.db: sealed legacy values");
        match finish {
            Finish::Now => {
                if let Err(error) = finish_store(conn, path, STORE) {
                    tracing::warn!("meetings.db: scrubbing freed pages deferred: {error:#}");
                }
            }
            Finish::NextStart => tracing::info!(
                "meetings.db: compaction of the freed plaintext pages runs at the next start"
            ),
        }
    }
    Ok(report.sealed)
}

fn default_db_path() -> PathBuf {
    crate::data_dir().join("meetings.db")
}

/// SQLite-backed meeting store. Cheap to clone (wraps `Arc`s).
#[derive(Clone)]
pub struct MeetingStore {
    conn: Arc<Mutex<Connection>>,
    tx: broadcast::Sender<MeetingEvent>,
    seal: StoreSeal,
    audio_seal: StoreSeal,
    unreadable: Arc<AtomicU64>,
}

impl MeetingStore {
    /// Open (or create) the store at the default path (`<plugin data dir>/meetings.db`).
    pub fn open_default() -> Result<Self> {
        Self::open(default_db_path())
    }

    /// Open (or create) the store at a specific path and run migrations,
    /// resolving this process's sealing key with the store's directory as the
    /// plugin data dir (see [`crate::seal_keys`]).
    pub fn open(path: PathBuf) -> Result<Self> {
        let dir = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let keys = PluginKeys::resolve(PLUGIN_ID, &dir)?;
        Self::open_with_keys(path, &keys)
    }

    /// [`Self::open`] with explicit keys.
    pub fn open_with_keys(path: PathBuf, keys: &PluginKeys) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating db dir {}", parent.display()))?;
        }
        let mut conn = Connection::open(&path)
            .with_context(|| format!("opening meetings db {}", path.display()))?;
        enable_secure_delete(&conn)?;
        Self::init_schema(&conn)?;
        let seal = sqlite_store_seal(keys, &conn, STORE)?;
        // The PCM logs derive from the same root, so they share the database's
        // recorded key state.
        let audio_seal = match &seal {
            StoreSeal::Sealing(_) => match keys.content_cipher(AUDIO_STORE) {
                Some(cipher) => StoreSeal::Sealing(cipher),
                None => StoreSeal::Compat,
            },
            other => other.clone(),
        };
        if seal.is_sealing() {
            if let Err(error) = run_pending_vacuum(&conn, &path, STORE) {
                tracing::warn!("meetings.db: deferred compaction failed: {error:#}");
            }
            let segments: i64 =
                conn.query_row("SELECT COUNT(*) FROM segments", [], |row| row.get(0))?;
            if segments <= sync_migration_max_rows() {
                migrate_content(&mut conn, &path, &seal, Finish::Now)?;
            } else {
                let (path, seal) = (path.clone(), seal.clone());
                std::thread::spawn(move || {
                    let run = || -> Result<u64> {
                        let mut conn = Connection::open(&path)?;
                        enable_secure_delete(&conn)?;
                        migrate_content(&mut conn, &path, &seal, Finish::NextStart)
                    };
                    if let Err(error) = run() {
                        tracing::warn!(
                            "meetings.db: background sealing stopped (resumes next start): {error:#}"
                        );
                    }
                });
            }
        }
        let (tx, _rx) = broadcast::channel(256);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            tx,
            seal,
            audio_seal,
            unreadable: Arc::new(AtomicU64::new(0)),
        })
    }

    /// How PCM logs are sealed (shares the database's key state).
    pub fn audio_seal(&self) -> &StoreSeal {
        &self.audio_seal
    }

    /// The sealing health item: `{state, detail?, unreadable_rows}`.
    pub fn seal_health(&self) -> serde_json::Value {
        let mut health = self.seal.health();
        health["unreadable_rows"] = self.unreadable.load(Ordering::Relaxed).into();
        health
    }

    fn skip_unreadable(&self, what: &str, error: &anyhow::Error) {
        let total = self.unreadable.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::warn!(
            total,
            "meetings: skipping a {what} that does not open: {error:#}"
        );
    }

    fn open_meeting(&self, id: &str, stored: &str) -> Result<Meeting> {
        let json = self.seal.open_text(stored, &meeting_ctx(id))?;
        serde_json::from_str(&json).context("deserializing meeting")
    }

    fn seal_meeting(&self, meeting: &Meeting) -> Result<String> {
        let json = serde_json::to_string(meeting).context("serializing meeting")?;
        self.seal.seal_text(&json, &meeting_ctx(&meeting.id))
    }

    /// Record how many PCM records a meeting's audio log has committed (so a
    /// log truncated at a record boundary is detected). Never decreases.
    pub async fn set_pcm_records(&self, id: &str, records: u64) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "UPDATE meetings SET pcm_records = MAX(pcm_records, ?2) WHERE id = ?1",
            params![id, records as i64],
        )
        .context("recording the pcm record count")?;
        Ok(())
    }

    /// The committed PCM record count recorded for a meeting (0 when unknown).
    pub async fn pcm_records(&self, id: &str) -> Result<u64> {
        let conn = self.conn.lock().await;
        let count: Option<i64> = conn
            .query_row(
                "SELECT pcm_records FROM meetings WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(count.unwrap_or(0).max(0) as u64)
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS meetings (
                 id          TEXT PRIMARY KEY,
                 json        TEXT NOT NULL,
                 created_at  TEXT NOT NULL,
                 updated_at  TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS segments (
                 id          INTEGER PRIMARY KEY AUTOINCREMENT,
                 meeting_id  TEXT NOT NULL,
                 t_offset_ms INTEGER NOT NULL,
                 speaker     TEXT,
                 text        TEXT NOT NULL,
                 created_at  TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_segments_meeting
                 ON segments(meeting_id, id);",
        )
        .context("initializing meetings schema")?;
        // Committed record count of the meeting's sealed PCM log.
        if let Err(err) = conn.execute(
            "ALTER TABLE meetings ADD COLUMN pcm_records INTEGER NOT NULL DEFAULT 0",
            [],
        ) {
            if !err.to_string().contains("duplicate column name") {
                return Err(err).context("adding meetings.pcm_records");
            }
        }
        Ok(())
    }

    // ---- meetings ---------------------------------------------------------

    /// Insert or replace a meeting definition.
    pub async fn upsert_meeting(&self, meeting: &Meeting) -> Result<()> {
        let json = self.seal_meeting(meeting)?;
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO meetings (id, json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET json = ?2, updated_at = ?4",
            params![meeting.id, json, meeting.created_at, meeting.updated_at],
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
    pub async fn list_meetings(&self) -> Result<Vec<Meeting>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT id, json FROM meetings ORDER BY created_at DESC")?;
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
    async fn mutate_meeting<F>(&self, id: &str, apply: F) -> Result<Option<(Meeting, bool)>>
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

    // ---- segments ---------------------------------------------------------

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

    // ---- events -----------------------------------------------------------

    /// Broadcast a meeting event to SSE subscribers. A send error just means no
    /// live subscribers — not a failure.
    pub fn emit(&self, event: MeetingEvent) {
        let _ = self.tx.send(event);
    }

    /// Subscribe to live meeting events (used by the SSE endpoint).
    pub fn subscribe(&self) -> broadcast::Receiver<MeetingEvent> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Meeting, MeetingSource, MeetingStatus};

    fn temp_store() -> MeetingStore {
        let dir = std::env::temp_dir().join(format!(
            "ryu-meetings-store-test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        MeetingStore::open(dir.join("meetings.db")).expect("open temp store")
    }

    fn meeting(id: &str, created_at: &str) -> Meeting {
        Meeting {
            id: id.to_string(),
            title: "T".to_string(),
            title_custom: false,
            icon: None,
            app: None,
            source: MeetingSource::Manual,
            status: MeetingStatus::Recording,
            started_at: created_at.to_string(),
            ended_at: None,
            participants: vec![],
            notes: None,
            space_id: None,
            doc_id: None,
            created_at: created_at.to_string(),
            updated_at: created_at.to_string(),
        }
    }

    #[tokio::test]
    async fn upsert_get_roundtrip_and_conflict_update() {
        let store = temp_store();
        let mut m = meeting("m1", "2026-01-01T00:00:00Z");
        store.upsert_meeting(&m).await.unwrap();
        assert_eq!(store.get_meeting("m1").await.unwrap().unwrap().title, "T");

        // Same id ⇒ ON CONFLICT updates the json in place (no duplicate row).
        m.title = "Updated".into();
        store.upsert_meeting(&m).await.unwrap();
        assert_eq!(
            store.get_meeting("m1").await.unwrap().unwrap().title,
            "Updated"
        );
        assert_eq!(store.list_meetings().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn get_missing_meeting_is_none() {
        let store = temp_store();
        assert!(store.get_meeting("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_orders_newest_created_first() {
        let store = temp_store();
        store
            .upsert_meeting(&meeting("old", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        store
            .upsert_meeting(&meeting("new", "2026-06-01T00:00:00Z"))
            .await
            .unwrap();
        let ids: Vec<String> = store
            .list_meetings()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.id)
            .collect();
        assert_eq!(ids, vec!["new".to_string(), "old".to_string()]);
    }

    #[tokio::test]
    async fn set_title_marks_custom_and_missing_is_none() {
        let store = temp_store();
        store
            .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        let updated = store.set_title("m", "Renamed").await.unwrap().unwrap();
        assert_eq!(updated.title, "Renamed");
        assert!(updated.title_custom);
        assert!(store.set_title("missing", "x").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn auto_set_title_respects_custom_guard() {
        let store = temp_store();
        store
            .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        // Not custom ⇒ auto title is written but title_custom stays false.
        let updated = store.auto_set_title("m", "Auto").await.unwrap().unwrap();
        assert_eq!(updated.title, "Auto");
        assert!(!updated.title_custom);

        // A manual rename locks it; a later auto attempt is refused.
        store.set_title("m", "Manual").await.unwrap();
        assert!(store.auto_set_title("m", "Auto2").await.unwrap().is_none());
        assert_eq!(
            store.get_meeting("m").await.unwrap().unwrap().title,
            "Manual"
        );

        // Missing id ⇒ None.
        assert!(store
            .auto_set_title("missing", "x")
            .await
            .unwrap()
            .is_none());
    }

    fn notes(summary: &str) -> MeetingNotes {
        MeetingNotes {
            summary: summary.to_string(),
            ..Default::default()
        }
    }

    /// A rename landing while notes generate must survive the finalize write:
    /// only notes/status/ended_at change, so `title_custom` keeps the auto-namer
    /// off the user's title too.
    #[tokio::test]
    async fn finish_meeting_preserves_concurrent_rename() {
        let store = temp_store();
        store
            .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        store
            .set_status("m", MeetingStatus::Processing)
            .await
            .unwrap();
        store.set_title("m", "Mine").await.unwrap();

        let done = store
            .finish_meeting("m", notes("summary"), "2026-01-01T01:00:00Z")
            .await
            .unwrap()
            .expect("meeting still exists");
        assert_eq!(done.title, "Mine");
        assert!(done.title_custom);
        assert_eq!(done.status, MeetingStatus::Done);
        assert_eq!(done.ended_at.as_deref(), Some("2026-01-01T01:00:00Z"));
        let stored = store.get_meeting("m").await.unwrap().unwrap();
        assert_eq!(stored.title, "Mine");
        assert!(stored.title_custom);
        assert_eq!(stored.notes.unwrap().summary, "summary");
    }

    /// Finishing (or linking) a meeting deleted mid-finalize must not re-create it.
    #[tokio::test]
    async fn finish_meeting_does_not_resurrect_deleted() {
        let store = temp_store();
        store
            .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        assert!(store.delete_meeting("m").await.unwrap());

        assert!(store
            .finish_meeting("m", notes("late"), "2026-01-01T01:00:00Z")
            .await
            .unwrap()
            .is_none());
        assert!(store.set_space("m", "s", "d").await.unwrap().is_none());
        assert!(store
            .set_status("m", MeetingStatus::Processing)
            .await
            .unwrap()
            .is_none());
        assert!(store.get_meeting("m").await.unwrap().is_none());
        assert!(store.list_meetings().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn revert_processing_only_moves_a_processing_meeting() {
        let store = temp_store();
        store
            .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        // Still recording ⇒ nothing to revert.
        assert!(!store
            .revert_processing("m", MeetingStatus::Recording)
            .await
            .unwrap());

        store
            .set_status("m", MeetingStatus::Processing)
            .await
            .unwrap();
        assert!(store
            .revert_processing("m", MeetingStatus::Recording)
            .await
            .unwrap());
        assert_eq!(
            store.get_meeting("m").await.unwrap().unwrap().status,
            MeetingStatus::Recording
        );
        assert!(!store
            .revert_processing("missing", MeetingStatus::Recording)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn delete_cascades_segments_and_reports_removed() {
        let store = temp_store();
        store
            .upsert_meeting(&meeting("m", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        store.insert_segment("m", 0, None, "hi").await.unwrap();
        assert!(store.delete_meeting("m").await.unwrap());
        assert!(store.list_segments("m").await.unwrap().is_empty());
        // Second delete ⇒ nothing removed.
        assert!(!store.delete_meeting("m").await.unwrap());
    }

    #[tokio::test]
    async fn segments_insert_order_and_speaker_update() {
        let store = temp_store();
        let s1 = store.insert_segment("m", 0, None, "first").await.unwrap();
        let s2 = store
            .insert_segment("m", 10, Some("Me"), "second")
            .await
            .unwrap();
        assert!(s2.id > s1.id);
        assert_eq!(s2.speaker.as_deref(), Some("Me"));

        store.set_segment_speaker(s1.id, "Speaker 1").await.unwrap();
        let listed = store.list_segments("m").await.unwrap();
        assert_eq!(listed.len(), 2);
        // Order is by id ASC.
        assert_eq!(listed[0].text, "first");
        assert_eq!(listed[0].speaker.as_deref(), Some("Speaker 1"));
        assert_eq!(listed[1].text, "second");
    }

    #[tokio::test]
    async fn list_segments_scopes_to_meeting() {
        let store = temp_store();
        store.insert_segment("a", 0, None, "a-seg").await.unwrap();
        store.insert_segment("b", 0, None, "b-seg").await.unwrap();
        let a = store.list_segments("a").await.unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].text, "a-seg");
    }

    #[tokio::test]
    async fn emit_reaches_a_live_subscriber() {
        let store = temp_store();
        let mut rx = store.subscribe();
        store.emit(MeetingEvent::Status {
            meeting_id: "m".into(),
            status: MeetingStatus::Processing,
        });
        match rx.try_recv().unwrap() {
            MeetingEvent::Status { meeting_id, status } => {
                assert_eq!(meeting_id, "m");
                assert_eq!(status, MeetingStatus::Processing);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn emit_without_subscribers_is_a_noop() {
        let store = temp_store();
        // No subscriber ⇒ send error is swallowed; must not panic.
        store.emit(MeetingEvent::Detected {
            app: "zoom".into(),
            title: "t".into(),
            detected_at: "now".into(),
        });
    }

    // ── at rest ──────────────────────────────────────────────────────────────

    const SECRET: &str = "zebra-meetings-marker";

    fn keys(n: u8) -> PluginKeys {
        use base64::Engine as _;
        let subkey = base64::engine::general_purpose::STANDARD.encode([n; 32]);
        PluginKeys::sealing(
            ryu_crypto::PluginCipher::from_subkey_b64(PLUGIN_ID, &subkey).unwrap(),
            crate::seal_keys::KeySource::Delivered,
        )
    }

    fn db_path() -> PathBuf {
        std::env::temp_dir()
            .join(format!(
                "ryu-meetings-seal-{}",
                uuid::Uuid::new_v4().simple()
            ))
            .join("meetings.db")
    }

    fn disk_has(path: &Path, needle: &str) -> bool {
        let mut bytes = std::fs::read(path).unwrap_or_default();
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        bytes.extend(std::fs::read(PathBuf::from(wal)).unwrap_or_default());
        bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
    }

    async fn seed(store: &MeetingStore) -> i64 {
        let mut m = meeting("m1", "2026-01-01T00:00:00Z");
        m.title = format!("title {SECRET}");
        store.upsert_meeting(&m).await.unwrap();
        let segment = store
            .insert_segment(
                "m1",
                0,
                Some(&format!("spk {SECRET}")),
                &format!("said {SECRET}"),
            )
            .await
            .unwrap();
        store
            .set_segment_speaker(segment.id, &format!("Me {SECRET}"))
            .await
            .unwrap();
        segment.id
    }

    async fn assert_reads(store: &MeetingStore) {
        assert_eq!(
            store.get_meeting("m1").await.unwrap().unwrap().title,
            format!("title {SECRET}")
        );
        let segments = store.list_segments("m1").await.unwrap();
        assert_eq!(segments[0].text, format!("said {SECRET}"));
        assert_eq!(
            segments[0].speaker.as_deref(),
            Some(format!("Me {SECRET}").as_str())
        );
    }

    #[tokio::test]
    async fn transcripts_and_meetings_are_sealed_on_disk() {
        let path = db_path();
        let store = MeetingStore::open_with_keys(path.clone(), &keys(1)).unwrap();
        seed(&store).await;
        assert_reads(&store).await;
        assert!(!disk_has(&path, SECRET));
        let conn = store.conn.lock().await;
        let cipher = keys(1).content_cipher(STORE).unwrap();
        ryu_crypto::content_sqlite::assert_all_open(&conn, &cipher, &column_plans().unwrap())
            .unwrap();
    }

    #[tokio::test]
    async fn legacy_plaintext_is_migrated_once_and_still_reads() {
        let path = db_path();
        {
            let store = MeetingStore::open_with_keys(path.clone(), &PluginKeys::compat()).unwrap();
            assert_eq!(store.seal_health()["state"], "compat");
            seed(&store).await;
        }
        assert!(disk_has(&path, SECRET));
        let store = MeetingStore::open_with_keys(path.clone(), &keys(2)).unwrap();
        assert_reads(&store).await;
        assert!(!disk_has(&path, SECRET));
        let snapshot = |conn: &Connection| -> Vec<String> {
            let mut stmt = conn
                .prepare("SELECT json FROM meetings UNION ALL SELECT text || speaker FROM segments")
                .unwrap();
            let rows = stmt.query_map([], |row| row.get(0)).unwrap();
            rows.map(Result::unwrap).collect()
        };
        let first = {
            let conn = store.conn.lock().await;
            let cipher = keys(2).content_cipher(STORE).unwrap();
            ryu_crypto::content_sqlite::assert_all_open(&conn, &cipher, &column_plans().unwrap())
                .unwrap();
            snapshot(&conn)
        };
        drop(store);
        let again = MeetingStore::open_with_keys(path, &keys(2)).unwrap();
        assert_eq!(snapshot(&*again.conn.lock().await), first);
    }

    /// What the next start leaves: `ok` once it vacuumed, `vacuum_pending`
    /// only when the disk is too full for a `VACUUM` (the freelist scrub
    /// still ran).
    fn expected_after_pending_vacuum(db: &std::path::Path) -> &'static str {
        let size = std::fs::metadata(db).map(|m| m.len()).unwrap_or(0);
        let needed = size
            .saturating_mul(2)
            .saturating_add(ryu_crypto::content_sqlite::VACUUM_HEADROOM_BYTES);
        match ryu_crypto::sealed_file::ensure_free_space(db.parent().unwrap(), needed) {
            Ok(()) => "ok",
            Err(_) => "vacuum_pending",
        }
    }

    #[tokio::test]
    async fn background_migration_leaves_compaction_to_the_next_start_and_inserts_keep_working() {
        use ryu_crypto::content_sqlite::{item_state, read_store_record};
        let path = db_path();
        {
            let legacy = MeetingStore::open_with_keys(path.clone(), &PluginKeys::compat()).unwrap();
            seed(&legacy).await;
            for index in 1..300 {
                legacy
                    .insert_segment("m1", index, None, &format!("said {SECRET} {index}"))
                    .await
                    .unwrap();
            }
        }
        // Every store counts as large: the migration runs on its own thread.
        super::TEST_SYNC_MIGRATION_MAX_ROWS.with(|rows| rows.set(Some(0)));
        let store = MeetingStore::open_with_keys(path.clone(), &keys(6)).unwrap();
        super::TEST_SYNC_MIGRATION_MAX_ROWS.with(|rows| rows.set(None));
        // Live transcript segments keep landing while it runs.
        for index in 300..340 {
            store
                .insert_segment("m1", index, None, &format!("live {index}"))
                .await
                .unwrap();
        }
        let plans = column_plans().unwrap();
        let watcher = Connection::open(&path).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !plans.iter().all(|plan| {
            item_state(&watcher, STORE, &plan.item())
                .unwrap()
                .as_deref()
                == Some("sealed")
        }) {
            assert!(
                std::time::Instant::now() < deadline,
                "background migration stalled"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(
            read_store_record(&watcher, STORE).unwrap().unwrap().state,
            "vacuum_pending",
            "the background thread never scrubs or vacuums under traffic"
        );
        assert_eq!(store.list_segments("m1").await.unwrap().len(), 340);
        drop(watcher);
        drop(store);
        // The next start compacts before serving.
        let reopened = MeetingStore::open_with_keys(path.clone(), &keys(6)).unwrap();
        let state = read_store_record(&*reopened.conn.lock().await, STORE)
            .unwrap()
            .unwrap()
            .state;
        assert_eq!(state, expected_after_pending_vacuum(&path));
        assert_reads(&reopened).await;
        assert!(
            !disk_has(&path, SECRET),
            "compaction scrubbed the plaintext"
        );
    }

    #[tokio::test]
    async fn a_segment_copied_to_another_row_does_not_open_and_is_skipped() {
        let store = MeetingStore::open_with_keys(db_path(), &keys(3)).unwrap();
        store
            .upsert_meeting(&meeting("m1", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        store.insert_segment("m1", 0, None, "first").await.unwrap();
        store.insert_segment("m1", 1, None, "second").await.unwrap();
        store
            .conn
            .lock()
            .await
            .execute(
                "UPDATE segments SET text = (SELECT text FROM segments ORDER BY id LIMIT 1)
                 WHERE id = (SELECT MAX(id) FROM segments)",
                [],
            )
            .unwrap();
        let segments = store.list_segments("m1").await.unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].text, "first");
        assert_eq!(store.seal_health()["unreadable_rows"], 1);
    }

    #[tokio::test]
    async fn another_key_or_no_key_refuses_writes_and_keeps_data() {
        let path = db_path();
        seed(&MeetingStore::open_with_keys(path.clone(), &keys(4)).unwrap()).await;
        for other in [keys(5), PluginKeys::compat()] {
            let store = MeetingStore::open_with_keys(path.clone(), &other).unwrap();
            assert_eq!(store.seal_health()["state"], "unhealthy");
            assert!(store.insert_segment("m1", 9, None, "x").await.is_err());
            assert!(store.set_title("m1", "x").await.is_err());
            assert!(!store.audio_seal().is_sealing());
        }
        let back = MeetingStore::open_with_keys(path, &keys(4)).unwrap();
        assert_reads(&back).await;
    }
}
