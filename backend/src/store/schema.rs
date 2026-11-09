//! The meetings database schema and its guarded migrations.

use anyhow::{Context, Result};
use rusqlite::Connection;

use super::MeetingStore;

impl MeetingStore {
    pub(super) fn init_schema(conn: &Connection) -> Result<()> {
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
        Self::init_recap_schema(conn)
    }

    /// Recap columns and tables (guarded, idempotent). `kind`, `owner_user_id`,
    /// `local_day` and `client_session_id` are plaintext copies of fields inside the
    /// sealed JSON so owner-scoped queries never open other members' rows.
    fn init_recap_schema(conn: &Connection) -> Result<()> {
        for (column, ddl) in [
            (
                "kind",
                "ALTER TABLE meetings ADD COLUMN kind TEXT NOT NULL DEFAULT 'meeting'",
            ),
            (
                "owner_user_id",
                "ALTER TABLE meetings ADD COLUMN owner_user_id TEXT",
            ),
            (
                "local_day",
                "ALTER TABLE meetings ADD COLUMN local_day TEXT",
            ),
            (
                "client_session_id",
                "ALTER TABLE meetings ADD COLUMN client_session_id TEXT",
            ),
        ] {
            if let Err(err) = conn.execute(ddl, []) {
                if !err.to_string().contains("duplicate column name") {
                    return Err(err).with_context(|| format!("adding meetings.{column}"));
                }
            }
        }
        conn.execute_batch(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_meetings_client_session
                 ON meetings(owner_user_id, client_session_id)
                 WHERE client_session_id IS NOT NULL;
             CREATE INDEX IF NOT EXISTS idx_meetings_kind_owner_day
                 ON meetings(kind, owner_user_id, local_day);
             CREATE TABLE IF NOT EXISTS recap_chunks (
                 meeting_id  TEXT NOT NULL,
                 seq         INTEGER NOT NULL,
                 offset_ms   INTEGER NOT NULL,
                 duration_ms INTEGER NOT NULL,
                 sha256      TEXT NOT NULL,
                 bytes       INTEGER NOT NULL,
                 status      TEXT NOT NULL,
                 segment_id  INTEGER,
                 received_at TEXT NOT NULL,
                 PRIMARY KEY (meeting_id, seq)
             );
             CREATE TABLE IF NOT EXISTS recap_digests (
                 id            TEXT PRIMARY KEY,
                 owner_user_id TEXT NOT NULL,
                 day           TEXT NOT NULL,
                 tz            TEXT NOT NULL,
                 json          TEXT NOT NULL,
                 source_ids    TEXT NOT NULL,
                 generated_at  TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_recap_digests_owner_day
                 ON recap_digests(owner_user_id, day);
             CREATE TABLE IF NOT EXISTS recap_settings (
                 owner_user_id TEXT PRIMARY KEY,
                 json          TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS recap_usage (
                 owner_user_id TEXT NOT NULL,
                 day           TEXT NOT NULL,
                 bytes         INTEGER NOT NULL,
                 PRIMARY KEY (owner_user_id, day)
             );",
        )
        .context("initializing recap schema")?;
        Ok(())
    }
}
