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

mod meetings;
mod recap_side;
#[cfg(test)]
mod recap_tests;
mod recaps;
mod schema;
#[cfg(test)]
mod seal_tests;
mod segments;
#[cfg(test)]
mod tests;

pub use recap_side::DigestRow;
pub use recaps::{is_constraint_violation, ChunkOutcome, ChunkRow, ChunkStatus, NewChunk};

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

use crate::seal_keys::{sqlite_store_seal, PluginKeys, StoreSeal};
use crate::{Meeting, MeetingEvent};

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
        ColumnPlan::text("recap_digests", "json", "id", KeyKind::Text)?,
        ColumnPlan::text("recap_settings", "json", "owner_user_id", KeyKind::Text)?,
    ])
}

/// A digest row's sealing context (`id` is `<owner>|<day>`).
fn digest_ctx(id: &str) -> ContentContext {
    ContentContext::for_id("recap_digests.json", id)
}

/// A member's recap settings sealing context.
fn settings_ctx(owner_user_id: &str) -> ContentContext {
    ContentContext::for_id("recap_settings.json", owner_user_id)
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
    /// Recap events, tagged with the recap id. Kept off [`Self::subscribe`] so the
    /// shared `/stream` never carries a recap; `/recaps/:id/events` filters it.
    recap_tx: broadcast::Sender<(String, MeetingEvent)>,
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
        let (recap_tx, _recap_rx) = broadcast::channel(256);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            tx,
            recap_tx,
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

    /// Broadcast an event about one recap to its owner-scoped stream only.
    pub fn emit_recap(&self, recap_id: &str, event: MeetingEvent) {
        let _ = self.recap_tx.send((recap_id.to_string(), event));
    }

    /// Subscribe to recap events (`/recaps/:id/events` filters by id after the
    /// owner check).
    pub fn subscribe_recaps(&self) -> broadcast::Receiver<(String, MeetingEvent)> {
        self.recap_tx.subscribe()
    }
}
