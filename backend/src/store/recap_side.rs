//! Per-member recap side tables: sealed settings, sealed daily digests, and the
//! daily upload byte counter.

use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};

use super::{digest_ctx, settings_ctx, MeetingStore};

/// A stored daily digest.
#[derive(Debug, Clone, PartialEq)]
pub struct DigestRow {
    pub owner_user_id: String,
    pub day: String,
    pub tz: String,
    pub json: serde_json::Value,
    /// The recap ids the digest was built from (sorted, comma-joined).
    pub source_ids: String,
    pub generated_at: String,
}

fn digest_id(owner: &str, day: &str) -> String {
    format!("{owner}|{day}")
}

impl MeetingStore {
    /// A member's recap settings JSON, if any were saved.
    pub async fn recap_settings(&self, owner: &str) -> Result<Option<serde_json::Value>> {
        let conn = self.conn.lock().await;
        let stored = conn
            .query_row(
                "SELECT json FROM recap_settings WHERE owner_user_id = ?1",
                params![owner],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading recap settings")?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        let json = self.seal.open_text(&stored, &settings_ctx(owner))?;
        Ok(Some(
            serde_json::from_str(&json).context("parsing recap settings")?,
        ))
    }

    /// Replace a member's recap settings JSON.
    pub async fn put_recap_settings(&self, owner: &str, value: &serde_json::Value) -> Result<()> {
        let sealed = self
            .seal
            .seal_text(&value.to_string(), &settings_ctx(owner))?;
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO recap_settings (owner_user_id, json) VALUES (?1, ?2)
             ON CONFLICT(owner_user_id) DO UPDATE SET json = ?2",
            params![owner, sealed],
        )
        .context("writing recap settings")?;
        Ok(())
    }

    /// Every member that has recap settings or recap rows (the tick's work list).
    pub async fn recap_owners(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT owner_user_id FROM recap_settings
             UNION SELECT owner_user_id FROM meetings
                 WHERE kind = 'recap' AND owner_user_id IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ---- digests ------------------------------------------------------------

    /// The owner's digest for `day`.
    pub async fn recap_digest(&self, owner: &str, day: &str) -> Result<Option<DigestRow>> {
        let id = digest_id(owner, day);
        let conn = self.conn.lock().await;
        let row = conn
            .query_row(
                "SELECT tz, json, source_ids, generated_at FROM recap_digests WHERE id = ?1",
                params![id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .context("reading recap digest")?;
        let Some((tz, stored, source_ids, generated_at)) = row else {
            return Ok(None);
        };
        let json = self.seal.open_text(&stored, &digest_ctx(&id))?;
        Ok(Some(DigestRow {
            owner_user_id: owner.to_string(),
            day: day.to_string(),
            tz,
            json: serde_json::from_str(&json).context("parsing recap digest")?,
            source_ids,
            generated_at,
        }))
    }

    /// Insert or replace the owner's digest for its day.
    pub async fn put_recap_digest(&self, row: &DigestRow) -> Result<()> {
        let id = digest_id(&row.owner_user_id, &row.day);
        let sealed = self
            .seal
            .seal_text(&row.json.to_string(), &digest_ctx(&id))?;
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO recap_digests (id, owner_user_id, day, tz, json, source_ids, generated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET tz = ?4, json = ?5, source_ids = ?6, generated_at = ?7",
            params![
                id,
                row.owner_user_id,
                row.day,
                row.tz,
                sealed,
                row.source_ids,
                row.generated_at
            ],
        )
        .context("writing recap digest")?;
        Ok(())
    }

    /// Delete the owner's digest for `day`. Returns true when one existed.
    pub async fn delete_recap_digest(&self, owner: &str, day: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "DELETE FROM recap_digests WHERE id = ?1",
            params![digest_id(owner, day)],
        )?;
        Ok(n > 0)
    }

    /// Delete the owner's digests for days before `day` (digest retention).
    pub async fn delete_recap_digests_before(&self, owner: &str, day: &str) -> Result<usize> {
        let conn = self.conn.lock().await;
        Ok(conn.execute(
            "DELETE FROM recap_digests WHERE owner_user_id = ?1 AND day < ?2",
            params![owner, day],
        )?)
    }

    // ---- usage ----------------------------------------------------------------

    /// Bytes the owner uploaded on `day` (UTC).
    pub async fn recap_usage(&self, owner: &str, day: &str) -> Result<i64> {
        let conn = self.conn.lock().await;
        let bytes = conn
            .query_row(
                "SELECT bytes FROM recap_usage WHERE owner_user_id = ?1 AND day = ?2",
                params![owner, day],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        Ok(bytes.unwrap_or(0))
    }

    /// Add to the owner's byte count for `day`; returns the new total.
    pub async fn add_recap_usage(&self, owner: &str, day: &str, bytes: i64) -> Result<i64> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO recap_usage (owner_user_id, day, bytes) VALUES (?1, ?2, ?3)
             ON CONFLICT(owner_user_id, day) DO UPDATE SET bytes = bytes + ?3",
            params![owner, day, bytes],
        )?;
        Ok(conn.query_row(
            "SELECT bytes FROM recap_usage WHERE owner_user_id = ?1 AND day = ?2",
            params![owner, day],
            |row| row.get::<_, i64>(0),
        )?)
    }

    /// Remove the owner's digests only (`DELETE /recaps`: the recaps are gone, so
    /// their digests go too, while settings, consent and the day's upload usage
    /// stay — deleting recaps must not reset the quota).
    pub async fn purge_recap_digests(&self, owner: &str) -> Result<usize> {
        let conn = self.conn.lock().await;
        Ok(conn.execute(
            "DELETE FROM recap_digests WHERE owner_user_id = ?1",
            params![owner],
        )?)
    }

    /// Remove every side row the owner has (settings, digests, usage) — the
    /// per-member purge. Recap rows are deleted separately (with their segments).
    pub async fn purge_recap_side_rows(&self, owner: &str) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "DELETE FROM recap_settings WHERE owner_user_id = ?1",
            params![owner],
        )?;
        conn.execute(
            "DELETE FROM recap_digests WHERE owner_user_id = ?1",
            params![owner],
        )?;
        conn.execute(
            "DELETE FROM recap_usage WHERE owner_user_id = ?1",
            params![owner],
        )?;
        Ok(())
    }
}
