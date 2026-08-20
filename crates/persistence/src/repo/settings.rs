//! Key/value app settings (`app_settings` table) — capability cache (STORE-022)
//! and other machine-local state.

use rstore_domain::{Clock, Error, Result, SystemClock};
use sqlx::sqlite::SqlitePool;

use crate::codec::{unix_secs, Insert, RowReader};
use crate::error::map_sqlx;

/// Access to the `app_settings` table.
#[derive(Clone)]
pub struct SettingsRepository {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: crate::actor::SqliteWriter,
}

impl SettingsRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self::new_with_pools(pool.clone(), pool, crate::actor::SqliteWriter::new(64))
    }

    pub fn new_with_pools(
        pool: SqlitePool,
        read_pool: SqlitePool,
        writer: crate::actor::SqliteWriter,
    ) -> Self {
        Self {
            pool,
            read_pool,
            writer,
        }
    }

    /// Read a setting's raw JSON/text value.
    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT value FROM app_settings WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.read_pool)
            .await
            .map_err(map_sqlx)?;
        row.map(|row| RowReader::new(row).text("value")).transpose()
    }

    /// Upsert a setting, stamping `updated_at`.
    pub async fn set(&self, key: &str, value: &str) -> Result<()> {
        let _write = self.writer.acquire().await?;
        Insert::into("app_settings")
            .column("key", key)
            .column("value", value)
            .column("updated_at", unix_secs(SystemClock.now()))
            .suffix(
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, \
                                           updated_at = excluded.updated_at",
            )
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        let _write = self.writer.acquire().await?;
        let affected = sqlx::query("DELETE FROM app_settings WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }

    /// Parsed JSON read helper.
    pub async fn get_json(&self, key: &str) -> Result<Option<serde_json::Value>> {
        // `serde_json` is re-exported through rstore-domain's dependency tree;
        // sqlx needs no json feature for a TEXT column we parse ourselves.
        match self.get(key).await? {
            None => Ok(None),
            Some(raw) => serde_json::from_str(&raw).map(Some).map_err(|e| {
                Error::database(format!("invalid JSON in setting `{key}`")).with_source(e)
            }),
        }
    }

    pub async fn set_json(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        let raw = serde_json::to_string(value)
            .map_err(|e| Error::database("serialise setting").with_source(e))?;
        self.set(key, &raw).await
    }
}
