//! Key/value app settings (`app_settings`) — capability cache and other machine-local state.
use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::SettingsStore;
use rstore_domain::{Clock, Error, Result};
use sqlx::sqlite::SqlitePool;

use crate::codec::{unix_secs, Insert, RowReader};
use crate::error::map_sqlx;
use crate::tx::WriteGate;

#[derive(Clone)]
/// Access to the `app_settings` table.
pub struct SettingsRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
    clock: Arc<dyn Clock>,
}

impl SettingsRepository {
    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool, clock: Arc<dyn Clock>) -> Self {
        Self {
            gate,
            read_pool,
            clock,
        }
    }
}

#[async_trait]
impl SettingsStore for SettingsRepository {
    /// Read a setting's raw JSON/text value.
    async fn get(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT value FROM app_settings WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.read_pool)
            .await
            .map_err(map_sqlx)?;
        row.map(|row| RowReader::new(row).text("value")).transpose()
    }

    /// Upsert a setting, stamping `updated_at` from the injected clock.
    async fn set(&self, key: &str, value: &str) -> Result<()> {
        let _write = self.gate.permit().await?;
        Insert::into("app_settings")
            .column("key", key)
            .column("value", value)
            .column("updated_at", unix_secs(self.clock.now()))
            .suffix(
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, \
                                           updated_at = excluded.updated_at",
            )
            .execute(self.gate.pool())
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query("DELETE FROM app_settings WHERE key = ?")
            .bind(key)
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }

    /// Parsed JSON read helper; stored state that fails to parse is a database error, not user input.
    async fn get_json(&self, key: &str) -> Result<Option<serde_json::Value>> {
        // `serde_json` arrives through rstore-domain's dependency tree; sqlx
        // needs no json feature for a TEXT column parsed here.
        match self.get(key).await? {
            None => Ok(None),
            Some(raw) => serde_json::from_str(&raw).map(Some).map_err(|e| {
                Error::database(format!("invalid JSON in setting `{key}`")).with_source(e)
            }),
        }
    }

    async fn set_json(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        let raw = serde_json::to_string(value)
            .map_err(|e| Error::database("serialise setting").with_source(e))?;
        self.set(key, &raw).await
    }
}
