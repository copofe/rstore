//! Key/value app settings (`app_settings`) — machine-local state such as the current workspace.
use std::sync::Arc;

use rstore_domain::{Clock, Result};
use sqlx::sqlite::SqlitePool;

use crate::db::codec::{unix_secs, Insert, RowReader};
use crate::db::error::map_sqlx;
use crate::db::tx::WriteGate;

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

impl SettingsRepository {
    /// Read a setting's raw JSON/text value.
    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT value FROM app_settings WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.read_pool)
            .await
            .map_err(map_sqlx)?;
        row.map(|row| RowReader::new(row).text("value")).transpose()
    }

    /// Upsert a setting, stamping `updated_at` from the injected clock.
    pub async fn set(&self, key: &str, value: &str) -> Result<()> {
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

    pub async fn delete(&self, key: &str) -> Result<bool> {
        let affected = sqlx::query("DELETE FROM app_settings WHERE key = ?")
            .bind(key)
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }
}
