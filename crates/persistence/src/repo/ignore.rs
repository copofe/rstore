//! Repository for bucket ignore rules.
use async_trait::async_trait;
use rstore_application_ports::IgnoreRuleStore;
use rstore_domain::id::BucketId;
use rstore_domain::workspace::{IgnoreRule, IgnoreRuleId};
use rstore_domain::Result;
use sqlx::sqlite::SqlitePool;

use crate::codec::{unix_secs, uuid_to_text, Insert, RowReader};
use crate::error::map_sqlx;
use crate::tx::WriteGate;

#[derive(Clone)]
/// CRUD access to `bucket_ignore_rules`.
pub struct IgnoreRuleRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
}

impl IgnoreRuleRepository {
    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool) -> Self {
        Self { gate, read_pool }
    }
}

#[async_trait]
impl IgnoreRuleStore for IgnoreRuleRepository {
    async fn list(&self, bucket: BucketId) -> Result<Vec<IgnoreRule>> {
        let rows = sqlx::query(
            "SELECT id, bucket_id, directory, enabled, created_at \
             FROM bucket_ignore_rules WHERE bucket_id = ? ORDER BY directory",
        )
        .bind(uuid_to_text(&bucket))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(row_to_rule).collect()
    }

    /// Insert a rule. The directory must already be normalised ([`IgnoreRule::normalise_directory`]);
    /// a duplicate directory is a `Conflict`.
    async fn insert(&self, rule: &IgnoreRule) -> Result<()> {
        let _write = self.gate.permit().await?;
        Insert::into("bucket_ignore_rules")
            .column("bucket_id", uuid_to_text(&rule.bucket_id))
            .column("directory", &rule.directory)
            .column("enabled", rule.enabled)
            .column("created_at", unix_secs(rule.created_at))
            .execute(self.gate.pool())
            .await
                        // map_sqlx classifies UNIQUE as Conflict; the remap adds the directory to the message.
            .map_err(|e| {
                crate::error::with_conflict_message(e, || {
                    format!("`{}` is already ignored", rule.directory)
                })
            })?;
        Ok(())
    }

    /// Delete by normalised directory; `true` when a rule was removed.
    async fn delete(&self, bucket: BucketId, directory: &str) -> Result<bool> {
        let _write = self.gate.permit().await?;
        let affected =
            sqlx::query("DELETE FROM bucket_ignore_rules WHERE bucket_id = ? AND directory = ?")
                .bind(uuid_to_text(&bucket))
                .bind(directory)
                .execute(self.gate.pool())
                .await
                .map_err(map_sqlx)?
                .rows_affected();
        Ok(affected > 0)
    }
}

fn row_to_rule(row: sqlx::sqlite::SqliteRow) -> Result<IgnoreRule> {
    let row = RowReader::new(row);
    Ok(IgnoreRule {
        id: IgnoreRuleId(row.integer("id")?),
        bucket_id: rstore_domain::BucketId::from_uuid(row.uuid("bucket_id")?),
        directory: row.text("directory")?,
        enabled: row.bool("enabled")?,
        created_at: row.timestamp("created_at")?,
    })
}
