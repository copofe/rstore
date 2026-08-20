//! Repository for bucket ignore rules (IGN-).

use rstore_domain::id::BucketId;
use rstore_domain::workspace::IgnoreRule;
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;

use crate::codec::{unix_secs, uuid_to_text, Insert, RowReader};
use crate::error::map_sqlx;

/// CRUD access to `bucket_ignore_rules`.
#[derive(Clone)]
pub struct IgnoreRuleRepository {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: crate::actor::SqliteWriter,
}

impl IgnoreRuleRepository {
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

    /// All rules for a bucket (IGN-002).
    pub async fn list(&self, bucket: BucketId) -> Result<Vec<IgnoreRule>> {
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

    /// Insert a rule. The directory must already be normalised
    /// ([`IgnoreRule::normalise_directory`]); a duplicate directory is a
    /// `Conflict` (IGN-013).
    pub async fn insert(&self, rule: &IgnoreRule) -> Result<()> {
        let _write = self.writer.acquire().await?;
        Insert::into("bucket_ignore_rules")
            .column("bucket_id", uuid_to_text(&rule.bucket_id))
            .column("directory", &rule.directory)
            .column("enabled", rule.enabled)
            .column("created_at", unix_secs(rule.created_at))
            .execute(&self.pool)
            .await
            .map_err(|e| {
                // map_sqlx already classifies UNIQUE as Conflict; this adds
                // the directory to the message.
                if e.code == rstore_domain::ErrorCode::Conflict {
                    Error::conflict(format!("`{}` is already ignored", rule.directory))
                } else {
                    e
                }
            })?;
        Ok(())
    }

    /// Delete by normalised directory; `true` when a rule was removed (IGN-003).
    pub async fn delete(&self, bucket: BucketId, directory: &str) -> Result<bool> {
        let _write = self.writer.acquire().await?;
        let affected =
            sqlx::query("DELETE FROM bucket_ignore_rules WHERE bucket_id = ? AND directory = ?")
                .bind(uuid_to_text(&bucket))
                .bind(directory)
                .execute(&self.pool)
                .await
                .map_err(map_sqlx)?
                .rows_affected();
        Ok(affected > 0)
    }
}

fn row_to_rule(row: sqlx::sqlite::SqliteRow) -> Result<IgnoreRule> {
    let row = RowReader::new(row);
    Ok(IgnoreRule {
        id: row.integer("id")?,
        bucket_id: rstore_domain::BucketId::from_uuid(row.uuid("bucket_id")?),
        directory: row.text("directory")?,
        enabled: row.bool("enabled")?,
        created_at: row.timestamp("created_at")?,
    })
}
