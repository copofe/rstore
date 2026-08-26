//! SQLite repair repository: the persistence side of the repair lifecycle.
//!
//! One write path ([`RepairRepository::insert`] upserting on the
//! deterministic identity) serves every producer; the outbox dead-letter
//! transaction reuses the same statement in-transaction through
//! [`upsert_repair`], so a repair row is written identically no matter
//! which producer opened it.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{RepairItem, RepairKind, RepairStatus, RepairStore};
use rstore_domain::id::{ReceiptId, RepairId};
use rstore_domain::{Clock, Error, Result};
use sqlx::sqlite::SqlitePool;
use time::OffsetDateTime;

use crate::codec::{unix_secs, RowReader};
use crate::error::map_sqlx;
use crate::tx::WriteGate;

#[derive(Clone)]
pub struct RepairRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
    clock: Arc<dyn Clock>,
}

impl RepairRepository {
    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool, clock: Arc<dyn Clock>) -> Self {
        Self {
            gate,
            read_pool,
            clock,
        }
    }
}

#[async_trait]
impl RepairStore for RepairRepository {
    async fn insert(&self, item: &RepairItem) -> Result<()> {
        let _write = self.gate.permit().await?;
        upsert_repair(self.gate.pool(), item, self.clock.now()).await
    }

    async fn list(&self, limit: u32) -> Result<Vec<RepairItem>> {
        let rows = sqlx::query(
            "SELECT id, event_id, receipt_id, kind, status, attempts, last_error, next_attempt_at, locator_json, operation \
             FROM repair_items WHERE status != 'resolved' \
             ORDER BY next_attempt_at, updated_at, id LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(decode_repair).collect()
    }

    async fn resolve(&self, id: RepairId, at: OffsetDateTime) -> Result<bool> {
        let _write = self.gate.permit().await?;
        let affected =
            sqlx::query("UPDATE repair_items SET status = 'resolved', updated_at = ? WHERE id = ?")
                .bind(unix_secs(at))
                .bind(id.to_string())
                .execute(self.gate.pool())
                .await
                .map_err(map_sqlx)?
                .rows_affected();
        Ok(affected > 0)
    }
}

/// Decodes one `repair_items` row into its port value.
fn decode_repair(row: sqlx::sqlite::SqliteRow) -> Result<RepairItem> {
    let reader = RowReader::new(row);
    Ok(RepairItem {
        id: reader.text("id")?.parse()?,
        event_id: reader
            .opt_text("event_id")?
            .map(|token| token.parse())
            .transpose()
            .map_err(|e| Error::database("decode repair event id").with_source(e))?,
        receipt_id: reader
            .opt_text("receipt_id")?
            .map(|token| token.parse())
            .transpose()?,
        kind: RepairKind::parse(&reader.text("kind")?)?,
        status: RepairStatus::parse(&reader.text("status")?)?,
        attempts: reader.integer("attempts")? as u32,
        last_error: reader.opt_text("last_error")?,
        locator: reader.opt_text("locator_json")?,
        next_attempt_at: reader.timestamp("next_attempt_at")?,
        operation: reader.opt_text("operation")?,
    })
}

/// The single repair write path: an upsert keyed by the deterministic
/// identity, so every producer (including the outbox's own dead-letter)
/// shares one statement, one column set, and one attempts rule — the
/// writer's value wins. Executor-generic so it runs on the pool or inside
/// another store's transaction.
pub(crate) async fn upsert_repair<'e, E>(
    executor: E,
    item: &RepairItem,
    now: OffsetDateTime,
) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query(
        "INSERT INTO repair_items \
         (id, event_id, receipt_id, kind, status, locator_json, operation, attempts, last_error, \
          next_attempt_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET status = excluded.status, \
             event_id = excluded.event_id, receipt_id = excluded.receipt_id, \
             locator_json = excluded.locator_json, operation = excluded.operation, \
             attempts = excluded.attempts, last_error = excluded.last_error, \
             next_attempt_at = excluded.next_attempt_at, updated_at = excluded.updated_at",
    )
    .bind(item.id.to_string())
    .bind(item.event_id.map(|id| id.to_string()))
    .bind(item.receipt_id.as_ref().map(ReceiptId::to_string))
    .bind(item.kind.as_str())
    .bind(item.status.as_str())
    .bind(&item.locator)
    .bind(&item.operation)
    .bind(i64::from(item.attempts))
    .bind(&item.last_error)
    .bind(unix_secs(item.next_attempt_at))
    .bind(unix_secs(now))
    .bind(unix_secs(now))
    .execute(executor)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_application_ports::RepairKind;
    use rstore_domain::id::ReceiptId;

    use crate::Database;

    #[tokio::test]
    async fn insert_upserts_on_the_deterministic_identity() {
        let db = Database::open_in_memory().await.unwrap();
        let repairs = db.repairs();
        let receipt = ReceiptId::new_random();
        let base = RepairItem {
            id: RepairId::from_receipt(receipt),
            event_id: None,
            receipt_id: Some(receipt),
            kind: RepairKind::CatalogProjection,
            status: RepairStatus::Required,
            attempts: 0,
            last_error: Some("first failure".into()),
            next_attempt_at: OffsetDateTime::UNIX_EPOCH,
            locator: None,
            operation: Some("copy".into()),
        };

        repairs.insert(&base).await.unwrap();
        let mut again = base.clone();
        again.last_error = Some("second failure".into());
        repairs.insert(&again).await.unwrap();

        let listed = repairs.list(10).await.unwrap();
        assert_eq!(listed.len(), 1, "same receipt, one row");
        assert_eq!(listed[0].receipt_id, Some(receipt));
        assert!(listed[0].last_error.as_deref().unwrap().contains("second"));
    }

    #[tokio::test]
    async fn resolve_removes_an_item_from_the_pending_list() {
        let db = Database::open_in_memory().await.unwrap();
        let repairs = db.repairs();
        let receipt = ReceiptId::new_random();
        repairs
            .insert(&RepairItem {
                id: RepairId::from_receipt(receipt),
                event_id: None,
                receipt_id: Some(receipt),
                kind: RepairKind::CatalogProjection,
                status: RepairStatus::Required,
                attempts: 0,
                last_error: Some("failure".into()),
                next_attempt_at: OffsetDateTime::UNIX_EPOCH,
                locator: None,
                operation: Some("copy".into()),
            })
            .await
            .unwrap();
        assert_eq!(repairs.list(10).await.unwrap().len(), 1);

        let resolved = repairs
            .resolve(RepairId::from_receipt(receipt), OffsetDateTime::UNIX_EPOCH)
            .await
            .unwrap();
        assert!(resolved, "the pending row resolves");
        assert!(repairs.list(10).await.unwrap().is_empty());
        let unknown = repairs
            .resolve(
                RepairId::from_receipt(ReceiptId::new_random()),
                OffsetDateTime::UNIX_EPOCH,
            )
            .await
            .unwrap();
        assert!(!unknown, "an unknown id reports not found");
    }
}
