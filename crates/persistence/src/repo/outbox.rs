//! SQLite outbox and repair repository.

use async_trait::async_trait;
use rstore_application_ports::{
    ConsumerCheckpoint, ConsumerCheckpointStore, OutboxEvent, OutboxStore, RemoteMutationReceipt,
    RepairItem, RepairRepository,
};
use rstore_domain::{Error, Result};
use sqlx::sqlite::{SqlitePool, SqliteRow};
use sqlx::Row;
use time::OffsetDateTime;

use crate::codec::{unix_secs, RowReader};
use crate::error::map_sqlx;

const MAX_ATTEMPTS: i64 = 8;

#[derive(Clone)]
pub struct OutboxRepository {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: crate::actor::SqliteWriter,
}

impl OutboxRepository {
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

    async fn append_event(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        event: &OutboxEvent,
    ) -> Result<OutboxEvent> {
        // Version zero is the application-port sentinel for "allocate the
        // next version in this transaction". Keeping allocation here makes
        // aggregate ordering a persistence guarantee instead of a best-effort
        // counter maintained by each use case.
        let mut stored = event.clone();
        if let Some(row) =
            sqlx::query("SELECT aggregate_version FROM outbox_events WHERE event_id = ?")
                .bind(&event.event_id)
                .fetch_optional(&mut **tx)
                .await
                .map_err(map_sqlx)?
        {
            stored.aggregate_version = row
                .try_get::<i64, _>("aggregate_version")
                .map_err(map_sqlx)? as u64;
            return Ok(stored);
        }

        let aggregate_version = if event.aggregate_version == 0 {
            sqlx::query(
                "INSERT INTO aggregate_versions \
                 (aggregate_type, aggregate_id, aggregate_version, updated_at) \
                 VALUES (?, ?, 1, ?) \
                 ON CONFLICT(aggregate_type, aggregate_id) DO UPDATE SET \
                 aggregate_version = aggregate_versions.aggregate_version + 1, \
                 updated_at = excluded.updated_at",
            )
            .bind(&event.aggregate_type)
            .bind(&event.aggregate_id)
            .bind(unix_secs(event.occurred_at))
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx)?;
            sqlx::query_scalar::<_, i64>(
                "SELECT aggregate_version FROM aggregate_versions \
                 WHERE aggregate_type = ? AND aggregate_id = ?",
            )
            .bind(&event.aggregate_type)
            .bind(&event.aggregate_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(map_sqlx)? as u64
        } else {
            sqlx::query(
                "INSERT INTO aggregate_versions \
                 (aggregate_type, aggregate_id, aggregate_version, updated_at) \
                 VALUES (?, ?, ?, ?) \
                 ON CONFLICT(aggregate_type, aggregate_id) DO UPDATE SET \
                 aggregate_version = MAX(aggregate_versions.aggregate_version, excluded.aggregate_version), \
                 updated_at = excluded.updated_at",
            )
            .bind(&event.aggregate_type)
            .bind(&event.aggregate_id)
            .bind(event.aggregate_version as i64)
            .bind(unix_secs(event.occurred_at))
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx)?;
            event.aggregate_version
        };
        stored.aggregate_version = aggregate_version;
        sqlx::query(
            "INSERT INTO outbox_events \
             (event_id, context, aggregate_type, aggregate_id, aggregate_version, \
              event_type, schema_version, occurred_at, payload_json, available_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(event_id) DO NOTHING",
        )
        .bind(&event.event_id)
        .bind(&event.context)
        .bind(&event.aggregate_type)
        .bind(&event.aggregate_id)
        .bind(stored.aggregate_version as i64)
        .bind(&event.event_type)
        .bind(event.schema_version as i64)
        .bind(unix_secs(event.occurred_at))
        .bind(event.payload.to_string())
        .bind(unix_secs(event.occurred_at))
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        Ok(stored)
    }

    async fn append_receipt(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        receipt: &RemoteMutationReceipt,
    ) -> Result<()> {
        crate::repo::receipt::insert_receipt(&mut **tx, receipt).await
    }

    pub(crate) async fn append_and_repair(&self, event: &OutboxEvent, error: &str) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let now = OffsetDateTime::now_utc();
        let event = event.clone();
        let error = error.to_owned();
        crate::tx::atomically(&self.pool, "outbox repair", move |tx| {
            Box::pin(async move {
                Self::append_event(tx, &event).await?;
                sqlx::query(
                    "UPDATE outbox_events SET status = 'repair_required', last_error = ? \
                     WHERE event_id = ?",
                )
                .bind(&error)
                .bind(&event.event_id)
                .execute(&mut **tx)
                .await
                .map_err(map_sqlx)?;
                sqlx::query(
                    "INSERT INTO repair_items \
                     (id, event_id, kind, status, aggregate_type, aggregate_id, operation, \
                      attempts, last_error, next_attempt_at, created_at, updated_at) \
                     VALUES (?, ?, 'outbox_delivery', 'repair_required', ?, ?, ?, 1, ?, ?, ?, ?) \
                     ON CONFLICT(id) DO UPDATE SET status = 'repair_required', \
                         attempts = repair_items.attempts + 1, last_error = excluded.last_error, \
                         next_attempt_at = excluded.next_attempt_at, updated_at = excluded.updated_at",
                )
                .bind(format!("repair_{}", event.event_id))
                .bind(&event.event_id)
                .bind(&event.aggregate_type)
                .bind(&event.aggregate_id)
                .bind(&event.event_type)
                .bind(&error)
                .bind(unix_secs(now))
                .bind(unix_secs(now))
                .bind(unix_secs(now))
                .execute(&mut **tx)
                .await
                .map_err(map_sqlx)?;
                Ok(())
            })
        })
        .await
    }

    pub(crate) async fn append_mutation(
        &self,
        receipt: &RemoteMutationReceipt,
        event: &OutboxEvent,
    ) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let receipt = receipt.clone();
        let event = event.clone();
        crate::tx::atomically(&self.pool, "remote mutation", move |tx| {
            Box::pin(async move {
                Self::append_receipt(tx, &receipt).await?;
                Self::append_event(tx, &event).await?;
                Ok(())
            })
        })
        .await
    }
}

#[async_trait]
impl ConsumerCheckpointStore for OutboxRepository {
    async fn checkpoint(&self, consumer_id: &str) -> Result<Option<ConsumerCheckpoint>> {
        let row = sqlx::query(
            "SELECT consumer_id, last_event_id, last_event_at, updated_at \
             FROM consumer_checkpoints WHERE consumer_id = ?",
        )
        .bind(consumer_id)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            let reader = RowReader::new(row);
            Ok(ConsumerCheckpoint {
                consumer_id: reader.text("consumer_id")?,
                last_event_id: reader.opt_text("last_event_id")?,
                last_event_at: reader.opt_timestamp("last_event_at")?,
                updated_at: reader.timestamp("updated_at")?,
            })
        })
        .transpose()
    }

    async fn save_checkpoint(&self, checkpoint: &ConsumerCheckpoint) -> Result<()> {
        let _write = self.writer.acquire().await?;
        sqlx::query(
            "INSERT INTO consumer_checkpoints \
             (consumer_id, last_event_id, last_event_at, updated_at) VALUES (?, ?, ?, ?) \
             ON CONFLICT(consumer_id) DO UPDATE SET last_event_id = excluded.last_event_id, \
                 last_event_at = excluded.last_event_at, updated_at = excluded.updated_at",
        )
        .bind(&checkpoint.consumer_id)
        .bind(&checkpoint.last_event_id)
        .bind(checkpoint.last_event_at.map(unix_secs))
        .bind(unix_secs(checkpoint.updated_at))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}

#[async_trait]
impl RepairRepository for OutboxRepository {
    async fn insert(&self, item: &RepairItem) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let now = OffsetDateTime::now_utc();
        sqlx::query(
            "INSERT INTO repair_items \
             (id, receipt_id, kind, status, locator_json, operation, attempts, last_error, \
              next_attempt_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(id) DO UPDATE SET status = excluded.status, \
                 receipt_id = excluded.receipt_id, \
                 locator_json = excluded.locator_json, operation = excluded.operation, \
                 attempts = excluded.attempts, last_error = excluded.last_error, \
                 next_attempt_at = excluded.next_attempt_at, updated_at = excluded.updated_at",
        )
        .bind(&item.id)
        .bind(&item.receipt_id)
        .bind(&item.kind)
        .bind(&item.status)
        .bind(&item.locator)
        .bind(&item.operation)
        .bind(i64::from(item.attempts))
        .bind(&item.last_error)
        .bind(unix_secs(item.next_attempt_at))
        .bind(unix_secs(now))
        .bind(unix_secs(now))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn list(&self, limit: u32) -> Result<Vec<RepairItem>> {
        OutboxStore::repairs(self, limit).await
    }

    async fn resolve(&self, id: &str, at: OffsetDateTime) -> Result<bool> {
        let _write = self.writer.acquire().await?;
        let affected =
            sqlx::query("UPDATE repair_items SET status = 'resolved', updated_at = ? WHERE id = ?")
                .bind(unix_secs(at))
                .bind(id)
                .execute(&self.pool)
                .await
                .map_err(map_sqlx)?
                .rows_affected();
        Ok(affected > 0)
    }
}

#[async_trait]
impl OutboxStore for OutboxRepository {
    async fn append(&self, event: &OutboxEvent) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let event = event.clone();
        crate::tx::atomically(&self.pool, "outbox append", move |tx| {
            Box::pin(async move { Self::append_event(tx, &event).await.map(|_| ()) })
        })
        .await
    }

    async fn claim_due(&self, now: OffsetDateTime, limit: u32) -> Result<Vec<OutboxEvent>> {
        let _write = self.writer.acquire().await?;
        crate::tx::atomically(&self.pool, "outbox claim", move |tx| {
            Box::pin(async move {
                let rows = sqlx::query(
                    "SELECT event_id, context, aggregate_type, aggregate_id, aggregate_version, \
                            event_type, schema_version, occurred_at, payload_json, attempts \
                     FROM outbox_events \
                     WHERE status = 'pending' AND available_at <= ? \
                     ORDER BY aggregate_id, aggregate_version, occurred_at, event_id LIMIT ?",
                )
                .bind(unix_secs(now))
                .bind(i64::from(limit))
                .fetch_all(&mut **tx)
                .await
                .map_err(map_sqlx)?;

                let mut events = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut event = row_to_event(row)?;
                    sqlx::query(
                        "UPDATE outbox_events SET status = 'processing', attempts = attempts + 1 \
                         WHERE event_id = ?",
                    )
                    .bind(&event.event_id)
                    .execute(&mut **tx)
                    .await
                    .map_err(map_sqlx)?;
                    event.attempts = event.attempts.saturating_add(1);
                    sqlx::query(
                        "INSERT INTO outbox_attempts \
                         (event_id, attempt_no, attempted_at, outcome) \
                         SELECT event_id, attempts, ?, 'claimed' FROM outbox_events WHERE event_id = ?",
                    )
                    .bind(unix_secs(now))
                    .bind(&event.event_id)
                    .execute(&mut **tx)
                    .await
                    .map_err(map_sqlx)?;
                    events.push(event);
                }
                Ok(events)
            })
        })
        .await
    }

    async fn recover_processing(&self, now: OffsetDateTime) -> Result<u64> {
        let _write = self.writer.acquire().await?;
        let affected = sqlx::query(
            "UPDATE outbox_events SET status = 'pending', available_at = ?, \
             last_error = COALESCE(last_error, 'recovered after process restart') \
             WHERE status = 'processing'",
        )
        .bind(unix_secs(now))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }

    async fn acknowledge(&self, event_id: &str) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let affected = sqlx::query(
            "UPDATE outbox_events SET status = 'published', published_at = unixepoch(), \
             last_error = NULL WHERE event_id = ?",
        )
        .bind(event_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        if affected == 0 {
            return Err(Error::not_found(format!(
                "outbox event `{event_id}` not found"
            )));
        }
        Ok(())
    }

    async fn retry(
        &self,
        event_id: &str,
        next_attempt_at: OffsetDateTime,
        error: &str,
    ) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let row = sqlx::query(
            "SELECT attempts, aggregate_type, aggregate_id, event_type \
             FROM outbox_events WHERE event_id = ?",
        )
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| Error::not_found(format!("outbox event `{event_id}` not found")))?;
        let attempts = row.try_get::<i64, _>("attempts").map_err(map_sqlx)?;
        if attempts >= MAX_ATTEMPTS {
            let event = sqlx::query(
                "SELECT event_id, context, aggregate_type, aggregate_id, aggregate_version, \
                        event_type, schema_version, occurred_at, payload_json, attempts \
                 FROM outbox_events WHERE event_id = ?",
            )
            .bind(event_id)
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx)?;
            drop(_write);
            return self.append_and_repair(&row_to_event(event)?, error).await;
        }

        let event_id = event_id.to_owned();
        let error = error.to_owned();
        crate::tx::atomically(&self.pool, "outbox retry", move |tx| {
            Box::pin(async move {
                sqlx::query(
                    "UPDATE outbox_events SET status = 'pending', available_at = ?, last_error = ? \
                     WHERE event_id = ?",
                )
                .bind(unix_secs(next_attempt_at))
                .bind(&error)
                .bind(&event_id)
                .execute(&mut **tx)
                .await
                .map_err(map_sqlx)?;
                sqlx::query(
                    "UPDATE outbox_attempts SET outcome = 'retrying', error = ? \
                     WHERE event_id = ? AND attempt_no = ?",
                )
                .bind(&error)
                .bind(&event_id)
                .bind(attempts)
                .execute(&mut **tx)
                .await
                .map_err(map_sqlx)?;
                Ok(())
            })
        })
        .await
    }

    async fn repairs(&self, limit: u32) -> Result<Vec<RepairItem>> {
        let rows = sqlx::query(
            "SELECT id, receipt_id, kind, status, attempts, last_error, next_attempt_at, locator_json, operation \
             FROM repair_items WHERE status != 'resolved' \
             ORDER BY next_attempt_at, updated_at, id LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let reader = RowReader::new(row);
                Ok(RepairItem {
                    id: reader.text("id")?,
                    receipt_id: reader.opt_text("receipt_id")?,
                    kind: reader.text("kind")?,
                    status: reader.text("status")?,
                    attempts: reader.integer("attempts")? as u32,
                    last_error: reader.opt_text("last_error")?,
                    next_attempt_at: reader.timestamp("next_attempt_at")?,
                    locator: reader.opt_text("locator_json")?,
                    operation: reader.opt_text("operation")?,
                })
            })
            .collect()
    }
}

fn row_to_event(row: SqliteRow) -> Result<OutboxEvent> {
    let reader = RowReader::new(row);
    let payload = serde_json::from_str(&reader.text("payload_json")?)
        .map_err(|e| Error::database("decode outbox payload").with_source(e))?;
    Ok(OutboxEvent {
        event_id: reader.text("event_id")?,
        context: reader.text("context")?,
        aggregate_type: reader.text("aggregate_type")?,
        aggregate_id: reader.text("aggregate_id")?,
        aggregate_version: reader.integer("aggregate_version")? as u64,
        event_type: reader.text("event_type")?,
        schema_version: reader.integer("schema_version")? as u32,
        occurred_at: reader.timestamp("occurred_at")?,
        payload,
        attempts: reader.integer("attempts")? as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    fn event(id: &str) -> OutboxEvent {
        OutboxEvent {
            event_id: id.to_owned(),
            context: "catalog".into(),
            aggregate_type: "remote_object".into(),
            aggregate_id: "obj-1".into(),
            aggregate_version: 1,
            event_type: "remote_object_confirmed".into(),
            schema_version: 1,
            occurred_at: OffsetDateTime::UNIX_EPOCH,
            payload: serde_json::json!({"object_id": "obj-1"}),
            attempts: 0,
        }
    }

    #[tokio::test]
    async fn append_claim_and_acknowledge_are_idempotent() {
        let db = Database::open_in_memory().await.unwrap();
        let outbox = db.outbox();
        let item = event("evt-1");

        outbox.append(&item).await.unwrap();
        outbox.append(&item).await.unwrap();

        let claimed = outbox
            .claim_due(OffsetDateTime::UNIX_EPOCH, 10)
            .await
            .unwrap();
        let mut claimed_item = item.clone();
        claimed_item.attempts = 1;
        assert_eq!(claimed, vec![claimed_item]);
        outbox.acknowledge("evt-1").await.unwrap();
        assert!(outbox
            .claim_due(OffsetDateTime::UNIX_EPOCH, 10)
            .await
            .unwrap()
            .is_empty());
        assert!(outbox.repairs(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_outbox_unit_rolls_back_event_and_aggregate_version() {
        let db = Database::open_in_memory().await.unwrap();
        let item = event("evt-rollback");
        let result: Result<()> = crate::tx::atomically(db.pool(), "outbox test", |tx| {
            Box::pin(async move {
                OutboxRepository::append_event(tx, &item).await?;
                Err(Error::conflict("force outbox rollback"))
            })
        })
        .await;

        assert_eq!(result.unwrap_err().code, rstore_domain::ErrorCode::Conflict);
        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_events WHERE event_id = 'evt-rollback'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        let version_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM aggregate_versions WHERE aggregate_id = 'obj-1'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(event_count, 0);
        assert_eq!(version_count, 0);
    }

    #[tokio::test]
    async fn retry_returns_event_to_the_due_queue() {
        let db = Database::open_in_memory().await.unwrap();
        let outbox = db.outbox();
        outbox.append(&event("evt-2")).await.unwrap();
        outbox
            .claim_due(OffsetDateTime::UNIX_EPOCH, 1)
            .await
            .unwrap();
        outbox
            .retry("evt-2", OffsetDateTime::UNIX_EPOCH, "temporary")
            .await
            .unwrap();

        assert_eq!(
            outbox
                .claim_due(OffsetDateTime::UNIX_EPOCH, 1)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn exhausted_delivery_becomes_repair_required() {
        let db = Database::open_in_memory().await.unwrap();
        let outbox = db.outbox();
        outbox.append(&event("evt-dead-letter")).await.unwrap();

        for attempt in 0..MAX_ATTEMPTS {
            let claimed = outbox
                .claim_due(OffsetDateTime::UNIX_EPOCH, 1)
                .await
                .unwrap();
            assert_eq!(claimed.len(), 1, "attempt {attempt}");
            outbox
                .retry(
                    "evt-dead-letter",
                    OffsetDateTime::UNIX_EPOCH,
                    "consumer failed",
                )
                .await
                .unwrap();
        }

        let repairs = outbox.repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].kind, "outbox_delivery");
        let status: String = sqlx::query_scalar(
            "SELECT status FROM outbox_events WHERE event_id = 'evt-dead-letter'",
        )
        .fetch_one(db.read_pool())
        .await
        .unwrap();
        assert_eq!(status, "repair_required");
    }
}
