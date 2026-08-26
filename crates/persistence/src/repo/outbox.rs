//! SQLite outbox and repair repository.
//!
//! Per-aggregate event ordering is allocated inside the append transaction;
//! exhausted delivery dead-letters the event and opens a repair item in the
//! same unit of work.
use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{
    OutboxEvent, OutboxStore, RemoteMutationReceipt, RepairItem, RepairKind, RepairRepository,
    RepairStatus,
};
use rstore_domain::id::{ReceiptId, RepairId};
use rstore_domain::{BoundedContext, Error, Result};
use sqlx::sqlite::{SqlitePool, SqliteRow};
use time::OffsetDateTime;

use crate::codec::{unix_secs, RowReader};
use crate::error::map_sqlx;
use crate::tx::WriteGate;

use rstore_domain::Clock;

const MAX_ATTEMPTS: i64 = 8;

/// The delivery decision for one failed delivery: retry after the backoff
/// schedule, or dead-letter at the attempt ceiling. A pure function so the
/// schedule and the ceiling have a direct test surface; `retry` owns the
/// transactional write around it.
fn delivery_decision(attempts: u32, now: OffsetDateTime) -> DeliveryDecision {
    if i64::from(attempts) >= MAX_ATTEMPTS {
        DeliveryDecision::DeadLetter
    } else {
        // Backoff schedule: 2^attempts seconds, clamped so the delay never
        // overflows a Duration.
        let backoff_seconds = 2_i64.pow(attempts.clamp(1, 8));
        DeliveryDecision::Retry {
            next_attempt_at: now + time::Duration::seconds(backoff_seconds),
        }
    }
}

enum DeliveryDecision {
    Retry { next_attempt_at: OffsetDateTime },
    DeadLetter,
}

#[derive(Clone)]
pub struct OutboxRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
    clock: Arc<dyn Clock>,
}

impl OutboxRepository {
    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool, clock: Arc<dyn Clock>) -> Self {
        Self {
            gate,
            read_pool,
            clock,
        }
    }

    async fn append_event(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        event: &OutboxEvent,
    ) -> Result<OutboxEvent> {
        let mut stored = event.clone();
        if let Some(row) =
            sqlx::query("SELECT aggregate_version FROM outbox_events WHERE event_id = ?")
                .bind(event.event_id.to_string())
                .fetch_optional(&mut **tx)
                .await
                .map_err(map_sqlx)?
        {
            stored.aggregate_version = RowReader::new(row).integer("aggregate_version")? as u64;
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
        .bind(event.event_id.to_string())
        .bind(event.context.as_str())
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

    pub(crate) async fn append_mutation(
        &self,
        receipt: &RemoteMutationReceipt,
        event: &OutboxEvent,
    ) -> Result<()> {
        let receipt = receipt.clone();
        let event = event.clone();
        self.gate
            .write_unit("remote mutation", move |tx| {
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
impl RepairRepository for OutboxRepository {
    async fn insert(&self, item: &RepairItem) -> Result<()> {
        let _write = self.gate.permit().await?;
        upsert_repair(self.gate.pool(), item, self.clock.now()).await
    }

    async fn list(&self, limit: u32) -> Result<Vec<RepairItem>> {
        OutboxStore::repairs(self, limit).await
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

#[async_trait]
impl OutboxStore for OutboxRepository {
    async fn append(&self, event: &OutboxEvent) -> Result<()> {
        let event = event.clone();
        self.gate
            .write_unit("outbox append", move |tx| {
                Box::pin(async move { Self::append_event(tx, &event).await.map(|_| ()) })
            })
            .await
    }

    async fn claim_due(&self, now: OffsetDateTime, limit: u32) -> Result<Vec<OutboxEvent>> {
        self.gate
            .write_unit("outbox claim", move |tx| {
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
                        .bind(event.event_id.to_string())
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
                        .bind(event.event_id.to_string())
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
        let _write = self.gate.permit().await?;
        let affected = sqlx::query(
            "UPDATE outbox_events SET status = 'pending', available_at = ?, \
             last_error = COALESCE(last_error, 'recovered after process restart') \
             WHERE status = 'processing'",
        )
        .bind(unix_secs(now))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }

    async fn acknowledge(&self, event_id: uuid::Uuid, at: OffsetDateTime) -> Result<()> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query(
            "UPDATE outbox_events SET status = 'published', published_at = ?, \
             last_error = NULL WHERE event_id = ?",
        )
        .bind(unix_secs(at))
        .bind(event_id.to_string())
        .execute(self.gate.pool())
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

    async fn retry(&self, event_id: uuid::Uuid, error: &str, now: OffsetDateTime) -> Result<()> {
        let error = error.to_owned();
        self.gate
            .write_unit("outbox retry", move |tx| {
                Box::pin(async move {
                    let row = sqlx::query(
                        "SELECT event_id, context, aggregate_type, aggregate_id, aggregate_version, \
                                event_type, schema_version, occurred_at, payload_json, attempts \
                         FROM outbox_events WHERE event_id = ?",
                    )
                    .bind(event_id.to_string())
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(map_sqlx)?
                    .ok_or_else(|| {
                        Error::not_found(format!("outbox event `{event_id}` not found"))
                    })?;
                    let event = row_to_event(row)?;
                    let attempts = i64::from(event.attempts);
                    if matches!(delivery_decision(event.attempts, now), DeliveryDecision::DeadLetter) {
                        Self::append_event(tx, &event).await?;
                        sqlx::query(
                            "UPDATE outbox_events SET status = 'repair_required', last_error = ? \
                             WHERE event_id = ?",
                        )
                        .bind(&error)
                        .bind(event.event_id.to_string())
                        .execute(&mut **tx)
                        .await
                        .map_err(map_sqlx)?;
                        // The dead-letter repair derives its identity from
                        // the event, so a repeat dead-letter refreshes the
                        // same row instead of opening another.
                        let item = RepairItem {
                            id: RepairId::from_event(event.event_id),
                            event_id: Some(event.event_id),
                            receipt_id: None,
                            kind: RepairKind::OutboxDelivery,
                            status: RepairStatus::Required,
                            attempts: event.attempts,
                            last_error: Some(error.clone()),
                            next_attempt_at: now,
                            locator: None,
                            operation: Some(event.event_type.clone()),
                        };
                        upsert_repair(&mut **tx, &item, now).await?;
                        return Ok(());
                    }

                    // The schedule lives in `delivery_decision`.
                    let DeliveryDecision::Retry { next_attempt_at } =
                        delivery_decision(event.attempts, now)
                    else {
                        unreachable!("dead-letter handled above")
                    };

                    sqlx::query(
                        "UPDATE outbox_events SET status = 'pending', available_at = ?, last_error = ? \
                         WHERE event_id = ?",
                    )
                    .bind(unix_secs(next_attempt_at))
                    .bind(&error)
                    .bind(event_id.to_string())
                    .execute(&mut **tx)
                    .await
                    .map_err(map_sqlx)?;
                    sqlx::query(
                        "UPDATE outbox_attempts SET outcome = 'retrying', error = ? \
                         WHERE event_id = ? AND attempt_no = ?",
                    )
                    .bind(&error)
                    .bind(event_id.to_string())
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
            "SELECT id, event_id, receipt_id, kind, status, attempts, last_error, next_attempt_at, locator_json, operation \
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
            })
            .collect()
    }
}

/// The single repair write path: an upsert keyed by the deterministic
/// identity, so every producer (including the store's own dead-letter)
/// shares one statement, one column set, and one attempts rule — the
/// writer's value wins.
async fn upsert_repair<'e, E>(executor: E, item: &RepairItem, now: OffsetDateTime) -> Result<()>
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

fn row_to_event(row: SqliteRow) -> Result<OutboxEvent> {
    let reader = RowReader::new(row);
    let payload = serde_json::from_str(&reader.text("payload_json")?)
        .map_err(|e| Error::database("decode outbox payload").with_source(e))?;
    Ok(OutboxEvent {
        event_id: uuid::Uuid::parse_str(&reader.text("event_id")?)
            .map_err(|e| Error::database("decode outbox event id").with_source(e))?,
        context: BoundedContext::parse(&reader.text("context")?)?,
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

    use uuid::Uuid;

    fn event(id: Uuid) -> OutboxEvent {
        OutboxEvent {
            event_id: id,
            context: BoundedContext::Catalog,
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

    #[test]
    fn backoff_schedule_is_exponential_and_bounded() {
        let now = OffsetDateTime::UNIX_EPOCH;
        for attempts in 1..8u32 {
            let DeliveryDecision::Retry { next_attempt_at } = delivery_decision(attempts, now)
            else {
                panic!("attempt {attempts} must retry")
            };
            assert_eq!(
                next_attempt_at - now,
                time::Duration::seconds(2_i64.pow(attempts.clamp(1, 8)))
            );
        }
    }

    #[test]
    fn attempt_ceiling_dead_letters() {
        let now = OffsetDateTime::UNIX_EPOCH;
        assert!(matches!(
            delivery_decision(8, now),
            DeliveryDecision::DeadLetter
        ));
        assert!(matches!(
            delivery_decision(20, now),
            DeliveryDecision::DeadLetter
        ));
    }

    const EVT1: Uuid = Uuid::from_u128(1);
    const EVT_ROLLBACK: Uuid = Uuid::from_u128(2);
    const EVT2: Uuid = Uuid::from_u128(3);
    const EVT_DEAD: Uuid = Uuid::from_u128(4);

    #[tokio::test]
    async fn append_claim_and_acknowledge_are_idempotent() {
        let db = Database::open_in_memory().await.unwrap();
        let outbox = db.outbox();
        let item = event(EVT1);

        outbox.append(&item).await.unwrap();
        outbox.append(&item).await.unwrap();

        let claimed = outbox
            .claim_due(OffsetDateTime::UNIX_EPOCH, 10)
            .await
            .unwrap();
        let mut claimed_item = item.clone();
        claimed_item.attempts = 1;
        assert_eq!(claimed, vec![claimed_item]);
        outbox
            .acknowledge(EVT1, OffsetDateTime::UNIX_EPOCH)
            .await
            .unwrap();
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
        let outbox = db.outbox();
        let item = event(EVT_ROLLBACK);
        let result: Result<()> = outbox
            .gate
            .write_unit("outbox test", |tx| {
                Box::pin(async move {
                    OutboxRepository::append_event(tx, &item).await?;
                    Err(Error::conflict("force outbox rollback"))
                })
            })
            .await;

        assert_eq!(result.unwrap_err().code, rstore_domain::ErrorCode::Conflict);
        let event_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE event_id = ?")
                .bind(EVT_ROLLBACK.to_string())
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
        outbox.append(&event(EVT2)).await.unwrap();
        outbox
            .claim_due(OffsetDateTime::UNIX_EPOCH, 1)
            .await
            .unwrap();
        outbox
            .retry(EVT2, "temporary", OffsetDateTime::UNIX_EPOCH)
            .await
            .unwrap();

        // The store owns the backoff schedule: the first failed attempt
        // (2^1 seconds) holds the event out of the due queue…
        assert_eq!(
            outbox
                .claim_due(OffsetDateTime::UNIX_EPOCH, 1)
                .await
                .unwrap()
                .len(),
            0
        );
        // …and releases it once the backoff elapses.
        assert_eq!(
            outbox
                .claim_due(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2), 1)
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
        outbox.append(&event(EVT_DEAD)).await.unwrap();

        // Drive the delivery attempts through the store's own backoff
        // schedule: after the n-th claim, retry pushes the next attempt
        // 2^n seconds out.
        let mut now = OffsetDateTime::UNIX_EPOCH;
        for attempt in 1..=MAX_ATTEMPTS {
            let claimed = outbox.claim_due(now, 1).await.unwrap();
            assert_eq!(claimed.len(), 1, "attempt {attempt}");
            outbox
                .retry(EVT_DEAD, "consumer failed", now)
                .await
                .unwrap();
            now += time::Duration::seconds(2_i64.pow(u32::try_from(attempt).unwrap()));
        }

        let repairs = outbox.repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].kind, RepairKind::OutboxDelivery);
        assert_eq!(repairs[0].event_id, Some(EVT_DEAD));
        assert_eq!(repairs[0].id, RepairId::from_event(EVT_DEAD));
        let status: String =
            sqlx::query_scalar("SELECT status FROM outbox_events WHERE event_id = ?")
                .bind(EVT_DEAD.to_string())
                .fetch_one(db.read_pool())
                .await
                .unwrap();
        assert_eq!(status, "repair_required");

        // A repeat dead-letter of the same event refreshes the same row
        // instead of opening another one.
        outbox
            .retry(EVT_DEAD, "consumer failed again", now)
            .await
            .unwrap();
        let repairs = outbox.repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert!(repairs[0].last_error.as_deref().unwrap().contains("again"));
    }

    #[tokio::test]
    async fn repair_insert_upserts_on_the_deterministic_identity() {
        let db = Database::open_in_memory().await.unwrap();
        let outbox = db.outbox();
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

        outbox.insert(&base).await.unwrap();
        let mut again = base.clone();
        again.last_error = Some("second failure".into());
        outbox.insert(&again).await.unwrap();

        let repairs = outbox.repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1, "same receipt, one row");
        assert_eq!(repairs[0].receipt_id, Some(receipt));
        assert!(repairs[0].last_error.as_deref().unwrap().contains("second"));
    }
}
