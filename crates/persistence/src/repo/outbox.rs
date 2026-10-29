//! SQLite outbox repository: the event-delivery lifecycle.
//!
//! Per-aggregate event ordering is allocated inside the append transaction;
//! exhausted delivery dead-letters the event and opens a repair item in the
//! same unit of work, written through the repair module's shared statement.
use async_trait::async_trait;
use rstore_application_ports::{
    OutboxEvent, OutboxStore, RemoteMutationReceipt, RepairItem, RepairKind, RepairStatus,
};
use rstore_domain::id::{EventId, RepairId};
use rstore_domain::{BoundedContext, Error, Result};
use sqlx::sqlite::SqliteRow;
use time::OffsetDateTime;

use crate::codec::{unix_secs, uuid_to_text, RowReader};
use crate::error::map_sqlx;
use crate::tx::WriteGate;

use super::repair::upsert_repair;

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
}

impl OutboxRepository {
    pub(crate) fn new(gate: WriteGate) -> Self {
        Self { gate }
    }

    async fn append_event(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        event: &OutboxEvent,
    ) -> Result<OutboxEvent> {
        let mut stored = event.clone();
        if let Some(row) =
            sqlx::query("SELECT aggregate_version FROM outbox_events WHERE event_id = ?")
                .bind(uuid_to_text(&event.event_id))
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
        .bind(uuid_to_text(&event.event_id))
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
                        .bind(uuid_to_text(&event.event_id))
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
                        .bind(uuid_to_text(&event.event_id))
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

    async fn release(&self, event_ids: &[EventId]) -> Result<()> {
        let ids: Vec<String> = event_ids.iter().map(uuid_to_text).collect();
        self.gate
            .write_unit("outbox release", move |tx| {
                Box::pin(async move {
                    for id in &ids {
                        // Drop the claim's attempt row first: its number is the
                        // event's current count.
                        sqlx::query(
                            "DELETE FROM outbox_attempts WHERE event_id = ? AND outcome = 'claimed' \
                             AND attempt_no = (SELECT attempts FROM outbox_events WHERE event_id = ?)",
                        )
                        .bind(id)
                        .bind(id)
                        .execute(&mut **tx)
                        .await
                        .map_err(map_sqlx)?;
                        sqlx::query(
                            "UPDATE outbox_events SET status = 'pending', attempts = attempts - 1 \
                             WHERE event_id = ? AND status = 'processing'",
                        )
                        .bind(id)
                        .execute(&mut **tx)
                        .await
                        .map_err(map_sqlx)?;
                    }
                    Ok(())
                })
            })
            .await
    }

    async fn acknowledge(&self, event_id: EventId, at: OffsetDateTime) -> Result<()> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query(
            "UPDATE outbox_events SET status = 'published', published_at = ?, \
             last_error = NULL WHERE event_id = ?",
        )
        .bind(unix_secs(at))
        .bind(uuid_to_text(&event_id))
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

    async fn retry(&self, event_id: EventId, error: &str, now: OffsetDateTime) -> Result<()> {
        let error = error.to_owned();
        self.gate
            .write_unit("outbox retry", move |tx| {
                Box::pin(async move {
                    let row = sqlx::query(
                        "SELECT event_id, context, aggregate_type, aggregate_id, aggregate_version, \
                                event_type, schema_version, occurred_at, payload_json, attempts \
                         FROM outbox_events WHERE event_id = ?",
                    )
                    .bind(uuid_to_text(&event_id))
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(map_sqlx)?
                    .ok_or_else(|| {
                        Error::not_found(format!("outbox event `{event_id}` not found"))
                    })?;
                    let event = row_to_event(row)?;
                    let attempts = i64::from(event.attempts);
                    match delivery_decision(event.attempts, now) {
                        DeliveryDecision::DeadLetter => {
                            Self::append_event(tx, &event).await?;
                            sqlx::query(
                                "UPDATE outbox_events SET status = 'repair_required', last_error = ? \
                                 WHERE event_id = ?",
                            )
                            .bind(&error)
                            .bind(uuid_to_text(&event.event_id))
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
                                // The event itself stays linked by `event_id`;
                                // a dead-letter is not tied to one mutation kind.
                                locator: None,
                                operation: None,
                            };
                            upsert_repair(&mut **tx, &item, now).await?;
                            Ok(())
                        }
                        DeliveryDecision::Retry { next_attempt_at } => {
                            sqlx::query(
                                "UPDATE outbox_events SET status = 'pending', available_at = ?, last_error = ? \
                                 WHERE event_id = ?",
                            )
                            .bind(unix_secs(next_attempt_at))
                            .bind(&error)
                            .bind(uuid_to_text(&event_id))
                            .execute(&mut **tx)
                            .await
                            .map_err(map_sqlx)?;
                            sqlx::query(
                                "UPDATE outbox_attempts SET outcome = 'retrying', error = ? \
                                 WHERE event_id = ? AND attempt_no = ?",
                            )
                            .bind(&error)
                            .bind(uuid_to_text(&event_id))
                            .bind(attempts)
                            .execute(&mut **tx)
                            .await
                            .map_err(map_sqlx)?;
                            Ok(())
                        }
                    }
                })
            })
            .await
    }
}

fn row_to_event(row: SqliteRow) -> Result<OutboxEvent> {
    let reader = RowReader::new(row);
    let payload = serde_json::from_str(&reader.text("payload_json")?)
        .map_err(|e| Error::database("decode outbox payload").with_source(e))?;
    Ok(OutboxEvent {
        event_id: uuid::Uuid::parse_str(&reader.text("event_id")?)
            .map(EventId::from_uuid)
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
