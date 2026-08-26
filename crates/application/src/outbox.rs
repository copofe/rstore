//! Application-owned outbox dispatcher.
//!
//! The dispatcher delivers durable domain facts; it does not publish progress or UI
//! notifications. A failed handler is recorded back into the outbox and retried with
//! exponential backoff. Once the adapter reaches its attempt limit, the adapter creates a
//! repair item that diagnostics can query.

use async_trait::async_trait;
use rstore_application_ports::{OutboxEvent, OutboxStore, RepairItem, RepairStore};
use rstore_domain::Result;
use std::sync::Arc;
use time::OffsetDateTime;

/// Per-cycle outcome of [`OutboxDispatcher::dispatch_once`].
///
/// `repair_required` counts currently pending repair items (capped at the batch size), not
/// repairs created by this cycle.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DispatchReport {
    pub claimed: u32,
    pub published: u32,
    pub retried: u32,
    pub repair_required: u32,
}

/// A consumer of outbox events; success acknowledges the event, failure schedules a retry.
#[async_trait]
pub trait OutboxHandler: Send + Sync {
    async fn handle(&self, event: &OutboxEvent) -> Result<()>;
}

/// Delivers due outbox events to an [`OutboxHandler`] in bounded batches.
#[derive(Clone)]
pub struct OutboxDispatcher {
    store: Arc<dyn OutboxStore>,
    repairs: Arc<dyn RepairStore>,
    batch_size: u32,
}

impl OutboxDispatcher {
    /// A dispatcher with the default batch size of 64 events per cycle.
    pub fn new(store: Arc<dyn OutboxStore>, repairs: Arc<dyn RepairStore>) -> Self {
        Self {
            store,
            repairs,
            batch_size: 64,
        }
    }

    /// Override the per-cycle batch size (clamped to at least 1).
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Reclaim events left in `processing` by a dead dispatcher so they become claimable
    /// again; returns the number reclaimed.
    pub async fn recover_processing(&self, now: OffsetDateTime) -> Result<u64> {
        self.store.recover_processing(now).await
    }

    /// Run one dispatch cycle.
    ///
    /// Flow: claim up to `batch_size` due events, then for each one
    /// acknowledge on handler success, or hand the failure to the store's
    /// `retry` — which owns the backoff schedule and the dead-letter
    /// decision. The report counts pending repairs after the cycle.
    pub async fn dispatch_once<H: OutboxHandler + ?Sized>(
        &self,
        now: OffsetDateTime,
        handler: &H,
    ) -> Result<DispatchReport> {
        let events = self.store.claim_due(now, self.batch_size).await?;
        let claimed = events.len() as u32;
        let mut published = 0;
        let mut retried = 0;
        for event in events {
            match handler.handle(&event).await {
                Ok(()) => {
                    self.store.acknowledge(event.event_id, now).await?;
                    published += 1;
                }
                Err(error) => {
                    self.store
                        .retry(event.event_id, &error.to_string(), now)
                        .await?;
                    retried += 1;
                }
            }
        }
        let repair_required = self.repairs.list(self.batch_size).await?.len() as u32;
        Ok(DispatchReport {
            claimed,
            published,
            retried,
            repair_required,
        })
    }

    /// List pending repair items, most pressing first, up to `limit`.
    pub async fn repairs(&self, limit: u32) -> Result<Vec<RepairItem>> {
        self.repairs.list(limit).await
    }
}

/// A handler for tests and dispatcher unit exercises: acknowledges events
/// without any delivery side effect.
pub struct NoopOutboxHandler;

#[async_trait]
impl OutboxHandler for NoopOutboxHandler {
    async fn handle(&self, _event: &OutboxEvent) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_application_ports::OutboxEvent;
    use rstore_persistence::Database;

    fn event() -> OutboxEvent {
        OutboxEvent {
            event_id: uuid::Uuid::new_v4(),
            context: rstore_domain::BoundedContext::Transfer,
            aggregate_type: "job".into(),
            aggregate_id: "job-1".into(),
            aggregate_version: 1,
            event_type: "job_started".into(),
            schema_version: 1,
            occurred_at: OffsetDateTime::UNIX_EPOCH,
            payload: serde_json::json!({}),
            attempts: 0,
        }
    }

    #[tokio::test]
    async fn successful_dispatch_acknowledges_the_event() {
        let db = Database::open_in_memory().await.unwrap();
        let store = Arc::new(db.outbox());
        let repairs: Arc<dyn rstore_application_ports::RepairStore> = Arc::new(db.repairs());
        store.append(&event()).await.unwrap();
        let dispatcher = OutboxDispatcher::new(store.clone(), repairs);
        let report = dispatcher
            .dispatch_once(OffsetDateTime::UNIX_EPOCH, &NoopOutboxHandler)
            .await
            .unwrap();
        assert_eq!(report.claimed, 1);
        assert_eq!(report.published, 1);
        assert_eq!(report.retried, 0);
        assert!(store
            .claim_due(OffsetDateTime::UNIX_EPOCH, 10)
            .await
            .unwrap()
            .is_empty());
    }
}
