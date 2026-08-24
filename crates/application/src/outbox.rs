//! Application-owned outbox dispatcher.
//!
//! The dispatcher delivers durable domain facts; it does not publish progress or UI
//! notifications. A failed handler is recorded back into the outbox and retried with
//! exponential backoff. Once the adapter reaches its attempt limit, the adapter creates a
//! repair item that diagnostics can query.

use async_trait::async_trait;
use rstore_application_ports::{OutboxEvent, OutboxStore, RepairItem};
use rstore_domain::Result;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

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
    batch_size: u32,
}

impl OutboxDispatcher {
    /// A dispatcher with the default batch size of 64 events per cycle.
    pub fn new(store: Arc<dyn OutboxStore>) -> Self {
        Self {
            store,
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
    /// Flow: claim up to `batch_size` due events, then for each one acknowledge on handler
    /// success, or on failure schedule a retry with `2^attempts` seconds of exponential
    /// backoff (attempts clamped so the delay stays bounded). Events that exhaust the store's
    /// attempt limit turn into repair items; the report counts pending repairs after the
    /// cycle.
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
                    let backoff_seconds = 2_i64.pow(event.attempts.clamp(1, 8));
                    let retry_at = now + Duration::seconds(backoff_seconds);
                    self.store
                        .retry(event.event_id, retry_at, &error.to_string())
                        .await?;
                    retried += 1;
                }
            }
        }
        let repair_required = self.store.repairs(self.batch_size).await?.len() as u32;
        Ok(DispatchReport {
            claimed,
            published,
            retried,
            repair_required,
        })
    }

    /// List pending repair items, most pressing first, up to `limit`.
    pub async fn repairs(&self, limit: u32) -> Result<Vec<RepairItem>> {
        self.store.repairs(limit).await
    }
}

/// A handler useful for tests and for a bootstrap that has no projection consumer configured
/// yet. It acknowledges events without adding a second delivery side effect.
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
        store.append(&event()).await.unwrap();
        let dispatcher = OutboxDispatcher::new(store.clone());
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
