//! Application-owned outbox dispatcher.
//!
//! The dispatcher delivers durable domain facts; it does not publish progress or UI
//! notifications. A failed handler is recorded back into the outbox and retried with
//! exponential backoff. Once the adapter reaches its attempt limit, the adapter creates a
//! repair item that diagnostics can query.

use async_trait::async_trait;
use rstore_application_ports::{OutboxEvent, OutboxStore, RepairStore};
use rstore_domain::{EventId, Result};
use std::sync::Arc;
use time::OffsetDateTime;

/// Per-cycle outcome of [`OutboxDispatcher::dispatch_once`].
///
/// `repair_required` counts currently pending repair items (capped at the batch size), not
/// repairs created by this cycle.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DispatchReport {
    pub claimed: u32,
    pub published: u32,
    pub retried: u32,
    /// Claimed but never handed to the handler because the deadline passed; released back
    /// to the due queue without spending an attempt.
    pub deferred: u32,
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
    /// decision. With a `deadline`, no event starts after it, and a handler
    /// still running at it is abandoned as a failed attempt; events never
    /// started are released (`deferred`) without spending one. The report
    /// counts pending repairs after the cycle.
    pub async fn dispatch_once<H: OutboxHandler + ?Sized>(
        &self,
        now: OffsetDateTime,
        handler: &H,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<DispatchReport> {
        let events = self.store.claim_due(now, self.batch_size).await?;
        let claimed = events.len() as u32;
        let mut published = 0;
        let mut retried = 0;
        let mut deferred = 0;
        let mut events = events.into_iter();
        while let Some(event) = events.next() {
            let outcome = match deadline {
                None => Some(handler.handle(&event).await),
                Some(deadline) if tokio::time::Instant::now() >= deadline => {
                    // Out of time before this event started: it and the rest
                    // of the batch go back untried.
                    let unstarted: Vec<EventId> = std::iter::once(event.event_id)
                        .chain(events.by_ref().map(|rest| rest.event_id))
                        .collect();
                    deferred = unstarted.len() as u32;
                    self.store.release(&unstarted).await?;
                    break;
                }
                Some(deadline) => tokio::time::timeout_at(deadline, handler.handle(&event))
                    .await
                    .ok(),
            };
            match outcome {
                Some(Ok(())) => {
                    self.store.acknowledge(event.event_id, now).await?;
                    published += 1;
                }
                Some(Err(error)) => {
                    self.store
                        .retry(event.event_id, &error.to_string(), now)
                        .await?;
                    retried += 1;
                }
                None => {
                    // The handler outlived the deadline: a failed attempt
                    // under the normal backoff; the untried rest goes back.
                    self.store
                        .retry(event.event_id, "handler exceeded the drain deadline", now)
                        .await?;
                    retried += 1;
                    let unstarted: Vec<EventId> =
                        events.by_ref().map(|rest| rest.event_id).collect();
                    deferred = unstarted.len() as u32;
                    if !unstarted.is_empty() {
                        self.store.release(&unstarted).await?;
                    }
                    break;
                }
            }
        }
        let repair_required = self.repairs.list(self.batch_size).await?.len() as u32;
        Ok(DispatchReport {
            claimed,
            published,
            retried,
            deferred,
            repair_required,
        })
    }
}
