//! Outbox consumer that re-verifies settled remote mutations against the
//! provider.
//!
//! The mutation coordinator commits a receipt and an outbox fact in one
//! transaction; that receipt alone proves local durability, not provider-side
//! state. This handler closes the loop: for every `RemoteMutationCompleted`
//! fact it re-stats the remote object and either lets the event be
//! acknowledged or records a repair item (permanent divergence). The expected
//! state follows the operation — uploads and copies must be present, deletes
//! must be absent. Transient provider errors return `Err`, so the
//! dispatcher's backoff retry applies instead of consuming the event.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{
    BucketStore, OutboxEvent, RepairItem, RepairKind, RepairStatus, RepairStore,
};
use rstore_domain::events::DomainEvent;
use rstore_domain::id::RepairId;
use rstore_domain::locator::RemoteLocator;
use rstore_domain::{Clock, ErrorCode, Result};

use crate::connection::ConnectionRef;
use crate::gateway::RemoteGateway;
use crate::outbox::OutboxHandler;

/// Re-stats each settled mutation's remote object through the gateway.
///
/// A missing object is permanent (the write was lost, or the object was since
/// deleted) and becomes a repair item; anything the provider reports as a
/// configuration problem — unknown connection, unsupported adapter — is also
/// permanent and lands in repairs. Everything else (timeouts, throttling,
/// network faults) is transient and stays queued for retry.
pub(crate) struct RemoteVerifyHandler {
    gateway: RemoteGateway,
    buckets: Arc<dyn BucketStore>,
    repairs: Arc<dyn RepairStore>,
    clock: Arc<dyn Clock>,
}

impl RemoteVerifyHandler {
    /// Assemble the handler over the shared gateway and repair store.
    pub(crate) fn new(
        gateway: RemoteGateway,
        buckets: Arc<dyn BucketStore>,
        repairs: Arc<dyn RepairStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            gateway,
            buckets,
            repairs,
            clock,
        }
    }

    async fn handle_event(&self, event: &OutboxEvent) -> Result<()> {
        // An undecodable payload can never succeed on retry: treat it like
        // any other permanent divergence so the event is not stuck in
        // perpetual backoff.
        let domain = match serde_json::from_value::<DomainEvent>(event.payload.clone()) {
            Ok(domain) => domain,
            Err(error) => {
                self.record_repair(event, None, None, format!("decode outbox payload: {error}"))
                    .await?;
                return Ok(());
            }
        };
        let (locator, operation) = match domain {
            DomainEvent::RemoteMutationCompleted { locator, operation } => (locator, operation),
        };
        // The operation decides which provider state counts as settled:
        // everything but a delete must still be present.
        let expect_present = operation != crate::mutation::MutationOperation::Delete.as_str();

        match self.verify_presence(&locator, expect_present).await {
            // Verified: the dispatcher acknowledges the fact.
            Ok(Ok(())) => Ok(()),
            // Permanent divergence: record it for `repair`, then acknowledge —
            // retrying could never succeed.
            Ok(Err(detail)) => {
                self.record_repair(event, Some(&locator), Some(&operation), detail)
                    .await?;
                Ok(())
            }
            // Transient: bubble up so the dispatcher schedules a backoff retry.
            Err(error) => Err(error),
        }
    }

    /// Outer `Err` is transient (retry later); inner `Err` carries the
    /// permanent-divergence detail for the repair row.
    async fn verify_presence(
        &self,
        locator: &RemoteLocator,
        expect_present: bool,
    ) -> Result<std::result::Result<(), String>> {
        let bucket = self
            .buckets
            .get(locator.bucket_id)
            .await?
            .ok_or_else(|| format!("bucket {} is no longer registered", locator.bucket_id));

        let bucket = match bucket {
            Ok(bucket) => bucket,
            Err(detail) => return Ok(Err(detail)),
        };

        // Unknown connection or unbuildable adapter is configuration damage:
        // permanent by code, everything else stays transient.
        let storage = match self
            .gateway
            .storage(&ConnectionRef::Id(locator.connection_id))
            .await
        {
            Ok((_, storage)) => storage,
            Err(error)
                if matches!(
                    error.code,
                    ErrorCode::NotFound
                        | ErrorCode::CapabilityUnsupported
                        | ErrorCode::InvalidInput
                        | ErrorCode::Config
                ) =>
            {
                return Ok(Err(format!("resolve connection: {error}")));
            }
            Err(error) => return Err(error),
        };

        match storage.stat(&bucket.name, &locator.path).await {
            Ok(_) if expect_present => Ok(Ok(())),
            Ok(_) => Ok(Err(format!(
                "object `{}/{}` still present after delete",
                bucket.name,
                locator.path.as_str()
            ))),
            Err(error) if error.code == ErrorCode::NotFound && expect_present => Ok(Err(format!(
                "object `{}/{}` missing after settlement",
                bucket.name,
                locator.path.as_str()
            ))),
            Err(error) if error.code == ErrorCode::NotFound => Ok(Ok(())),
            Err(error) => Err(error),
        }
    }

    async fn record_repair(
        &self,
        event: &OutboxEvent,
        locator: Option<&RemoteLocator>,
        operation: Option<&str>,
        detail: String,
    ) -> Result<()> {
        let item = RepairItem {
            // Identity derives from the event: a repeat verification of the
            // same fact refreshes the same repair row.
            id: RepairId::from_event(event.event_id),
            event_id: Some(event.event_id),
            receipt_id: None,
            kind: RepairKind::RemoteMutation,
            status: RepairStatus::Required,
            attempts: event.attempts,
            last_error: Some(detail),
            next_attempt_at: self.clock.now(),
            locator: locator.and_then(|value| serde_json::to_string(value).ok()),
            operation: operation.map(str::to_owned),
        };
        self.repairs.insert(&item).await
    }
}

#[async_trait]
impl OutboxHandler for RemoteVerifyHandler {
    async fn handle(&self, event: &OutboxEvent) -> Result<()> {
        self.handle_event(event).await
    }
}
