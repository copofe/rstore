//! Outbox consumer that re-verifies settled remote mutations against the
//! provider.
//!
//! The mutation coordinator commits a receipt and an outbox fact in one
//! transaction; that receipt alone proves local durability, not provider-side
//! presence. This handler closes the loop: for every `RemoteMutationCompleted`
//! fact it re-stats the remote object and either lets the event be
//! acknowledged (object present) or records a repair item (permanent
//! divergence). Transient provider errors return `Err`, so the dispatcher's
//! backoff retry applies instead of consuming the event.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{
    BucketStore, OutboxEvent, RepairItem, RepairKind, RepairRepository, RepairStatus,
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
pub struct RemoteVerifyHandler {
    gateway: RemoteGateway,
    buckets: Arc<dyn BucketStore>,
    repairs: Arc<dyn RepairRepository>,
    clock: Arc<dyn Clock>,
}

impl RemoteVerifyHandler {
    /// Assemble the handler over the shared gateway and repair store.
    pub fn new(
        gateway: RemoteGateway,
        buckets: Arc<dyn BucketStore>,
        repairs: Arc<dyn RepairRepository>,
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

        match self.verify_presence(&locator).await {
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
            Ok(_) => Ok(Ok(())),
            Err(error) if error.code == ErrorCode::NotFound => Ok(Err(format!(
                "object `{}/{}` missing after settlement",
                bucket.name,
                locator.path.as_str()
            ))),
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
            id: RepairId::new_random(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_application_ports::{ConnectionStore as _, OutboxStore};
    use rstore_domain::bucket::{Bucket, BucketConfig};
    use rstore_domain::connection::{AddressingStyle, ConnectionConfig, Region};
    use rstore_domain::remote::ObjectPath;
    use rstore_domain::{ConnectionId, Error, Secret};
    use rstore_persistence::Database;
    use rstore_test_support::FakeStorageFactory;
    use rstore_test_support::StorageMethod;
    use time::OffsetDateTime;

    fn now() -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH
    }

    async fn db() -> Database {
        Database::open_in_memory().await.unwrap()
    }

    async fn seed_connection(db: &Database, id: ConnectionId) {
        db.connections()
            .insert(&ConnectionConfig {
                id,
                name: "nas".into(),
                endpoint: "http://localhost:9000".into(),
                public_base_url: None,
                cdn_base_url: None,
                region: Region::Auto,
                addressing_style: AddressingStyle::Path,
                tls_verify: true,
                access_key: Some(Secret::new("AK".into())),
                secret_key: Some(Secret::new("SK".into())),
                created_at: now(),
                updated_at: now(),
                last_tested_at: None,
                last_test_status: None,
            })
            .await
            .unwrap();
    }

    async fn seed_bucket(db: &Database, id: rstore_domain::BucketId, connection: ConnectionId) {
        db.buckets()
            .insert(&Bucket {
                id,
                connection_id: connection,
                name: "media".into(),
                config: BucketConfig::default(),
                created_at: now(),
                updated_at: now(),
            })
            .await
            .unwrap();
    }

    fn event(locator: RemoteLocator) -> OutboxEvent {
        into_event(
            DomainEvent::RemoteMutationCompleted {
                locator,
                operation: "upload".into(),
            }
            .envelope(uuid::Uuid::new_v4(), 0, now()),
        )
    }

    fn into_event(envelope: rstore_domain::events::EventEnvelope) -> OutboxEvent {
        OutboxEvent {
            event_id: envelope.event_id,
            context: envelope.context,
            aggregate_type: envelope.aggregate_type,
            aggregate_id: envelope.aggregate_id,
            aggregate_version: envelope.aggregate_version,
            event_type: envelope.event_type,
            schema_version: envelope.schema_version,
            occurred_at: envelope.occurred_at,
            payload: envelope.payload,
            attempts: 0,
        }
    }

    fn locator(
        connection: ConnectionId,
        bucket: rstore_domain::BucketId,
        key: &str,
    ) -> RemoteLocator {
        RemoteLocator {
            connection_id: connection,
            bucket_id: bucket,
            path: ObjectPath::new(key).unwrap(),
        }
    }

    fn handler_for(db: &Database, factory: Arc<FakeStorageFactory>) -> RemoteVerifyHandler {
        let gateway = crate::gateway::RemoteGateway::new(Arc::new(db.connections()), Some(factory));
        RemoteVerifyHandler::new(
            gateway,
            Arc::new(db.buckets()),
            Arc::new(db.outbox()),
            db.clock(),
        )
    }

    #[tokio::test]
    async fn verified_event_passes_through_without_repairs() {
        let database = db().await;
        let connection = ConnectionId::new_random();
        let bucket = rstore_domain::BucketId::new_random();
        seed_connection(&database, connection).await;
        seed_bucket(&database, bucket, connection).await;

        let factory = Arc::new(FakeStorageFactory::new());
        factory.storage().seed("media", "a.txt", b"x", None);

        let handler = handler_for(&database, factory);
        handler
            .handle(&event(locator(connection, bucket, "a.txt")))
            .await
            .expect("verified fact acknowledges");
        assert!(database.outbox().repairs(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn missing_object_records_a_repair_and_acknowledges() {
        let database = db().await;
        let connection = ConnectionId::new_random();
        let bucket = rstore_domain::BucketId::new_random();
        seed_connection(&database, connection).await;
        seed_bucket(&database, bucket, connection).await;

        // No object seeded: the write was lost.
        let handler = handler_for(&database, Arc::new(FakeStorageFactory::new()));
        handler
            .handle(&event(locator(connection, bucket, "lost.txt")))
            .await
            .expect("permanent divergence still acknowledges");

        let repairs = database.outbox().repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].kind, RepairKind::RemoteMutation);
        assert!(matches!(repairs[0].status, RepairStatus::Required));
        assert!(repairs[0]
            .last_error
            .as_deref()
            .unwrap()
            .contains("missing after settlement"));
    }

    #[tokio::test]
    async fn transient_stat_failure_is_not_consumed() {
        let database = db().await;
        let connection = ConnectionId::new_random();
        let bucket = rstore_domain::BucketId::new_random();
        seed_connection(&database, connection).await;
        seed_bucket(&database, bucket, connection).await;

        let factory = Arc::new(FakeStorageFactory::new());
        factory.storage().seed("media", "a.txt", b"x", None);
        // A persistent fault survives the gateway's internal adapter retries:
        // what escapes to the handler is exactly the "still failing after
        // retry" case the dispatcher's backoff exists for.
        factory
            .storage()
            .fail_until_cleared(StorageMethod::Stat, Error::timeout("provider outage"));

        let handler = handler_for(&database, factory.clone());
        assert!(
            handler
                .handle(&event(locator(connection, bucket, "a.txt")))
                .await
                .is_err(),
            "transient failure must stay queued"
        );
        assert!(database.outbox().repairs(10).await.unwrap().is_empty());

        // Outage over: the same event now verifies cleanly.
        factory.storage().clear_fault(StorageMethod::Stat);
        handler
            .handle(&event(locator(connection, bucket, "a.txt")))
            .await
            .expect("recovers after the transient fault");
    }

    #[tokio::test]
    async fn unregistered_bucket_is_permanent_divergence() {
        let database = db().await;
        let connection = ConnectionId::new_random();
        seed_connection(&database, connection).await;

        let handler = handler_for(&database, Arc::new(FakeStorageFactory::new()));
        handler
            .handle(&event(locator(
                connection,
                rstore_domain::BucketId::new_random(),
                "a.txt",
            )))
            .await
            .expect("acknowledged with a repair row");

        let repairs = database.outbox().repairs(10).await.unwrap();
        assert!(repairs[0]
            .last_error
            .as_deref()
            .unwrap()
            .contains("no longer registered"));
    }

    #[tokio::test]
    async fn undecodable_payload_does_not_loop_forever() {
        let database = db().await;
        let mut poison = event(locator(
            ConnectionId::new_random(),
            rstore_domain::BucketId::new_random(),
            "x",
        ));
        poison.payload = serde_json::json!({"type": "something_else"});

        let handler = handler_for(&database, Arc::new(FakeStorageFactory::new()));
        handler.handle(&poison).await.expect("poison acknowledged");
        let repairs = database.outbox().repairs(10).await.unwrap();
        assert!(repairs[0]
            .last_error
            .as_deref()
            .unwrap()
            .contains("decode outbox payload"));
    }

    #[tokio::test]
    async fn dispatcher_end_to_end_acks_and_counts() {
        let database = db().await;
        let connection = ConnectionId::new_random();
        let bucket = rstore_domain::BucketId::new_random();
        seed_connection(&database, connection).await;
        seed_bucket(&database, bucket, connection).await;

        let store: Arc<dyn OutboxStore> = Arc::new(database.outbox());
        store
            .append(&event(locator(connection, bucket, "gone.txt")))
            .await
            .unwrap();

        let dispatcher = crate::outbox::OutboxDispatcher::new(store.clone());
        let handler = handler_for(&database, Arc::new(FakeStorageFactory::new()));
        let report = dispatcher.dispatch_once(now(), &handler).await.unwrap();

        assert_eq!(report.published, 1);
        assert_eq!(report.retried, 0);
        assert_eq!(report.repair_required, 1);
    }
}
