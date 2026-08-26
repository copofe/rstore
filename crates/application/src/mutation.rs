//! Remote mutation settlement: the durability seam between remote storage
//! writes and the local catalog projection.
//!
//! Contract: every confirmed remote mutation first commits a receipt plus
//! outbox event as one atomic unit-of-work; only then does the coordinator
//! apply catalog projections. Any projection failure is persisted as a repair
//! item so it can be retried, never lost and never re-executed against the
//! remote side.
//!
//! The coordinator is the shared machine: one settlement primitive
//! ([`MutationCoordinator::settle_confirmed`]) behind every confirmed
//! remote mutation — upload, delete, and copy alike — plus the single
//! failure-recording entry ([`MutationCoordinator::note_failure`]) that
//! operation-specific follow-ups (tag mirroring, move verification) use;
//! copy- and move-specific orchestration lives in [`crate::settlement`].

use std::sync::Arc;

// Membership preservation needs the narrow catalog membership trait, not the
// wide workspace store: the dependency bundle hands out the catalog-typed
// handle here (`CatalogTagPort`) on purpose.
use rstore_application_ports::{
    OutboxEvent, RemoteMutationReceipt, RepairItem, RepairKind, RepairStatus, UnitOfWorkOperation,
};
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::events::DomainEvent;
use rstore_domain::id::{BucketId, ConnectionId, JobId, ReceiptId, RepairId};
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Clock, Error, RemoteLocator};
use rstore_storage_api::model::ObjectMetadata;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::ports::{RepairPort, UnitOfWorkPort};

/// Kind of remote mutation being settled.
#[derive(Debug, Clone, Copy)]
pub(crate) enum MutationOperation {
    Upload,
    Copy,
    Delete,
}

impl MutationOperation {
    /// Stable wire/persistence name for this operation kind.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Copy => "copy",
            Self::Delete => "delete",
        }
    }
}

/// Catalog projection steps that can fail after a durable settlement; each
/// maps to a distinct repair stage.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ProjectionStage {
    ObjectUpsert,
    SourceLookup,
    TargetLookup,
    TagRead,
    TagWrite,
    Tombstone,
    TargetVerify,
    SourceDelete,
}

impl ProjectionStage {
    /// Stable persistence name for this projection stage.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ObjectUpsert => "object_upsert",
            Self::SourceLookup => "source_lookup",
            Self::TargetLookup => "target_lookup",
            Self::TagRead => "tag_read",
            Self::TagWrite => "tag_write",
            Self::Tombstone => "tombstone",
            Self::TargetVerify => "target_verify",
            Self::SourceDelete => "source_delete",
        }
    }
}

/// Durability result of committing a remote mutation receipt.
#[derive(Debug, Clone)]
pub(crate) enum RemoteSettlement {
    /// The receipt/outbox unit committed locally; the mutation is durable.
    Durable { receipt_id: ReceiptId },

    /// The commit failed but a repair was queued; the mutation must be
    /// re-applied by the repair loop.
    RepairQueued { receipt_id: ReceiptId },
}

impl RemoteSettlement {
    /// Receipt id carried by either settlement flavor.
    pub(crate) fn receipt_id(&self) -> ReceiptId {
        match self {
            Self::Durable { receipt_id } | Self::RepairQueued { receipt_id } => *receipt_id,
        }
    }
}

/// Coordinates remote mutation settlement.
///
/// Sequencing rule per mutation: record the receipt+outbox unit atomically
/// (`record_remote_success`), then project into the catalog; projection
/// failures are recorded as repairs keyed to the receipt, leaving the remote
/// side untouched.
#[derive(Clone)]
pub(crate) struct MutationCoordinator {
    unit_of_work: UnitOfWorkPort,
    repairs: RepairPort,
    catalog: Arc<dyn RemoteObjectRepository>,
    clock: Arc<dyn Clock>,
}

impl MutationCoordinator {
    /// Wires the coordinator to its persistence ports and catalog stores.
    pub(crate) fn new(
        unit_of_work: UnitOfWorkPort,
        repairs: RepairPort,
        catalog: Arc<dyn RemoteObjectRepository>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            unit_of_work,
            repairs,
            catalog,
            clock,
        }
    }

    /// Settles a confirmed remote upload through the shared primitive:
    /// records the receipt, then upserts the confirmed object into the
    /// catalog. Never fails the caller; projection problems land in the
    /// repair queue instead.
    pub(crate) async fn settle_upload(
        &self,
        connection: ConnectionId,
        bucket: BucketId,
        metadata: &ObjectMetadata,
        at: OffsetDateTime,
    ) {
        let path = match ObjectPath::new(&metadata.key) {
            Ok(path) => path,
            Err(error) => {
                tracing::error!(%error, "remote write returned an invalid object path");
                return;
            }
        };
        let locator = RemoteLocator::new(connection, bucket, path);
        self.settle_confirmed(MutationOperation::Upload, &locator, Some(metadata), at)
            .await;
    }

    /// Settles a confirmed remote delete through the shared primitive:
    /// records the receipt, then tombstones the catalog row. Tombstone
    /// failures are queued as repairs.
    pub(crate) async fn settle_delete(
        &self,
        connection: ConnectionId,
        bucket: BucketId,
        path: &ObjectPath,
        at: OffsetDateTime,
    ) {
        let locator = RemoteLocator::new(connection, bucket, path.clone());
        self.settle_confirmed(MutationOperation::Delete, &locator, None, at)
            .await;
    }

    /// The one settlement primitive for every confirmed remote mutation:
    /// commits the receipt/outbox unit, then projects the confirmation into
    /// the catalog. The projection follows the operation — uploads and
    /// copies upsert the confirmed object, deletes tombstone the row. Never
    /// fails the caller; a failed projection lands in the repair queue
    /// keyed to the receipt, leaving the remote side untouched.
    pub(crate) async fn settle_confirmed(
        &self,
        operation: MutationOperation,
        locator: &RemoteLocator,
        metadata: Option<&ObjectMetadata>,
        at: OffsetDateTime,
    ) -> RemoteSettlement {
        let settlement = self
            .record_remote_success(operation, locator, metadata, None)
            .await;
        let stage = match operation {
            MutationOperation::Delete => ProjectionStage::Tombstone,
            MutationOperation::Upload | MutationOperation::Copy => ProjectionStage::ObjectUpsert,
        };
        let projection = match operation {
            MutationOperation::Delete => {
                self.catalog
                    .tombstone(locator.connection_id, locator.bucket_id, &locator.path, at)
                    .await
            }
            MutationOperation::Upload | MutationOperation::Copy => {
                // Upsert projections need the confirmed object's metadata;
                // upload and copy settlements always carry it.
                let Some(metadata) = metadata else {
                    tracing::error!(
                        operation = operation.as_str(),
                        locator = %locator,
                        "upsert settlement missing object metadata"
                    );
                    return settlement;
                };
                crate::catalog_follow::upsert_confirmed(
                    self.catalog.as_ref(),
                    locator.connection_id,
                    locator.bucket_id,
                    metadata,
                    at,
                )
                .await
            }
        };
        if let Err(error) = projection {
            self.note_failure(&settlement, operation, stage, locator, &error)
                .await;
        }
        settlement
    }

    /// Records a settlement follow-up failure as a repair keyed to the
    /// receipt and logs a warning. The single entry for every producer: the
    /// shared primitive's projection step and operation-specific follow-ups
    /// (tag mirroring, move verification) alike.
    pub(crate) async fn note_failure(
        &self,
        settlement: &RemoteSettlement,
        operation: MutationOperation,
        stage: ProjectionStage,
        locator: &RemoteLocator,
        error: &Error,
    ) {
        self.record_projection_failure(settlement.receipt_id(), operation, stage, locator, error)
            .await;
        tracing::warn!(
            %error,
            operation = operation.as_str(),
            stage = stage.as_str(),
            "settlement follow-up failed"
        );
    }

    /// Commits the atomic settlement unit: one remote-mutation receipt plus
    /// its outbox event, written together via the unit of work. On commit
    /// failure queues a `RemoteMutation` repair so the mutation is retried.
    pub(crate) async fn record_remote_success(
        &self,
        operation: MutationOperation,
        locator: &RemoteLocator,
        metadata: Option<&ObjectMetadata>,
        job_id: Option<JobId>,
    ) -> RemoteSettlement {
        let now = self.clock.now();
        let receipt_id = ReceiptId::new_random();
        let event_id = Uuid::new_v4();
        let event = DomainEvent::RemoteMutationCompleted {
            locator: locator.clone(),
            operation: operation.as_str().to_owned(),
        }
        .envelope(event_id, 0, now);
        let receipt = RemoteMutationReceipt {
            receipt_id,
            job_id,
            operation: operation.as_str().to_owned(),
            locator_json: serde_json::to_string(locator)
                .expect("RemoteLocator is always serializable"),
            remote_version: metadata.and_then(|value| value.version_id.clone()),
            remote_etag: metadata.and_then(|value| value.etag.clone()),
            completed_at: now,
            payload: metadata.and_then(|value| serde_json::to_value(value).ok()),
        };
        let outbox_event = OutboxEvent {
            event_id: event.event_id,
            context: event.context,
            aggregate_type: event.aggregate_type,
            aggregate_id: event.aggregate_id,
            aggregate_version: event.aggregate_version,
            event_type: event.event_type,
            schema_version: event.schema_version,
            occurred_at: event.occurred_at,
            payload: event.payload,
            attempts: 0,
        };

        let result = self
            .unit_of_work
            .commit(UnitOfWorkOperation::RemoteMutation {
                receipt,
                event: outbox_event,
            })
            .await;

        match result {
            Ok(()) => RemoteSettlement::Durable { receipt_id },
            Err(error) => {
                self.record_repair(
                    receipt_id,
                    RepairKind::RemoteMutation,
                    operation,
                    locator,
                    &format!("remote mutation receipt/outbox failed: {error}"),
                )
                .await;
                RemoteSettlement::RepairQueued { receipt_id }
            }
        }
    }

    /// Records a failed catalog projection stage as a queryable repair.
    pub(crate) async fn record_projection_failure(
        &self,
        receipt_id: ReceiptId,
        operation: MutationOperation,
        stage: ProjectionStage,
        locator: &RemoteLocator,
        error: &Error,
    ) {
        self.record_repair(
            receipt_id,
            RepairKind::CatalogProjection,
            operation,
            locator,
            &error.to_string(),
        )
        .await;
        tracing::debug!(
            operation = operation.as_str(),
            stage = stage.as_str(),
            locator = %locator,
            error = %error,
            "catalog projection failure recorded"
        );
    }

    /// Inserts a repair row; a failing insert itself is logged loudly because
    /// there is no deeper fallback for losing repair records.
    ///
    /// Identity derives from the receipt, so re-recording the same failed
    /// follow-up refreshes the same row instead of accumulating.
    pub(crate) async fn record_repair(
        &self,
        receipt_id: ReceiptId,
        kind: RepairKind,
        operation: MutationOperation,
        locator: &RemoteLocator,
        error: &str,
    ) {
        let item = RepairItem {
            id: RepairId::from_receipt(receipt_id),
            event_id: None,
            receipt_id: Some(receipt_id),
            kind,
            status: RepairStatus::Required,
            attempts: 0,
            last_error: Some(error.to_owned()),
            next_attempt_at: self.clock.now(),
            locator: serde_json::to_string(locator).ok(),
            operation: Some(operation.as_str().to_owned()),
        };

        if let Err(insert_error) = self.repairs.insert(&item).await {
            tracing::error!(
                %insert_error,
                %error,
                operation = operation.as_str(),
                locator = %locator,
                "failed to persist remote mutation repair"
            );
        }
    }
}
