//! Remote mutation settlement: the durability seam between remote storage
//! writes and the local catalog projection.
//!
//! Contract: every confirmed remote mutation first commits a receipt plus
//! outbox event as one atomic unit-of-work; only then does the coordinator
//! apply catalog projections. Any projection failure is persisted as a repair
//! item so it can be retried, never lost and never re-executed against the
//! remote side.

use std::sync::Arc;

// Membership preservation needs the narrow catalog membership trait, not the
// wide workspace store: the dependency bundle hands out the catalog-typed
// handle here (`CatalogTagPort`) on purpose.
use rstore_application_ports::{
    OutboxEvent, RemoteMutationReceipt, RepairItem, RepairKind, RepairStatus, UnitOfWorkOperation,
};
use rstore_catalog::{RemoteObjectRepository, TagStore};
use rstore_domain::events::DomainEvent;
use rstore_domain::id::{BucketId, ConnectionId, JobId, ReceiptId, RepairId};
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Clock, Error, RemoteLocator};
use rstore_storage_api::model::ObjectMetadata;
use rstore_storage_api::Storage;
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

/// Locator triple identifying one remote object in the catalog.
pub(crate) struct CatalogObjectRef<'a> {
    connection: ConnectionId,
    bucket: BucketId,
    path: &'a ObjectPath,
}

impl<'a> CatalogObjectRef<'a> {
    /// Builds a reference to a remote object at the given locator.
    pub(crate) fn new(connection: ConnectionId, bucket: BucketId, path: &'a ObjectPath) -> Self {
        Self {
            connection,
            bucket,
            path,
        }
    }

    fn locator(&self) -> RemoteLocator {
        RemoteLocator::new(self.connection, self.bucket, self.path.clone())
    }
}

/// Post-copy bookkeeping payload for settling a completed remote copy,
/// including optional tag propagation from source to target.
pub(crate) struct CopyFollowUp<'a> {
    source: CatalogObjectRef<'a>,
    target: CatalogObjectRef<'a>,
    metadata: &'a ObjectMetadata,
    preserve_tags: bool,
    at: OffsetDateTime,
}

impl<'a> CopyFollowUp<'a> {
    /// Builds the settlement payload for a copy from `source` to `target`.
    pub(crate) fn new(
        source: CatalogObjectRef<'a>,
        target: CatalogObjectRef<'a>,
        metadata: &'a ObjectMetadata,
        preserve_tags: bool,
        at: OffsetDateTime,
    ) -> Self {
        Self {
            source,
            target,
            metadata,
            preserve_tags,
            at,
        }
    }
}

/// Storage-side endpoint of a move operation: the handle used to verify or
/// delete an object plus its catalog identity.
pub(crate) struct MoveEndpoint<'a> {
    pub(crate) storage: &'a dyn Storage,
    pub(crate) connection: ConnectionId,
    pub(crate) bucket_id: BucketId,
    pub(crate) bucket_name: &'a str,
    pub(crate) path: &'a ObjectPath,
}

/// Outcome of finishing a move after its copy settled durably.
#[derive(Debug)]
pub(crate) enum MoveFinish {
    Completed,
    VerifyFailed(String),
    DeleteFailed(String),
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
    tags: Arc<dyn TagStore>,
    clock: Arc<dyn Clock>,
}

impl MutationCoordinator {
    /// Wires the coordinator to its persistence ports and catalog stores.
    pub(crate) fn new(
        unit_of_work: UnitOfWorkPort,
        repairs: RepairPort,
        catalog: Arc<dyn RemoteObjectRepository>,
        tags: Arc<dyn TagStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            unit_of_work,
            repairs,
            catalog,
            tags,
            clock,
        }
    }

    #[cfg(test)]
    pub(crate) fn catalog_for_test(&self) -> Arc<dyn RemoteObjectRepository> {
        self.catalog.clone()
    }

    /// Settles a confirmed remote upload: records the receipt, then upserts
    /// the confirmed object into the catalog. Never fails the caller;
    /// projection problems land in the repair queue instead.
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
        let settlement = self
            .record_remote_success(MutationOperation::Upload, &locator, Some(metadata), None)
            .await;
        if let Err(error) = crate::catalog_follow::upsert_confirmed(
            self.catalog.as_ref(),
            connection,
            bucket,
            metadata,
            at,
        )
        .await
        {
            self.projection_failure(
                settlement.receipt_id(),
                MutationOperation::Upload,
                ProjectionStage::ObjectUpsert,
                &locator,
                &error,
            )
            .await;
        }
    }

    /// Settles a confirmed remote copy: records the receipt, upserts the
    /// target object, and when tag preservation is requested mirrors the
    /// source's tag associations onto the target. Each failed stage is
    /// repaired independently; earlier successes are never rolled back.
    pub(crate) async fn settle_copy(&self, follow_up: &CopyFollowUp<'_>) -> RemoteSettlement {
        let target_locator = follow_up.target.locator();
        let settlement = self
            .record_remote_success(
                MutationOperation::Copy,
                &target_locator,
                Some(follow_up.metadata),
                None,
            )
            .await;

        if let Err(error) = crate::catalog_follow::upsert_confirmed(
            self.catalog.as_ref(),
            follow_up.target.connection,
            follow_up.target.bucket,
            follow_up.metadata,
            follow_up.at,
        )
        .await
        {
            self.projection_failure(
                settlement.receipt_id(),
                MutationOperation::Copy,
                ProjectionStage::ObjectUpsert,
                &target_locator,
                &error,
            )
            .await;
        }

        if !follow_up.preserve_tags {
            return settlement;
        }

        // Tag mirroring reads through the catalog rather than trusting caller
        // state, so partially projected copies still pick up their tags.
        let source = match self
            .catalog
            .find_by_locator(
                follow_up.source.connection,
                follow_up.source.bucket,
                follow_up.source.path,
            )
            .await
        {
            Ok(Some(source)) => source,
            Ok(None) => return settlement,
            Err(error) => {
                self.projection_failure(
                    settlement.receipt_id(),
                    MutationOperation::Copy,
                    ProjectionStage::SourceLookup,
                    &target_locator,
                    &error,
                )
                .await;
                return settlement;
            }
        };
        let target = match self
            .catalog
            .find_by_locator(
                follow_up.target.connection,
                follow_up.target.bucket,
                follow_up.target.path,
            )
            .await
        {
            Ok(Some(target)) => target,
            Ok(None) => return settlement,
            Err(error) => {
                self.projection_failure(
                    settlement.receipt_id(),
                    MutationOperation::Copy,
                    ProjectionStage::TargetLookup,
                    &target_locator,
                    &error,
                )
                .await;
                return settlement;
            }
        };
        let associations = match self.tags.associations_for_object(source.id).await {
            Ok(associations) => associations,
            Err(error) => {
                self.projection_failure(
                    settlement.receipt_id(),
                    MutationOperation::Copy,
                    ProjectionStage::TagRead,
                    &target_locator,
                    &error,
                )
                .await;
                return settlement;
            }
        };
        for (workspace, tag) in associations {
            if let Err(error) = self
                .tags
                .attach(workspace, target.id, tag, follow_up.at)
                .await
            {
                self.projection_failure(
                    settlement.receipt_id(),
                    MutationOperation::Copy,
                    ProjectionStage::TagWrite,
                    &target_locator,
                    &error,
                )
                .await;
            }
        }
        settlement
    }

    /// Settles a confirmed remote delete: records the receipt, then tombstones
    /// the catalog row. Tombstone failures are queued as repairs.
    pub(crate) async fn settle_delete(
        &self,
        connection: ConnectionId,
        bucket: BucketId,
        path: &ObjectPath,
        at: OffsetDateTime,
    ) {
        let locator = RemoteLocator::new(connection, bucket, path.clone());
        let settlement = self
            .record_remote_success(MutationOperation::Delete, &locator, None, None)
            .await;
        if let Err(error) = self.catalog.tombstone(connection, bucket, path, at).await {
            self.projection_failure(
                settlement.receipt_id(),
                MutationOperation::Delete,
                ProjectionStage::Tombstone,
                &locator,
                &error,
            )
            .await;
        }
    }

    /// Finishes a move whose copy already settled: verifies the target size,
    /// deletes the source, and settles the source delete in the catalog. A
    /// verification or deletion failure keeps the source intact and records a
    /// `MoveFollowup` repair instead of losing the copied data.
    pub(crate) async fn finish_move(
        &self,
        copy: &RemoteSettlement,
        follow_up: &CopyFollowUp<'_>,
        source: &MoveEndpoint<'_>,
        target: &MoveEndpoint<'_>,
    ) -> MoveFinish {
        // Size check is the integrity gate: only delete the source once the
        // target provably holds the expected bytes.
        match target.storage.stat(target.bucket_name, target.path).await {
            Ok(verified) if verified.size == follow_up.metadata.size => {}
            Ok(_) => {
                let message = "target size mismatch after copy; source kept".to_owned();
                self.move_failure(copy, ProjectionStage::TargetVerify, source, &message)
                    .await;
                return MoveFinish::VerifyFailed(message);
            }
            Err(error) => {
                let message = format!("target verification failed; source kept: {error}");
                self.move_failure(copy, ProjectionStage::TargetVerify, source, &message)
                    .await;
                return MoveFinish::VerifyFailed(message);
            }
        }
        if let Err(error) = source.storage.delete(source.bucket_name, source.path).await {
            let message = format!("copied, but delete failed: {error}");
            self.move_failure(copy, ProjectionStage::SourceDelete, source, &message)
                .await;
            return MoveFinish::DeleteFailed(message);
        }
        self.settle_delete(
            source.connection,
            source.bucket_id,
            source.path,
            follow_up.at,
        )
        .await;
        MoveFinish::Completed
    }

    /// Records a move-stage failure as a repair anchored to the copy receipt.
    async fn move_failure(
        &self,
        copy: &RemoteSettlement,
        stage: ProjectionStage,
        source: &MoveEndpoint<'_>,
        message: &str,
    ) {
        let locator = RemoteLocator::new(source.connection, source.bucket_id, source.path.clone());
        self.record_repair(
            copy.receipt_id(),
            RepairKind::MoveFollowup,
            MutationOperation::Copy,
            &locator,
            message,
        )
        .await;
        tracing::warn!(
            stage = stage.as_str(),
            locator = %locator,
            "{message}; repair recorded"
        );
    }

    /// Persists a projection failure as a repair and logs a warning.
    async fn projection_failure(
        &self,
        receipt_id: ReceiptId,
        operation: MutationOperation,
        stage: ProjectionStage,
        locator: &RemoteLocator,
        error: &Error,
    ) {
        self.record_projection_failure(receipt_id, operation, stage, locator, error)
            .await;
        tracing::warn!(
            %error,
            operation = operation.as_str(),
            stage = stage.as_str(),
            "catalog follow-up failed"
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
    async fn record_repair(
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_application_ports::{OutboxStore, RemoteMutationRepository};
    use rstore_catalog::{InMemoryCatalog, RemoteObjectRepository};
    use rstore_domain::remote::ObjectPath;
    use rstore_domain::remote::RemoteObject;
    use rstore_domain::SystemClock;
    use rstore_persistence::Database;
    use rstore_test_support::{FakeStorage, InMemoryTagStore, StorageMethod};
    use time::OffsetDateTime;

    async fn coordinator(
        catalog: Arc<InMemoryCatalog>,
        tags: Arc<InMemoryTagStore>,
    ) -> MutationCoordinator {
        let db = Database::open_in_memory().await.unwrap();
        MutationCoordinator::new(
            Arc::new(db.clone()),
            Arc::new(db.outbox()),
            catalog,
            tags,
            Arc::new(SystemClock),
        )
    }

    fn metadata(key: &str) -> ObjectMetadata {
        ObjectMetadata {
            key: key.to_owned(),
            size: 7,
            etag: Some("etag".into()),
            content_type: Some("text/plain".into()),
            last_modified: OffsetDateTime::UNIX_EPOCH,
            storage_class: None,
            version_id: None,
        }
    }

    #[tokio::test]
    async fn upload_settlement_projects_the_confirmed_object() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let coordinator = coordinator(catalog.clone(), Arc::new(InMemoryTagStore::new())).await;
        let connection = rstore_domain::ConnectionId::new_random();
        let bucket = rstore_domain::BucketId::new_random();
        let metadata = metadata("docs/upload.txt");

        coordinator
            .settle_upload(connection, bucket, &metadata, OffsetDateTime::UNIX_EPOCH)
            .await;

        let object = catalog
            .find_by_locator(
                connection,
                bucket,
                &ObjectPath::new("docs/upload.txt").unwrap(),
            )
            .await
            .unwrap()
            .expect("upload was projected");
        assert_eq!(object.size, 7);
        assert_eq!(object.state, rstore_domain::ObjectState::Confirmed);
    }

    #[tokio::test]
    async fn copy_settlement_preserves_source_tags() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let tags = Arc::new(InMemoryTagStore::new());
        let coordinator = coordinator(catalog.clone(), tags.clone()).await;
        let connection = rstore_domain::ConnectionId::new_random();
        let bucket = rstore_domain::BucketId::new_random();
        let workspace = rstore_domain::WorkspaceId::new_random();
        let source_path = ObjectPath::new("docs/source.txt").unwrap();
        let source = RemoteObject {
            id: rstore_domain::RemoteObjectId::new_random(),
            connection_id: connection,
            bucket_id: bucket,
            path: source_path.clone(),
            size: 3,
            etag: None,
            content_type: None,
            last_modified: OffsetDateTime::UNIX_EPOCH,
            storage_class: None,
            version_id: None,
            confirmed_at: OffsetDateTime::UNIX_EPOCH,
            state: rstore_domain::ObjectState::Confirmed,
        };
        catalog.upsert(&source).await.unwrap();
        let tag = rstore_domain::TagId::new_random();
        tags.attach(workspace, source.id, tag, OffsetDateTime::UNIX_EPOCH)
            .await
            .unwrap();

        let target_path = ObjectPath::new("docs/target.txt").unwrap();
        let target_metadata = metadata("docs/target.txt");
        let follow_up = CopyFollowUp::new(
            CatalogObjectRef::new(connection, bucket, &source_path),
            CatalogObjectRef::new(connection, bucket, &target_path),
            &target_metadata,
            true,
            OffsetDateTime::UNIX_EPOCH,
        );
        coordinator.settle_copy(&follow_up).await;

        let target = catalog
            .find_by_locator(connection, bucket, &target_path)
            .await
            .unwrap()
            .expect("copy was projected");
        assert!(
            tags.associations().contains(&(workspace, target.id, tag)),
            "the copied object keeps the source association"
        );
    }

    #[tokio::test]
    async fn delete_settlement_removes_the_catalog_row() {
        let catalog = Arc::new(InMemoryCatalog::new());
        let coordinator = coordinator(catalog.clone(), Arc::new(InMemoryTagStore::new())).await;
        let connection = rstore_domain::ConnectionId::new_random();
        let bucket = rstore_domain::BucketId::new_random();
        let path = ObjectPath::new("docs/delete.txt").unwrap();
        catalog
            .upsert(&RemoteObject {
                id: rstore_domain::RemoteObjectId::new_random(),
                connection_id: connection,
                bucket_id: bucket,
                path: path.clone(),
                size: 1,
                etag: None,
                content_type: None,
                last_modified: OffsetDateTime::UNIX_EPOCH,
                storage_class: None,
                version_id: None,
                confirmed_at: OffsetDateTime::UNIX_EPOCH,
                state: rstore_domain::ObjectState::Confirmed,
            })
            .await
            .unwrap();

        coordinator
            .settle_delete(connection, bucket, &path, OffsetDateTime::UNIX_EPOCH)
            .await;

        assert!(catalog
            .find_by_locator(connection, bucket, &path)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn remote_receipt_and_outbox_are_committed_as_one_local_unit() {
        let db = Database::open_in_memory().await.unwrap();
        let outbox = Arc::new(db.outbox());
        let coordinator = MutationCoordinator::new(
            Arc::new(db.clone()),
            outbox.clone(),
            Arc::new(InMemoryCatalog::new()),
            Arc::new(InMemoryTagStore::new()),
            Arc::new(SystemClock),
        );
        let locator = RemoteLocator::new(
            rstore_domain::ConnectionId::new_random(),
            rstore_domain::BucketId::new_random(),
            ObjectPath::new("docs/a.txt").unwrap(),
        );

        let settlement = coordinator
            .record_remote_success(MutationOperation::Upload, &locator, None, None)
            .await;
        let receipt_id = settlement.receipt_id();
        assert!(
            matches!(settlement, RemoteSettlement::Durable { .. }),
            "a committed unit-of-work settles durably"
        );
        let receipt = db
            .mutation_receipts()
            .get(receipt_id)
            .await
            .unwrap()
            .expect("receipt is durable");
        assert_eq!(receipt.operation, "upload");
        assert_eq!(receipt.receipt_id, receipt_id);

        let events = outbox
            .claim_due(OffsetDateTime::now_utc(), 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].aggregate_version, 1);
        assert_eq!(events[0].event_type, "remote_mutation_completed");
    }

    #[tokio::test]
    async fn projection_failure_is_queryable_repair_state() {
        let db = Database::open_in_memory().await.unwrap();
        let outbox = Arc::new(db.outbox());
        let coordinator = MutationCoordinator::new(
            Arc::new(db.clone()),
            outbox.clone(),
            Arc::new(InMemoryCatalog::new()),
            Arc::new(InMemoryTagStore::new()),
            Arc::new(SystemClock),
        );
        let locator = RemoteLocator::new(
            rstore_domain::ConnectionId::new_random(),
            rstore_domain::BucketId::new_random(),
            ObjectPath::new("docs/a.txt").unwrap(),
        );
        let receipt_id = coordinator
            .record_remote_success(MutationOperation::Copy, &locator, None, None)
            .await
            .receipt_id();
        coordinator
            .record_projection_failure(
                receipt_id,
                MutationOperation::Copy,
                ProjectionStage::ObjectUpsert,
                &locator,
                &Error::database("catalog unavailable"),
            )
            .await;

        let repairs = outbox.repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].receipt_id, Some(receipt_id));
        assert_eq!(repairs[0].kind, RepairKind::CatalogProjection);
        assert_eq!(repairs[0].status, RepairStatus::Required);
    }

    struct MoveFixture {
        storage: Arc<FakeStorage>,
        connection: rstore_domain::ConnectionId,
        bucket: rstore_domain::BucketId,
        source_path: ObjectPath,
        target_path: ObjectPath,
    }

    async fn move_coordinator(
        storage: Arc<FakeStorage>,
    ) -> (
        MutationCoordinator,
        Arc<rstore_persistence::OutboxRepository>,
    ) {
        let db = Database::open_in_memory().await.unwrap();
        let outbox = Arc::new(db.outbox());
        let coordinator = MutationCoordinator::new(
            Arc::new(db),
            outbox.clone(),
            Arc::new(InMemoryCatalog::new()),
            Arc::new(InMemoryTagStore::new()),
            Arc::new(SystemClock),
        );
        let _ = storage;
        (coordinator, outbox)
    }

    fn move_endpoint<'a>(fixture: &'a MoveFixture, path: &'a ObjectPath) -> MoveEndpoint<'a> {
        MoveEndpoint {
            storage: fixture.storage.as_ref(),
            connection: fixture.connection,
            bucket_id: fixture.bucket,
            bucket_name: "media",
            path,
        }
    }

    #[tokio::test]
    async fn finish_move_completes_and_settles_the_delete() {
        let storage = Arc::new(FakeStorage::new());
        storage.seed("media", "docs/source.txt", b"source", None);
        storage.seed("media", "docs/target.txt", b"7bytes!", None);
        let (coordinator, outbox) = move_coordinator(storage.clone()).await;
        let fixture = MoveFixture {
            storage: storage.clone(),
            connection: rstore_domain::ConnectionId::new_random(),
            bucket: rstore_domain::BucketId::new_random(),
            source_path: ObjectPath::new("docs/source.txt").unwrap(),
            target_path: ObjectPath::new("docs/target.txt").unwrap(),
        };
        let target_metadata = metadata("docs/target.txt");
        let follow_up = {
            let source =
                CatalogObjectRef::new(fixture.connection, fixture.bucket, &fixture.source_path);
            let target =
                CatalogObjectRef::new(fixture.connection, fixture.bucket, &fixture.target_path);
            CopyFollowUp::new(
                source,
                target,
                &target_metadata,
                false,
                OffsetDateTime::UNIX_EPOCH,
            )
        };
        let source = move_endpoint(&fixture, &fixture.source_path);
        let target = move_endpoint(&fixture, &fixture.target_path);
        let settlement = coordinator.settle_copy(&follow_up).await;

        let finish = coordinator
            .finish_move(&settlement, &follow_up, &source, &target)
            .await;

        assert!(matches!(finish, MoveFinish::Completed));
        let source_path = ObjectPath::new("docs/source.txt").unwrap();
        assert!(storage.stat("media", &source_path).await.is_err());
        assert!(outbox.repairs(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn finish_move_delete_failure_is_queryable_repair() {
        let storage = Arc::new(FakeStorage::new());
        storage.seed("media", "docs/source.txt", b"source", None);
        storage.seed("media", "docs/target.txt", b"7bytes!", None);
        storage.fail_next(
            StorageMethod::Delete,
            Error::database("injected delete failure"),
        );
        let (coordinator, outbox) = move_coordinator(storage.clone()).await;
        let fixture = MoveFixture {
            storage: storage.clone(),
            connection: rstore_domain::ConnectionId::new_random(),
            bucket: rstore_domain::BucketId::new_random(),
            source_path: ObjectPath::new("docs/source.txt").unwrap(),
            target_path: ObjectPath::new("docs/target.txt").unwrap(),
        };
        let target_metadata = metadata("docs/target.txt");
        let follow_up = {
            let source =
                CatalogObjectRef::new(fixture.connection, fixture.bucket, &fixture.source_path);
            let target =
                CatalogObjectRef::new(fixture.connection, fixture.bucket, &fixture.target_path);
            CopyFollowUp::new(
                source,
                target,
                &target_metadata,
                false,
                OffsetDateTime::UNIX_EPOCH,
            )
        };
        let source = move_endpoint(&fixture, &fixture.source_path);
        let target = move_endpoint(&fixture, &fixture.target_path);
        let settlement = coordinator.settle_copy(&follow_up).await;

        let finish = coordinator
            .finish_move(&settlement, &follow_up, &source, &target)
            .await;

        match finish {
            MoveFinish::DeleteFailed(message) => {
                assert!(message.contains("copied, but delete failed"));
            }
            other => panic!("expected DeleteFailed, got {other:?}"),
        }

        let repairs = outbox.repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].kind, RepairKind::MoveFollowup);
        assert_eq!(repairs[0].status, RepairStatus::Required);
        assert_eq!(repairs[0].receipt_id, Some(settlement.receipt_id()));
    }

    #[tokio::test]
    async fn finish_move_verify_mismatch_keeps_source_and_repairs() {
        let storage = Arc::new(FakeStorage::new());
        storage.seed("media", "docs/source.txt", b"source", None);

        storage.seed("media", "docs/target.txt", b"wrong", None);
        let (coordinator, outbox) = move_coordinator(storage.clone()).await;
        let fixture = MoveFixture {
            storage: storage.clone(),
            connection: rstore_domain::ConnectionId::new_random(),
            bucket: rstore_domain::BucketId::new_random(),
            source_path: ObjectPath::new("docs/source.txt").unwrap(),
            target_path: ObjectPath::new("docs/target.txt").unwrap(),
        };
        let target_metadata = metadata("docs/target.txt");
        let follow_up = {
            let source =
                CatalogObjectRef::new(fixture.connection, fixture.bucket, &fixture.source_path);
            let target =
                CatalogObjectRef::new(fixture.connection, fixture.bucket, &fixture.target_path);
            CopyFollowUp::new(
                source,
                target,
                &target_metadata,
                false,
                OffsetDateTime::UNIX_EPOCH,
            )
        };
        let source = move_endpoint(&fixture, &fixture.source_path);
        let target = move_endpoint(&fixture, &fixture.target_path);
        let settlement = coordinator.settle_copy(&follow_up).await;

        let finish = coordinator
            .finish_move(&settlement, &follow_up, &source, &target)
            .await;

        match finish {
            MoveFinish::VerifyFailed(message) => {
                assert!(message.contains("target size mismatch"));
            }
            other => panic!("expected VerifyFailed, got {other:?}"),
        }
        assert!(storage.stat("media", &fixture.source_path).await.is_ok());
        let repairs = outbox.repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].kind, RepairKind::MoveFollowup);
    }
}
