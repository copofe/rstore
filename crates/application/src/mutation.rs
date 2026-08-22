//! Durable coordination for remote mutations and local catalog projection.
//!
//! A provider mutation cannot share a transaction with SQLite. This seam
//! records the completed remote fact first, then lets catalog follow-up report
//! a queryable repair item instead of silently downgrading the remote result.

use std::sync::Arc;

use rstore_application_ports::{
    OutboxEvent, RemoteMutationReceipt, RepairItem, UnitOfWorkOperation,
};
use rstore_catalog::{RemoteObjectRepository, TagStore};
use rstore_domain::events::DomainEvent;
use rstore_domain::id::{BucketId, ConnectionId, JobId};
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Clock, Error, RemoteLocator};
use rstore_storage_api::model::ObjectMetadata;
use rstore_storage_api::Storage;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::ports::{RepairPort, UnitOfWorkPort};

#[derive(Debug, Clone, Copy)]
pub(crate) enum MutationOperation {
    Upload,
    Copy,
    Delete,
}

impl MutationOperation {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Copy => "copy",
            Self::Delete => "delete",
        }
    }
}

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

/// The catalog identity of one remote object involved in a copy or move.
pub(crate) struct CatalogObjectRef<'a> {
    connection: ConnectionId,
    bucket: BucketId,
    path: &'a ObjectPath,
}

impl<'a> CatalogObjectRef<'a> {
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

/// Everything the settlement module needs after one remote copy completes.
pub(crate) struct CopyFollowUp<'a> {
    source: CatalogObjectRef<'a>,
    target: CatalogObjectRef<'a>,
    metadata: &'a ObjectMetadata,
    preserve_tags: bool,
    at: OffsetDateTime,
}

impl<'a> CopyFollowUp<'a> {
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

/// One resolved endpoint of a move completion: the storage handle plus the
/// catalog identity of the object involved. The coordinator needs the remote
/// handle to verify and delete; it never constructs adapters itself.
pub(crate) struct MoveEndpoint<'a> {
    pub(crate) storage: &'a dyn Storage,
    pub(crate) connection: ConnectionId,
    pub(crate) bucket_id: BucketId,
    pub(crate) bucket_name: &'a str,
    pub(crate) path: &'a ObjectPath,
}

/// Result of a move's verify→delete completion after the remote copy
/// succeeded. Failure arms carry the message the job step reports; the
/// durable repair item has already been recorded by the coordinator — a
/// half-finished move is never only a message in the job log.
#[derive(Debug)]
pub(crate) enum MoveFinish {
    Completed,
    VerifyFailed(String),
    DeleteFailed(String),
}

impl ProjectionStage {
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

/// Outcome of recording one completed remote mutation locally.
#[derive(Debug, Clone)]
pub(crate) enum RemoteSettlement {
    /// The receipt and its outbox fact committed as one local unit.
    Durable { receipt_id: String },
    /// The local commit failed and degraded into a repair item. The receipt
    /// id is a correlation handle only — no receipt row exists.
    RepairQueued { receipt_id: String },
}

impl RemoteSettlement {
    /// The correlation handle for repair items, durable or not.
    pub(crate) fn receipt_id(&self) -> &str {
        match self {
            Self::Durable { receipt_id } | Self::RepairQueued { receipt_id } => receipt_id,
        }
    }
}

/// Application-owned coordinator for the remote-mutation consistency seam.
///
/// The durability ports are required, not optional: a remote-mutation
/// receipt and its outbox fact are committed atomically through
/// [`UnitOfWorkPort`], and every projection failure lands in the repair
/// repository. "No durability configured" is not a state this seam can be
/// in — a composition root either provides it or does not build the graph.
#[derive(Clone)]
pub(crate) struct MutationCoordinator {
    unit_of_work: UnitOfWorkPort,
    repairs: RepairPort,
    catalog: Arc<dyn RemoteObjectRepository>,
    tags: Arc<dyn TagStore>,
    clock: Arc<dyn Clock>,
}

impl MutationCoordinator {
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

    /// Settle a completed upload: record the remote fact, then project the
    /// confirmed object locally. Projection failure never reclassifies the
    /// already-completed provider operation.
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
                Some(settlement.receipt_id()),
                MutationOperation::Upload,
                ProjectionStage::ObjectUpsert,
                &locator,
                &error,
            )
            .await;
        }
    }

    /// Settle a completed copy and, when requested, carry tag associations
    /// from the source catalog row to the target row. Returns the settlement
    /// of the copy receipt — [`Self::finish_move`] correlates move repairs
    /// against it.
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
                Some(settlement.receipt_id()),
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
                    Some(settlement.receipt_id()),
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
                    Some(settlement.receipt_id()),
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
                    Some(settlement.receipt_id()),
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
                    Some(settlement.receipt_id()),
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

    /// Settle a completed delete by recording the fact and tombstoning the
    /// local row. A failed tombstone becomes repair state.
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
                Some(settlement.receipt_id()),
                MutationOperation::Delete,
                ProjectionStage::Tombstone,
                &locator,
                &error,
            )
            .await;
        }
    }

    /// Complete a move after the remote copy succeeded: verify the target
    /// landed (MOVE-014), delete the source, then settle the delete. Every
    /// failure arm leaves a queryable repair item against the source locator
    /// — the actionable object for a repair tool — and the copy settlement
    /// is never reclassified: the provider copy already succeeded.
    pub(crate) async fn finish_move(
        &self,
        copy: &RemoteSettlement,
        follow_up: &CopyFollowUp<'_>,
        source: &MoveEndpoint<'_>,
        target: &MoveEndpoint<'_>,
    ) -> MoveFinish {
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

    /// Record a failed move completion as durable repair state, correlated
    /// with the copy settlement.
    async fn move_failure(
        &self,
        copy: &RemoteSettlement,
        stage: ProjectionStage,
        source: &MoveEndpoint<'_>,
        message: &str,
    ) {
        let locator = RemoteLocator::new(source.connection, source.bucket_id, source.path.clone());
        self.record_repair(
            Some(copy.receipt_id()),
            "move_followup",
            MutationOperation::Copy.as_str(),
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

    async fn projection_failure(
        &self,
        receipt_id: Option<&str>,
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

    /// Persist a receipt and a domain fact after the provider has confirmed a
    /// mutation. A failure is converted into a repair item because returning
    /// it as the remote operation's error would report a false remote failure;
    /// the settlement distinguishes the durable case from the degraded one so
    /// callers cannot mistake a correlation handle for a committed receipt.
    pub(crate) async fn record_remote_success(
        &self,
        operation: MutationOperation,
        locator: &RemoteLocator,
        metadata: Option<&ObjectMetadata>,
        job_id: Option<JobId>,
    ) -> RemoteSettlement {
        let now = self.clock.now();
        let receipt_id = format!("receipt_{}", Uuid::new_v4());
        let event_id = Uuid::new_v4();
        let event = DomainEvent::RemoteMutationCompleted {
            locator: locator.clone(),
            operation: operation.as_str().to_owned(),
        }
        .envelope(event_id, 0, now);
        let receipt = RemoteMutationReceipt {
            receipt_id: receipt_id.clone(),
            job_id: job_id.map(|id| id.to_string()),
            operation: operation.as_str().to_owned(),
            locator_json: serde_json::to_string(locator)
                .expect("RemoteLocator is always serializable"),
            remote_version: metadata.and_then(|value| value.version_id.clone()),
            remote_etag: metadata.and_then(|value| value.etag.clone()),
            completed_at: now,
            payload: metadata.and_then(|value| serde_json::to_value(value).ok()),
        };
        let outbox_event = OutboxEvent {
            event_id: event.event_id.to_string(),
            context: event.context.to_string(),
            aggregate_type: event.aggregate_type.to_owned(),
            aggregate_id: event.aggregate_id,
            aggregate_version: event.aggregate_version,
            event_type: event.event_type,
            schema_version: event.schema_version,
            occurred_at: event.occurred_at,
            payload: event.payload,
            attempts: 0,
        };

        // The atomicity guarantee lives here — and only here: the receipt and
        // its outbox fact commit as one local unit, or the failure becomes a
        // queryable repair below.
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
                    Some(&receipt_id),
                    "remote_mutation",
                    operation.as_str(),
                    locator,
                    &format!("remote mutation receipt/outbox failed: {error}"),
                )
                .await;
                RemoteSettlement::RepairQueued { receipt_id }
            }
        }
    }

    /// Convert any local projection failure into durable repair state.
    pub(crate) async fn record_projection_failure(
        &self,
        receipt_id: Option<&str>,
        operation: MutationOperation,
        stage: ProjectionStage,
        locator: &RemoteLocator,
        error: &Error,
    ) {
        self.record_repair(
            receipt_id,
            "catalog_projection",
            operation.as_str(),
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

    async fn record_repair(
        &self,
        receipt_id: Option<&str>,
        kind: &str,
        operation: &str,
        locator: &RemoteLocator,
        error: &str,
    ) {
        let item = RepairItem {
            id: format!("repair_{}", Uuid::new_v4()),
            receipt_id: receipt_id.map(str::to_owned),
            kind: kind.to_owned(),
            status: "repair_required".to_owned(),
            attempts: 0,
            last_error: Some(error.to_owned()),
            next_attempt_at: self.clock.now(),
            locator: serde_json::to_string(locator).ok(),
            operation: Some(operation.to_owned()),
        };
        // Deliberate last-resort logging, not an error path: the remote
        // operation already succeeded and must never be reclassified as
        // failed (architecture-guide §1.2). If even the repair insert fails,
        // the database itself is broken — surfacing that here would turn a
        // durable fact into a lie.
        if let Err(insert_error) = self.repairs.insert(&item).await {
            tracing::error!(
                %insert_error,
                %error,
                %operation,
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
    use rstore_catalog::{InMemoryCatalog, RemoteObjectRepository, TagStore};
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
        let connection = rstore_domain::ConnectionId::new();
        let bucket = rstore_domain::BucketId::new();
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
        let connection = rstore_domain::ConnectionId::new();
        let bucket = rstore_domain::BucketId::new();
        let workspace = rstore_domain::WorkspaceId::new();
        let source_path = ObjectPath::new("docs/source.txt").unwrap();
        let source = RemoteObject {
            id: rstore_domain::RemoteObjectId::new(),
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
        let tag = rstore_domain::TagId::new();
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
        let connection = rstore_domain::ConnectionId::new();
        let bucket = rstore_domain::BucketId::new();
        let path = ObjectPath::new("docs/delete.txt").unwrap();
        catalog
            .upsert(&RemoteObject {
                id: rstore_domain::RemoteObjectId::new(),
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
            rstore_domain::ConnectionId::new(),
            rstore_domain::BucketId::new(),
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
            rstore_domain::ConnectionId::new(),
            rstore_domain::BucketId::new(),
            ObjectPath::new("docs/a.txt").unwrap(),
        );
        let receipt_id = coordinator
            .record_remote_success(MutationOperation::Copy, &locator, None, None)
            .await
            .receipt_id()
            .to_owned();
        coordinator
            .record_projection_failure(
                Some(&receipt_id),
                MutationOperation::Copy,
                ProjectionStage::ObjectUpsert,
                &locator,
                &Error::database("catalog unavailable"),
            )
            .await;

        let repairs = outbox.repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].receipt_id.as_deref(), Some(receipt_id.as_str()));
        assert_eq!(repairs[0].kind, "catalog_projection");
        assert_eq!(repairs[0].status, "repair_required");
    }

    /// A move fixture: one FakeStorage standing in for both endpoints, a
    /// seeded source, and a follow-up whose metadata matches the target seed.
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
            connection: rstore_domain::ConnectionId::new(),
            bucket: rstore_domain::BucketId::new(),
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
            connection: rstore_domain::ConnectionId::new(),
            bucket: rstore_domain::BucketId::new(),
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
        // The undeleted source is queryable repair state, correlated with the
        // copy settlement — it no longer lives only in the job log.
        let repairs = outbox.repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].kind, "move_followup");
        assert_eq!(repairs[0].status, "repair_required");
        assert_eq!(
            repairs[0].receipt_id.as_deref(),
            Some(settlement.receipt_id())
        );
    }

    #[tokio::test]
    async fn finish_move_verify_mismatch_keeps_source_and_repairs() {
        let storage = Arc::new(FakeStorage::new());
        storage.seed("media", "docs/source.txt", b"source", None);
        // Target seed disagrees with the follow-up metadata (7 bytes): the
        // verify arm must stop before touching the source.
        storage.seed("media", "docs/target.txt", b"wrong", None);
        let (coordinator, outbox) = move_coordinator(storage.clone()).await;
        let fixture = MoveFixture {
            storage: storage.clone(),
            connection: rstore_domain::ConnectionId::new(),
            bucket: rstore_domain::BucketId::new(),
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
        assert_eq!(repairs[0].kind, "move_followup");
    }
}
