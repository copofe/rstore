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
use time::OffsetDateTime;
use uuid::Uuid;

use crate::ports::{MutationReceiptPort, OutboxPort, RepairPort, UnitOfWorkPort};

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

impl ProjectionStage {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ObjectUpsert => "object_upsert",
            Self::SourceLookup => "source_lookup",
            Self::TargetLookup => "target_lookup",
            Self::TagRead => "tag_read",
            Self::TagWrite => "tag_write",
            Self::Tombstone => "tombstone",
        }
    }
}

/// Application-owned coordinator for the remote-mutation consistency seam.
#[derive(Clone)]
pub(crate) struct MutationCoordinator {
    unit_of_work: Option<UnitOfWorkPort>,
    outbox: Option<OutboxPort>,
    repairs: Option<RepairPort>,
    receipts: Option<MutationReceiptPort>,
    catalog: Arc<dyn RemoteObjectRepository>,
    tags: Arc<dyn TagStore>,
    clock: Arc<dyn Clock>,
}

impl MutationCoordinator {
    pub(crate) fn new(
        unit_of_work: Option<UnitOfWorkPort>,
        outbox: Option<OutboxPort>,
        repairs: Option<RepairPort>,
        receipts: Option<MutationReceiptPort>,
        catalog: Arc<dyn RemoteObjectRepository>,
        tags: Arc<dyn TagStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            unit_of_work,
            outbox,
            repairs,
            receipts,
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
        let receipt_id = self
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
                Some(&receipt_id),
                MutationOperation::Upload,
                ProjectionStage::ObjectUpsert,
                &locator,
                &error,
            )
            .await;
        }
    }

    /// Settle a completed copy and, when requested, carry tag associations
    /// from the source catalog row to the target row.
    pub(crate) async fn settle_copy(&self, follow_up: &CopyFollowUp<'_>) {
        let target_locator = follow_up.target.locator();
        let receipt_id = self
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
                Some(&receipt_id),
                MutationOperation::Copy,
                ProjectionStage::ObjectUpsert,
                &target_locator,
                &error,
            )
            .await;
        }

        if !follow_up.preserve_tags {
            return;
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
            Ok(None) => return,
            Err(error) => {
                self.projection_failure(
                    Some(&receipt_id),
                    MutationOperation::Copy,
                    ProjectionStage::SourceLookup,
                    &target_locator,
                    &error,
                )
                .await;
                return;
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
            Ok(None) => return,
            Err(error) => {
                self.projection_failure(
                    Some(&receipt_id),
                    MutationOperation::Copy,
                    ProjectionStage::TargetLookup,
                    &target_locator,
                    &error,
                )
                .await;
                return;
            }
        };
        let associations = match self.tags.associations_for_object(source.id).await {
            Ok(associations) => associations,
            Err(error) => {
                self.projection_failure(
                    Some(&receipt_id),
                    MutationOperation::Copy,
                    ProjectionStage::TagRead,
                    &target_locator,
                    &error,
                )
                .await;
                return;
            }
        };
        for (workspace, tag) in associations {
            if let Err(error) = self
                .tags
                .attach(workspace, target.id, tag, follow_up.at)
                .await
            {
                self.projection_failure(
                    Some(&receipt_id),
                    MutationOperation::Copy,
                    ProjectionStage::TagWrite,
                    &target_locator,
                    &error,
                )
                .await;
            }
        }
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
        let receipt_id = self
            .record_remote_success(MutationOperation::Delete, &locator, None, None)
            .await;
        if let Err(error) = self.catalog.tombstone(connection, bucket, path, at).await {
            self.projection_failure(
                Some(&receipt_id),
                MutationOperation::Delete,
                ProjectionStage::Tombstone,
                &locator,
                &error,
            )
            .await;
        }
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
    /// it as the remote operation's error would report a false remote failure.
    pub(crate) async fn record_remote_success(
        &self,
        operation: MutationOperation,
        locator: &RemoteLocator,
        metadata: Option<&ObjectMetadata>,
        job_id: Option<JobId>,
    ) -> String {
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

        let result = if let Some(unit_of_work) = &self.unit_of_work {
            unit_of_work
                .commit(UnitOfWorkOperation::RemoteMutation {
                    receipt,
                    event: outbox_event,
                })
                .await
        } else {
            self.record_without_unit_of_work(&receipt, &outbox_event)
                .await
        };

        if let Err(error) = result {
            self.record_repair(
                Some(&receipt_id),
                "remote_mutation",
                operation.as_str(),
                locator,
                &format!("remote mutation receipt/outbox failed: {error}"),
            )
            .await;
        }
        receipt_id
    }

    async fn record_without_unit_of_work(
        &self,
        receipt: &RemoteMutationReceipt,
        event: &OutboxEvent,
    ) -> rstore_domain::Result<()> {
        if let Some(receipts) = &self.receipts {
            receipts.record(receipt).await?;
        }
        if let Some(outbox) = &self.outbox {
            outbox.append(event).await?;
        }
        Ok(())
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
        let Some(repairs) = &self.repairs else {
            tracing::error!(
                %error,
                %operation,
                locator = %locator,
                kind,
                "durable repair repository is not configured"
            );
            return;
        };
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
        if let Err(insert_error) = repairs.insert(&item).await {
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
    use rstore_test_support::InMemoryTagStore;
    use time::OffsetDateTime;

    fn coordinator(
        catalog: Arc<InMemoryCatalog>,
        tags: Arc<InMemoryTagStore>,
    ) -> MutationCoordinator {
        MutationCoordinator::new(None, None, None, None, catalog, tags, Arc::new(SystemClock))
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
        let coordinator = coordinator(catalog.clone(), Arc::new(InMemoryTagStore::new()));
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
        let coordinator = coordinator(catalog.clone(), tags.clone());
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
        let coordinator = coordinator(catalog.clone(), Arc::new(InMemoryTagStore::new()));
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
            Some(Arc::new(db.clone())),
            Some(outbox.clone()),
            Some(outbox.clone()),
            Some(Arc::new(db.mutation_receipts())),
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
            .record_remote_success(MutationOperation::Upload, &locator, None, None)
            .await;
        let receipt = db
            .mutation_receipts()
            .get(&receipt_id)
            .await
            .unwrap()
            .expect("receipt is durable");
        assert_eq!(receipt.operation, "upload");

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
            Some(Arc::new(db.clone())),
            Some(outbox.clone()),
            Some(outbox.clone()),
            Some(Arc::new(db.mutation_receipts())),
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
            .await;
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
}
