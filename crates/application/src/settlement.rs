//! Copy/move settlement — everything between a succeeded copy item and its
//! final verdict step.
//!
//! The run loop hands a succeeded item's verdict here; this module owns the
//! whole chain: rederive the landed target key from the adapter's metadata,
//! commit the receipt/outbox unit through the mutation coordinator, project
//! the catalog (tag mirroring included), and — for moves, whose item carries
//! the delete-guarded source adapter — verify the target then delete the
//! source, revising the verdict on failure. Failures map to repair rows,
//! never to lost follow-ups; [`Step`](crate::job_run::Step) is the only
//! thing that comes back.

use std::sync::Arc;

use rstore_application_ports::RepairKind;
use rstore_catalog::{RemoteObjectRepository, TagStore};
use rstore_domain::bucket::Bucket;
use rstore_domain::id::{BucketId, ConnectionId};
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Clock, RemoteLocator};
use rstore_storage_api::model::ObjectMetadata;
use rstore_storage_api::Storage;
use time::OffsetDateTime;

use crate::copy::Item;
use crate::job_run::{ItemVerdict, Step};
use crate::mutation::{MutationCoordinator, MutationOperation, ProjectionStage, RemoteSettlement};

/// Settles confirmed copies (and their move follow-through).
pub(crate) struct Settlement {
    mutations: Arc<MutationCoordinator>,
    catalog: Arc<dyn RemoteObjectRepository>,
    tags: Arc<dyn TagStore>,
    clock: Arc<dyn Clock>,
}

impl Settlement {
    /// Wire the settlement over the shared mutation machine and stores.
    pub(crate) fn new(
        mutations: Arc<MutationCoordinator>,
        catalog: Arc<dyn RemoteObjectRepository>,
        tags: Arc<dyn TagStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            mutations,
            catalog,
            tags,
            clock,
        }
    }

    /// Settles one succeeded copy item and returns its final step.
    ///
    /// Moves are recognized by the item's delete-guarded source adapter;
    /// a failed move verification or source deletion revises the verdict to
    /// a failure, exactly as the run loop's settle hook expects.
    pub(crate) async fn settle(&self, item: &Item, verdict: ItemVerdict, keep_tags: bool) -> Step {
        // The loop settles only succeeded items, so the verdict is a Done.
        let bytes = match verdict.step {
            Step::Done { bytes, .. } => bytes,
            Step::Interrupted => 0,
        };
        let Some(copied) = verdict.metadata.as_ref() else {
            // Settling runs only for succeeded items; those always carry the
            // adapter's metadata.
            return Step::succeeded();
        };
        // The conflict policy may have renamed the target; the adapter's
        // metadata reports the key that actually landed.
        let dst_path = match ObjectPath::new(copied.key.clone()) {
            Ok(path) => path,
            Err(error) => return Step::failed(&error),
        };

        let now = self.clock.now();
        let source = CatalogObjectRef::new(&item.src.bucket, &item.src_path);
        let target = CatalogObjectRef::new(&item.dst.bucket, &dst_path);
        let settlement = self
            .settle_copy(&source, &target, copied, keep_tags, now)
            .await;

        // A move carries its delete-guarded source adapter; presence is the
        // move flag.
        if let Some(delete_storage) = item.src_delete.as_ref() {
            return match self
                .finish_move(
                    &settlement,
                    &source,
                    &target,
                    copied,
                    now,
                    delete_storage.as_ref(),
                    item.dst_storage.as_ref(),
                )
                .await
            {
                MoveFinish::Completed => Step::Done {
                    disposition: crate::batch_outcome::Disposition::Succeeded,
                    detail: None,
                    bytes,
                },
                MoveFinish::Failed(message) => Step::failed_msg(message),
            };
        }

        Step::Done {
            disposition: crate::batch_outcome::Disposition::Succeeded,
            detail: None,
            bytes,
        }
    }

    /// Settles a confirmed remote copy: records the receipt, upserts the
    /// target object, and when tag preservation is requested mirrors the
    /// source's tag associations onto the target. Each failed stage is
    /// repaired independently; earlier successes are never rolled back.
    async fn settle_copy(
        &self,
        source: &CatalogObjectRef<'_>,
        target: &CatalogObjectRef<'_>,
        metadata: &ObjectMetadata,
        preserve_tags: bool,
        at: OffsetDateTime,
    ) -> RemoteSettlement {
        let target_locator = target.locator();
        let settlement = self
            .mutations
            .record_remote_success(
                MutationOperation::Copy,
                &target_locator,
                Some(metadata),
                None,
            )
            .await;

        if let Err(error) = crate::catalog_follow::upsert_confirmed(
            self.catalog.as_ref(),
            target.connection,
            target.bucket,
            metadata,
            at,
        )
        .await
        {
            self.mutations
                .projection_failure(
                    settlement.receipt_id(),
                    MutationOperation::Copy,
                    ProjectionStage::ObjectUpsert,
                    &target_locator,
                    &error,
                )
                .await;
        }

        if !preserve_tags {
            return settlement;
        }

        // Tag mirroring reads through the catalog rather than trusting caller
        // state, so partially projected copies still pick up their tags.
        let source_object = match self
            .catalog
            .find_by_locator(source.connection, source.bucket, source.path)
            .await
        {
            Ok(Some(source_object)) => source_object,
            Ok(None) => return settlement,
            Err(error) => {
                self.mutations
                    .projection_failure(
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
        let target_object = match self
            .catalog
            .find_by_locator(target.connection, target.bucket, target.path)
            .await
        {
            Ok(Some(target_object)) => target_object,
            Ok(None) => return settlement,
            Err(error) => {
                self.mutations
                    .projection_failure(
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
        let associations = match self.tags.associations_for_object(source_object.id).await {
            Ok(associations) => associations,
            Err(error) => {
                self.mutations
                    .projection_failure(
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
            if let Err(error) = self.tags.attach(workspace, target_object.id, tag, at).await {
                self.mutations
                    .projection_failure(
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

    /// Finishes a move whose copy already settled: verifies the target size,
    /// deletes the source, and settles the source delete in the catalog. A
    /// verification or deletion failure keeps the source intact and records a
    /// `MoveFollowup` repair instead of losing the copied data.
    #[allow(clippy::too_many_arguments)]
    async fn finish_move(
        &self,
        copy: &RemoteSettlement,
        source: &CatalogObjectRef<'_>,
        target: &CatalogObjectRef<'_>,
        metadata: &ObjectMetadata,
        at: OffsetDateTime,
        delete_storage: &dyn Storage,
        target_storage: &dyn Storage,
    ) -> MoveFinish {
        // Size check is the integrity gate: only delete the source once the
        // target provably holds the expected bytes.
        match target_storage.stat(&target.bucket_name, target.path).await {
            Ok(verified) if verified.size == metadata.size => {}
            Ok(_) => {
                let message = "target size mismatch after copy; source kept".to_owned();
                self.move_failure(copy, ProjectionStage::TargetVerify, source, &message)
                    .await;
                return MoveFinish::Failed(message);
            }
            Err(error) => {
                let message = format!("target verification failed; source kept: {error}");
                self.move_failure(copy, ProjectionStage::TargetVerify, source, &message)
                    .await;
                return MoveFinish::Failed(message);
            }
        }
        if let Err(error) = delete_storage
            .delete(&source.bucket_name, source.path)
            .await
        {
            let message = format!("copied, but delete failed: {error}");
            self.move_failure(copy, ProjectionStage::SourceDelete, source, &message)
                .await;
            return MoveFinish::Failed(message);
        }
        self.mutations
            .settle_delete(source.connection, source.bucket, source.path, at)
            .await;
        MoveFinish::Completed
    }

    /// Records a move-stage failure as a repair anchored to the copy receipt.
    async fn move_failure(
        &self,
        copy: &RemoteSettlement,
        stage: ProjectionStage,
        source: &CatalogObjectRef<'_>,
        message: &str,
    ) {
        let locator = source.locator();
        self.mutations
            .record_repair(
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
}

/// Locator triple identifying one remote object in the catalog; the bucket
/// name carries so storage-side move steps (verify, delete) can address it.
struct CatalogObjectRef<'a> {
    connection: ConnectionId,
    bucket: BucketId,
    bucket_name: String,
    path: &'a ObjectPath,
}

impl<'a> CatalogObjectRef<'a> {
    fn new(bucket: &Bucket, path: &'a ObjectPath) -> Self {
        Self {
            connection: bucket.connection_id,
            bucket: bucket.id,
            bucket_name: bucket.name.clone(),
            path,
        }
    }

    fn locator(&self) -> RemoteLocator {
        RemoteLocator::new(self.connection, self.bucket, self.path.clone())
    }
}

/// Terminal state of a move's follow-through after its copy settled.
enum MoveFinish {
    Completed,
    Failed(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch_outcome::Disposition;
    use crate::batch_plan::Endpoint;
    use rstore_application_ports::{OutboxStore, RepairStatus};
    use rstore_catalog::InMemoryCatalog;
    use rstore_domain::bucket::{Bucket, BucketConfig};
    use rstore_domain::remote::RemoteObject;
    use rstore_domain::{ConnectionId, SystemClock, TagId, WorkspaceId};
    use rstore_persistence::Database;
    use rstore_test_support::{FakeStorage, InMemoryTagStore, StorageMethod};

    struct Fixture {
        db: Database,
        catalog: Arc<InMemoryCatalog>,
        tags: Arc<InMemoryTagStore>,
        storage: Arc<FakeStorage>,
        connection: ConnectionId,
        bucket: Bucket,
    }

    async fn fixture() -> Fixture {
        let db = Database::open_in_memory().await.unwrap();
        let catalog = Arc::new(InMemoryCatalog::new());
        let tags = Arc::new(InMemoryTagStore::new());
        let storage = Arc::new(FakeStorage::new());
        storage.seed("media", "docs/source.txt", b"source", None);
        storage.seed("media", "docs/target.txt", b"7bytes!", None);
        let connection = ConnectionId::new_random();
        let bucket = Bucket {
            id: rstore_domain::BucketId::new_random(),
            connection_id: connection,
            name: "media".into(),
            config: BucketConfig::default(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        };
        Fixture {
            db,
            catalog,
            tags,
            storage,
            connection,
            bucket,
        }
    }

    impl Fixture {
        fn settlement(&self) -> Settlement {
            let mutations = Arc::new(MutationCoordinator::new(
                Arc::new(Database::clone(&self.db)),
                Arc::new(self.db.outbox()),
                self.catalog.clone(),
                Arc::new(SystemClock),
            ));
            Settlement::new(
                mutations,
                self.catalog.clone(),
                self.tags.clone(),
                Arc::new(SystemClock),
            )
        }

        /// A move item over the fixture's single bucket: both endpoints and
        /// both guard slots ride the same fake adapter.
        fn move_item(&self) -> Item {
            let endpoint = Arc::new(Endpoint {
                bucket: clone_bucket(&self.bucket),
                storage: self.storage.clone(),
                base: String::new(),
            });
            Item::for_test(
                endpoint.clone(),
                ObjectPath::new("docs/source.txt").unwrap(),
                endpoint,
                ObjectPath::new("docs/target.txt").unwrap(),
                self.storage.clone(),
                Some(self.storage.clone()),
            )
        }
    }

    fn clone_bucket(bucket: &Bucket) -> Bucket {
        Bucket {
            id: bucket.id,
            connection_id: bucket.connection_id,
            name: bucket.name.clone(),
            config: bucket.config.clone(),
            created_at: bucket.created_at,
            updated_at: bucket.updated_at,
        }
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

    fn done_verdict(metadata: ObjectMetadata) -> ItemVerdict {
        ItemVerdict {
            step: Step::Done {
                disposition: Disposition::Succeeded,
                detail: None,
                bytes: 7,
            },
            metadata: Some(metadata),
        }
    }

    #[tokio::test]
    async fn copy_settlement_preserves_source_tags() {
        let fixture = fixture().await;
        let settle = fixture.settlement();
        let workspace = WorkspaceId::new_random();
        let source_path = ObjectPath::new("docs/source.txt").unwrap();
        let source_object = RemoteObject {
            id: rstore_domain::RemoteObjectId::new_random(),
            connection_id: fixture.connection,
            bucket_id: fixture.bucket.id,
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
        fixture.catalog.upsert(&source_object).await.unwrap();
        let tag = TagId::new_random();
        fixture
            .tags
            .attach(workspace, source_object.id, tag, OffsetDateTime::UNIX_EPOCH)
            .await
            .unwrap();

        let mut item = fixture.move_item();
        item.src_delete = None; // a plain copy, not a move
        let step = settle
            .settle(&item, done_verdict(metadata("docs/target.txt")), true)
            .await;

        assert!(matches!(step, Step::Done { bytes: 7, .. }));
        let target = fixture
            .catalog
            .find_by_locator(
                fixture.connection,
                fixture.bucket.id,
                &ObjectPath::new("docs/target.txt").unwrap(),
            )
            .await
            .unwrap()
            .expect("copy was projected");
        assert!(
            fixture
                .tags
                .associations()
                .contains(&(workspace, target.id, tag)),
            "the copied object keeps the source association"
        );
    }

    #[tokio::test]
    async fn move_finishes_verify_delete_and_settle() {
        let fixture = fixture().await;
        let settle = fixture.settlement();

        let step = settle
            .settle(
                &fixture.move_item(),
                done_verdict(metadata("docs/target.txt")),
                false,
            )
            .await;

        assert!(matches!(step, Step::Done { bytes: 7, .. }));
        assert!(fixture
            .storage
            .stat("media", &ObjectPath::new("docs/source.txt").unwrap())
            .await
            .is_err());
        assert!(fixture.db.outbox().repairs(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn move_delete_failure_is_queryable_repair() {
        let fixture = fixture().await;
        fixture.storage.fail_next(
            StorageMethod::Delete,
            rstore_domain::Error::database("injected delete failure"),
        );
        let settle = fixture.settlement();

        let step = settle
            .settle(
                &fixture.move_item(),
                done_verdict(metadata("docs/target.txt")),
                false,
            )
            .await;

        assert!(
            matches!(
                step,
                Step::Done {
                    disposition: Disposition::Failed(_),
                    ..
                }
            ),
            "a failed source delete revises the verdict"
        );
        assert!(fixture
            .storage
            .stat("media", &ObjectPath::new("docs/source.txt").unwrap())
            .await
            .is_ok());
        let repairs = fixture.db.outbox().repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].kind, RepairKind::MoveFollowup);
        assert_eq!(repairs[0].status, RepairStatus::Required);
    }

    #[tokio::test]
    async fn move_verify_mismatch_keeps_source_and_repairs() {
        let fixture = fixture().await;
        // The target holds fewer bytes than the copy reported (seed
        // overwrites in place).
        fixture
            .storage
            .seed("media", "docs/target.txt", b"wrong", None);
        let settle = fixture.settlement();

        let step = settle
            .settle(
                &fixture.move_item(),
                done_verdict(metadata("docs/target.txt")),
                false,
            )
            .await;

        assert!(
            matches!(
                step,
                Step::Done {
                    disposition: Disposition::Failed(_),
                    ..
                }
            ),
            "a size mismatch revises the verdict"
        );
        assert!(fixture
            .storage
            .stat("media", &ObjectPath::new("docs/source.txt").unwrap())
            .await
            .is_ok());
        let repairs = fixture.db.outbox().repairs(10).await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].kind, RepairKind::MoveFollowup);
        assert!(repairs[0]
            .last_error
            .as_deref()
            .unwrap()
            .contains("target size mismatch"));
    }
}
