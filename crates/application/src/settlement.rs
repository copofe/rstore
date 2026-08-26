//! Copy/move settlement — everything between a succeeded copy item and its
//! final verdict step.
//!
//! The run loop hands a succeeded item's verdict here; this module owns the
//! whole chain: rederive the landed target key from the adapter's metadata,
//! settle through the mutation coordinator's shared primitive (receipt/outbox
//! unit plus catalog projection), then mirror tags — and for moves, marked
//! by the item's `ItemKind`, verify the target and delete the source,
//! revising the verdict on failure. Failures map to repair rows, never to
//! lost follow-ups; [`Step`](crate::job_run::Step) is the only thing that
//! comes back.

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

use crate::copy::{Item, ItemKind};
use crate::job_run::{ItemVerdict, Step};
use crate::mutation::{MutationCoordinator, MutationOperation, ProjectionStage, RemoteSettlement};

/// Settles confirmed copies (and their move follow-through).
#[derive(Clone)]
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
    /// Moves are recognized by the item's `ItemKind`; a failed move
    /// verification or source deletion revises the verdict to a failure,
    /// exactly as the run loop's settle hook expects.
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

        match item.kind {
            ItemKind::Copy => Step::Done {
                disposition: crate::batch_outcome::Disposition::Succeeded,
                detail: None,
                bytes,
            },
            ItemKind::Move => {
                // The kind and the delete adapter are issued together by
                // expansion; absence is internal corruption, surfaced
                // explicitly rather than silently ignored.
                let Some(delete_storage) = item.src_delete.as_ref() else {
                    return Step::failed_msg("move item carries no delete-guarded adapter");
                };
                match self
                    .finish_move(MoveFinishContext {
                        copy: &settlement,
                        source: &source,
                        target: &target,
                        metadata: copied,
                        at: now,
                        delete_storage: delete_storage.as_ref(),
                        target_storage: item.dst_storage.as_ref(),
                    })
                    .await
                {
                    MoveFinish::Completed => Step::Done {
                        disposition: crate::batch_outcome::Disposition::Succeeded,
                        detail: None,
                        bytes,
                    },
                    MoveFinish::Failed(message) => Step::failed_msg(message),
                }
            }
        }
    }

    /// Settles a confirmed remote copy through the shared primitive (receipt
    /// unit plus object projection), then — when tag preservation is
    /// requested — mirrors the source's tag associations onto the target.
    /// Each failed stage is repaired independently; earlier successes are
    /// never rolled back.
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
            .settle_confirmed(MutationOperation::Copy, &target_locator, Some(metadata), at)
            .await;

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
                self.fail_stage(
                    &settlement,
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
                self.fail_stage(
                    &settlement,
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
                self.fail_stage(
                    &settlement,
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
                self.fail_stage(
                    &settlement,
                    ProjectionStage::TagWrite,
                    &target_locator,
                    &error,
                )
                .await;
            }
        }
        settlement
    }

    /// Records one failed tag-mirroring stage through the coordinator's
    /// single failure entry, keyed to the copy receipt.
    async fn fail_stage(
        &self,
        settlement: &RemoteSettlement,
        stage: ProjectionStage,
        target_locator: &RemoteLocator,
        error: &rstore_domain::Error,
    ) {
        self.mutations
            .note_failure(
                settlement,
                MutationOperation::Copy,
                stage,
                target_locator,
                error,
            )
            .await;
    }

    /// Finishes a move whose copy already settled: verifies the target size,
    /// deletes the source, and settles the source delete in the catalog. A
    /// verification or deletion failure keeps the source intact and records a
    /// `MoveFollowup` repair instead of losing the copied data.
    async fn finish_move(&self, ctx: MoveFinishContext<'_>) -> MoveFinish {
        // Size check is the integrity gate: only delete the source once the
        // target provably holds the expected bytes.
        match ctx
            .target_storage
            .stat(&ctx.target.bucket_name, ctx.target.path)
            .await
        {
            Ok(verified) if verified.size == ctx.metadata.size => {}
            Ok(_) => {
                let message = "target size mismatch after copy; source kept".to_owned();
                self.move_failure(
                    ctx.copy,
                    ProjectionStage::TargetVerify,
                    ctx.source,
                    &message,
                )
                .await;
                return MoveFinish::Failed(message);
            }
            Err(error) => {
                let message = format!("target verification failed; source kept: {error}");
                self.move_failure(
                    ctx.copy,
                    ProjectionStage::TargetVerify,
                    ctx.source,
                    &message,
                )
                .await;
                return MoveFinish::Failed(message);
            }
        }
        if let Err(error) = ctx
            .delete_storage
            .delete(&ctx.source.bucket_name, ctx.source.path)
            .await
        {
            let message = format!("copied, but delete failed: {error}");
            self.move_failure(
                ctx.copy,
                ProjectionStage::SourceDelete,
                ctx.source,
                &message,
            )
            .await;
            return MoveFinish::Failed(message);
        }
        self.mutations
            .settle_delete(
                ctx.source.connection,
                ctx.source.bucket,
                ctx.source.path,
                ctx.at,
            )
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

/// Everything a move's follow-through needs once its copy settled.
struct MoveFinishContext<'a> {
    copy: &'a RemoteSettlement,
    source: &'a CatalogObjectRef<'a>,
    target: &'a CatalogObjectRef<'a>,
    metadata: &'a ObjectMetadata,
    at: OffsetDateTime,
    delete_storage: &'a dyn Storage,
    target_storage: &'a dyn Storage,
}

/// Terminal state of a move's follow-through after its copy settled.
enum MoveFinish {
    Completed,
    Failed(String),
}
