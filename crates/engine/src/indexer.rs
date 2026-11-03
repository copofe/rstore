//! Indexing service: scan remote buckets into the catalog with persisted,
//! resumable runs, plus catalog GC, stats, and per-bucket status.
//!
//! Indexing is opt-in per bucket: it is what makes a bucket searchable by
//! name and attributes. Naming a bucket in a run switches its index on; a run
//! over a workspace or everything covers the buckets already indexed.

use std::sync::Arc;

use crate::transfer::CancellationToken;
use async_trait::async_trait;
use rstore_domain::bucket::Bucket;
use rstore_domain::id::{BucketId, IndexRunId, WorkspaceId};
use rstore_domain::workspace::{IgnoreRule, IndexRun, IndexRunState};
use rstore_domain::{Clock, Error, Result};
use rstore_storage::model::ListPage;
use rstore_storage::Storage;

use crate::batch_outcome::{BatchOutcome, ItemOutcome};
use crate::bucket::{BucketRef, BucketService};
use crate::expand::{scan_pages, PageSink};
use crate::index_run::{IndexRunLifecycle, ScanCompletion};

use crate::gateway::RemoteGateway;
pub use crate::index_run::IndexMode;

/// Outcome of one index run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexReport {
    pub run_id: IndexRunId,
    pub bucket: BucketId,
    pub state: IndexRunState,
    pub objects_seen: u64,
    pub objects_upserted: u64,
    pub objects_pruned: u64,
    pub objects_stale: u64,
}

/// Result of a catalog garbage-collection pass.
#[derive(Debug, Clone, serde::Serialize)]
pub struct GcReport {
    pub dry_run: bool,

    /// Untagged stale rows removed (or that would be removed, under
    /// `dry_run`). Tagged stale rows always stay.
    pub removed: u64,
}

/// Catalog storage statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexStats {
    pub object_rows: u64,
    pub stale_rows: u64,
    pub db_bytes: u64,
}

/// The buckets one index run covers.
#[derive(Debug, Clone)]
pub enum IndexScope {
    /// Exactly these buckets; naming one switches its index on.
    Buckets(Vec<BucketRef>),
    /// The index-enabled members of a workspace.
    Workspace(WorkspaceId),
    /// Every index-enabled registered bucket.
    All,
}

/// Per-bucket snapshot of index state for status output.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BucketIndexStatus {
    pub bucket: BucketId,
    pub index_enabled: bool,
    pub object_count: u64,
    pub stale_count: u64,
    pub last_run: Option<IndexRun>,
    pub ignore_rules: u64,
}

/// Catalog indexing operations: runs over a bucket scope, checkpoint
/// cancellation, reporting, and GC.
pub struct IndexService {
    gateway: RemoteGateway,
    buckets: Arc<BucketService>,
    workspaces: Arc<crate::db::WorkspaceRepository>,
    catalog: Arc<crate::db::CatalogRepository>,
    ignores: Arc<crate::db::IgnoreRuleRepository>,
    runs: Arc<crate::db::IndexRunRepository>,
    stats: Arc<crate::db::Database>,
    clock: Arc<dyn Clock>,
    bus: crate::events::EventBus,
}

impl IndexService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        gateway: RemoteGateway,
        buckets: Arc<BucketService>,
        workspaces: Arc<crate::db::WorkspaceRepository>,
        catalog: Arc<crate::db::CatalogRepository>,
        ignores: Arc<crate::db::IgnoreRuleRepository>,
        runs: Arc<crate::db::IndexRunRepository>,
        stats: Arc<crate::db::Database>,
        clock: Arc<dyn Clock>,
        bus: crate::events::EventBus,
    ) -> Self {
        Self {
            gateway,
            buckets,
            workspaces,
            catalog,
            ignores,
            runs,
            stats,
            clock,
            bus,
        }
    }

    /// The buckets `scope` covers, in registry order.
    async fn scope_buckets(&self, scope: &IndexScope) -> Result<Vec<Bucket>> {
        match scope {
            IndexScope::Buckets(references) => {
                let mut buckets = Vec::with_capacity(references.len());
                for reference in references {
                    let mut bucket = self.buckets.resolve(reference).await?;
                    if !bucket.config.index_enabled {
                        bucket = self
                            .buckets
                            .update(
                                bucket.id,
                                crate::bucket::UpdateBucketInput {
                                    index_enabled: Some(true),
                                    ..Default::default()
                                },
                            )
                            .await?;
                    }
                    buckets.push(bucket);
                }
                Ok(buckets)
            }
            IndexScope::Workspace(workspace) => {
                let members = self.workspaces.buckets(*workspace).await?;
                Ok(self
                    .buckets
                    .list()
                    .await?
                    .into_iter()
                    .filter(|b| b.config.index_enabled && members.contains(&b.id))
                    .collect())
            }
            IndexScope::All => Ok(self
                .buckets
                .list()
                .await?
                .into_iter()
                .filter(|b| b.config.index_enabled)
                .collect()),
        }
    }

    /// Move the bucket's run in one of `active_states` to `target`,
    /// persisting the transition and optionally stamping `finished_at`
    /// and clearing the resume checkpoint.
    ///
    /// Errors with `none_message` when no matching run exists, or when the
    /// state machine rejects the transition.
    #[allow(clippy::too_many_arguments)]
    async fn transition_active(
        &self,
        bucket_id: BucketId,
        active_states: &[IndexRunState],
        target: IndexRunState,
        finish: bool,
        clear_checkpoint: bool,
        none_message: &str,
    ) -> Result<IndexRun> {
        let active = self
            .runs
            .latest_per_bucket()
            .await?
            .into_iter()
            .find(|r| r.bucket_id == bucket_id && active_states.contains(&r.state));
        let Some(run) = active else {
            return Err(Error::conflict(none_message));
        };
        let mut next = run;
        next.state = next.state.transition(target)?;
        let finished_at = finish.then_some(self.clock.now());
        if finish {
            next.finished_at = finished_at;
        }
        self.runs
            .update(
                next.id,
                Some(next.state),
                finished_at,
                next.objects_seen,
                next.error_count,
            )
            .await?;
        if clear_checkpoint {
            self.runs.clear_checkpoint(bucket_id).await?;
        }
        Ok(next)
    }

    /// Drive the paginated scan for one run, feeding pages through
    /// [`ScanSink`] until complete, cancelled (checkpoint saved, paused),
    /// or exhausted.
    async fn scan(
        &self,
        bucket: &Bucket,
        storage: Arc<dyn Storage>,
        token: Option<String>,
        cancel: &CancellationToken,
        lifecycle: &mut IndexRunLifecycle,
    ) -> Result<ScanCompletion> {
        let rules = self.ignores.list(bucket.id).await?;

        if cancel.check().is_err() {
            lifecycle.checkpoint_on_cancel(token.as_deref()).await?;
            return Ok(ScanCompletion::Paused);
        }
        let mut sink = ScanSink {
            bucket,
            rules,
            cancel,
            token,
            completion: ScanCompletion::Complete,
            lifecycle,
        };
        let start = sink.token.clone();
        scan_pages(storage.as_ref(), &bucket.name, None, start, 1000, &mut sink).await?;
        Ok(sink.completion)
    }

    /// Execute one run end-to-end in `mode`; on any failure the run is
    /// persisted as failed before the error propagates (a persistence
    /// failure while doing so is logged, never masks the original error).
    async fn run_one(
        &self,
        bucket: &BucketRef,
        cancel: &CancellationToken,
        mode: IndexMode,
    ) -> Result<IndexReport> {
        let (bucket, storage, mut lifecycle) = self.begin(bucket, true).await?;
        let result = async {
            let token = lifecycle.prepare(mode, self.catalog.as_ref()).await?;
            let completion = self
                .scan(&bucket, storage, token, cancel, &mut lifecycle)
                .await?;
            lifecycle
                .reconcile(mode, completion, self.catalog.as_ref())
                .await?;
            Ok(completion)
        }
        .await;

        match result {
            Ok(completion) => lifecycle.finish(completion).await,
            Err(error) => {
                if let Err(persist_error) = lifecycle.fail().await {
                    tracing::warn!("run lifecycle could not persist failed index run");
                    tracing::debug!(
                        %persist_error,
                        "failed index run persistence error"
                    );
                }
                Err(error)
            }
        }
    }

    /// Resolve the bucket, enforce indexing being enabled when required,
    /// connect to it, and open the run's persisted lifecycle.
    async fn begin(
        &self,
        bucket: &BucketRef,
        require_enabled: bool,
    ) -> Result<(Bucket, Arc<dyn Storage>, IndexRunLifecycle)> {
        let b = self.buckets.resolve(bucket).await?;
        if require_enabled && !b.config.index_enabled {
            return Err(Error::invalid_input(format!(
                "bucket `{}` is not indexed; name it in `index run` to index it",
                b.name
            )));
        }
        let storage = self.gateway.bucket_storage(&b).await?;
        let lifecycle =
            IndexRunLifecycle::begin(b.id, self.runs.clone(), self.clock.clone()).await?;
        Ok((b, storage, lifecycle))
    }
}

/// Page consumer for one scan: filters entries through the bucket's ignore
/// rules, publishes each page durably, and pauses at cancellation.
struct ScanSink<'a> {
    bucket: &'a Bucket,
    rules: Vec<IgnoreRule>,
    cancel: &'a CancellationToken,
    token: Option<String>,
    completion: ScanCompletion,
    lifecycle: &'a mut IndexRunLifecycle,
}

#[async_trait]
impl PageSink for ScanSink<'_> {
    async fn page(&mut self, page: &ListPage) -> Result<bool> {
        if self.cancel.check().is_err() {
            self.lifecycle
                .checkpoint_on_cancel(self.token.as_deref())
                .await?;
            self.completion = ScanCompletion::Paused;
            return Ok(false);
        }
        let confirmed_at = self.lifecycle.now();
        self.lifecycle.observe_page(page.objects.len() as u64);

        let kept = page
            .objects
            .iter()
            .filter(|entry| {
                !self
                    .rules
                    .iter()
                    .any(|r| r.enabled && r.matches(&entry.metadata.key))
            })
            .collect::<Vec<_>>();
        let objects = kept
            .iter()
            .map(|entry| {
                crate::write_through::confirmed_object(self.bucket, &entry.metadata, confirmed_at)
            })
            .collect::<Result<Vec<_>>>()?;
        let next_token = page.continuation_token.clone();
        self.lifecycle
            .publish_page(&objects, next_token.as_deref())
            .await?;
        self.token = next_token;
        if self.token.is_none() {
            self.completion = ScanCompletion::Complete;
            return Ok(false);
        }
        Ok(true)
    }
}

impl IndexService {
    /// Remove untagged stale rows — objects the last complete refresh no
    /// longer saw; with `dry_run`, only counts them.
    pub async fn gc(&self, dry_run: bool) -> Result<GcReport> {
        let removed = if dry_run {
            self.catalog.count_purgeable_stale().await?
        } else {
            self.catalog.purge_stale().await?
        };
        Ok(GcReport { dry_run, removed })
    }

    /// Aggregate catalog statistics.
    pub async fn stats(&self) -> Result<IndexStats> {
        Ok(IndexStats {
            object_rows: self.stats.count_remote_objects().await?,
            stale_rows: self.stats.count_stale_objects().await?,
            db_bytes: self.stats.size_bytes().await?,
        })
    }

    /// Scan every bucket in `scope` into the catalog, one after another, in
    /// `mode`. Failures are per bucket; explicit bucket references resolve
    /// before any scan starts, so a typo fails the whole call. When `cancel`
    /// fires, the running scan checkpoints and reports `paused`, and the
    /// remaining buckets are not started.
    pub async fn run(
        &self,
        scope: &IndexScope,
        mode: IndexMode,
        cancel: &CancellationToken,
    ) -> Result<BatchOutcome<IndexReport>> {
        let buckets = self.scope_buckets(scope).await?;
        let mut items = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            if cancel.check().is_err() {
                break;
            }
            let label = self.gateway.bucket_label(&bucket).await;
            let outcome = self.run_one(&BucketRef::Id(bucket.id), cancel, mode).await;
            self.bus
                .publish(crate::events::EngineEvent::CatalogChanged { bucket: bucket.id });
            items.push(match outcome {
                Ok(report) => ItemOutcome::succeeded(label, report),
                Err(error) => ItemOutcome::failed(label, &error),
            });
        }
        Ok(BatchOutcome::new(items))
    }

    /// Status of every registered bucket.
    pub async fn status(&self) -> Result<Vec<BucketIndexStatus>> {
        let buckets = self.buckets.list().await?;
        let runs = self.runs.latest_per_bucket().await?;
        let mut out = Vec::with_capacity(buckets.len());
        for b in buckets {
            let last_run = runs.iter().find(|r| r.bucket_id == b.id).cloned();
            let object_count = self.catalog.count_in_bucket(b.id).await?;
            let stale_count = self.catalog.count_stale(b.id).await?;
            let ignore_rules = self.ignores.list(b.id).await?.len() as u64;
            out.push(BucketIndexStatus {
                bucket: b.id,
                index_enabled: b.config.index_enabled,
                object_count,
                stale_count,
                last_run,
                ignore_rules,
            });
        }
        Ok(out)
    }

    /// Discard the bucket's active (running or paused) index and clear its
    /// checkpoint; rows already published stay. Errors when no run is
    /// active.
    pub async fn cancel(&self, bucket: &BucketRef) -> Result<IndexRun> {
        let b = self.buckets.resolve(bucket).await?;
        self.transition_active(
            b.id,
            &[IndexRunState::Running, IndexRunState::Paused],
            IndexRunState::Cancelled,
            true,
            true,
            "no active index for this bucket",
        )
        .await
    }
}
