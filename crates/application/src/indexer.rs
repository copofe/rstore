//! Indexing service: scan remote buckets into the catalog with persisted,
//! resumable runs, plus catalog GC, stats, and per-bucket status.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{
    IgnoreRuleStore, IndexRunStore, RuntimeStats, SettingsStore, WorkspaceStore,
};
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::bucket::Bucket;
use rstore_domain::id::{BucketId, IndexRunId, WorkspaceId};
use rstore_domain::workspace::{IgnoreRule, IndexRun, IndexRunState};
use rstore_domain::{Clock, Error, Result};
use rstore_storage_api::model::ListPage;
use rstore_storage_api::Storage;
use rstore_transfer::CancellationToken;

use crate::batch_outcome::{BatchOutcome, ItemOutcome};
use crate::bucket::{BucketRef, BucketService};
use crate::expand::{scan_pages, PageSink};
use crate::index_run::{IndexRunLifecycle, ScanCompletion};

pub use crate::index_run::IndexMode;
use crate::locator::CatalogLocator;

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
    pub catalog_revision: i64,
}

/// Result of a tombstone garbage-collection pass.
#[derive(Debug, Clone, serde::Serialize)]
pub struct GcReport {
    pub dry_run: bool,

    /// Retention window applied, in seconds (override or setting).
    pub retention_seconds: u64,

    /// Tombstones purged (or that would be purged, under `dry_run`).
    pub expired: u64,

    /// Tombstones still within retention after the pass.
    pub retained: u64,
}

/// Catalog storage statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexStats {
    pub object_rows: u64,
    pub stale_rows: u64,
    pub tombstone_rows: u64,
    pub db_bytes: u64,
}

/// The buckets one index run covers.
#[derive(Debug, Clone)]
pub enum IndexScope {
    /// Exactly these buckets; one with indexing disabled fails as its own item.
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
    pub catalog_revision: Option<i64>,
    pub ignore_rules: u64,
}

/// Catalog indexing operations: runs over a bucket scope, checkpoint
/// cancellation, reporting, and GC.
#[async_trait]
pub trait IndexService: Send + Sync {
    /// Scan every bucket in `scope` into the catalog, one after another, in
    /// `mode`. Failures are per bucket; explicit bucket references resolve
    /// before any scan starts, so a typo fails the whole call. When `cancel`
    /// fires, the running scan checkpoints and reports `paused`, and the
    /// remaining buckets are not started.
    async fn run(
        &self,
        scope: &IndexScope,
        mode: IndexMode,
        cancel: &CancellationToken,
    ) -> Result<BatchOutcome<IndexReport>>;

    /// Status of every registered bucket.
    async fn status(&self) -> Result<Vec<BucketIndexStatus>>;

    /// Discard the bucket's active (running or paused) index and clear its
    /// checkpoint; rows already published stay. Errors when no run is
    /// active.
    async fn cancel(&self, bucket: &BucketRef) -> Result<IndexRun>;

    /// Purge expired tombstones older than the retention window (setting
    /// `catalog.tombstone_retention_secs`, overridable here); with
    /// `dry_run`, only counts them.
    async fn gc(&self, retention_override: Option<u64>, dry_run: bool) -> Result<GcReport>;

    /// Aggregate catalog statistics.
    async fn stats(&self) -> Result<IndexStats>;
}

/// Production [`IndexService`] over the locator, catalog repository,
/// ignore rules, run store, settings, and runtime stats.
pub struct DefaultIndexService {
    locator: CatalogLocator,
    buckets: Arc<dyn BucketService>,
    workspaces: Arc<dyn WorkspaceStore>,
    catalog: Arc<dyn RemoteObjectRepository>,
    ignores: Arc<dyn IgnoreRuleStore>,
    runs: Arc<dyn IndexRunStore>,
    settings: Arc<dyn SettingsStore>,

    stats: Arc<dyn RuntimeStats>,
    clock: Arc<dyn Clock>,
}

impl DefaultIndexService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        locator: CatalogLocator,
        buckets: Arc<dyn BucketService>,
        workspaces: Arc<dyn WorkspaceStore>,
        catalog: Arc<dyn RemoteObjectRepository>,
        ignores: Arc<dyn IgnoreRuleStore>,
        runs: Arc<dyn IndexRunStore>,
        settings: Arc<dyn SettingsStore>,
        stats: Arc<dyn RuntimeStats>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            locator,
            buckets,
            workspaces,
            catalog,
            ignores,
            runs,
            settings,
            stats,
            clock,
        }
    }

    /// Effective tombstone retention in seconds: the override when given,
    /// else the setting, else 30 days.
    async fn retention_secs(&self, override_secs: Option<u64>) -> Result<u64> {
        Ok(match override_secs {
            Some(secs) => secs,
            None => self
                .settings
                .get("catalog.tombstone_retention_secs")
                .await?
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(30 * 86_400),
        })
    }

    async fn resolve_bucket(&self, bucket: &BucketRef) -> Result<Bucket> {
        self.buckets.resolve(bucket).await
    }

    /// The buckets `scope` covers, in registry order.
    async fn scope_buckets(&self, scope: &IndexScope) -> Result<Vec<Bucket>> {
        match scope {
            IndexScope::Buckets(references) => {
                let mut buckets = Vec::with_capacity(references.len());
                for reference in references {
                    buckets.push(self.resolve_bucket(reference).await?);
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
        let b = self.resolve_bucket(bucket).await?;
        if require_enabled && !b.config.index_enabled {
            return Err(Error::invalid_input(format!(
                "bucket `{}` has indexing disabled; enable it with `bucket edit {} --index`",
                b.name, b.name
            )));
        }
        let (_, storage) = self.locator.resolve_bucket_target(&b).await?;
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
        let objects = crate::catalog_follow::confirmed_objects(
            self.bucket.connection_id,
            self.bucket.id,
            kept.iter().copied(),
            confirmed_at,
        )?;
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

#[async_trait]
impl IndexService for DefaultIndexService {
    async fn gc(&self, retention_override: Option<u64>, dry_run: bool) -> Result<GcReport> {
        let retention = self.retention_secs(retention_override).await?;
        let now = self.clock.now();
        let expired = if dry_run {
            self.catalog
                .count_expired_tombstones(now, retention)
                .await?
        } else {
            self.catalog
                .purge_expired_tombstones(now, retention)
                .await?
        };
        let total = self.stats.count_tombstones().await?;
        Ok(GcReport {
            dry_run,
            retention_seconds: retention,
            expired,
            retained: total.saturating_sub(expired),
        })
    }

    async fn stats(&self) -> Result<IndexStats> {
        Ok(IndexStats {
            object_rows: self.stats.count_remote_objects().await?,
            stale_rows: self.stats.count_stale_objects().await?,
            tombstone_rows: self.stats.count_tombstones().await?,
            db_bytes: self.stats.size_bytes().await?,
        })
    }

    async fn run(
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
            let label = self.locator.bucket_label(&bucket).await;
            items.push(
                match self.run_one(&BucketRef::Id(bucket.id), cancel, mode).await {
                    Ok(report) => ItemOutcome::succeeded(label, report),
                    Err(error) => ItemOutcome::failed(label, &error),
                },
            );
        }
        Ok(BatchOutcome::new(items))
    }

    async fn status(&self) -> Result<Vec<BucketIndexStatus>> {
        let buckets = self.buckets.list().await?;
        let runs = self.runs.latest_per_bucket().await?;
        let mut out = Vec::with_capacity(buckets.len());
        for b in buckets {
            let last_run = runs.iter().find(|r| r.bucket_id == b.id).cloned();
            let catalog_revision = self.runs.revision(b.id).await?.map(|(r, _)| r);
            let object_count = self.catalog.count_in_bucket(b.id).await?;
            let stale_count = self.catalog.count_stale(b.id).await?;
            let ignore_rules = self.ignores.list(b.id).await?.len() as u64;
            out.push(BucketIndexStatus {
                bucket: b.id,
                index_enabled: b.config.index_enabled,
                object_count,
                stale_count,
                last_run,
                catalog_revision,
                ignore_rules,
            });
        }
        Ok(out)
    }

    async fn cancel(&self, bucket: &BucketRef) -> Result<IndexRun> {
        let b = self.resolve_bucket(bucket).await?;
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
