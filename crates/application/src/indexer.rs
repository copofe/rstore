//! Index service (IDX-001..017, 022/025).
//!
//! `start` scans a bucket from its root with paged listings, upserting each
//! object into the catalog immediately (IDX-003: queryable before the scan
//! ends). Ignore rules prune whole subtrees (IDX-004/IGN-005). The
//! continuation token persists after every page (IDX-002), so a paused run
//! resumes exactly where it stopped (IDX-006/007). `refresh` reconciles the
//! full listing against the catalog and marks missing objects stale
//! (IDX-012); `rebuild` marks the current rows stale, rescans, and only prunes
//! rows left unseen after a complete scan while keeping tag definitions
//! (IDX-013 / DATA-018).
//!
//! Pause and cancel act on persisted run state; an in-flight scan stops at the
//! next page boundary via the cancellation token. Resume re-runs the scan from
//! the persisted checkpoint.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{IgnoreRuleStore, IndexRunStore, RuntimeStats, SettingsStore};
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::bucket::Bucket;
use rstore_domain::id::{BucketId, IndexRunId};
use rstore_domain::workspace::{IgnoreRule, IndexRun, IndexRunState};
use rstore_domain::{Clock, Error, Result};
use rstore_storage_api::model::ListPage;
use rstore_storage_api::Storage;
use rstore_transfer::CancellationToken;

use crate::bucket::{BucketRef, BucketService};
use crate::expand::{scan_pages, PageSink};
use crate::index_run::{IndexRunLifecycle, RunMode, ScanCompletion};
use crate::locator::CatalogLocator;

/// Progress and outcome of a scan run (IDX-005).
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

/// Outcome of `catalog gc` (IDX-020).
#[derive(Debug, Clone, serde::Serialize)]
pub struct GcReport {
    pub dry_run: bool,
    /// Retention actually applied, in seconds.
    pub retention_seconds: u64,
    /// Tombstones past retention (deleted, or counted when `dry_run`).
    pub expired: u64,
    /// Tombstones still within retention.
    pub retained: u64,
}

/// Catalog size overview for `index stats` (IDX-021).
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexStats {
    pub object_rows: u64,
    pub stale_rows: u64,
    pub tombstone_rows: u64,
    pub db_bytes: u64,
}

/// Per-bucket status for `index status` (IDX-014).
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

/// Scan/reconcile orchestration (IDX-).
#[async_trait]
pub trait IndexService: Send + Sync {
    /// Full scan from the bucket root (IDX-001); resumes from the persisted
    /// checkpoint when one exists (IDX-007).
    async fn start(&self, bucket: &BucketRef, cancel: &CancellationToken) -> Result<IndexReport>;
    /// Per-bucket status overview (IDX-014).
    async fn status(&self) -> Result<Vec<BucketIndexStatus>>;
    /// Mark a run paused (IDX-006); an in-flight scan stops at the next page.
    /// Returns the persisted run — callers echo observed state, never
    /// fabricated state.
    async fn pause(&self, bucket: &BucketRef) -> Result<IndexRun>;
    /// Mark a run cancelled; completed data is kept (IDX-008). Returns the
    /// persisted run.
    async fn cancel(&self, bucket: &BucketRef) -> Result<IndexRun>;
    /// Purge tombstones past retention (IDX-020). Retention comes from the
    /// `catalog.tombstone_retention_secs` setting unless `retention_override`
    /// is given; `dry_run` counts without deleting.
    async fn gc(&self, retention_override: Option<u64>, dry_run: bool) -> Result<GcReport>;

    /// Catalog size overview (IDX-021).
    async fn stats(&self) -> Result<IndexStats>;

    /// Full reconciliation: catalog rows missing remotely become stale
    /// (IDX-012).
    async fn refresh(&self, bucket: &BucketRef, cancel: &CancellationToken) -> Result<IndexReport>;
    /// Rescan the bucket and prune rows left unseen only after a complete scan;
    /// tag definitions survive (IDX-013 / DATA-018).
    async fn rebuild(&self, bucket: &BucketRef, cancel: &CancellationToken) -> Result<IndexReport>;
}

pub struct DefaultIndexService {
    /// Tier-1 resolution: a registered bucket row → its adapter (the one
    /// path every live remote operation resolves through).
    locator: CatalogLocator,
    buckets: Arc<dyn BucketService>,
    catalog: Arc<dyn RemoteObjectRepository>,
    ignores: Arc<dyn IgnoreRuleStore>,
    runs: Arc<dyn IndexRunStore>,
    settings: Arc<dyn SettingsStore>,
    /// Row/tombstone counts and the on-disk size come from the database the
    /// composition root already opened (IDX-020/021).
    stats: Arc<dyn RuntimeStats>,
    clock: Arc<dyn Clock>,
}

impl DefaultIndexService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        locator: CatalogLocator,
        buckets: Arc<dyn BucketService>,
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
            catalog,
            ignores,
            runs,
            settings,
            stats,
            clock,
        }
    }

    /// Retention in seconds: the `catalog.tombstone_retention_secs` setting
    /// (TAG-026) with a 30-day default, or an explicit override (IDX-020).
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

    /// The one persisted-state transition behind `pause` and `cancel`: find
    /// the bucket's active run among `active_states`, move it to `target`
    /// through the domain transition table, finish it when asked, and
    /// optionally drop the scan checkpoint.
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
            return Err(Error::invalid_input(none_message));
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

    /// One full pass of paged listing with ignore pruning + upserts
    /// (IDX-002..004). Stops at a page boundary when cancelled, persisting
    /// the cursor for `resume`. The walk itself is [`scan_pages`]; the
    /// per-page policy lives in [`ScanSink`].
    async fn scan(
        &self,
        bucket: &Bucket,
        storage: Arc<dyn Storage>,
        token: Option<String>,
        cancel: &CancellationToken,
        lifecycle: &mut IndexRunLifecycle,
    ) -> Result<ScanCompletion> {
        let rules = self.ignores.list(bucket.id).await?;
        // A run cancelled before it lists anything still persists its pause
        // (the original loop checked ahead of every page, the first included).
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

    /// Own one complete Index run lifecycle. Callers choose the mode; this
    /// module alone decides when reconciliation and revision changes are
    /// valid, and every operational error attempts to persist `Failed`.
    async fn run(
        &self,
        bucket: &BucketRef,
        cancel: &CancellationToken,
        mode: RunMode,
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

    /// Shared prelude: resolve bucket + adapter, create the run row.
    async fn begin(
        &self,
        bucket: &BucketRef,
        require_enabled: bool,
    ) -> Result<(Bucket, Arc<dyn Storage>, IndexRunLifecycle)> {
        let b = self.resolve_bucket(bucket).await?;
        if require_enabled && !b.config.index_enabled {
            return Err(Error::invalid_input(format!(
                "bucket `{}` has indexing disabled (BKT-009)",
                b.name
            )));
        }
        let (_, storage) = self.locator.resolve_bucket_target(&b).await?;
        let lifecycle =
            IndexRunLifecycle::begin(b.id, self.runs.clone(), self.clock.clone()).await?;
        Ok((b, storage, lifecycle))
    }
}

/// The indexer's per-page policy inside one [`scan_pages`] walk: ignore
/// pruning, confirmed upserts, atomic page publication (IDX-002), run
/// bookkeeping, and the pause/complete
/// decision at page boundaries (IDX-006/007).
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
        // IDX-004/IGN-005: skip ignored subtrees entirely.
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

    async fn start(&self, bucket: &BucketRef, cancel: &CancellationToken) -> Result<IndexReport> {
        self.run(bucket, cancel, RunMode::Start).await
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

    async fn pause(&self, bucket: &BucketRef) -> Result<IndexRun> {
        let b = self.resolve_bucket(bucket).await?;
        self.transition_active(
            b.id,
            &[IndexRunState::Running],
            IndexRunState::Paused,
            false,
            false,
            "no running index for this bucket",
        )
        .await
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

    async fn refresh(&self, bucket: &BucketRef, cancel: &CancellationToken) -> Result<IndexReport> {
        self.run(bucket, cancel, RunMode::Refresh).await
    }

    async fn rebuild(&self, bucket: &BucketRef, cancel: &CancellationToken) -> Result<IndexReport> {
        self.run(bucket, cancel, RunMode::Rebuild).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstore_domain::{Error, ErrorCode};
    use rstore_test_support::{StorageMethod, TestClock};

    use super::*;

    #[tokio::test]
    async fn paused_refresh_does_not_reconcile_or_advance_revision() {
        let (app, factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;
        factory
            .storage()
            .seed("media", "docs/report.pdf", b"report", None);
        let clock = Arc::new(TestClock::default());
        let app = app.with_clock(clock.clone());
        let bucket = BucketRef::Name("media".into());

        let first = app
            .api()
            .catalog()
            .indexer()
            .start(&bucket, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(first.catalog_revision, 1);
        clock.advance_secs(1);

        let cancel = CancellationToken::new();
        cancel.cancel();
        let paused = app
            .api()
            .catalog()
            .indexer()
            .refresh(&bucket, &cancel)
            .await
            .unwrap();

        assert_eq!(paused.state, IndexRunState::Paused);
        assert_eq!(paused.objects_stale, 0);
        assert_eq!(paused.catalog_revision, 1);
        let status = app.api().catalog().indexer().status().await.unwrap();
        assert_eq!(status[0].stale_count, 0);
        assert_eq!(status[0].catalog_revision, Some(1));
        assert_eq!(
            status[0].last_run.as_ref().map(|run| run.state),
            Some(IndexRunState::Paused)
        );
    }

    #[tokio::test]
    async fn scan_failure_persists_a_failed_run() {
        let (app, factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;
        factory.storage().fail_until_cleared(
            StorageMethod::List,
            Error::invalid_input("injected listing failure"),
        );
        let bucket = BucketRef::Name("media".into());

        let error = app
            .api()
            .catalog()
            .indexer()
            .start(&bucket, &CancellationToken::new())
            .await
            .unwrap_err();
        // The scan propagates the adapter's own classification — the old
        // `Error::network` wrap reclassified every listing failure as a
        // network error.
        assert_eq!(error.code, ErrorCode::InvalidInput);

        let status = app.api().catalog().indexer().status().await.unwrap();
        let failed = status[0].last_run.as_ref().unwrap();
        assert_eq!(failed.state, IndexRunState::Failed);
        assert_eq!(failed.error_count, 1);
        assert!(failed.finished_at.is_some());
        assert_eq!(status[0].catalog_revision, None);
    }
}
