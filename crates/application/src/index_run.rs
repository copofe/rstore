//! Persisted index-run lifecycle: create the run row, page-by-page
//! checkpointed publication, and terminal-state reconciliation against the
//! catalog.

use std::sync::Arc;

use rstore_application_ports::{IndexPageCommit, IndexRunCommit, IndexRunStore};
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::id::{BucketId, IndexRunId};
use rstore_domain::remote::RemoteObject;
use rstore_domain::workspace::{IndexRun, IndexRunState};
use rstore_domain::{Clock, Result};

use super::IndexReport;

/// What an index run starts from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMode {
    /// Resume from the bucket's saved checkpoint, if any.
    Resume,
    /// Fresh scan from the bucket root; stale rows are marked afterwards.
    Refresh,
    /// Fresh scan after marking every existing row stale; unreached rows
    /// are deleted afterwards.
    Rebuild,
}

/// Whether a scan finished or was paused at a checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanCompletion {
    Complete,
    Paused,
}

/// Stateful helper owning one index run's persisted progress: the run row,
/// per-page commits, checkpoints, and the final report.
pub(crate) struct IndexRunLifecycle {
    bucket_id: BucketId,
    runs: Arc<dyn IndexRunStore>,
    clock: Arc<dyn Clock>,
    run: IndexRun,
    report: IndexReport,
}

impl IndexRunLifecycle {
    /// Create and persist a new `Running` run row for `bucket_id` with a
    /// zeroed report.
    pub(crate) async fn begin(
        bucket_id: BucketId,
        runs: Arc<dyn IndexRunStore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let run = IndexRun {
            id: IndexRunId::new_random(),
            bucket_id,
            state: IndexRunState::Running,
            started_at: clock.now(),
            finished_at: None,
            objects_seen: 0,
            error_count: 0,
        };
        runs.create(&run).await?;
        let report = IndexReport {
            run_id: run.id,
            bucket: bucket_id,
            state: IndexRunState::Running,
            objects_seen: 0,
            objects_upserted: 0,
            objects_pruned: 0,
            objects_stale: 0,
            catalog_revision: 0,
        };
        Ok(Self {
            bucket_id,
            runs,
            clock,
            run,
            report,
        })
    }

    /// Prepare the scan start position for `mode`, returning an optional
    /// continuation token to resume from. Rebuild additionally marks all
    /// existing catalog rows in the bucket stale before scanning.
    pub(crate) async fn prepare(
        &mut self,
        mode: IndexMode,
        catalog: &dyn RemoteObjectRepository,
    ) -> Result<Option<String>> {
        match mode {
            IndexMode::Resume => self
                .runs
                .checkpoint(self.bucket_id)
                .await
                .map(|checkpoint| checkpoint.and_then(|value| value.continuation_token)),
            IndexMode::Refresh => {
                self.runs.clear_checkpoint(self.bucket_id).await?;
                Ok(None)
            }
            IndexMode::Rebuild => {
                // Every row that exists when a rebuild begins was confirmed
                // before it started — the refresh spelling with the run's
                // own start marks exactly those.
                catalog
                    .mark_stale_older_than(self.bucket_id, self.run.started_at)
                    .await?;
                self.runs.clear_checkpoint(self.bucket_id).await?;
                Ok(None)
            }
        }
    }

    /// Record one listing page's object count toward `objects_seen`.
    pub(crate) fn observe_page(&mut self, objects: u64) {
        self.run.objects_seen += objects;
    }

    pub(crate) fn now(&self) -> time::OffsetDateTime {
        self.clock.now()
    }

    /// Persist `token` as the bucket's resume checkpoint (used when a scan
    /// is cancelled mid-run).
    pub(crate) async fn checkpoint_on_cancel(&self, token: Option<&str>) -> Result<()> {
        self.runs
            .save_checkpoint(self.bucket_id, token, self.clock.now())
            .await
    }

    /// Commit one scanned page — upserts, continuation token, counters —
    /// as a single durable unit and advance the report's upsert count.
    pub(crate) async fn publish_page(
        &mut self,
        objects: &[RemoteObject],
        token: Option<&str>,
    ) -> Result<()> {
        let commit = IndexPageCommit {
            run: self.run.id,
            bucket: self.bucket_id,
            objects: objects.to_vec(),
            continuation_token: token.map(str::to_owned),
            at: self.clock.now(),
            objects_seen: self.run.objects_seen,
            error_count: self.run.error_count,
        };
        self.runs.publish_page(&commit).await?;
        self.report.objects_upserted += objects.len() as u64;
        Ok(())
    }

    /// Apply mode-specific reconciliation over stale rows once the scan is
    /// known to be complete: refresh marks pre-run rows stale, rebuild
    /// deletes them. Paused scans reconcile nothing — their stale marks
    /// belong to the resuming run.
    pub(crate) async fn reconcile(
        &mut self,
        mode: IndexMode,
        completion: ScanCompletion,
        catalog: &dyn RemoteObjectRepository,
    ) -> Result<()> {
        if mode == IndexMode::Refresh && completion == ScanCompletion::Complete {
            // Rows not seen by this scan were last confirmed before it
            // started, i.e. they may have vanished remotely.
            self.report.objects_stale = catalog
                .mark_stale_older_than(self.bucket_id, self.run.started_at)
                .await?;
        }
        if mode == IndexMode::Rebuild && completion == ScanCompletion::Complete {
            self.report.objects_pruned = catalog.delete_stale_in_bucket(self.bucket_id).await?;
        }
        Ok(())
    }

    /// Transition the run to its terminal state (`Succeeded` on complete,
    /// `Paused` otherwise), persist the finish commit, and return the
    /// final report with the post-run catalog revision.
    ///
    /// Errors when the persisted run's current state does not allow the
    /// transition (e.g. it was failed concurrently).
    pub(crate) async fn finish(&mut self, completion: ScanCompletion) -> Result<IndexReport> {
        let target = match completion {
            ScanCompletion::Complete => IndexRunState::Succeeded,
            ScanCompletion::Paused => IndexRunState::Paused,
        };
        self.run.state = self.run.state.transition(target)?;
        let at = self.clock.now();
        let finished_at = (self.run.state == IndexRunState::Succeeded).then_some(at);
        let commit = IndexRunCommit {
            run: self.run.id,
            bucket: self.bucket_id,
            state: self.run.state,
            finished_at,
            objects_seen: self.run.objects_seen,
            error_count: self.run.error_count,
            at,
        };
        let revision = self.runs.finish(&commit).await?;
        self.report.state = self.run.state;
        self.report.objects_seen = self.run.objects_seen;
        self.report.catalog_revision = if let Some(revision) = revision {
            revision
        } else {
            self.runs
                .revision(self.bucket_id)
                .await?
                .map_or(0, |(revision, _)| revision)
        };
        Ok(self.report.clone())
    }

    /// Persist a `Failed` transition with a bumped error count.
    pub(crate) async fn fail(&mut self) -> Result<()> {
        self.run.state = self.run.state.transition(IndexRunState::Failed)?;
        self.run.error_count += 1;
        self.runs
            .update(
                self.run.id,
                Some(self.run.state),
                Some(self.clock.now()),
                self.run.objects_seen,
                self.run.error_count,
            )
            .await
    }
}
