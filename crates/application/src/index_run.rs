//! Persistent lifecycle for one catalog index run.
//!
//! The storage page walker reports facts to this module. This module owns the
//! state transitions and the rules for checkpoints, failures, refresh
//! reconciliation, and catalog revisions.

use std::sync::Arc;

use rstore_application_ports::{IndexPageCommit, IndexRunCommit, IndexRunStore};
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::id::{BucketId, IndexRunId};
use rstore_domain::remote::RemoteObject;
use rstore_domain::workspace::{IndexRun, IndexRunState};
use rstore_domain::{Clock, Result};

use super::IndexReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunMode {
    Start,
    Refresh,
    Rebuild,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanCompletion {
    Complete,
    Paused,
}

/// The stateful, persisted part of one index run.
pub(crate) struct IndexRunLifecycle {
    bucket_id: BucketId,
    runs: Arc<dyn IndexRunStore>,
    clock: Arc<dyn Clock>,
    run: IndexRun,
    report: IndexReport,
}

impl IndexRunLifecycle {
    pub(crate) async fn begin(
        bucket_id: BucketId,
        runs: Arc<dyn IndexRunStore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let run = IndexRun {
            id: IndexRunId::new(),
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

    pub(crate) async fn prepare(
        &mut self,
        mode: RunMode,
        catalog: &dyn RemoteObjectRepository,
    ) -> Result<Option<String>> {
        match mode {
            RunMode::Start => self
                .runs
                .checkpoint(self.bucket_id)
                .await
                .map(|checkpoint| checkpoint.and_then(|value| value.continuation_token)),
            RunMode::Refresh => {
                self.runs.clear_checkpoint(self.bucket_id).await?;
                Ok(None)
            }
            RunMode::Rebuild => {
                catalog.mark_all_stale_in_bucket(self.bucket_id).await?;
                self.runs.clear_checkpoint(self.bucket_id).await?;
                Ok(None)
            }
        }
    }

    pub(crate) fn observe_page(&mut self, objects: u64) {
        self.run.objects_seen += objects;
    }

    pub(crate) fn now(&self) -> time::OffsetDateTime {
        self.clock.now()
    }

    pub(crate) async fn checkpoint_on_cancel(&self, token: Option<&str>) -> Result<()> {
        self.runs
            .save_checkpoint(self.bucket_id, token, self.clock.now())
            .await
    }

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

    pub(crate) async fn reconcile(
        &mut self,
        mode: RunMode,
        completion: ScanCompletion,
        catalog: &dyn RemoteObjectRepository,
    ) -> Result<()> {
        if mode == RunMode::Refresh && completion == ScanCompletion::Complete {
            // Only a complete root scan proves that unseen rows are missing
            // remotely. A paused scan must not reconcile.
            self.report.objects_stale = catalog
                .mark_stale_older_than(self.bucket_id, self.run.started_at)
                .await?;
        }
        if mode == RunMode::Rebuild && completion == ScanCompletion::Complete {
            self.report.objects_pruned = catalog.delete_stale_in_bucket(self.bucket_id).await?;
        }
        Ok(())
    }

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
