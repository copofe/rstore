//! Upload/download orchestration: classifies download modes, turns remote
//! references into plans, resolves endpoints with retry, and drives the
//! transfer engine per item while recording jobs and catalog settlements.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::transfer::{CancellationToken, ProgressSink, TransferConfig, TransferEngine};
use rstore_domain::job::JobKind;
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::{Error, Result};
use serde::ser::{Serialize, SerializeStruct, Serializer};

use crate::batch_outcome::{count_dispositions, OutcomeCounts};
use crate::reference::RemoteUri;
use crate::transfer_plan::EngineItem;

pub use crate::transfer_plan::{DownloadPlan, UploadPlan};
pub use crate::transfer_report::{TransferReport, TransferReportItem};

/// Whether a download targets one object or fans out over a directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadMode {
    Single,
    Batch,
}

/// Picks the download mode from source count and target shape.
///
/// A batch requires an existing target directory; every item then mirrors
/// beneath it.
///
/// # Errors
/// Returns `invalid_input` for zero sources or a batch without a directory.
pub fn classify_download(source_count: usize, target: &Path) -> Result<DownloadMode> {
    if source_count == 0 {
        return Err(Error::invalid_input(
            "download needs at least one remote source reference",
        ));
    }
    if source_count > 1 {
        if !target.is_dir() {
            return Err(Error::invalid_input(
                "batch download needs an existing target directory",
            ));
        }
        return Ok(DownloadMode::Batch);
    }
    Ok(DownloadMode::Single)
}

/// One plan per expanded remote source, all mirroring into `target`.
pub fn download_plans(
    references: &[RemoteUri],
    target: PathBuf,
    conflict: ConflictPolicy,
    recursive: bool,
) -> Vec<DownloadPlan> {
    references
        .iter()
        .map(|uri| DownloadPlan {
            connection: uri.connection.clone(),
            bucket: uri.bucket.clone(),
            prefix: uri.key.clone(),
            target: target.clone(),
            conflict,
            recursive,
        })
        .collect()
}

/// Aggregate view over several sequential downloads.
#[derive(Debug, Clone)]
pub struct BatchDownloadReport {
    pub reports: Vec<TransferReport>,
}

impl BatchDownloadReport {
    fn counts(&self) -> OutcomeCounts {
        count_dispositions(
            self.reports
                .iter()
                .flat_map(|report| report.items.iter().map(|item| &item.disposition)),
        )
    }

    /// Items that completed as requested across all child runs.
    pub fn succeeded(&self) -> usize {
        self.counts().succeeded
    }

    /// Items skipped by policy or blocked before execution.
    pub fn skipped(&self) -> usize {
        self.counts().skipped
    }

    /// Items that failed across all child runs.
    pub fn failed(&self) -> usize {
        self.counts().failed
    }

    /// Items blocked before execution across all child runs.
    pub fn blocked(&self) -> usize {
        self.counts().blocked
    }

    /// Whether any child run ended by cancellation or interruption.
    pub fn is_interrupted(&self) -> bool {
        self.reports.iter().any(TransferReport::is_interrupted)
    }

    /// The aggregate verdict over every child item.
    pub fn severity(&self) -> crate::batch_outcome::Severity {
        crate::batch_outcome::severity_of(
            self.reports
                .iter()
                .flat_map(|report| report.items.iter().map(|item| &item.disposition)),
        )
    }
}

impl Serialize for BatchDownloadReport {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let counts = self.counts();
        let mut out = serializer.serialize_struct("BatchDownloadReport", 6)?;
        out.serialize_field("reports", &self.reports)?;
        out.serialize_field("succeeded", &counts.succeeded)?;
        out.serialize_field("skipped", &counts.skipped)?;
        out.serialize_field("failed", &counts.failed)?;
        out.serialize_field("blocked", &counts.blocked)?;
        out.serialize_field("interrupted", &self.is_interrupted())?;
        out.end()
    }
}

/// Runtime knobs shared by upload and download runs.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunOptions {
    pub dry_run: bool,
    pub config: TransferConfig,
    pub retry: crate::transfer::RetryPolicy,
}

/// Settle hook for upload/download runs: a successful upload updates the
/// catalog cache; downloads have no cache follow-up of their own.
struct UploadSettle {
    cache: crate::write_through::WriteThrough,
    bucket: rstore_domain::Bucket,
}

#[async_trait::async_trait]
impl crate::job_run::SettleHook<EngineItem> for UploadSettle {
    async fn settle(
        &self,
        item: EngineItem,
        verdict: crate::job_run::ItemVerdict,
    ) -> crate::job_run::Step {
        match (&item, verdict.metadata) {
            (EngineItem::Upload(_), Some(metadata)) => {
                let note = self.cache.written(&self.bucket, &metadata).await;
                verdict.step.noted(note)
            }
            _ => verdict.step,
        }
    }
}

pub struct TransferService {
    gateway: crate::gateway::RemoteGateway,
    env: crate::job_run::JobEnv,
    checkpoints: Arc<crate::db::MultipartStore>,
    cache: crate::write_through::WriteThrough,
}

impl TransferService {
    pub(crate) fn new(
        gateway: crate::gateway::RemoteGateway,
        env: crate::job_run::JobEnv,
        checkpoints: Arc<crate::db::MultipartStore>,
        cache: crate::write_through::WriteThrough,
    ) -> Self {
        Self {
            gateway,
            env,
            checkpoints,
            cache,
        }
    }

    /// Uploads local files (optionally whole trees) to one bucket.
    ///
    /// Plan validation runs on a blocking thread before endpoint resolution,
    /// so bad sources fail without ever building a storage adapter.
    pub async fn run_upload(
        &self,
        plan: &UploadPlan,
        options: &RunOptions,
        progress: &dyn ProgressSink,
        cancel: &CancellationToken,
    ) -> Result<TransferReport> {
        // Recursive directory walks are blocking I/O; keep them off the
        // async runtime threads.
        let prepared = {
            let plan = plan.clone();
            tokio::task::spawn_blocking(move || crate::transfer_plan::prepare_upload(&plan))
                .await
                .map_err(|error| Error::database("upload planning task").with_source(error))??
        };
        let (bucket, storage) = self
            .gateway
            .resolve_target_with_retry(
                &plan.connection,
                &plan.bucket,
                options.retry,
                cancel.clone(),
            )
            .await?;

        // The write guard is the gateway's writer adapter: the engine's
        // every write call re-checks the bucket's guard bits, so a
        // read-only target fails here before any item runs.
        let storage = self.gateway.writer(storage, &bucket)?;

        let items =
            crate::transfer_plan::upload_items(prepared, bucket.connection_id, &bucket.name)
                .into_iter()
                .map(EngineItem::into_run_item)
                .collect();
        self.execute(
            JobKind::Upload,
            items,
            options.dry_run,
            bucket,
            progress,
            cancel,
            storage,
            options.config,
        )
        .await
    }

    /// Downloads objects under a prefix or exact key to a local target.
    ///
    /// Plan validation is purely local and runs before endpoint resolution.
    pub async fn run_download(
        &self,
        plan: &DownloadPlan,
        options: &RunOptions,
        progress: &dyn ProgressSink,
        cancel: &CancellationToken,
    ) -> Result<TransferReport> {
        let prepared = crate::transfer_plan::prepare_download(plan)?;
        let (bucket, storage) = self
            .gateway
            .resolve_target_with_retry(
                &plan.connection,
                &plan.bucket,
                options.retry,
                cancel.clone(),
            )
            .await?;

        let items = crate::transfer_plan::download_items(
            prepared,
            bucket.connection_id,
            &bucket.name,
            storage.as_ref(),
        )
        .await?
        .into_iter()
        .map(EngineItem::into_run_item)
        .collect();

        self.execute(
            JobKind::Download,
            items,
            options.dry_run,
            bucket,
            progress,
            cancel,
            storage,
            options.config,
        )
        .await
    }

    /// Runs plans sequentially, aggregating counts into one batch report.
    ///
    /// Stops after the first interrupted child so cancellation propagates
    /// promptly instead of queuing further downloads.
    pub async fn run_batch_download(
        &self,
        plans: &[DownloadPlan],
        options: &RunOptions,
        progress: &dyn ProgressSink,
        cancel: &CancellationToken,
    ) -> Result<BatchDownloadReport> {
        if plans.is_empty() {
            return Err(Error::invalid_input("nothing to download"));
        }
        let mut batch = BatchDownloadReport {
            reports: Vec::with_capacity(plans.len()),
        };
        for plan in plans {
            let report = self.run_download(plan, options, progress, cancel).await?;
            let interrupted = report.is_interrupted();
            batch.reports.push(report);
            if interrupted {
                break;
            }
        }
        Ok(batch)
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute(
        &self,
        kind: JobKind,
        items: Vec<crate::job_run::RunItem<EngineItem>>,
        dry_run: bool,
        bucket: rstore_domain::Bucket,
        progress: &dyn ProgressSink,
        cancel: &CancellationToken,
        storage: Arc<dyn rstore_storage::Storage>,
        config: TransferConfig,
    ) -> Result<TransferReport> {
        let engine = TransferEngine::new(storage, self.checkpoints.clone());
        let run = self.env.begin(cancel, progress);
        let context = crate::transfer::RunContext::new(run.cancel(), config, run.progress());
        crate::job_run::run_items(
            &run,
            kind,
            items,
            dry_run,
            |item| {
                let context = &context;
                let engine = &engine;
                async move {
                    if dry_run {
                        return crate::job_run::ItemVerdict::preview(item.preview());
                    }
                    let result = match &item {
                        EngineItem::Upload(req) => engine.upload(req, context).await,
                        EngineItem::Download { request, .. } => {
                            engine.download(request, context).await
                        }
                    };
                    crate::job_run::ItemVerdict::from_engine_result(result)
                }
            },
            &UploadSettle {
                cache: self.cache.clone(),
                bucket,
            },
        )
        .await
    }
}
