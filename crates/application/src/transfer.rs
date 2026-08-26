//! Upload/download orchestration: classifies download modes, turns remote
//! references into plans, resolves endpoints with retry, and drives the
//! transfer engine per item while recording jobs and catalog settlements.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rstore_application_ports::JobRepository;
use rstore_domain::job::JobKind;
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::{Clock, Error, Result};
use rstore_transfer::{
    CancellationToken, MultipartStore, ProgressSink, TransferConfig, TransferEngine,
};

use crate::connection::ConnectionRef;
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

/// Parses canonical remote references into one plan per source.
///
/// Parsing happens once here so later stages never re-expand references.
pub fn download_plans(
    references: &[String],
    target: PathBuf,
    conflict: ConflictPolicy,
    recursive: bool,
) -> Result<Vec<DownloadPlan>> {
    references
        .iter()
        .map(|uri| {
            let parsed = RemoteUri::parse(uri)?;
            Ok(DownloadPlan {
                connection: parsed.connection,
                bucket: parsed.bucket,
                prefix: parsed.key,
                target: target.clone(),
                conflict,
                recursive,
            })
        })
        .collect()
}

/// Aggregate counters over several sequential downloads.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BatchDownloadReport {
    pub reports: Vec<TransferReport>,
    pub succeeded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub blocked: usize,
    pub interrupted: bool,
}

impl BatchDownloadReport {
    /// The worst child severity — the batch verdict entry layers act on.
    pub fn severity(&self) -> crate::batch_outcome::Severity {
        self.reports
            .iter()
            .map(TransferReport::severity)
            .max()
            .unwrap_or(crate::batch_outcome::Severity::AllOk)
    }
}

/// Runtime knobs shared by upload and download runs.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunOptions {
    pub dry_run: bool,
    pub config: TransferConfig,
    pub retry: rstore_transfer::RetryPolicy,
}

/// Settle hook for upload/download runs: only successful uploads carry
/// engine metadata and index it; downloads have no catalog follow-up of
/// their own.
struct UploadSettle {
    mutations: Arc<crate::mutation::MutationCoordinator>,
    bucket: rstore_domain::Bucket,
    clock: Arc<dyn Clock>,
}

#[async_trait::async_trait]
impl crate::job_run::SettleHook<EngineItem> for UploadSettle {
    async fn settle(
        &self,
        item: EngineItem,
        verdict: crate::job_run::ItemVerdict,
    ) -> crate::job_run::Step {
        if let (EngineItem::Upload(_), Some(metadata)) = (&item, verdict.metadata) {
            self.mutations
                .settle_upload(
                    self.bucket.connection_id,
                    self.bucket.id,
                    &metadata,
                    self.clock.now(),
                )
                .await;
        }
        verdict.step
    }
}

pub struct DefaultTransferService {
    gateway: crate::gateway::RemoteGateway,
    locator: crate::locator::CatalogLocator,
    jobs: Arc<dyn JobRepository>,
    checkpoints: Arc<dyn MultipartStore>,
    clock: Arc<dyn Clock>,
    mutations: Arc<crate::mutation::MutationCoordinator>,
}

impl DefaultTransferService {
    pub(crate) fn new(
        gateway: crate::gateway::RemoteGateway,
        locator: crate::locator::CatalogLocator,
        jobs: Arc<dyn JobRepository>,
        checkpoints: Arc<dyn MultipartStore>,
        clock: Arc<dyn Clock>,
        mutations: Arc<crate::mutation::MutationCoordinator>,
    ) -> Self {
        Self {
            gateway,
            locator,
            jobs,
            checkpoints,
            clock,
            mutations,
        }
    }

    /// Resolves the endpoint pair with retry and cancellation support.
    async fn plan_target_with_retry(
        &self,
        connection: &ConnectionRef,
        bucket: &str,
        retry: rstore_transfer::RetryPolicy,
        cancel: &CancellationToken,
    ) -> Result<(
        rstore_domain::ConnectionConfig,
        rstore_domain::Bucket,
        std::sync::Arc<dyn rstore_storage_api::Storage>,
    )> {
        self.locator
            .resolve_pair_target_with_retry(connection, bucket, retry, cancel.clone())
            .await
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
        let (config, bucket, storage) = self
            .plan_target_with_retry(&plan.connection, &plan.bucket, options.retry, cancel)
            .await?;

        // The write guard is the gateway's writer adapter: the engine's
        // every write call re-checks the bucket's guard bits, so a
        // read-only target fails here before any item runs.
        let storage = self.gateway.writer(storage, &bucket)?;

        let engine = TransferEngine::new(storage, self.checkpoints.clone());
        let items = crate::transfer_plan::upload_items(prepared, config.id, &bucket.name)
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
            &engine,
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
        let (config, bucket, storage) = self
            .plan_target_with_retry(&plan.connection, &plan.bucket, options.retry, cancel)
            .await?;

        let items = crate::transfer_plan::download_items(
            prepared,
            config.id,
            &bucket.name,
            storage.as_ref(),
        )
        .await?
        .into_iter()
        .map(EngineItem::into_run_item)
        .collect();

        let engine = TransferEngine::new(storage, self.checkpoints.clone());
        self.execute(
            JobKind::Download,
            items,
            options.dry_run,
            bucket,
            progress,
            cancel,
            &engine,
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
            succeeded: 0,
            skipped: 0,
            failed: 0,
            blocked: 0,
            interrupted: false,
        };
        for plan in plans {
            let report = self.run_download(plan, options, progress, cancel).await?;
            batch.succeeded += report.succeeded;
            batch.skipped += report.skipped;
            batch.failed += report.failed;
            batch.blocked += report.blocked;
            batch.interrupted |= report.is_interrupted();
            batch.reports.push(report);
            if batch.interrupted {
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
        engine: &TransferEngine,
        config: TransferConfig,
    ) -> Result<TransferReport> {
        crate::job_run::run_items(
            self.jobs.as_ref(),
            self.clock.as_ref(),
            kind,
            items,
            dry_run,
            progress,
            cancel,
            |item| async move {
                if dry_run {
                    return crate::job_run::ItemVerdict::preview(item.preview());
                }
                let ctx = rstore_transfer::RunContext::new(cancel, config, progress);
                let result = match &item {
                    EngineItem::Upload(req) => engine.upload(req, &ctx).await,
                    EngineItem::Download { request, .. } => engine.download(request, &ctx).await,
                };
                crate::job_run::ItemVerdict::from_engine_result(result)
            },
            &UploadSettle {
                mutations: self.mutations.clone(),
                bucket,
                clock: self.clock.clone(),
            },
        )
        .await
    }
}
