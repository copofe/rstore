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

    #[cfg(test)]
    async fn plan_target(
        &self,
        connection: &ConnectionRef,
        bucket: &str,
    ) -> Result<(
        rstore_domain::ConnectionConfig,
        rstore_domain::Bucket,
        std::sync::Arc<dyn rstore_storage_api::Storage>,
    )> {
        self.locator.resolve_pair_target(connection, bucket).await
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use rstore_domain::job::{ItemState, JobState};
    use rstore_domain::policy::ConflictPolicy;
    use rstore_domain::remote::ObjectPath;
    use rstore_test_support::FakeStorageFactory;
    use rstore_transfer::NullProgressSink;

    async fn setup() -> Arc<DefaultTransferService> {
        setup_with_factory().await.0
    }

    async fn setup_with_factory() -> (Arc<DefaultTransferService>, Arc<FakeStorageFactory>) {
        let (app, factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;
        (app.transfer_service(), factory)
    }

    #[cfg(test)]
    fn catalog_for_test(
        svc: &DefaultTransferService,
    ) -> Arc<dyn rstore_catalog::RemoteObjectRepository> {
        svc.mutations.catalog_for_test()
    }

    fn upload_plan(sources: Vec<PathBuf>, prefix: &str, recursive: bool) -> UploadPlan {
        UploadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: prefix.into(),
            sources,
            conflict: ConflictPolicy::Overwrite,
            recursive,
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rstore-xfer-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn classify_download_keeps_target_rules_in_application() {
        let target = temp_dir("classify-download");
        assert_eq!(classify_download(1, &target).unwrap(), DownloadMode::Single);
        assert_eq!(classify_download(2, &target).unwrap(), DownloadMode::Batch);

        let missing = target.join("missing");
        let error = classify_download(2, &missing).unwrap_err();
        assert_eq!(error.code, rstore_domain::ErrorCode::InvalidInput);
    }

    #[test]
    fn download_plans_parse_canonical_references_once() {
        let plans = download_plans(
            &["nas/media/docs/a.txt".into(), "nas/media/docs/b.txt".into()],
            PathBuf::from("downloads"),
            ConflictPolicy::Overwrite,
            false,
        )
        .unwrap();

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].bucket, "media");
        assert_eq!(plans[0].prefix, "docs/a.txt");
        assert_eq!(plans[1].prefix, "docs/b.txt");
    }

    #[tokio::test]
    async fn upload_validation_precedes_endpoint_resolution() {
        let (svc, factory) = setup_with_factory().await;
        let before = factory.built();
        let plan = upload_plan(
            vec![temp_dir("validate-before-upload-adapter").join("missing.txt")],
            "",
            false,
        );

        let error = svc
            .run_upload(
                &plan,
                &RunOptions::default(),
                &NullProgressSink,
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code, rstore_domain::ErrorCode::NotFound);
        assert_eq!(factory.built(), before);
    }

    #[tokio::test]
    async fn download_validation_precedes_endpoint_resolution() {
        let (svc, factory) = setup_with_factory().await;
        let before = factory.built();
        let plan = DownloadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: "docs/".into(),
            target: temp_dir("validate-before-download-adapter"),
            conflict: ConflictPolicy::Overwrite,
            recursive: false,
        };

        let error = svc
            .run_download(
                &plan,
                &RunOptions::default(),
                &NullProgressSink,
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code, rstore_domain::ErrorCode::InvalidInput);
        assert_eq!(factory.built(), before);
    }

    #[tokio::test]
    async fn conflict_skip_reports_skipped_not_succeeded() {
        use rstore_storage_api::Storage as _;
        let (svc, factory) = setup_with_factory().await;
        factory.storage().seed("media", "docs/a.txt", b"old", None);
        let dir = temp_dir("skip");
        std::fs::write(dir.join("a.txt"), b"new").unwrap();

        let plan = UploadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: "docs".into(),
            sources: vec![dir.join("a.txt")],
            conflict: ConflictPolicy::Skip,
            recursive: false,
        };
        let report = svc
            .run_upload(
                &plan,
                &RunOptions::default(),
                &NullProgressSink,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(report.skipped, 1);
        assert_eq!(report.succeeded, 0);
        assert!(matches!(
            report.items[0].disposition,
            crate::batch_outcome::Disposition::Skipped(_)
                | crate::batch_outcome::Disposition::Blocked(_)
        ));
        assert!(report.items[0]
            .detail
            .as_deref()
            .unwrap()
            .contains("docs/a.txt"));
        let meta = factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("docs/a.txt").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(meta.size, 3);
    }

    #[tokio::test]
    async fn upload_and_download_round_trip_with_job_and_catalog() {
        let svc = setup().await;
        let dir = temp_dir("rt");
        std::fs::write(dir.join("a.txt"), b"alpha").unwrap();
        std::fs::write(dir.join("b.log"), b"beta").unwrap();

        let plan = UploadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: "docs".into(),
            sources: vec![dir.join("a.txt"), dir.join("b.log")],
            conflict: ConflictPolicy::Overwrite,
            recursive: false,
        };
        let report = svc
            .run_upload(
                &plan,
                &RunOptions::default(),
                &NullProgressSink,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 2);
        assert_eq!(report.state, JobState::Succeeded);

        let (_, bucket, _) = svc
            .plan_target(&ConnectionRef::Name("nas".into()), "media")
            .await
            .unwrap();
        let indexed = catalog_for_test(&svc)
            .find_by_locator(
                bucket.connection_id,
                bucket.id,
                &ObjectPath::new("docs/a.txt").unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(indexed.size, 5);
        assert!(indexed.etag.is_some());

        let target_dir = temp_dir("dl");
        let dl = DownloadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: "docs/".into(),
            target: target_dir.clone(),
            conflict: ConflictPolicy::Overwrite,
            recursive: true,
        };
        let report = svc
            .run_download(
                &dl,
                &RunOptions::default(),
                &NullProgressSink,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 2);
        assert_eq!(
            std::fs::read(target_dir.join("a.txt")).unwrap(),
            b"alpha".to_vec()
        );

        let jobs = svc.jobs.list().await.unwrap();
        assert!(jobs
            .iter()
            .any(|j| j.kind == JobKind::Upload && j.state == JobState::Succeeded));
    }

    #[tokio::test]
    async fn dry_run_touches_nothing() {
        let svc = setup().await;
        let dir = temp_dir("dry");
        std::fs::write(dir.join("c.txt"), b"x").unwrap();
        let plan = UploadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: String::new(),
            sources: vec![dir.join("c.txt")],
            conflict: ConflictPolicy::Overwrite,
            recursive: false,
        };
        let report = svc
            .run_upload(
                &plan,
                &RunOptions {
                    dry_run: true,
                    ..Default::default()
                },
                &NullProgressSink,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(report.dry_run);

        let dl = DownloadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: String::new(),
            target: dir.join("out"),
            conflict: ConflictPolicy::Overwrite,
            recursive: true,
        };
        assert!(svc
            .run_download(
                &dl,
                &RunOptions::default(),
                &NullProgressSink,
                &CancellationToken::new()
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn cancellation_persists_interrupted_state() {
        let svc = setup().await;
        let dir = temp_dir("cancel");
        std::fs::write(dir.join("z.txt"), b"payload").unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let plan = UploadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: String::new(),
            sources: vec![dir.join("z.txt")],
            conflict: ConflictPolicy::Overwrite,
            recursive: false,
        };
        let report = svc
            .run_upload(&plan, &RunOptions::default(), &NullProgressSink, &token)
            .await
            .unwrap();
        assert_eq!(report.state, JobState::Interrupted);
        let job = svc.jobs.list().await.unwrap().remove(0);
        assert_eq!(job.state, JobState::Interrupted);
    }

    #[tokio::test]
    async fn download_resolves_the_remote_endpoint_once() {
        let (svc, factory) = setup_with_factory().await;
        factory
            .storage()
            .seed("media", "img/logo.png", b"logo", None);
        let before = factory.built();
        let target = temp_dir("resolve-once").join("logo.png");

        let report = svc
            .run_download(
                &DownloadPlan {
                    connection: ConnectionRef::Name("nas".into()),
                    bucket: "media".into(),
                    prefix: "img/logo.png".into(),
                    target,
                    conflict: ConflictPolicy::Overwrite,
                    recursive: false,
                },
                &RunOptions::default(),
                &NullProgressSink,
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(report.succeeded, 1);
        assert_eq!(factory.built() - before, 1);
    }

    #[tokio::test]
    async fn batch_download_aggregates_child_job_results() {
        let (svc, factory) = setup_with_factory().await;
        factory.storage().seed("media", "one.txt", b"1", None);
        factory.storage().seed("media", "two.txt", b"22", None);
        let target = temp_dir("batch-download");
        let plans: Vec<DownloadPlan> = ["one.txt", "missing.txt", "two.txt"]
            .into_iter()
            .map(|key| DownloadPlan {
                connection: ConnectionRef::Name("nas".into()),
                bucket: "media".into(),
                prefix: key.into(),
                target: target.clone(),
                conflict: ConflictPolicy::Overwrite,
                recursive: false,
            })
            .collect();

        let batch = svc
            .run_batch_download(
                &plans,
                &RunOptions::default(),
                &NullProgressSink,
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(batch.reports.len(), 3);
        assert_eq!(batch.succeeded, 2);
        assert_eq!(batch.failed, 1);
        assert_eq!(std::fs::read(target.join("one.txt")).unwrap(), b"1");
        assert_eq!(std::fs::read(target.join("two.txt")).unwrap(), b"22");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn partial_failure_reports_partial_success() {
        let svc = setup().await;
        let dir = temp_dir("partial");
        std::fs::write(dir.join("ok.txt"), b"fine").unwrap();
        let unreadable = dir.join("bad.txt");
        std::fs::write(&unreadable, b"secret").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();

        // Running as root makes the unreadable file readable; skip there.
        if std::fs::read(&unreadable).is_ok() {
            return;
        }

        let report = svc
            .run_upload(
                &upload_plan(vec![dir.join("ok.txt"), unreadable], "", false),
                &RunOptions::default(),
                &NullProgressSink,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(report.state, JobState::PartialSuccess);

        let job = svc.jobs.list().await.unwrap().remove(0);
        assert_eq!(job.state, JobState::PartialSuccess);
        let failed_item = job
            .items
            .iter()
            .find(|i| i.state == ItemState::Failed)
            .unwrap();

        assert!(failed_item.error.as_deref().unwrap().len() <= 200);
    }
}
