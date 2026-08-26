//! Stable context-oriented API groups for CLI and desktop callers.
//!
//! The service implementations remain internal composition details; this module is the
//! application surface. One exposure rule per group: a group either hands out service handles
//! (accessor style, when it is pure grouping) or owns operations (method style, when the group
//! carries a rule — the bounded repair log on [`JobApi`], the conflict-policy precedence and run
//! orchestration on [`TransferApi`]). Never mix both for the same service.

use std::path::Path;
use std::sync::Arc;

use crate::bucket::BucketService;
use crate::commands::{CopyObjectsCommand, DownloadObjectsCommand, UploadObjectsCommand};
use crate::config_bundle::{ConfigBundle, DefaultConfigTransferService, ImportReport};
use crate::connection::ConnectionService;
use crate::copy::DefaultCopyService;
use crate::diagnostics::ConnectionDiagnostics;
use crate::files::FilesService;
use crate::health::{HealthReport, HealthService};
use crate::indexer::IndexService;
use crate::job::JobService;
use crate::link::{DefaultLinkService, GeneratedLink, LinkProfileInput, LinkRequest};
use crate::remote::RemoteService;
use crate::tag::TagService;
use crate::transfer::DefaultTransferService;
use crate::workspace::WorkspaceService;
use rstore_application_ports::RepairStore;
use rstore_domain::Result;

/// Every wired service handle in one bundle.
///
/// Adding a service touches exactly three places: a field here, its construction in
/// `app::wire`, and the group struct that exposes it — instead of also threading it through a
/// positional constructor and a re-listing `api()` call.
#[derive(Clone)]
pub(crate) struct Services {
    pub(crate) connections: Arc<dyn ConnectionService>,
    pub(crate) diagnostics: Arc<ConnectionDiagnostics>,
    pub(crate) buckets: Arc<dyn BucketService>,
    pub(crate) remote: Arc<dyn RemoteService>,
    pub(crate) jobs: Arc<dyn JobService>,
    pub(crate) transfers: Arc<DefaultTransferService>,
    pub(crate) copies: Arc<DefaultCopyService>,
    pub(crate) links: Arc<DefaultLinkService>,
    pub(crate) config_io: Arc<DefaultConfigTransferService>,
    pub(crate) workspaces: Arc<dyn WorkspaceService>,
    pub(crate) tags: Arc<dyn TagService>,
    pub(crate) indexer: Arc<dyn IndexService>,
    pub(crate) files: Arc<dyn FilesService>,
    pub(crate) health: Arc<HealthService>,
    pub(crate) repairs: Arc<dyn RepairStore>,
}

/// Upper bound on repair items [`JobApi::pending_repairs`] surfaces per query.
const PENDING_REPAIRS_LIMIT: u32 = 100;

/// The stable application API, organised into context-oriented groups.
///
/// Obtain it from [`crate::Application::api`]; groups clone cheaply.
#[derive(Clone)]
pub struct ApplicationApi {
    registry: RegistryApi,
    catalog: CatalogApi,
    transfer: TransferApi,
    access: AccessApi,
    diagnostics: DiagnosticsApi,
    jobs: JobApi,
    config: ConfigApi,
}

impl ApplicationApi {
    pub(crate) fn new(services: Services, storage_adapter: Option<&'static str>) -> Self {
        Self {
            registry: RegistryApi {
                connections: services.connections.clone(),
                buckets: services.buckets.clone(),
            },
            catalog: CatalogApi {
                workspaces: services.workspaces.clone(),
                tags: services.tags.clone(),
                indexer: services.indexer.clone(),
                files: services.files.clone(),
            },
            transfer: TransferApi {
                remote: services.remote.clone(),
                transfers: services.transfers.clone(),
                copies: services.copies.clone(),
                buckets: services.buckets.clone(),
            },
            access: AccessApi {
                links: services.links.clone(),
            },
            diagnostics: DiagnosticsApi {
                diagnostics: services.diagnostics.clone(),
                health: services.health.clone(),
                storage_adapter,
            },
            jobs: JobApi {
                jobs: services.jobs.clone(),
                repairs: services.repairs.clone(),
            },
            config: ConfigApi {
                config_io: services.config_io.clone(),
            },
        }
    }

    /// Connections and buckets — the registry contexts.
    pub fn registry(&self) -> RegistryApi {
        self.registry.clone()
    }

    /// Workspaces, tags, index, and file listing — the catalog contexts.
    pub fn catalog(&self) -> CatalogApi {
        self.catalog.clone()
    }

    /// Uploads, downloads, copies, and remote discovery — transfer orchestration.
    pub fn transfer(&self) -> TransferApi {
        self.transfer.clone()
    }

    /// Link generation for remote objects.
    pub fn access(&self) -> AccessApi {
        self.access.clone()
    }

    /// Connection probes and whole-catalog health.
    pub fn diagnostics(&self) -> DiagnosticsApi {
        self.diagnostics.clone()
    }

    /// Job inspection and control.
    pub fn jobs(&self) -> JobApi {
        self.jobs.clone()
    }

    /// Registry import/export.
    pub fn config(&self) -> ConfigApi {
        self.config.clone()
    }
}

/// Connection and bucket lifecycle handles (accessor style).
#[derive(Clone)]
pub struct RegistryApi {
    connections: Arc<dyn ConnectionService>,
    buckets: Arc<dyn BucketService>,
}

impl RegistryApi {
    /// Connection CRUD and resolution.
    pub fn connections(&self) -> Arc<dyn ConnectionService> {
        self.connections.clone()
    }

    /// Bucket registration, guards, and resolution.
    pub fn buckets(&self) -> Arc<dyn BucketService> {
        self.buckets.clone()
    }
}

/// Workspace, tag, index, and mixed-file-list handles (accessor style).
#[derive(Clone)]
pub struct CatalogApi {
    workspaces: Arc<dyn WorkspaceService>,
    tags: Arc<dyn TagService>,
    indexer: Arc<dyn IndexService>,
    files: Arc<dyn FilesService>,
}

impl CatalogApi {
    /// Workspace lifecycle and membership.
    pub fn workspaces(&self) -> Arc<dyn WorkspaceService> {
        self.workspaces.clone()
    }

    /// Tag lifecycle and associations.
    pub fn tags(&self) -> Arc<dyn TagService> {
        self.tags.clone()
    }

    /// Index scans over registered buckets.
    pub fn indexer(&self) -> Arc<dyn IndexService> {
        self.indexer.clone()
    }

    /// Mixed file list and search over the workspace catalog.
    pub fn files(&self) -> Arc<dyn FilesService> {
        self.files.clone()
    }
}

/// Transfer orchestration (method style): this group owns rules, not just handles.
#[derive(Clone)]
pub struct TransferApi {
    buckets: Arc<dyn BucketService>,
    remote: Arc<dyn RemoteService>,
    transfers: Arc<DefaultTransferService>,
    copies: Arc<DefaultCopyService>,
}

impl TransferApi {
    /// Resolve the non-interactive part of the conflict-policy chain.
    ///
    /// Precedence: an explicit policy wins; otherwise the target bucket's default; otherwise
    /// [`rstore_domain::ConflictPolicy::Ask`]. Callers may still decide how to handle `Ask`
    /// (prompt, `--yes`, or a blocked item), but the source precedence belongs here.
    pub async fn conflict_policy(
        &self,
        explicit: Option<rstore_domain::ConflictPolicy>,
        target: Option<(&crate::connection::ConnectionRef, &str)>,
    ) -> Result<rstore_domain::ConflictPolicy> {
        if let Some(explicit) = explicit {
            return Ok(explicit);
        }
        let Some((connection, bucket)) = target else {
            return Ok(rstore_domain::ConflictPolicy::Ask);
        };
        let bucket = self
            .buckets
            .resolve(&crate::bucket::BucketRef::Scoped(
                connection.clone(),
                bucket.to_string(),
            ))
            .await?;
        Ok(bucket.config.default_conflict)
    }

    /// Remote listing and object inspection.
    pub fn remote(&self) -> Arc<dyn RemoteService> {
        self.remote.clone()
    }

    /// Execute an upload command's plan with progress reporting and cancellation.
    pub async fn run_upload(
        &self,
        command: &UploadObjectsCommand,
        progress: &dyn rstore_transfer::ProgressSink,
        cancel: &rstore_transfer::CancellationToken,
    ) -> Result<crate::transfer::TransferReport> {
        self.transfers
            .run_upload(&command.plan, &command.options, progress, cancel)
            .await
    }

    /// Execute a download command's plan with progress reporting and cancellation.
    pub async fn run_download(
        &self,
        command: &DownloadObjectsCommand,
        progress: &dyn rstore_transfer::ProgressSink,
        cancel: &rstore_transfer::CancellationToken,
    ) -> Result<crate::transfer::TransferReport> {
        self.transfers
            .run_download(&command.plan, &command.options, progress, cancel)
            .await
    }

    /// Execute several download plans as one batch run.
    pub async fn run_batch_download(
        &self,
        plans: &[crate::transfer::DownloadPlan],
        options: &crate::transfer::RunOptions,
        progress: &dyn rstore_transfer::ProgressSink,
        cancel: &rstore_transfer::CancellationToken,
    ) -> Result<crate::transfer::BatchDownloadReport> {
        self.transfers
            .run_batch_download(plans, options, progress, cancel)
            .await
    }

    /// Execute a server-side copy command.
    pub async fn run_copy(
        &self,
        command: &CopyObjectsCommand,
        cancel: &rstore_transfer::CancellationToken,
        progress: &dyn rstore_transfer::ProgressSink,
    ) -> Result<crate::transfer::TransferReport> {
        self.copies
            .run_copy(&command.pairs, &command.options, cancel, progress)
            .await
    }

    /// Execute a move command (copy followed by source deletion).
    pub async fn run_move(
        &self,
        command: &CopyObjectsCommand,
        cancel: &rstore_transfer::CancellationToken,
        progress: &dyn rstore_transfer::ProgressSink,
    ) -> Result<crate::transfer::TransferReport> {
        self.copies
            .run_move(&command.pairs, &command.options, cancel, progress)
            .await
    }
}

/// Link generation operations.
#[derive(Clone)]
pub struct AccessApi {
    links: Arc<DefaultLinkService>,
}

impl AccessApi {
    /// Read the effective link profile for a bucket.
    pub async fn profile_for(
        &self,
        bucket: &crate::bucket::BucketRef,
    ) -> Result<rstore_domain::LinkProfile> {
        self.links.profile_for(bucket).await
    }

    /// Update and persist a bucket's link profile.
    pub async fn set_profile(
        &self,
        bucket: &crate::bucket::BucketRef,
        input: LinkProfileInput,
    ) -> Result<rstore_domain::LinkProfile> {
        self.links.set_profile(bucket, input).await
    }

    /// Turn remote references into paths, URIs, URLs, and documents.
    pub async fn generate(
        &self,
        uris: &[String],
        request: &LinkRequest,
    ) -> crate::batch_outcome::BatchOutcome<GeneratedLink> {
        self.links.generate(uris, request).await
    }
}

/// Connection probes, health snapshot, and adapter identity.
#[derive(Clone)]
pub struct DiagnosticsApi {
    diagnostics: Arc<ConnectionDiagnostics>,
    health: Arc<HealthService>,
    storage_adapter: Option<&'static str>,
}

impl DiagnosticsApi {
    /// Whole-catalog doctor report.
    pub async fn health(&self, db_path: &Path) -> HealthReport {
        self.health.health(db_path, self.storage_adapter).await
    }

    /// Registered storage adapter name, or `None` when no factory was injected.
    pub fn storage_adapter(&self) -> Option<&'static str> {
        self.storage_adapter
    }

    /// Probe and report the connection's capability set (cached when fresh).
    pub async fn capabilities(
        &self,
        reference: &crate::connection::ConnectionRef,
    ) -> Result<crate::diagnostics::CapabilityReport> {
        self.diagnostics.capabilities(reference).await
    }

    /// Run the connection test, optionally including a write probe.
    pub async fn test(
        &self,
        reference: &crate::connection::ConnectionRef,
        write_test: bool,
    ) -> Result<crate::diagnostics::ConnectionTestReport> {
        self.diagnostics.test(reference, write_test).await
    }
}

/// Job inspection and control plus the bounded repair log view.
#[derive(Clone)]
pub struct JobApi {
    jobs: Arc<dyn JobService>,
    repairs: Arc<dyn RepairStore>,
}

impl JobApi {
    /// All jobs, newest first.
    pub async fn list(&self) -> Result<Vec<rstore_domain::Job>> {
        self.jobs.list().await
    }

    /// Resolve a job by id or list ordinal.
    pub async fn resolve(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.resolve(reference).await
    }

    /// Pause a running job.
    pub async fn pause(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.pause(reference).await
    }

    /// Resume a paused or interrupted job.
    pub async fn resume(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.resume(reference).await
    }

    /// Re-run a failed or partially successful job.
    pub async fn retry(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.retry(reference).await
    }

    /// Cancel a job in any non-terminal state.
    pub async fn cancel(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.cancel(reference).await
    }

    /// Surface pending repair items, capped at a fixed batch limit.
    ///
    /// The cap keeps `task list` from streaming an unbounded repair log; the full log stays
    /// queryable through `doctor`/repair tooling.
    pub async fn pending_repairs(&self) -> Result<Vec<rstore_application_ports::RepairItem>> {
        self.repairs.list(PENDING_REPAIRS_LIMIT).await
    }
}

/// Registry import/export operations.
#[derive(Clone)]
pub struct ConfigApi {
    config_io: Arc<DefaultConfigTransferService>,
}

impl ConfigApi {
    /// Export the registry as a portable bundle.
    pub async fn export(&self) -> Result<ConfigBundle> {
        self.config_io.export().await
    }

    /// Import a portable registry bundle, optionally as a dry run.
    pub async fn import(&self, bundle: &ConfigBundle, dry_run: bool) -> Result<ImportReport> {
        self.config_io.import(bundle, dry_run).await
    }
}
