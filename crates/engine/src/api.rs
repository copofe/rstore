//! Stable context-oriented API groups for CLI and desktop callers.
//!
//! The service implementations remain internal composition details; this module is the
//! application surface. Every group is a view over the one wired [`Services`] bundle. One
//! exposure rule per group: a group either hands out service handles (accessor style, when it
//! is pure grouping) or owns operations (method style, when the group carries a rule — the
//! conflict-policy precedence and run orchestration on [`TransferApi`]). Never mix both for the
//! same service.

use std::sync::Arc;

use crate::bucket::BucketService;
use crate::commands::{
    BatchDownloadCommand, CopyObjectsCommand, DownloadObjectsCommand, RunControl,
    UploadObjectsCommand,
};
use crate::config_bundle::{ConfigBundle, ConfigTransferService, ImportReport};
use crate::connection::ConnectionService;
use crate::copy::CopyService;
use crate::diagnostics::ConnectionDiagnostics;
use crate::events::EventBus;
use crate::files::FilesService;
use crate::health::{HealthReport, HealthService};
use crate::indexer::IndexService;
use crate::job::JobService;
use crate::link::{GeneratedLink, LinkProfileInput, LinkRequest, LinkService};
use crate::onboarding::Onboarding;
use crate::reference::References;
use crate::remote::RemoteService;
use crate::tag::TagService;
use crate::transfer_service::TransferService;
use crate::workspace::WorkspaceService;
use rstore_domain::Result;

/// Every wired service handle in one bundle.
///
/// Adding a service touches exactly three places: a field here, its construction in
/// `Application::new`, and the accessor of the group that exposes it.
pub(crate) struct Services {
    pub(crate) connections: Arc<ConnectionService>,
    pub(crate) diagnostics: Arc<ConnectionDiagnostics>,
    pub(crate) buckets: Arc<BucketService>,
    pub(crate) references: Arc<References>,
    pub(crate) onboarding: Arc<Onboarding>,
    pub(crate) remote: Arc<RemoteService>,
    pub(crate) jobs: Arc<JobService>,
    pub(crate) transfers: Arc<TransferService>,
    pub(crate) copies: Arc<CopyService>,
    pub(crate) links: Arc<LinkService>,
    pub(crate) config_io: Arc<ConfigTransferService>,
    pub(crate) workspaces: Arc<WorkspaceService>,
    pub(crate) tags: Arc<TagService>,
    pub(crate) indexer: Arc<IndexService>,
    pub(crate) files: Arc<FilesService>,
    pub(crate) health: Arc<HealthService>,
}

/// The stable application API, organised into context-oriented groups.
///
/// Obtain it from [`crate::Application::api`]; it and its groups clone cheaply.
#[derive(Clone)]
pub struct ApplicationApi {
    services: Arc<Services>,
    events: EventBus,
}

impl ApplicationApi {
    pub(crate) fn new(services: Arc<Services>, events: EventBus) -> Self {
        Self { services, events }
    }

    /// The engine's event stream: job lifecycle and progress, catalog and tag
    /// changes. Best-effort runtime messages; re-query the API for state.
    pub fn events(&self) -> &EventBus {
        &self.events
    }

    /// Connections, buckets, and the reference grammar — the registry contexts.
    pub fn registry(&self) -> RegistryApi {
        RegistryApi(self.services.clone())
    }

    /// Workspaces, tags, index, and file listing — the catalog contexts.
    pub fn catalog(&self) -> CatalogApi {
        CatalogApi(self.services.clone())
    }

    /// Uploads, downloads, copies, and remote discovery — transfer orchestration.
    pub fn transfer(&self) -> TransferApi {
        TransferApi(self.services.clone())
    }

    /// Link generation for remote objects.
    pub fn access(&self) -> AccessApi {
        AccessApi(self.services.clone())
    }

    /// Connection probes and whole-store health.
    pub fn diagnostics(&self) -> DiagnosticsApi {
        DiagnosticsApi(self.services.clone())
    }

    /// Job inspection and control.
    pub fn jobs(&self) -> JobApi {
        JobApi(self.services.clone())
    }

    /// Registry import/export.
    pub fn config(&self) -> ConfigApi {
        ConfigApi(self.services.clone())
    }
}

/// Connection and bucket lifecycle handles plus the reference grammar (accessor style).
#[derive(Clone)]
pub struct RegistryApi(Arc<Services>);

impl RegistryApi {
    /// Connection CRUD and resolution.
    pub fn connections(&self) -> Arc<ConnectionService> {
        self.0.connections.clone()
    }

    /// Bucket registration, guards, and resolution.
    pub fn buckets(&self) -> Arc<BucketService> {
        self.0.buckets.clone()
    }

    /// Short-reference expansion and transfer routing against the registry.
    pub fn references(&self) -> Arc<References> {
        self.0.references.clone()
    }

    /// Bucket registration that also enrolls into a workspace.
    pub fn onboarding(&self) -> Arc<Onboarding> {
        self.0.onboarding.clone()
    }
}

/// Workspace, tag, index, and mixed-file-list handles (accessor style).
#[derive(Clone)]
pub struct CatalogApi(Arc<Services>);

impl CatalogApi {
    /// Workspace lifecycle and membership.
    pub fn workspaces(&self) -> Arc<WorkspaceService> {
        self.0.workspaces.clone()
    }

    /// Tag lifecycle and associations.
    pub fn tags(&self) -> Arc<TagService> {
        self.0.tags.clone()
    }

    /// Index scans over registered buckets.
    pub fn indexer(&self) -> Arc<IndexService> {
        self.0.indexer.clone()
    }

    /// Mixed file list and search over the workspace catalog.
    pub fn files(&self) -> Arc<FilesService> {
        self.0.files.clone()
    }
}

/// Transfer orchestration (method style): this group owns rules, not just handles.
#[derive(Clone)]
pub struct TransferApi(Arc<Services>);

impl TransferApi {
    /// Resolve the non-interactive part of the conflict-policy chain.
    ///
    /// Precedence: an explicit policy wins; otherwise the target bucket's default; otherwise
    /// [`rstore_domain::ConflictPolicy::Ask`]. Callers may still decide how to handle `Ask`
    /// (prompt, an explicit flag, or a blocked item), but the source precedence belongs here.
    pub async fn conflict_policy(
        &self,
        explicit: Option<rstore_domain::ConflictPolicy>,
        target: Option<&crate::reference::RemoteUri>,
    ) -> Result<rstore_domain::ConflictPolicy> {
        if let Some(explicit) = explicit {
            return Ok(explicit);
        }
        let Some(target) = target else {
            return Ok(rstore_domain::ConflictPolicy::Ask);
        };
        let bucket = self
            .0
            .buckets
            .resolve(&crate::bucket::BucketRef::Scoped(
                target.connection.clone(),
                target.bucket.clone(),
            ))
            .await?;
        Ok(bucket.config.default_conflict)
    }

    /// Remote listing and object inspection.
    pub fn remote(&self) -> Arc<RemoteService> {
        self.0.remote.clone()
    }

    /// Execute an upload command's plan.
    pub async fn run_upload(
        &self,
        command: &UploadObjectsCommand,
        control: RunControl<'_>,
    ) -> Result<crate::transfer_service::TransferReport> {
        self.0
            .transfers
            .run_upload(
                &command.plan,
                &command.options,
                control.progress,
                control.cancel,
            )
            .await
    }

    /// Execute a download command's plan.
    pub async fn run_download(
        &self,
        command: &DownloadObjectsCommand,
        control: RunControl<'_>,
    ) -> Result<crate::transfer_service::TransferReport> {
        self.0
            .transfers
            .run_download(
                &command.plan,
                &command.options,
                control.progress,
                control.cancel,
            )
            .await
    }

    /// Execute several download plans as one batch run.
    pub async fn run_batch_download(
        &self,
        command: &BatchDownloadCommand,
        control: RunControl<'_>,
    ) -> Result<crate::transfer_service::BatchDownloadReport> {
        self.0
            .transfers
            .run_batch_download(
                &command.plans,
                &command.options,
                control.progress,
                control.cancel,
            )
            .await
    }

    /// Execute a remote copy command (server-side on one connection, relayed
    /// across connections).
    pub async fn run_copy(
        &self,
        command: &CopyObjectsCommand,
        control: RunControl<'_>,
    ) -> Result<crate::transfer_service::TransferReport> {
        self.0
            .copies
            .run_copy(
                &command.pairs,
                &command.options,
                control.cancel,
                control.progress,
            )
            .await
    }

    /// Execute a move command (copy, verify, then delete the source).
    pub async fn run_move(
        &self,
        command: &CopyObjectsCommand,
        control: RunControl<'_>,
    ) -> Result<crate::transfer_service::TransferReport> {
        self.0
            .copies
            .run_move(
                &command.pairs,
                &command.options,
                control.cancel,
                control.progress,
            )
            .await
    }
}

/// Link generation operations.
#[derive(Clone)]
pub struct AccessApi(Arc<Services>);

impl AccessApi {
    /// Read the effective link profile for a bucket.
    pub async fn profile_for(
        &self,
        bucket: &crate::bucket::BucketRef,
    ) -> Result<rstore_domain::LinkProfile> {
        self.0.links.profile_for(bucket).await
    }

    /// Update and persist a bucket's link profile.
    pub async fn set_profile(
        &self,
        bucket: &crate::bucket::BucketRef,
        input: LinkProfileInput,
    ) -> Result<rstore_domain::LinkProfile> {
        self.0.links.set_profile(bucket, input).await
    }

    /// Turn remote references into paths, URIs, URLs, and documents.
    pub async fn generate(
        &self,
        uris: &[crate::reference::RemoteUri],
        request: &LinkRequest,
    ) -> crate::batch_outcome::BatchOutcome<GeneratedLink> {
        self.0.links.generate(uris, request).await
    }
}

/// Connection probes and the health snapshot.
#[derive(Clone)]
pub struct DiagnosticsApi(Arc<Services>);

impl DiagnosticsApi {
    /// Whole-catalog doctor report.
    pub async fn health(&self) -> HealthReport {
        self.0.health.health().await
    }

    /// Probe and report the connection's capability set, live.
    pub async fn capabilities(
        &self,
        reference: &crate::connection::ConnectionRef,
    ) -> Result<crate::diagnostics::CapabilityReport> {
        self.0.diagnostics.capabilities(reference).await
    }

    /// Run the connection test, optionally including a write probe.
    pub async fn test(
        &self,
        reference: &crate::connection::ConnectionRef,
        write_test: bool,
    ) -> Result<crate::diagnostics::ConnectionTestReport> {
        self.0.diagnostics.test(reference, write_test).await
    }
}

/// Job inspection and control.
#[derive(Clone)]
pub struct JobApi(Arc<Services>);

impl JobApi {
    /// All jobs, newest first.
    pub async fn list(&self) -> Result<Vec<rstore_domain::Job>> {
        self.0.jobs.list().await
    }

    /// Resolve a job by id or list ordinal.
    pub async fn resolve(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.0.jobs.resolve(reference).await
    }

    /// Cancel a job in any non-terminal state, wherever it runs.
    pub async fn cancel(&self, reference: &crate::job::JobRef) -> Result<crate::job::CancelReport> {
        self.0.jobs.cancel(reference).await
    }
}

/// Registry import/export operations.
#[derive(Clone)]
pub struct ConfigApi(Arc<Services>);

impl ConfigApi {
    /// Export the registry as a portable bundle.
    pub async fn export(&self) -> Result<ConfigBundle> {
        self.0.config_io.export().await
    }

    /// Import a portable registry bundle, optionally as a dry run.
    pub async fn import(&self, bundle: &ConfigBundle, dry_run: bool) -> Result<ImportReport> {
        self.0.config_io.import(bundle, dry_run).await
    }
}
