//! Stable application-facing API groups.
//!
//! The existing service implementations remain internal composition details;
//! this module gives CLI and desktop callers the context-oriented API surface.

use std::sync::Arc;

use crate::bucket::BucketService;
use crate::commands::{CopyObjectsCommand, DownloadObjectsCommand, UploadObjectsCommand};
use crate::config_bundle::DefaultConfigTransferService;
use crate::connection::ConnectionService;
use crate::copy::DefaultCopyService;
use crate::diagnostics::ConnectionDiagnostics;
use crate::files::FilesService;
use crate::health::HealthService;
use crate::indexer::IndexService;
use crate::job::JobService;
use crate::link::DefaultLinkService;
use crate::remote::RemoteService;
use crate::tag::TagService;
use crate::transfer::DefaultTransferService;
use crate::workspace::WorkspaceService;
use rstore_application_ports::RepairRepository;
use rstore_domain::Result;

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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        connections: Arc<dyn ConnectionService>,
        buckets: Arc<dyn BucketService>,
        workspaces: Arc<dyn WorkspaceService>,
        tags: Arc<dyn TagService>,
        indexer: Arc<dyn IndexService>,
        files: Arc<dyn FilesService>,
        remote: Arc<dyn RemoteService>,
        transfers: Arc<DefaultTransferService>,
        copies: Arc<DefaultCopyService>,
        links: Arc<DefaultLinkService>,
        diagnostics: Arc<ConnectionDiagnostics>,
        health: Arc<HealthService>,
        jobs: Arc<dyn JobService>,
        config_io: Arc<DefaultConfigTransferService>,
        storage_adapter: Option<&'static str>,
        repairs: Option<Arc<dyn RepairRepository>>,
    ) -> Self {
        Self {
            registry: RegistryApi {
                connections,
                buckets: buckets.clone(),
            },
            catalog: CatalogApi {
                workspaces,
                tags,
                indexer,
                files,
            },
            transfer: TransferApi {
                buckets: buckets.clone(),
                remote,
                transfers,
                copies,
            },
            access: AccessApi { links },
            diagnostics: DiagnosticsApi {
                diagnostics,
                health,
                storage_adapter,
            },
            jobs: JobApi { jobs, repairs },
            config: ConfigApi { config_io },
        }
    }

    pub fn registry(&self) -> RegistryApi {
        self.registry.clone()
    }

    pub fn catalog(&self) -> CatalogApi {
        self.catalog.clone()
    }

    pub fn transfer(&self) -> TransferApi {
        self.transfer.clone()
    }

    pub fn access(&self) -> AccessApi {
        self.access.clone()
    }

    pub fn diagnostics(&self) -> DiagnosticsApi {
        self.diagnostics.clone()
    }

    pub fn jobs(&self) -> JobApi {
        self.jobs.clone()
    }

    pub fn config(&self) -> ConfigApi {
        self.config.clone()
    }
}

#[derive(Clone)]
pub struct RegistryApi {
    connections: Arc<dyn ConnectionService>,
    buckets: Arc<dyn BucketService>,
}

impl RegistryApi {
    pub fn connections(&self) -> Arc<dyn ConnectionService> {
        self.connections.clone()
    }

    pub fn buckets(&self) -> Arc<dyn BucketService> {
        self.buckets.clone()
    }
}

#[derive(Clone)]
pub struct CatalogApi {
    workspaces: Arc<dyn WorkspaceService>,
    tags: Arc<dyn TagService>,
    indexer: Arc<dyn IndexService>,
    files: Arc<dyn FilesService>,
}

impl CatalogApi {
    pub fn workspaces(&self) -> Arc<dyn WorkspaceService> {
        self.workspaces.clone()
    }

    pub fn tags(&self) -> Arc<dyn TagService> {
        self.tags.clone()
    }

    pub fn indexer(&self) -> Arc<dyn IndexService> {
        self.indexer.clone()
    }

    pub fn files(&self) -> Arc<dyn FilesService> {
        self.files.clone()
    }
}

#[derive(Clone)]
pub struct TransferApi {
    buckets: Arc<dyn BucketService>,
    remote: Arc<dyn RemoteService>,
    transfers: Arc<DefaultTransferService>,
    copies: Arc<DefaultCopyService>,
}

impl TransferApi {
    /// Resolve the non-interactive part of the conflict-policy chain. The
    /// CLI/Desktop may still decide how to handle `Ask` (prompt, `--yes`, or
    /// a blocked item), but the source precedence belongs to the application.
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

    pub fn remote(&self) -> Arc<dyn RemoteService> {
        self.remote.clone()
    }

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

#[derive(Clone)]
pub struct AccessApi {
    links: Arc<DefaultLinkService>,
}

impl AccessApi {
    pub fn links(&self) -> Arc<DefaultLinkService> {
        self.links.clone()
    }
}

#[derive(Clone)]
pub struct DiagnosticsApi {
    diagnostics: Arc<ConnectionDiagnostics>,
    health: Arc<HealthService>,
    storage_adapter: Option<&'static str>,
}

impl DiagnosticsApi {
    pub fn connections(&self) -> Arc<ConnectionDiagnostics> {
        self.diagnostics.clone()
    }

    pub fn health(&self) -> Arc<HealthService> {
        self.health.clone()
    }

    pub fn storage_adapter(&self) -> Option<&'static str> {
        self.storage_adapter
    }

    pub async fn secret_ref_resolvable(&self, secret_ref: &str) -> bool {
        self.diagnostics.secret_ref_resolvable(secret_ref).await
    }

    pub async fn capabilities(
        &self,
        reference: &crate::connection::ConnectionRef,
    ) -> Result<crate::diagnostics::CapabilityReport> {
        self.diagnostics.capabilities(reference).await
    }

    pub async fn test(
        &self,
        reference: &crate::connection::ConnectionRef,
        write_test: bool,
    ) -> Result<crate::diagnostics::ConnectionTestReport> {
        self.diagnostics.test(reference, write_test).await
    }
}

#[derive(Clone)]
pub struct JobApi {
    jobs: Arc<dyn JobService>,
    repairs: Option<Arc<dyn RepairRepository>>,
}

impl JobApi {
    pub fn jobs(&self) -> Arc<dyn JobService> {
        self.jobs.clone()
    }

    pub async fn list(&self) -> Result<Vec<rstore_domain::Job>> {
        self.jobs.list().await
    }

    pub async fn resolve(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.resolve(reference).await
    }

    pub async fn pause(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.pause(reference).await
    }

    pub async fn resume(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.resume(reference).await
    }

    pub async fn retry(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.retry(reference).await
    }

    pub async fn cancel(&self, reference: &crate::job::JobRef) -> Result<rstore_domain::Job> {
        self.jobs.cancel(reference).await
    }

    pub async fn pending_repairs(&self) -> Result<Vec<rstore_application_ports::RepairItem>> {
        match &self.repairs {
            Some(repairs) => repairs.list(100).await,
            None => Ok(Vec::new()),
        }
    }
}

#[derive(Clone)]
pub struct ConfigApi {
    config_io: Arc<DefaultConfigTransferService>,
}

impl ConfigApi {
    pub fn transfer(&self) -> Arc<DefaultConfigTransferService> {
        self.config_io.clone()
    }
}
