//! Composition root for the application layer (CORE-003).
//!
//! Bootstrap supplies infrastructure-neutral ports, clocks and the storage
//! factory; this module wires them into the Application Core services.
//! The concrete S3 adapter is selected outside this crate.
//!
//! Wiring happens exactly once, when the raw dependencies are set (the
//! `with_*` builder methods re-wire on their way through construction).
//! Accessors hand out the cached handles and never re-assemble the dependency
//! graph; callers reach services through them, not through the raw database /
//! clock dependencies.

use std::sync::Arc;

use rstore_catalog::{CatalogQuery, RemoteObjectRepository};

use rstore_domain::{Clock, Result};
use rstore_storage_api::StorageFactory;

use crate::bucket::DefaultBucketService;
use crate::connection::DefaultConnectionService;
use crate::gateway::RemoteGateway;
use crate::ports::ApplicationDependencies;

/// The wired application. Services are constructed once and shared; accessors
/// clone the `Arc` handle, so the dependency graph is never re-assembled.
pub struct Application {
    dependencies: ApplicationDependencies,
    services: crate::api::Services,
}

impl Application {
    /// Construct the application core from infrastructure-neutral ports.
    ///
    /// Bootstrap owns the concrete repositories and storage factory. This
    /// constructor only assembles use-case services and therefore remains
    /// usable by both the CLI and the future desktop runtime.
    pub fn from_dependencies(dependencies: ApplicationDependencies) -> Self {
        Self::wire(dependencies)
    }

    /// Complete startup recovery after the composition root has built the
    /// application. Keeping this separate makes construction deterministic
    /// while retaining the existing interrupted-job recovery rule.
    pub async fn initialize(self) -> Result<Self> {
        self.services.jobs.mark_stale_interrupted().await?;
        Ok(self)
    }

    /// Attach the storage adapter factory (the binary injects the S3 one) and
    /// re-wire.
    pub fn with_storage_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.dependencies.replace_storage_factory(factory);
        self.rewire()
    }

    /// Replace the clock (tests) and re-wire.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.dependencies.replace_clock(clock);
        self.rewire()
    }

    /// Point the catalog seam at a second adapter (tests, future backends)
    /// and re-wire: the locator, remote, indexer, transfer and copy services
    /// receive its `RemoteObjectRepository` side, `files` its `CatalogQuery`
    /// side. The adapter must satisfy the shared `catalog_contract`.
    pub fn with_catalog<C>(mut self, catalog: Arc<C>) -> Self
    where
        C: RemoteObjectRepository + CatalogQuery + 'static,
    {
        self.dependencies.replace_catalog(catalog);
        self.rewire()
    }

    /// The one place the dependency graph is assembled.
    fn wire(dependencies: ApplicationDependencies) -> Self {
        let gateway = RemoteGateway::new(
            dependencies.registry.connections.clone(),
            dependencies.runtime.storage_factory.clone(),
        );
        let buckets: Arc<dyn crate::bucket::BucketService> = Arc::new(DefaultBucketService::new(
            dependencies.registry.buckets.clone(),
            gateway.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let catalog = dependencies.catalog.catalog.clone();
        let files_catalog = dependencies.catalog.catalog_query.clone();
        let mutations = Arc::new(crate::mutation::MutationCoordinator::new(
            dependencies.runtime.unit_of_work.clone(),
            dependencies.runtime.repairs.clone(),
            catalog.clone(),
            dependencies.catalog.catalog_tags.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let locator = crate::locator::CatalogLocator::new(
            dependencies.registry.connections.clone(),
            dependencies.registry.buckets.clone(),
            dependencies.registry.workspaces.clone(),
            catalog.clone(),
            gateway.clone(),
        );
        let connections: Arc<dyn crate::connection::ConnectionService> =
            Arc::new(DefaultConnectionService::new(
                dependencies.registry.connections.clone(),
                dependencies.runtime.clock.clone(),
            ));
        let diagnostics = Arc::new(crate::diagnostics::ConnectionDiagnostics::new(
            dependencies.registry.connections.clone(),
            gateway.clone(),
            dependencies.registry.settings.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let job_repo = dependencies.transfer.jobs.clone();
        let remote = Arc::new(crate::remote::DefaultRemoteService::new(
            buckets.clone(),
            locator.clone(),
            catalog.clone(),
            job_repo.clone(),
            dependencies.runtime.clock.clone(),
            mutations.clone(),
        ));
        let jobs: Arc<dyn crate::job::JobService> = Arc::new(crate::job::DefaultJobService::new(
            job_repo.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let transfers = Arc::new(crate::transfer::DefaultTransferService::new(
            locator.clone(),
            buckets.clone(),
            job_repo.clone(),
            dependencies.transfer.multipart.clone(),
            dependencies.runtime.clock.clone(),
            mutations.clone(),
        ));
        let copies = Arc::new(crate::copy::DefaultCopyService::new(
            locator.clone(),
            buckets.clone(),
            job_repo.clone(),
            dependencies.runtime.clock.clone(),
            mutations,
        ));
        let workspaces = Arc::new(crate::workspace::DefaultWorkspaceService::new(
            dependencies.registry.workspaces.clone(),
            buckets.clone(),
            dependencies.registry.ignore_rules.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let tags = Arc::new(crate::tag::DefaultTagService::new(
            dependencies.registry.tags.clone(),
            dependencies.registry.workspaces.clone(),
            locator.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let indexer = Arc::new(crate::indexer::DefaultIndexService::new(
            locator.clone(),
            buckets.clone(),
            catalog.clone(),
            dependencies.registry.ignore_rules.clone(),
            dependencies.catalog.index_runs.clone(),
            dependencies.registry.settings.clone(),
            dependencies.runtime.stats.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let files = Arc::new(crate::files::DefaultFilesService::new(
            files_catalog,
            dependencies.registry.workspaces.clone(),
            dependencies.registry.tags.clone(),
            dependencies.registry.connections.clone(),
            dependencies.registry.buckets.clone(),
            locator.clone(),
        ));
        let links = Arc::new(crate::link::DefaultLinkService::new(
            gateway.clone(),
            buckets.clone(),
            locator.clone(),
            dependencies.registry.links.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let config_io = Arc::new(crate::config_bundle::DefaultConfigTransferService::new(
            connections.clone(),
            buckets.clone(),
        ));
        let health = Arc::new(crate::health::HealthService::new(
            dependencies.runtime.stats.clone(),
            dependencies.registry.connections.clone(),
            job_repo,
        ));
        let repairs = dependencies.runtime.repairs.clone();
        Self {
            dependencies,
            services: crate::api::Services {
                connections,
                diagnostics,
                buckets,
                remote,
                jobs,
                transfers,
                copies,
                links,
                config_io,
                workspaces,
                tags,
                indexer,
                files,
                health,
                repairs,
            },
        }
    }

    /// Re-run [`Self::wire`] with the current raw dependencies (construction
    /// path only — `with_*` builder calls).
    fn rewire(self) -> Self {
        Self::wire(self.dependencies)
    }

    /// Current time per the wired clock — for output envelopes (`CLI-004`).
    /// The clock itself stays inside the composition root.
    pub fn now(&self) -> time::OffsetDateTime {
        self.dependencies.runtime.clock.now()
    }

    /// Stable context-oriented application surface for CLI and desktop.
    pub fn api(&self) -> crate::api::ApplicationApi {
        crate::api::ApplicationApi::new(
            self.services.clone(),
            self.dependencies
                .runtime
                .storage_factory
                .as_ref()
                .map(|f| f.name()),
        )
    }

    #[cfg(test)]
    pub(crate) fn transfer_service(&self) -> Arc<crate::transfer::DefaultTransferService> {
        self.services.transfers.clone()
    }

    #[cfg(test)]
    pub(crate) fn copy_service(&self) -> Arc<crate::copy::DefaultCopyService> {
        self.services.copies.clone()
    }

    #[cfg(test)]
    pub(crate) fn link_service(&self) -> Arc<crate::link::DefaultLinkService> {
        self.services.links.clone()
    }
}
