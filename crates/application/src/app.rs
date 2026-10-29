//! Composition root for the application layer.
//!
//! Bootstrap supplies infrastructure-neutral ports, clocks, and the storage factory; this module
//! wires them into the application services. The concrete S3 adapter is selected outside this
//! crate. Wiring happens exactly once when the raw dependencies are set — the `with_*` builder
//! methods re-wire on their way through construction — while accessors hand out cached handles
//! and never re-assemble the dependency graph.

use std::sync::Arc;

use rstore_application_ports::ApplicationDependencies;
use rstore_domain::Result;

use crate::bucket::DefaultBucketService;
use crate::connection::DefaultConnectionService;
use crate::gateway::RemoteGateway;

/// The wired application.
///
/// Services are constructed once and shared; accessors clone the `Arc` handle, so the dependency
/// graph is never re-assembled. Callers reach services through them, not through the raw database
/// or clock dependencies.
pub struct Application {
    dependencies: ApplicationDependencies,
    services: crate::api::Services,
    dispatcher: crate::outbox::OutboxDispatcher,
    verifier: Arc<crate::verify::RemoteVerifyHandler>,
}

impl Application {
    /// Construct the application core from infrastructure-neutral ports.
    ///
    /// Bootstrap owns the concrete repositories and storage factory. This constructor only
    /// assembles use-case services and therefore remains usable by both the CLI and the desktop
    /// runtime.
    pub fn from_dependencies(dependencies: ApplicationDependencies) -> Self {
        Self::wire(dependencies)
    }

    /// Complete startup recovery after the composition root has built the application.
    ///
    /// Keeping this separate makes construction deterministic while retaining the
    /// recovery rules: orphaned non-terminal jobs are marked interrupted, and
    /// outbox events stuck mid-processing return to the due queue.
    pub async fn initialize(self) -> Result<Self> {
        self.services.jobs.mark_stale_interrupted().await?;
        self.dispatcher
            .recover_processing(self.dependencies.runtime.clock.now())
            .await?;
        Ok(self)
    }

    /// The one place the dependency graph is assembled.
    fn wire(dependencies: ApplicationDependencies) -> Self {
        let gateway = RemoteGateway::new(
            dependencies.registry.connections.clone(),
            dependencies.runtime.storage_factory.clone(),
        );
        // The outbox consumer and dispatcher ride the same wiring — one
        // gateway serves the whole graph, and no layer re-transcribes the
        // dependency list to build them.
        let verifier = Arc::new(crate::verify::RemoteVerifyHandler::new(
            gateway.clone(),
            dependencies.registry.buckets.clone(),
            dependencies.runtime.repairs.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let dispatcher = crate::outbox::OutboxDispatcher::new(
            dependencies.runtime.outbox.clone(),
            dependencies.runtime.repairs.clone(),
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
        let references = Arc::new(crate::reference::References::new(
            connections.clone(),
            buckets.clone(),
        ));
        let diagnostics = Arc::new(crate::diagnostics::ConnectionDiagnostics::new(
            dependencies.registry.connections.clone(),
            gateway.clone(),
            dependencies.registry.settings.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let job_repo = dependencies.transfer.jobs.clone();
        let remote = Arc::new(crate::remote::DefaultRemoteService::new(
            gateway.clone(),
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
            gateway.clone(),
            locator.clone(),
            job_repo.clone(),
            dependencies.transfer.multipart.clone(),
            dependencies.runtime.clock.clone(),
            mutations.clone(),
        ));
        let copies = Arc::new(crate::copy::DefaultCopyService::new(
            gateway.clone(),
            locator.clone(),
            job_repo.clone(),
            dependencies.runtime.clock.clone(),
            crate::settlement::Settlement::new(
                mutations.clone(),
                catalog.clone(),
                dependencies.catalog.catalog_tags.clone(),
                dependencies.runtime.clock.clone(),
            ),
        ));
        let workspaces = Arc::new(crate::workspace::DefaultWorkspaceService::new(
            dependencies.registry.workspaces.clone(),
            buckets.clone(),
            dependencies.registry.ignore_rules.clone(),
            dependencies.registry.settings.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let onboarding = Arc::new(crate::onboarding::Onboarding::new(
            connections.clone(),
            buckets.clone(),
            workspaces.clone(),
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
            dependencies.registry.workspaces.clone(),
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
        ));
        let repairs = dependencies.runtime.repairs.clone();
        Self {
            dependencies,
            dispatcher,
            verifier,
            services: crate::api::Services {
                connections,
                diagnostics,
                buckets,
                references,
                onboarding,
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

    /// Deliver due outbox facts to the remote-verify consumer until the
    /// backlog is gone, a safety cap of 16 cycles is hit (each cycle claims up
    /// to one batch), or `budget` is spent — whichever comes first. A fact
    /// still being verified at the deadline counts as a failed attempt; facts
    /// not yet started go back to the due queue untouched.
    /// Runs under the process write lock; the entry point decides when.
    pub async fn drain_outbox(
        &self,
        budget: std::time::Duration,
    ) -> Result<crate::outbox::DispatchReport> {
        let deadline = tokio::time::Instant::now() + budget;
        let mut total = crate::outbox::DispatchReport::default();
        for _ in 0..16 {
            let report = self
                .dispatcher
                .dispatch_once(
                    self.dependencies.runtime.clock.now(),
                    self.verifier.as_ref(),
                    Some(deadline),
                )
                .await?;
            total.claimed += report.claimed;
            total.published += report.published;
            total.retried += report.retried;
            total.deferred += report.deferred;
            // A snapshot of pending repairs, not a per-cycle delta.
            total.repair_required = report.repair_required;
            if report.claimed == 0 || report.deferred > 0 || tokio::time::Instant::now() >= deadline
            {
                break;
            }
        }
        Ok(total)
    }

    /// Current time per the wired clock, for output envelopes.
    ///
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
}
