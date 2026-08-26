//! Composition root for the application layer.
//!
//! Bootstrap supplies infrastructure-neutral ports, clocks, and the storage factory; this module
//! wires them into the application services. The concrete S3 adapter is selected outside this
//! crate. Wiring happens exactly once when the raw dependencies are set — the `with_*` builder
//! methods re-wire on their way through construction — while accessors hand out cached handles
//! and never re-assemble the dependency graph.

use std::sync::Arc;

use rstore_catalog::{CatalogQuery, RemoteObjectRepository};

use rstore_domain::{Clock, Result};
use rstore_storage_api::StorageFactory;

use crate::bucket::DefaultBucketService;
use crate::connection::DefaultConnectionService;
use crate::gateway::RemoteGateway;
use crate::ports::ApplicationDependencies;

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

    /// Attach the storage adapter factory (the binary injects the S3 one) and re-wire.
    pub fn with_storage_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.dependencies.replace_storage_factory(factory);
        self.rewire()
    }

    /// Replace the clock (tests) and re-wire.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.dependencies.replace_clock(clock);
        self.rewire()
    }

    /// Point the catalog seam at a second adapter (tests, future backends) and re-wire.
    ///
    /// The locator, remote, indexer, transfer, and copy services receive its
    /// `RemoteObjectRepository` side, `files` its `CatalogQuery` side. The adapter must satisfy
    /// the shared `catalog_contract`.
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
        // The outbox consumer and dispatcher ride the same wiring — one
        // gateway serves the whole graph, and no layer re-transcribes the
        // dependency list to build them.
        let verifier = Arc::new(crate::verify::RemoteVerifyHandler::new(
            gateway.clone(),
            dependencies.registry.buckets.clone(),
            dependencies.runtime.repairs.clone(),
            dependencies.runtime.clock.clone(),
        ));
        let dispatcher = crate::outbox::OutboxDispatcher::new(dependencies.runtime.outbox.clone());
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

    /// Deliver every due outbox fact to the remote-verify consumer, looping
    /// until the backlog is gone or a safety cap of 16 cycles is hit (each
    /// cycle claims up to one batch). The startup drain runs under the
    /// process write lock; a long-lived host would schedule it instead.
    pub async fn drain_outbox(&self) -> Result<crate::outbox::DispatchReport> {
        let mut total = crate::outbox::DispatchReport {
            claimed: 0,
            published: 0,
            retried: 0,
            repair_required: 0,
        };
        for _ in 0..16 {
            let report = self
                .dispatcher
                .dispatch_once(
                    self.dependencies.runtime.clock.now(),
                    self.verifier.as_ref(),
                )
                .await?;
            total.claimed += report.claimed;
            total.published += report.published;
            total.retried += report.retried;
            total.repair_required += report.repair_required;
            if report.claimed == 0 {
                break;
            }
        }
        Ok(total)
    }

    /// Re-run [`Self::wire`] with the current raw dependencies.
    ///
    /// Construction path only (`with_*` builder calls).
    fn rewire(self) -> Self {
        Self::wire(self.dependencies)
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

#[cfg(test)]
mod tests {
    use rstore_storage_api::Storage as _;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rstore-app-drain-{}-{name}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Drives the drain through a real settlement: remove an object, then
    /// let the wired consumer verify the fact. A delete's expected provider
    /// state is absence, so a clean removal acknowledges without repairs.
    #[tokio::test]
    async fn drain_acknowledges_settled_deletes_through_the_wired_consumer() {
        let (app, factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;
        factory.storage().seed("media", "gone.txt", b"x", None);

        let report = app
            .api()
            .transfer()
            .remote()
            .remove(
                &["nas/media/gone.txt".into()],
                false,
                false,
                &rstore_transfer::CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            report.outcome.severity(),
            crate::batch_outcome::Severity::AllOk,
            "object removed"
        );

        let drained = app.drain_outbox().await.unwrap();
        assert_eq!(drained.claimed, 1);
        assert_eq!(drained.published, 1);
        assert!(
            app.api().jobs().pending_repairs().await.unwrap().is_empty(),
            "a verified delete settles without a repair"
        );
    }

    /// The same loop catches divergence: an uploaded object that vanished
    /// before the drain becomes one repair row, keyed by the event.
    #[tokio::test]
    async fn drain_records_a_repair_for_a_lost_upload() {
        let dir = temp_dir("lost-upload");
        std::fs::write(dir.join("a.txt"), b"payload").unwrap();
        let (app, factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;

        let command = crate::commands::UploadObjectsCommand {
            plan: crate::transfer_plan::UploadPlan {
                connection: crate::connection::ConnectionRef::parse("nas"),
                bucket: "media".into(),
                prefix: String::new(),
                sources: vec![dir.join("a.txt")],
                conflict: rstore_domain::policy::ConflictPolicy::Overwrite,
                recursive: false,
            },
            options: Default::default(),
        };
        let report = app
            .api()
            .transfer()
            .run_upload(
                &command,
                &rstore_transfer::NullProgressSink,
                &rstore_transfer::CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1, "object uploaded and settled");

        // The provider loses the object between settlement and the drain.
        factory
            .storage()
            .delete(
                "media",
                &rstore_domain::remote::ObjectPath::new("a.txt").unwrap(),
            )
            .await
            .unwrap();

        let drained = app.drain_outbox().await.unwrap();
        assert_eq!(drained.claimed, 1);
        let repairs = app.api().jobs().pending_repairs().await.unwrap();
        assert_eq!(repairs.len(), 1);
        assert!(repairs[0]
            .last_error
            .as_deref()
            .unwrap()
            .contains("missing after settlement"));
    }
}
