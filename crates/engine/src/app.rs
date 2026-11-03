//! The wired engine: one place assembles every service over the opened store.
//!
//! Wiring happens exactly once per [`Application`]; accessors hand out cached
//! handles and never re-assemble the graph. Construction stays deterministic;
//! startup recovery is the separate [`Application::initialize`] step so a
//! caller can construct without recovering.

use std::sync::Arc;

use rstore_domain::{Clock, Result};
use rstore_storage::StorageFactory;

use crate::api::Services;
use crate::db::Database;
use crate::events::EventBus;
use crate::gateway::RemoteGateway;
use crate::job_run::{JobEnv, LiveJobs};
use crate::write_through::WriteThrough;

/// The wired application.
pub struct Application {
    clock: Arc<dyn Clock>,
    bus: EventBus,
    services: Arc<Services>,
}

impl Application {
    /// Wire every service over the opened store and the storage factory.
    pub(crate) fn new(db: &Database, storage_factory: Arc<dyn StorageFactory>) -> Self {
        let clock = db.clock();
        let storage_adapter = storage_factory.name();
        let bus = EventBus::default();

        let connections_repo = Arc::new(db.connections());
        let buckets_repo = Arc::new(db.buckets());
        let workspaces_repo = Arc::new(db.workspaces());
        let ignore_rules = Arc::new(db.ignore_rules());
        let tags_repo = Arc::new(db.tags());
        let catalog = Arc::new(db.catalog());
        let stats = Arc::new(db.clone());

        let gateway = RemoteGateway::new(
            connections_repo.clone(),
            buckets_repo.clone(),
            storage_factory,
        );
        let env = JobEnv {
            jobs: Arc::new(db.jobs()),
            clock: clock.clone(),
            bus: bus.clone(),
            live: LiveJobs::default(),
        };
        let cache = WriteThrough::new(
            catalog.clone(),
            tags_repo.clone(),
            clock.clone(),
            bus.clone(),
        );
        let buckets: Arc<crate::bucket::BucketService> =
            Arc::new(crate::bucket::BucketService::new(
                buckets_repo.clone(),
                catalog.clone(),
                gateway.clone(),
                clock.clone(),
            ));
        let connections: Arc<crate::connection::ConnectionService> = Arc::new(
            crate::connection::ConnectionService::new(connections_repo.clone(), clock.clone()),
        );
        let references = Arc::new(crate::reference::References::new(
            connections.clone(),
            buckets.clone(),
        ));
        let diagnostics = Arc::new(crate::diagnostics::ConnectionDiagnostics::new(
            connections_repo.clone(),
            gateway.clone(),
            clock.clone(),
        ));
        let remote = Arc::new(crate::remote::RemoteService::new(
            gateway.clone(),
            env.clone(),
            cache.clone(),
        ));
        let jobs: Arc<crate::job::JobService> = Arc::new(crate::job::JobService::new(env.clone()));
        let transfers = Arc::new(crate::transfer_service::TransferService::new(
            gateway.clone(),
            env.clone(),
            Arc::new(db.multipart()),
            cache.clone(),
        ));
        let copies = Arc::new(crate::copy::CopyService::new(
            gateway.clone(),
            env.clone(),
            cache,
        ));
        let workspaces = Arc::new(crate::workspace::WorkspaceService::new(
            workspaces_repo.clone(),
            buckets.clone(),
            ignore_rules.clone(),
            Arc::new(db.settings()),
            clock.clone(),
        ));
        let onboarding = Arc::new(crate::onboarding::Onboarding::new(
            connections.clone(),
            buckets.clone(),
            workspaces.clone(),
        ));
        let tags = Arc::new(crate::tag::TagService::new(
            tags_repo.clone(),
            catalog.clone(),
            workspaces_repo.clone(),
            gateway.clone(),
            clock.clone(),
            bus.clone(),
        ));
        let indexer = Arc::new(crate::indexer::IndexService::new(
            gateway.clone(),
            buckets.clone(),
            workspaces_repo.clone(),
            catalog.clone(),
            ignore_rules,
            Arc::new(db.index_runs()),
            stats.clone(),
            clock.clone(),
            bus.clone(),
        ));
        let files = Arc::new(crate::files::FilesService::new(
            catalog,
            workspaces_repo,
            tags_repo,
            connections_repo,
            buckets_repo,
            gateway.clone(),
        ));
        let links = Arc::new(crate::link::LinkService::new(
            gateway,
            buckets.clone(),
            Arc::new(db.links()),
            clock.clone(),
        ));
        let config_io = Arc::new(crate::config_bundle::ConfigTransferService::new(
            connections.clone(),
            buckets.clone(),
        ));
        let health = Arc::new(crate::health::HealthService::new(stats, storage_adapter));
        Self {
            clock,
            bus,
            services: Arc::new(Services {
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
            }),
        }
    }

    /// Startup recovery: jobs whose owner stopped renewing their lease are
    /// marked interrupted. Jobs other processes are still running stay
    /// untouched.
    pub async fn initialize(self) -> Result<Self> {
        self.services.jobs.mark_stale_interrupted().await?;
        Ok(self)
    }

    /// Current time per the wired clock, for output envelopes.
    pub fn now(&self) -> time::OffsetDateTime {
        self.clock.now()
    }

    /// Stable context-oriented application surface for CLI and desktop.
    pub fn api(&self) -> crate::api::ApplicationApi {
        crate::api::ApplicationApi::new(self.services.clone(), self.bus.clone())
    }
}
