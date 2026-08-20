//! Application-owned dependency contracts used by the composition root.
//!
//! The aliases keep service constructors readable while the actual traits
//! live in `rstore-application-ports`. This module deliberately contains no
//! SQLite, SQLx, provider SDK, or UI type.

use std::sync::Arc;

use rstore_application_ports::{
    BucketStore, ConnectionStore, IgnoreRuleStore, IndexRunStore, LinkProfileStore, OutboxStore,
    RemoteMutationRepository, RepairRepository, RuntimeStats, SecretSourceStats, SettingsStore,
    TagStore, UnitOfWork, WorkspaceStore,
};
use rstore_catalog::{CatalogQuery, RemoteObjectRepository};
use rstore_domain::{Clock, JobRepository};
use rstore_secrets::SecretStore;
use rstore_storage_api::{MultipartStore, StorageFactory};

pub type ConnectionPort = Arc<dyn ConnectionStore>;
pub type BucketPort = Arc<dyn BucketStore>;
pub type WorkspacePort = Arc<dyn WorkspaceStore>;
pub type IgnoreRulePort = Arc<dyn IgnoreRuleStore>;
pub type IndexRunPort = Arc<dyn IndexRunStore>;
pub type SettingsPort = Arc<dyn SettingsStore>;
pub type LinkProfilePort = Arc<dyn LinkProfileStore>;
pub type TagPort = Arc<dyn TagStore>;
pub type CatalogTagPort = Arc<dyn rstore_catalog::TagStore>;
pub type StatsPort = Arc<dyn RuntimeStats>;
pub type OutboxPort = Arc<dyn OutboxStore>;
pub type UnitOfWorkPort = Arc<dyn UnitOfWork>;
pub type RepairPort = Arc<dyn RepairRepository>;
pub type MutationReceiptPort = Arc<dyn RemoteMutationRepository>;
pub type CatalogPort = Arc<dyn RemoteObjectRepository>;
pub type CatalogQueryPort = Arc<dyn CatalogQuery>;
pub type JobPort = Arc<dyn JobRepository>;
pub type MultipartPort = Arc<dyn MultipartStore>;
pub type SecretSourceStatsPort = Arc<dyn SecretSourceStats>;

/// Registry-facing inputs to the application graph.
pub struct RegistryDependencies {
    pub connections: ConnectionPort,
    pub buckets: BucketPort,
    pub workspaces: WorkspacePort,
    pub ignore_rules: IgnoreRulePort,
    pub settings: SettingsPort,
    pub links: LinkProfilePort,
    pub tags: TagPort,
}

/// Catalog-facing inputs to the application graph.
pub struct CatalogDependencies {
    pub catalog: CatalogPort,
    pub catalog_query: CatalogQueryPort,
    pub index_runs: IndexRunPort,
    pub catalog_tags: CatalogTagPort,
}

/// Transfer-facing inputs to the application graph.
pub struct TransferDependencies {
    pub jobs: JobPort,
    pub multipart: MultipartPort,
}

/// Process/runtime inputs and optional mutation durability ports.
pub struct RuntimeDependencies {
    pub stats: StatsPort,
    pub clock: Arc<dyn Clock>,
    pub secrets: Arc<dyn SecretStore>,
    pub secret_source_stats: SecretSourceStatsPort,
    pub storage_factory: Option<Arc<dyn StorageFactory>>,
    pub outbox: Option<OutboxPort>,
    pub unit_of_work: Option<UnitOfWorkPort>,
    pub repairs: Option<RepairPort>,
    pub mutation_receipts: Option<MutationReceiptPort>,
}

/// All dependencies required to construct the application core.
///
/// Bootstrap owns the concrete values and creates these groups. Keeping the
/// grouping here makes replacement seams explicit: callers provide
/// capabilities, never a database handle or a provider SDK.
pub struct ApplicationDependencies {
    pub registry: RegistryDependencies,
    pub catalog: CatalogDependencies,
    pub transfer: TransferDependencies,
    pub runtime: RuntimeDependencies,
}

impl ApplicationDependencies {
    pub fn new(
        registry: RegistryDependencies,
        catalog: CatalogDependencies,
        transfer: TransferDependencies,
        runtime: RuntimeDependencies,
    ) -> Self {
        Self {
            registry,
            catalog,
            transfer,
            runtime,
        }
    }

    pub fn replace_secrets(&mut self, secrets: Arc<dyn SecretStore>) {
        self.runtime.secrets = secrets;
    }

    pub fn replace_storage_factory(&mut self, factory: Arc<dyn StorageFactory>) {
        self.runtime.storage_factory = Some(factory);
    }

    pub fn replace_clock(&mut self, clock: Arc<dyn Clock>) {
        self.runtime.clock = clock;
    }

    pub fn replace_catalog<C>(&mut self, catalog: Arc<C>)
    where
        C: RemoteObjectRepository + CatalogQuery + 'static,
    {
        self.catalog.catalog = catalog.clone();
        self.catalog.catalog_query = catalog;
    }
}
