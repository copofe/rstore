//! Dependency-graph vocabulary for the application core.
//!
//! The port aliases and dependency-group structs live here — next to the
//! traits they name — so the persistence layer can offer a single assembly
//! point ([`assemble`](crate::assemble) over in `rstore-persistence`) without
//! depending on the application crate. This module contains no SQLite, SQLx,
//! provider SDK, or UI type.

use std::sync::Arc;

use rstore_catalog::{CatalogQuery, RemoteObjectRepository, TagStore as CatalogTagStore};
use rstore_domain::{Clock, JobRepository};
use rstore_storage_api::{MultipartStore, StorageFactory};

use crate::{
    BucketStore, ConnectionStore, IgnoreRuleStore, IndexRunStore, LinkProfileStore,
    RepairRepository, RuntimeStats, SettingsStore, TagStore, UnitOfWork, WorkspaceStore,
};

pub type ConnectionPort = Arc<dyn ConnectionStore>;
pub type BucketPort = Arc<dyn BucketStore>;
pub type WorkspacePort = Arc<dyn WorkspaceStore>;
pub type IgnoreRulePort = Arc<dyn IgnoreRuleStore>;
pub type IndexRunPort = Arc<dyn IndexRunStore>;
pub type SettingsPort = Arc<dyn SettingsStore>;
pub type LinkProfilePort = Arc<dyn LinkProfileStore>;
pub type TagPort = Arc<dyn TagStore>;
pub type CatalogTagPort = Arc<dyn CatalogTagStore>;
pub type StatsPort = Arc<dyn RuntimeStats>;
pub type UnitOfWorkPort = Arc<dyn UnitOfWork>;
pub type RepairPort = Arc<dyn RepairRepository>;
pub type CatalogPort = Arc<dyn RemoteObjectRepository>;
pub type CatalogQueryPort = Arc<dyn CatalogQuery>;
pub type JobPort = Arc<dyn JobRepository>;
pub type MultipartPort = Arc<dyn MultipartStore>;

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

/// Process/runtime inputs.
///
/// The durability ports are **required**: atomicity of a remote-mutation
/// receipt and its outbox fact is guaranteed by the type system, not by
/// every caller remembering to wire an `Option`. Composition roots that
/// genuinely have nowhere durable to write should not construct this graph
/// at all.
pub struct RuntimeDependencies {
    pub stats: StatsPort,
    pub clock: Arc<dyn Clock>,
    pub storage_factory: Option<Arc<dyn StorageFactory>>,
    pub unit_of_work: UnitOfWorkPort,
    pub repairs: RepairPort,
}

/// All dependencies required to construct the application core.
///
/// The composition root owns the concrete values and creates these groups.
/// Keeping the grouping here makes replacement seams explicit: callers
/// provide capabilities, never a database handle or a provider SDK.
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
