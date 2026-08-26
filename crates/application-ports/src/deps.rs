//! Dependency-graph vocabulary for the application core.
//!
//! The port aliases and dependency-group structs live here — next to the
//! traits they name — so the persistence layer can offer a single assembly
//! point (`rstore_persistence::assemble`) without
//! depending on the application crate. This module contains no SQLite, SQLx,
//! provider SDK, or UI type.

use std::sync::Arc;

use rstore_catalog::{CatalogQuery, RemoteObjectRepository, TagStore as CatalogTagStore};
use rstore_domain::Clock;
use rstore_storage_api::{MultipartStore, StorageFactory};

use crate::{
    BucketStore, ConnectionStore, IgnoreRuleStore, IndexRunStore, JobRepository, LinkProfileStore,
    OutboxStore, RepairStore, RuntimeStats, SettingsStore, TagStore, UnitOfWork, WorkspaceStore,
};

/// Shared handle to the [`ConnectionStore`] port.
pub type ConnectionPort = Arc<dyn ConnectionStore>;
/// Shared handle to the [`BucketStore`] port.
pub type BucketPort = Arc<dyn BucketStore>;
/// Shared handle to the [`WorkspaceStore`] port.
pub type WorkspacePort = Arc<dyn WorkspaceStore>;
/// Shared handle to the [`IgnoreRuleStore`] port.
pub type IgnoreRulePort = Arc<dyn IgnoreRuleStore>;
/// Shared handle to the [`IndexRunStore`] port.
pub type IndexRunPort = Arc<dyn IndexRunStore>;
/// Shared handle to the [`SettingsStore`] port.
pub type SettingsPort = Arc<dyn SettingsStore>;
/// Shared handle to the [`LinkProfileStore`] port.
pub type LinkProfilePort = Arc<dyn LinkProfileStore>;
/// Shared handle to the wide workspace [`TagStore`] port.
pub type TagPort = Arc<dyn TagStore>;
/// Shared handle to the catalog seam's narrow tag port
/// (`rstore_catalog::TagStore`).
pub type CatalogTagPort = Arc<dyn CatalogTagStore>;
/// Shared handle to the [`RuntimeStats`] port.
pub type StatsPort = Arc<dyn RuntimeStats>;
/// Shared handle to the [`UnitOfWork`] port.
pub type UnitOfWorkPort = Arc<dyn UnitOfWork>;
/// Shared handle to the [`RepairStore`] port.
pub type RepairPort = Arc<dyn RepairStore>;
/// Shared handle to the [`OutboxStore`] port.
pub type OutboxPort = Arc<dyn OutboxStore>;
/// Shared handle to the catalog write port (`rstore_catalog::RemoteObjectRepository`).
pub type CatalogPort = Arc<dyn RemoteObjectRepository>;
/// Shared handle to the catalog read port (`rstore_catalog::CatalogQuery`).
pub type CatalogQueryPort = Arc<dyn CatalogQuery>;
/// Shared handle to the [`JobRepository`] port.
pub type JobPort = Arc<dyn JobRepository>;
/// Shared handle to the multipart upload port (`rstore_storage_api::MultipartStore`).
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
    pub outbox: OutboxPort,
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
    /// Bundles the four dependency groups into the application-core inputs.
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
}
