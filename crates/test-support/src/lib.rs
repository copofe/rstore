//! Shared test fixtures (CORE-014 / QA-004).
//!
//! - [`FakeStorage`]: an in-memory [`Storage`] so tests never touch the network.
//! - [`FakeStorageFactory`]: a [`StorageFactory`] handing out that fake.
//! - [`TempDb`]: a migrated SQLite catalog in a throwaway directory.
//! - [`TestClock`]: a deterministic, advanceable [`Clock`].
//! - [`catalog_contract`]: the shared behavioral suite every catalog adapter
//!   runs (SQLite repositories and `rstore_catalog::InMemoryCatalog`).

mod catalog_contract;
mod clock;
mod factory;
mod fake;
mod storage_contract;
mod tag_store;
mod temp_db;

pub use catalog_contract::{catalog_contract, CatalogFixture};
pub use clock::TestClock;
pub use factory::FakeStorageFactory;
pub use fake::{FakeStorage, StorageMethod};
pub use storage_contract::storage_contract;
pub use tag_store::InMemoryTagStore;
pub use temp_db::TempDb;
