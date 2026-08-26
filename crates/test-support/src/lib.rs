//! Shared test fixtures.
//!
//! - [`FakeStorage`]: an in-memory [`rstore_storage_api::Storage`] so tests
//!   never touch the network.
//! - [`FakeStorageFactory`]: a [`rstore_storage_api::StorageFactory`] handing
//!   out that fake.
//! - [`TempDb`]: a migrated SQLite catalog in a throwaway directory.
//! - [`TestClock`]: a deterministic, advanceable [`rstore_domain::Clock`].
//! - [`storage_contract`] / [`catalog_contract`]: the shared behavioral
//!   suites every storage / catalog adapter runs, so fakes and real
//!   adapters are pinned to the same semantics.

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
pub use storage_contract::{multipart_legality_negatives, storage_contract};
pub use tag_store::InMemoryTagStore;
pub use temp_db::TempDb;
