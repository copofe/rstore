//! A [`StorageFactory`] producing shared [`FakeStorage`] instances for tests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rstore_domain::connection::ConnectionConfig;
use rstore_domain::Result;
use rstore_storage_api::{Storage, StorageCredentials, StorageFactory};

use crate::fake::FakeStorage;

/// Hands out clones of one shared [`FakeStorage`] and counts how many
/// adapters were built — so tests can assert the factory was consulted.
pub struct FakeStorageFactory {
    storage: Arc<FakeStorage>,
    built: AtomicUsize,
}

impl FakeStorageFactory {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(FakeStorage::new()),
            built: AtomicUsize::new(0),
        }
    }

    /// The shared fake store (seed it directly for fixtures).
    pub fn storage(&self) -> Arc<FakeStorage> {
        self.storage.clone()
    }

    /// How many adapters have been built so far.
    pub fn built(&self) -> usize {
        self.built.load(Ordering::SeqCst)
    }
}

impl Default for FakeStorageFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageFactory for FakeStorageFactory {
    fn name(&self) -> &'static str {
        "fake"
    }

    fn build(
        &self,
        _config: &ConnectionConfig,
        _creds: &StorageCredentials,
    ) -> Result<Arc<dyn Storage>> {
        self.built.fetch_add(1, Ordering::SeqCst);
        Ok(self.storage.clone())
    }
}
