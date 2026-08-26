//! Remote access gateway: resolves a connection reference into a connected
//! [`Storage`] adapter.
//!
//! One place owns the chain *resolve config → build adapter*, so every
//! consumer (bucket, remote, transfer) reports identical, well-classified
//! failures for a broken connection. Mutations never leave through this
//! seam unguarded: [`Self::writer`] and [`Self::deleter`] wrap the adapter
//! in the write guard (see [`crate::guarded_storage`]), so the adapter a
//! use case executes through carries the bucket's guard bits with it.
use std::sync::Arc;

use rstore_application_ports::ConnectionStore;
use rstore_domain::connection::ConnectionConfig;
use rstore_domain::{Error, Result};
use rstore_storage_api::{Storage, StorageFactory};
use rstore_transfer::{CancellationToken, RetryPolicy, RetryingStorage};

use crate::connection::ConnectionRef;

/// Builds connected adapters from connection references.
///
/// Clone shares the underlying connection store; adapters themselves are
/// built per call.
#[derive(Clone)]
pub struct RemoteGateway {
    connections: Arc<dyn ConnectionStore>,
    factory: Option<Arc<dyn StorageFactory>>,
}

impl RemoteGateway {
    /// Create a gateway over the given connection store. Without a
    /// [`StorageFactory`] every adapter build fails with a config error.
    pub fn new(
        connections: Arc<dyn ConnectionStore>,
        factory: Option<Arc<dyn StorageFactory>>,
    ) -> Self {
        Self {
            connections,
            factory,
        }
    }

    /// Resolve a connection reference to its stored configuration.
    ///
    /// Errors when no connection matches the reference's id or name.
    pub async fn config(&self, reference: &ConnectionRef) -> Result<ConnectionConfig> {
        crate::connection::resolve_connection(self.connections.as_ref(), reference).await
    }

    /// Resolve a reference to a connected adapter plus its configuration.
    pub async fn storage(
        &self,
        reference: &ConnectionRef,
    ) -> Result<(ConnectionConfig, Arc<dyn Storage>)> {
        let config = self.config(reference).await?;
        let storage = self.storage_for_config(&config).await?;
        Ok((config, storage))
    }

    /// Build the adapter for an already-resolved configuration under the
    /// default retry policy. Transfer jobs use
    /// [`Self::storage_for_config_with_retry`] with the run's policy instead.
    pub async fn storage_for_config(&self, config: &ConnectionConfig) -> Result<Arc<dyn Storage>> {
        self.storage_for_config_with_retry(config, RetryPolicy::default(), CancellationToken::new())
            .await
    }

    /// Build a retry-decorated adapter with the caller's run policy and
    /// cancellation token.
    pub async fn storage_for_config_with_retry(
        &self,
        config: &ConnectionConfig,
        retry: RetryPolicy,
        cancel: CancellationToken,
    ) -> Result<Arc<dyn Storage>> {
        let storage = self.raw_storage_for_config(config).await?;
        Ok(Arc::new(RetryingStorage::new(storage, retry, cancel)))
    }

    async fn raw_storage_for_config(&self, config: &ConnectionConfig) -> Result<Arc<dyn Storage>> {
        let factory = self
            .factory
            .as_ref()
            .ok_or_else(|| Error::config("no storage adapter is configured in this build"))?;
        let (access_key, secret_key) = match (&config.access_key, &config.secret_key) {
            (Some(access_key), Some(secret_key)) => (access_key.clone(), secret_key.clone()),
            _ => {
                return Err(Error::config(format!(
                    "connection {} has no complete credentials; add them with \
                     --access-key/--secret-key or connection edit",
                    config.name
                )));
            }
        };
        factory.build(
            config,
            &rstore_storage_api::StorageCredentials {
                access_key,
                secret_key,
            },
        )
    }

    /// Mutation adapter for one bucket: the given adapter wrapped in the
    /// write guard. `Err(Forbidden)` when the bucket is read-only — the
    /// guard is the door, not a convention on callers. The returned
    /// adapter re-checks the guard bits on every write call, so it can
    /// travel into the transfer engine, the relay, or the copy entry
    /// without opening a bypass.
    pub fn writer(
        &self,
        storage: Arc<dyn Storage>,
        bucket: &rstore_domain::Bucket,
    ) -> Result<Arc<dyn Storage>> {
        crate::guarded_storage::GuardedStorage::writer(storage, bucket)
    }

    /// Delete adapter for one bucket. `Err(Forbidden)` unless deletions
    /// are allowed *and* the bucket is not read-only; the two flags are
    /// independent on purpose. The returned adapter guards every delete
    /// call the same way [`Self::writer`] guards mutations.
    pub fn deleter(
        &self,
        storage: Arc<dyn Storage>,
        bucket: &rstore_domain::Bucket,
    ) -> Result<Arc<dyn Storage>> {
        crate::guarded_storage::GuardedStorage::deleter(storage, bucket)
    }
}
