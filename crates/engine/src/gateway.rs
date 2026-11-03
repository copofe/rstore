//! Remote access gateway: the one path from a reference to a registered
//! bucket and a connected [`Storage`] adapter.
//!
//! Resolution (connection reference → config → registered bucket) and
//! adapter issuance live together, so every consumer reports identical,
//! well-classified failures for a broken connection or an unregistered
//! bucket. Every issued adapter is retry-decorated. Mutations never leave
//! through this seam unguarded: [`RemoteGateway::writer`] and
//! [`RemoteGateway::deleter`] wrap the adapter in the write guard, so the
//! adapter a use case executes through carries the bucket's guard bits with
//! it.
use std::sync::Arc;

use crate::transfer::{CancellationToken, RetryPolicy, RetryingStorage};
use rstore_domain::connection::ConnectionConfig;
use rstore_domain::{Bucket, Error, Result};
use rstore_storage::{Storage, StorageFactory};

use crate::connection::ConnectionRef;
use crate::reference::RemoteUri;

/// Resolves references against the registry and builds connected adapters.
///
/// Clone shares the underlying stores; adapters themselves are built per
/// call.
#[derive(Clone)]
pub struct RemoteGateway {
    connections: Arc<crate::db::ConnectionRepository>,
    buckets: Arc<crate::db::BucketRepository>,
    factory: Arc<dyn StorageFactory>,
}

impl RemoteGateway {
    /// Create a gateway over the registry stores and the storage factory.
    pub fn new(
        connections: Arc<crate::db::ConnectionRepository>,
        buckets: Arc<crate::db::BucketRepository>,
        factory: Arc<dyn StorageFactory>,
    ) -> Self {
        Self {
            connections,
            buckets,
            factory,
        }
    }

    /// Resolve a connection reference to its stored configuration.
    ///
    /// Errors when no connection matches the reference's id or name.
    pub async fn config(&self, reference: &ConnectionRef) -> Result<ConnectionConfig> {
        crate::connection::resolve_connection(self.connections.as_ref(), reference).await
    }

    /// Resolve a URI's connection reference and bucket against the registry.
    pub async fn resolve_bucket(&self, uri: &RemoteUri) -> Result<(ConnectionConfig, Bucket)> {
        self.resolve_pair(&uri.connection, &uri.bucket).await
    }

    /// Resolve a connection reference plus bucket name.
    ///
    /// Errors when the connection is unknown or the bucket is not registered
    /// under it (with a registration hint in the context).
    pub async fn resolve_pair(
        &self,
        connection: &ConnectionRef,
        bucket: &str,
    ) -> Result<(ConnectionConfig, Bucket)> {
        let config = self.config(connection).await?;
        let bucket = self
            .buckets
            .get_by_name(config.id, bucket)
            .await?
            .ok_or_else(|| {
                Error::not_found(format!(
                    "bucket `{}/{}` is not registered",
                    connection.display(),
                    bucket
                ))
                .with_context(
                    "hint",
                    format!(
                        "register it first: `rstore bucket add` {}/{}",
                        connection.display(),
                        bucket
                    ),
                )
            })?;
        Ok((config, bucket))
    }

    /// Resolve a URI to its registered bucket and a connected adapter under
    /// the default retry policy.
    pub async fn resolve_target(&self, uri: &RemoteUri) -> Result<(Bucket, Arc<dyn Storage>)> {
        let (config, bucket) = self.resolve_bucket(uri).await?;
        let storage = self.storage_for_config(&config).await?;
        Ok((bucket, storage))
    }

    /// [`Self::resolve_pair`] plus an adapter decorated with the caller's run
    /// policy and cancellation token.
    pub async fn resolve_target_with_retry(
        &self,
        connection: &ConnectionRef,
        bucket: &str,
        retry: RetryPolicy,
        cancel: CancellationToken,
    ) -> Result<(Bucket, Arc<dyn Storage>)> {
        let (config, bucket) = self.resolve_pair(connection, bucket).await?;
        let storage = self
            .storage_for_config_with_retry(&config, retry, cancel)
            .await?;
        Ok((bucket, storage))
    }

    /// The adapter for an already-registered bucket.
    ///
    /// Errors when the bucket's owning connection no longer exists.
    pub async fn bucket_storage(&self, bucket: &Bucket) -> Result<Arc<dyn Storage>> {
        let config = self
            .connections
            .get(bucket.connection_id)
            .await?
            .ok_or_else(|| {
                Error::not_found(format!(
                    "connection for registered bucket `{}` not found",
                    bucket.name
                ))
            })?;
        self.storage_for_config(&config).await
    }

    /// `<connection>/<bucket>` for report labels; the connection id stands
    /// in when its row is gone.
    pub async fn bucket_label(&self, bucket: &Bucket) -> String {
        let connection = match self.connections.get(bucket.connection_id).await {
            Ok(Some(config)) => config.name,
            _ => bucket.connection_id.to_string(),
        };
        format!("{connection}/{}", bucket.name)
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
    /// [`Self::resolve_target_with_retry`] with the run's policy instead.
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
        let storage = self.factory.build(
            config,
            &rstore_storage::StorageCredentials {
                access_key,
                secret_key,
            },
        )?;
        Ok(Arc::new(RetryingStorage::new(storage, retry, cancel)))
    }

    /// Mutation adapter for one bucket: the given adapter wrapped in the
    /// write guard. `Err(Forbidden)` when the bucket is read-only — the
    /// guard is the door, not a convention on callers. The returned
    /// adapter re-checks the guard bits on every write call, so it can
    /// travel into the transfer engine, the relay, or the copy entry
    /// without opening a bypass.
    pub fn writer(&self, storage: Arc<dyn Storage>, bucket: &Bucket) -> Result<Arc<dyn Storage>> {
        crate::guarded_storage::GuardedStorage::writer(storage, bucket)
    }

    /// Delete adapter for one bucket. `Err(Forbidden)` unless deletions
    /// are allowed *and* the bucket is not read-only; the two flags are
    /// independent on purpose. The returned adapter guards every delete
    /// call the same way [`Self::writer`] guards mutations.
    pub fn deleter(&self, storage: Arc<dyn Storage>, bucket: &Bucket) -> Result<Arc<dyn Storage>> {
        crate::guarded_storage::GuardedStorage::deleter(storage, bucket)
    }
}
