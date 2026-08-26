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

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_persistence::Database;
    use rstore_test_support::FakeStorageFactory;
    use time::OffsetDateTime;

    async fn gateway() -> RemoteGateway {
        let db = Database::open_in_memory().await.unwrap();
        RemoteGateway::new(
            Arc::new(db.connections()),
            Some(Arc::new(FakeStorageFactory::new())),
        )
    }

    fn bucket_with(read_only: bool, allow_delete: bool) -> rstore_domain::Bucket {
        use rstore_domain::bucket::{Bucket, BucketConfig};
        use rstore_domain::id::{BucketId, ConnectionId};
        Bucket {
            id: BucketId::new_random(),
            connection_id: ConnectionId::new_random(),
            name: "media".into(),
            config: BucketConfig {
                read_only,
                allow_delete,
                ..BucketConfig::default()
            },
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[tokio::test]
    async fn writer_and_deleter_guards_are_independent() {
        let gateway = gateway().await;

        // Read-only: writes and deletions refused.
        let bucket = bucket_with(true, true);
        let err = match gateway.writer(fake_storage(), &bucket) {
            Err(e) => e,
            Ok(_) => panic!("read-only bucket must refuse a writer"),
        };
        assert_eq!(err.code, rstore_domain::ErrorCode::Forbidden);
        assert!(gateway.deleter(fake_storage(), &bucket).is_err());

        // Deletions disallowed, writes fine: the two flags are independent.
        let bucket = bucket_with(false, false);
        assert!(gateway.writer(fake_storage(), &bucket).is_ok());
        let err = match gateway.deleter(fake_storage(), &bucket) {
            Err(e) => e,
            Ok(_) => panic!("no-delete bucket must refuse a deleter"),
        };
        assert_eq!(err.code, rstore_domain::ErrorCode::Forbidden);

        // Permissive bucket: both handles issue.
        let bucket = bucket_with(false, true);
        assert!(gateway.writer(fake_storage(), &bucket).is_ok());
        assert!(gateway.deleter(fake_storage(), &bucket).is_ok());
    }

    fn fake_storage() -> std::sync::Arc<dyn Storage> {
        std::sync::Arc::new(rstore_test_support::FakeStorage::new())
    }

    fn sample_config(with_credentials: bool) -> ConnectionConfig {
        ConnectionConfig {
            id: rstore_domain::ConnectionId::new_random(),
            name: "x".into(),
            endpoint: "http://localhost:9000".into(),
            public_base_url: None,
            cdn_base_url: None,
            region: rstore_domain::Region::Auto,
            addressing_style: rstore_domain::AddressingStyle::Path,
            tls_verify: true,
            access_key: with_credentials.then(|| rstore_domain::Secret::new("AK".to_string())),
            secret_key: with_credentials.then(|| rstore_domain::Secret::new("SK".to_string())),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            last_tested_at: None,
            last_test_status: None,
        }
    }

    #[tokio::test]
    async fn resolves_credentials_and_builds() {
        let gateway = gateway().await;
        let storage = gateway
            .storage_for_config(&sample_config(true))
            .await
            .unwrap();
        assert_eq!(storage.name(), "fake");
    }

    #[tokio::test]
    async fn without_credentials_is_config_error() {
        let gateway = gateway().await;
        let err = match gateway.storage_for_config(&sample_config(false)).await {
            Err(e) => e,
            Ok(_) => panic!("expected a config error for a credential-less connection"),
        };
        assert_eq!(err.code, rstore_domain::ErrorCode::Config);
    }
}
