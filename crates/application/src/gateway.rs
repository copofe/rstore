//! Remote access gateway: resolves a connection reference into a connected
//! [`Storage`] adapter (CON-001/007).
//!
//! One place owns the chain *resolve config → build adapter*, so every service
//! (bucket, remote, transfer) reports identical, well-classified failures for
//! a broken connection.

use std::sync::Arc;

use rstore_application_ports::ConnectionStore;
use rstore_domain::connection::ConnectionConfig;
use rstore_domain::{Error, Result};
use rstore_storage_api::{Storage, StorageFactory};
use rstore_transfer::{CancellationToken, RetryPolicy, RetryingStorage};

use crate::connection::ConnectionRef;

/// Builds connected adapters from connection references.
#[derive(Clone)]
pub struct RemoteGateway {
    connections: Arc<dyn ConnectionStore>,
    factory: Option<Arc<dyn StorageFactory>>,
}

impl RemoteGateway {
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
    pub async fn config(&self, reference: &ConnectionRef) -> Result<ConnectionConfig> {
        let found = match reference {
            ConnectionRef::Id(id) => self.connections.get(*id).await?,
            ConnectionRef::Name(name) => self.connections.get_by_name(name).await?,
        };
        found.ok_or_else(|| {
            Error::not_found(format!(
                "connection `{}` not found",
                match reference {
                    ConnectionRef::Id(id) => id.to_string(),
                    ConnectionRef::Name(n) => n.clone(),
                }
            ))
        })
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

    /// Build the adapter for an already-resolved configuration.
    pub async fn storage_for_config(&self, config: &ConnectionConfig) -> Result<Arc<dyn Storage>> {
        self.storage_for_config_with_retry(config, RetryPolicy::default(), CancellationToken::new())
            .await
    }

    /// Build a retry-decorated adapter with the caller's run policy and
    /// cancellation token. Transfer jobs use this path; other application
    /// services use [`Self::storage_for_config`] and the default policy.
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

    fn sample_config(with_credentials: bool) -> ConnectionConfig {
        ConnectionConfig {
            id: rstore_domain::ConnectionId::new(),
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
