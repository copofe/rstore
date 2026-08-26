//! Remote-reference resolution — the one path from a user-supplied
//! `conn/bucket/key` reference to a registered bucket and a catalog row.
//!
//! Both tiers live here so the workspace-membership invariant cannot
//! drift: registry resolution (parse → connection → bucket) for services,
//! and catalog resolution (… → catalog row → workspace membership) for
//! operations that address already-indexed objects.

use std::sync::Arc;

use rstore_application_ports::{BucketStore, ConnectionStore, WorkspaceStore};
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::id::WorkspaceId;
use rstore_domain::remote::{ObjectPath, RemoteObject};
use rstore_domain::{ConnectionConfig, Error, Result};

use crate::gateway::RemoteGateway;
use crate::reference::RemoteUri;

/// Resolves remote locators against the registry and the catalog.
#[derive(Clone)]
pub struct CatalogLocator {
    connections: Arc<dyn ConnectionStore>,
    buckets: Arc<dyn BucketStore>,
    workspaces: Arc<dyn WorkspaceStore>,
    catalog: Arc<dyn RemoteObjectRepository>,
    gateway: RemoteGateway,
}

impl CatalogLocator {
    /// Create a locator over the given stores and gateway.
    pub fn new(
        connections: Arc<dyn ConnectionStore>,
        buckets: Arc<dyn BucketStore>,
        workspaces: Arc<dyn WorkspaceStore>,
        catalog: Arc<dyn RemoteObjectRepository>,
        gateway: RemoteGateway,
    ) -> Self {
        Self {
            connections,
            buckets,
            workspaces,
            catalog,
            gateway,
        }
    }

    /// Resolve a URI's connection reference and bucket against the
    /// registry.
    ///
    /// Errors when the connection is unknown or the bucket is not
    /// registered under it (with a registration hint in the context).
    pub async fn resolve_bucket(
        &self,
        uri: &RemoteUri,
    ) -> Result<(ConnectionConfig, rstore_domain::Bucket)> {
        self.resolve_pair(&uri.connection, &uri.bucket).await
    }

    /// Resolve a connection reference plus bucket name without a parsed
    /// [`RemoteUri`], with the same error behavior as
    /// [`Self::resolve_bucket`].
    pub async fn resolve_pair(
        &self,
        connection: &crate::connection::ConnectionRef,
        bucket: &str,
    ) -> Result<(ConnectionConfig, rstore_domain::Bucket)> {
        let config = self.gateway.config(connection).await?;
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

    /// Resolve a URI to its config, bucket, and a connected adapter (no
    /// retry decoration).
    pub async fn resolve_target(
        &self,
        uri: &RemoteUri,
    ) -> Result<(
        ConnectionConfig,
        rstore_domain::Bucket,
        Arc<dyn rstore_storage_api::Storage>,
    )> {
        let (config, bucket) = self.resolve_bucket(uri).await?;
        let storage = self.storage_for(&config, None).await?;
        Ok((config, bucket, storage))
    }

    /// [`Self::resolve_pair`] with the caller's run policy and cancellation
    /// token decorating the adapter.
    pub async fn resolve_pair_target_with_retry(
        &self,
        connection: &crate::connection::ConnectionRef,
        bucket: &str,
        retry: rstore_transfer::RetryPolicy,
        cancel: rstore_transfer::CancellationToken,
    ) -> Result<(
        ConnectionConfig,
        rstore_domain::Bucket,
        Arc<dyn rstore_storage_api::Storage>,
    )> {
        let (config, bucket) = self.resolve_pair(connection, bucket).await?;
        let storage = self.storage_for(&config, Some((retry, cancel))).await?;
        Ok((config, bucket, storage))
    }

    /// Resolve an already-registered bucket to its connection config and a
    /// connected adapter.
    ///
    /// Errors when the bucket's owning connection no longer exists.
    pub async fn resolve_bucket_target(
        &self,
        bucket: &rstore_domain::Bucket,
    ) -> Result<(ConnectionConfig, Arc<dyn rstore_storage_api::Storage>)> {
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
        let storage = self.storage_for(&config, None).await?;
        Ok((config, storage))
    }

    async fn storage_for(
        &self,
        config: &ConnectionConfig,
        retry: Option<(
            rstore_transfer::RetryPolicy,
            rstore_transfer::CancellationToken,
        )>,
    ) -> Result<Arc<dyn rstore_storage_api::Storage>> {
        match retry {
            None => self.gateway.storage_for_config(config).await,
            Some((retry, cancel)) => {
                self.gateway
                    .storage_for_config_with_retry(config, retry, cancel)
                    .await
            }
        }
    }

    /// Resolve a URI string to a catalog row that must exist and belong to
    /// the given workspace.
    ///
    /// A not-found error covers three distinct cases on purpose — unknown
    /// locator shape, unindexed object ("run `index start` first"), and an
    /// object outside the workspace — so callers cannot distinguish a
    /// missing object from one they must not see.
    pub async fn resolve(&self, uri: &str, workspace: WorkspaceId) -> Result<RemoteObject> {
        let parsed = RemoteUri::parse(uri)?;
        let (config, bucket) = self.resolve_bucket(&parsed).await?;
        let path = ObjectPath::new(&parsed.key)?;
        let object = self
            .catalog
            .find_by_locator(config.id, bucket.id, &path)
            .await?
            .ok_or_else(|| {
                Error::not_found(format!(
                    "`{uri}` is not in the catalog; run `index start` first"
                ))
            })?;
        let member = self.workspaces.buckets(workspace).await?;
        if !member.contains(&object.bucket_id) {
            return Err(Error::not_found(format!(
                "`{uri}` is not part of this workspace"
            )));
        }
        Ok(object)
    }
}
