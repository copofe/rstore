//! Remote-reference resolution (REM-/WSP-013) — the one path from a
//! user-supplied `conn/bucket/key` reference to a registered bucket and a catalog row.
//!
//! Before this module existed the same chain (parse → connection → bucket →
//! catalog row → workspace membership) was hand-rolled per service and the
//! membership rule drifted: `tag add` accepted objects outside the workspace.
//! Both tiers live here so the WSP-013 invariant cannot be forgotten again.

use std::sync::Arc;

use rstore_application_ports::{BucketStore, ConnectionStore, WorkspaceStore};
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::id::WorkspaceId;
use rstore_domain::remote::{ObjectPath, RemoteObject};
use rstore_domain::{ConnectionConfig, Error, Result};

use crate::gateway::RemoteGateway;
use crate::remote::RemoteUri;

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

    /// URI → (connection config, registered bucket). The key stays on the
    /// [`RemoteUri`] — callers interpret it as a listing prefix or a strict
    /// [`ObjectPath`] per their operation. A `NotFound` carries the
    /// `bucket add` hint, so every command resolves unregistered locators
    /// with the same message.
    pub async fn resolve_bucket(
        &self,
        uri: &RemoteUri,
    ) -> Result<(ConnectionConfig, rstore_domain::Bucket)> {
        self.resolve_pair(&uri.connection, &uri.bucket).await
    }

    /// (connection, bucket name) → (connection config, registered bucket) —
    /// the resolution core behind the URI entries, for callers that hold a
    /// [`ConnectionRef`] and a bucket name (transfer plans) instead of a
    /// parsed URI. The same not-found hints as every other entry.
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

    /// URI → (connection config, registered bucket, storage adapter) —
    /// tier 1, the one path every live remote operation (ls/stat/rm, cp/mv,
    /// upload/download) resolves through. Presign-only callers that never
    /// touch storage should
    /// prefer [`Self::resolve_bucket`].
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

    /// (connection, bucket name) → tier 1 without a URI: transfer plans hold
    /// a [`ConnectionRef`] plus a bucket name — no fabricated keys to fit
    /// the URI entries.
    pub async fn resolve_pair_target(
        &self,
        connection: &crate::connection::ConnectionRef,
        bucket: &str,
    ) -> Result<(
        ConnectionConfig,
        rstore_domain::Bucket,
        Arc<dyn rstore_storage_api::Storage>,
    )> {
        let (config, bucket) = self.resolve_pair(connection, bucket).await?;
        let storage = self.storage_for(&config, None).await?;
        Ok((config, bucket, storage))
    }

    /// The retry flavour of [`Self::resolve_pair_target`].
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

    /// A registered bucket row → its connection config and storage adapter.
    /// Tier 1 for callers that resolved a registry reference themselves (the
    /// indexer scans a bucket it already holds) — one connection lookup, no
    /// gateway bypass, no URI fabrication.
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

    /// One adapter construction path for the tier-1 entries: with or without
    /// a job-scoped retry policy.
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

    /// URI → catalog row, enforcing that the object's bucket belongs to the
    /// workspace (WSP-013). A foreign object reports `NotFound`, matching
    /// `files show`: existence is not leaked across workspaces.
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
