//! Bucket management service.
//!
//! Discover lists remote buckets; manual add works without ListBuckets permission and
//! optionally verifies readability at add time. The read-only and allow-delete guards live
//! here so every later write path funnels through them.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::BucketStore;
use rstore_domain::bucket::{Bucket, BucketConfig};
use rstore_domain::id::BucketId;
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::{Clock, Error, Result};
use rstore_storage_api::model::ListOptions;

use crate::connection::ConnectionRef;
use crate::gateway::RemoteGateway;

/// Reference a bucket by id, `connection/name`, or bare name (unique only).
#[derive(Debug, Clone)]
pub enum BucketRef {
    Id(BucketId),
    /// `<connection-name-or-id>/<bucket-name>`.
    Scoped(ConnectionRef, String),
    /// A bare bucket name — resolves only when unambiguous.
    Name(String),
}

impl BucketRef {
    /// Parse: `bkt_…` id, `scope/name`, else a bare name.
    pub fn parse(s: &str) -> Self {
        if s.starts_with("bkt_") {
            if let Ok(id) = s.parse() {
                return BucketRef::Id(id);
            }
        }
        match s.split_once('/') {
            Some((connection, name)) => {
                BucketRef::Scoped(ConnectionRef::parse(connection), name.to_string())
            }
            None => BucketRef::Name(s.to_string()),
        }
    }
}

impl BucketRef {
    /// Human-readable form for messages.
    pub fn display(&self) -> String {
        match self {
            BucketRef::Id(id) => id.to_string(),
            BucketRef::Scoped(connection, name) => {
                format!("{}/{}", connection.display(), name)
            }
            BucketRef::Name(name) => name.clone(),
        }
    }
}

/// Result of the add-time readability probe.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadabilityCheck {
    pub can_list: bool,
    pub detail: String,
}

/// A remote bucket discovered via ListBuckets.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DiscoveredBucket {
    pub name: String,
    pub already_registered: bool,
}

/// Input for registering a bucket.
#[derive(Debug, Clone)]
pub struct AddBucketInput {
    pub connection: ConnectionRef,
    pub name: String,
    pub config: BucketConfig,
}

/// Editable bucket fields.
///
/// Follows the `Option<Option<T>>` clearing convention: outer `None` keeps the value,
/// `Some(None)` clears it, `Some(Some(v))` replaces it (`display_name`, `concurrency`).
#[derive(Debug, Clone, Default)]
pub struct UpdateBucketInput {
    pub display_name: Option<Option<String>>,
    pub read_only: Option<bool>,
    pub allow_delete: Option<bool>,
    pub index_enabled: Option<bool>,
    pub default_conflict: Option<ConflictPolicy>,
    pub concurrency: Option<Option<u32>>,
}

/// Bucket lifecycle operations.
#[async_trait]
pub trait BucketService: Send + Sync {
    /// List remote buckets and whether each is already registered.
    async fn discover(&self, connection: &ConnectionRef) -> Result<Vec<DiscoveredBucket>>;
    /// Register a bucket locally; `verify` runs the readability probe.
    ///
    /// Returns the registered bucket plus the probe outcome when requested. A failed probe is
    /// reported, not fatal — manual add is the escape hatch for restricted credentials.
    async fn add(
        &self,
        input: AddBucketInput,
        verify: bool,
    ) -> Result<(Bucket, Option<ReadabilityCheck>)>;
    async fn list(&self) -> Result<Vec<Bucket>>;
    async fn resolve(&self, reference: &BucketRef) -> Result<Bucket>;
    async fn update(&self, id: BucketId, input: UpdateBucketInput) -> Result<Bucket>;
    async fn remove(&self, reference: &BucketRef) -> Result<()>;
}

/// SQLite-backed default implementation.
pub struct DefaultBucketService {
    buckets: Arc<dyn BucketStore>,
    gateway: RemoteGateway,
    clock: Arc<dyn Clock>,
}

impl DefaultBucketService {
    pub fn new(
        buckets: Arc<dyn BucketStore>,
        gateway: RemoteGateway,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            buckets,
            gateway,
            clock,
        }
    }
}

fn not_found(id: BucketId) -> Error {
    Error::not_found(format!("bucket `{id}` not found"))
}

#[async_trait]
impl BucketService for DefaultBucketService {
    async fn discover(&self, connection: &ConnectionRef) -> Result<Vec<DiscoveredBucket>> {
        let (config, storage) = self.gateway.storage(connection).await?;
        let remote = storage.list_buckets().await.map_err(|e| {
            // No ListBuckets permission must not block manual add.
            e.with_context("hint", "manual `bucket add` works without list permission")
        })?;
        let registered = self.buckets.list_by_connection(config.id).await?;
        Ok(remote
            .into_iter()
            .map(|b| DiscoveredBucket {
                already_registered: registered.iter().any(|r| r.name == b.name),
                name: b.name,
            })
            .collect())
    }

    async fn add(
        &self,
        input: AddBucketInput,
        verify: bool,
    ) -> Result<(Bucket, Option<ReadabilityCheck>)> {
        let config = self.gateway.config(&input.connection).await?;
        if input.name.trim().is_empty() {
            return Err(Error::invalid_input("bucket name must not be empty"));
        }
        if self
            .buckets
            .get_by_name(config.id, input.name.trim())
            .await?
            .is_some()
        {
            return Err(Error::conflict(format!(
                "bucket `{}` is already registered on connection `{}`",
                input.name, config.name
            )));
        }

        // Probe readability unless explicitly skipped. A failed probe does not block
        // registration (manual add is the escape hatch).
        let check = if verify {
            let storage = self.gateway.storage_for_config(&config).await?;
            let probe = match storage
                .list(
                    input.name.trim(),
                    &ListOptions {
                        max_keys: Some(1),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(page) => ReadabilityCheck {
                    can_list: true,
                    detail: format!("listed ({} object(s) visible)", page.objects.len()),
                },
                Err(e) => ReadabilityCheck {
                    can_list: false,
                    detail: e.to_string(),
                },
            };
            Some(probe)
        } else {
            None
        };

        let now = self.clock.now();
        let bucket = Bucket {
            id: BucketId::new_random(),
            connection_id: config.id,
            name: input.name.trim().to_string(),
            config: input.config,
            created_at: now,
            updated_at: now,
        };
        self.buckets.insert(&bucket).await?;
        Ok((bucket, check))
    }

    async fn list(&self) -> Result<Vec<Bucket>> {
        self.buckets.list().await
    }

    async fn resolve(&self, reference: &BucketRef) -> Result<Bucket> {
        match reference {
            BucketRef::Id(id) => self.buckets.get(*id).await?.ok_or_else(|| not_found(*id)),
            BucketRef::Scoped(connection, name) => {
                let config = self.gateway.config(connection).await?;
                self.buckets
                    .get_by_name(config.id, name)
                    .await?
                    .ok_or_else(|| {
                        Error::not_found(format!(
                            "bucket `{name}` is not registered on connection `{}`",
                            config.name
                        ))
                    })
            }
            BucketRef::Name(name) => {
                let all = self.buckets.list().await?;
                let hits: Vec<&Bucket> = all.iter().filter(|b| &b.name == name).collect();
                match hits.as_slice() {
                    [] => Err(Error::not_found(format!("bucket `{name}` not found"))),
                    [one] => Ok((*one).clone()),
                    many => Err(Error::invalid_input(format!(
                        "bucket `{name}` is registered on {} connections; use \
                         `<connection>/{name}`",
                        many.len()
                    ))),
                }
            }
        }
    }

    async fn update(&self, id: BucketId, input: UpdateBucketInput) -> Result<Bucket> {
        let mut bucket = self.buckets.get(id).await?.ok_or_else(|| not_found(id))?;
        if let Some(display_name) = input.display_name {
            bucket.config.display_name = display_name;
        }
        if let Some(read_only) = input.read_only {
            bucket.config.read_only = read_only;
        }
        if let Some(allow_delete) = input.allow_delete {
            bucket.config.allow_delete = allow_delete;
        }
        if let Some(index_enabled) = input.index_enabled {
            bucket.config.index_enabled = index_enabled;
        }
        if let Some(default_conflict) = input.default_conflict {
            bucket.config.default_conflict = default_conflict;
        }
        if let Some(concurrency) = input.concurrency {
            bucket.config.concurrency = concurrency;
        }
        bucket.updated_at = self.clock.now();
        self.buckets.update(&bucket).await?;
        Ok(bucket)
    }

    async fn remove(&self, reference: &BucketRef) -> Result<()> {
        let bucket = self.resolve(reference).await?;
        if !self.buckets.delete(bucket.id).await? {
            return Err(not_found(bucket.id));
        }
        Ok(())
    }
}
