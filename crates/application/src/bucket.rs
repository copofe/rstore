//! Bucket management service (BKT-).
//!
//! Discover lists remote buckets (BKT-001); manual add works without
//! ListBuckets permission (BKT-002) and optionally verifies readability at
//! add time (BKT-003). The read-only and allow-delete guards (SEC-004/005)
//! live here so every later write path funnels through them.

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

/// Result of the add-time readability probe (BKT-003).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadabilityCheck {
    pub can_list: bool,
    pub detail: String,
}

/// A remote bucket discovered via ListBuckets (BKT-001).
#[derive(Debug, Clone, serde::Serialize)]
pub struct DiscoveredBucket {
    pub name: String,
    pub already_registered: bool,
}

/// Input for registering a bucket (BKT-002).
#[derive(Debug, Clone)]
pub struct AddBucketInput {
    pub connection: ConnectionRef,
    pub name: String,
    pub config: BucketConfig,
}

/// Editable fields (BKT-006..010).
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
    /// List remote buckets and whether each is already registered (BKT-001).
    async fn discover(&self, connection: &ConnectionRef) -> Result<Vec<DiscoveredBucket>>;
    /// Register a bucket locally; `verify` runs the readability probe.
    async fn add(
        &self,
        input: AddBucketInput,
        verify: bool,
    ) -> Result<(Bucket, Option<ReadabilityCheck>)>;
    async fn list(&self) -> Result<Vec<Bucket>>;
    async fn resolve(&self, reference: &BucketRef) -> Result<Bucket>;
    async fn update(&self, id: BucketId, input: UpdateBucketInput) -> Result<Bucket>;
    async fn remove(&self, reference: &BucketRef) -> Result<()>;
    /// Guard: the bucket must accept writes (SEC-004).
    async fn assert_writable(&self, id: BucketId) -> Result<()>;
    /// Guard: the bucket must accept deletions (SEC-005).
    async fn assert_deletable(&self, id: BucketId) -> Result<()>;
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
            // BKT-001: no ListBuckets permission must not block manual add.
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

        // BKT-003: probe readability unless explicitly skipped. A failed probe
        // does not block registration (manual add is the escape hatch).
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
            id: BucketId::new(),
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

    async fn assert_writable(&self, id: BucketId) -> Result<()> {
        let bucket = self.buckets.get(id).await?.ok_or_else(|| not_found(id))?;
        if bucket.config.read_only {
            return Err(Error::forbidden(format!(
                "bucket `{}` is read-only (SEC-004); disable with `bucket edit`",
                bucket.name
            )));
        }
        Ok(())
    }

    async fn assert_deletable(&self, id: BucketId) -> Result<()> {
        let bucket = self.buckets.get(id).await?.ok_or_else(|| not_found(id))?;
        if !bucket.config.allow_delete {
            return Err(Error::forbidden(format!(
                "bucket `{}` does not allow deletions (SEC-005); enable with `bucket edit`",
                bucket.name
            )));
        }
        if bucket.config.read_only {
            return Err(Error::forbidden(format!(
                "bucket `{}` is read-only (SEC-004)",
                bucket.name
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_domain::ErrorCode;
    use rstore_domain::SystemClock;
    use rstore_persistence::Database;
    use rstore_test_support::FakeStorageFactory;

    use crate::connection::{AddConnectionInput, ConnectionService as _, DefaultConnectionService};

    async fn setup() -> (
        DefaultBucketService,
        DefaultConnectionService,
        Arc<FakeStorageFactory>,
    ) {
        let db = Database::open_in_memory().await.unwrap();
        let factory = Arc::new(FakeStorageFactory::new());
        factory.storage().seed_bucket("media");
        factory.storage().seed_bucket("backups");
        let gateway = RemoteGateway::new(Arc::new(db.connections()), Some(factory.clone()));
        let buckets =
            DefaultBucketService::new(Arc::new(db.buckets()), gateway, Arc::new(SystemClock));
        let connections =
            DefaultConnectionService::new(Arc::new(db.connections()), Arc::new(SystemClock));
        (buckets, connections, factory)
    }

    #[tokio::test]
    async fn discover_add_list_edit_remove_and_guards() {
        let (buckets, connections, _factory) = setup().await;
        let conn = connections
            .add(AddConnectionInput {
                name: "nas".into(),
                endpoint: "http://localhost:9000".into(),
                public_base_url: None,
                cdn_base_url: None,
                region: rstore_domain::Region::Auto,
                addressing_style: rstore_domain::AddressingStyle::Path,
                tls_verify: true,
                credentials: Some(rstore_domain::Secret::new(
                    crate::connection::CredentialInput {
                        access_key: "AK".into(),
                        secret_key: "SK".into(),
                    },
                )),
            })
            .await
            .unwrap();

        // Discover marks registered state.
        let found = buckets
            .discover(&ConnectionRef::Name("nas".into()))
            .await
            .unwrap();
        assert_eq!(found.len(), 2);
        assert!(found.iter().all(|b| !b.already_registered));

        // Manual add + readability probe against the fake.
        let (bucket, check) = buckets
            .add(
                AddBucketInput {
                    connection: ConnectionRef::Id(conn.id),
                    name: "media".into(),
                    config: BucketConfig::default(),
                },
                true,
            )
            .await
            .unwrap();
        let probe = check.expect("verify requested");
        assert!(probe.can_list, "detail: {}", probe.detail);

        // Duplicate registration is a conflict.
        let dup = buckets
            .add(
                AddBucketInput {
                    connection: ConnectionRef::Id(conn.id),
                    name: "media".into(),
                    config: BucketConfig::default(),
                },
                false,
            )
            .await;
        assert_eq!(dup.unwrap_err().code, ErrorCode::Conflict);

        // Scoped and bare-name resolution.
        assert_eq!(
            buckets
                .resolve(&BucketRef::Scoped(
                    ConnectionRef::Name("nas".into()),
                    "media".into()
                ))
                .await
                .unwrap()
                .id,
            bucket.id
        );
        assert_eq!(
            buckets
                .resolve(&BucketRef::Name("media".into()))
                .await
                .unwrap()
                .id,
            bucket.id
        );

        // Read-only guard blocks writes (SEC-004)…
        buckets
            .update(
                bucket.id,
                UpdateBucketInput {
                    read_only: Some(true),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let blocked = buckets.assert_writable(bucket.id).await.unwrap_err();
        assert_eq!(blocked.code, ErrorCode::Forbidden);

        // …and delete guard is independent (SEC-005).
        buckets
            .update(
                bucket.id,
                UpdateBucketInput {
                    read_only: Some(false),
                    allow_delete: Some(false),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let no_delete = buckets.assert_deletable(bucket.id).await.unwrap_err();
        assert_eq!(no_delete.code, ErrorCode::Forbidden);
        assert!(buckets.assert_writable(bucket.id).await.is_ok());

        buckets.remove(&BucketRef::Id(bucket.id)).await.unwrap();
        assert!(buckets.list().await.unwrap().is_empty());
    }
}
