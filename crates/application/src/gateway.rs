//! Remote access gateway: resolves a connection reference into a connected
//! [`Storage`] adapter.
//!
//! One place owns the chain *resolve config → build adapter*, so every
//! consumer (bucket, remote, transfer) reports identical, well-classified
//! failures for a broken connection.
use std::sync::Arc;

use bytes::Bytes;

use rstore_application_ports::ConnectionStore;
use rstore_domain::connection::ConnectionConfig;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, Result};
use rstore_storage_api::model::{
    CopyRequest, DeleteOutcome, ListOptions, ListPage, ObjectMetadata, PresignRequest,
    PresignedUrl, PutOptions, ReadRange,
};
use rstore_storage_api::multipart::MultipartSession;
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

    /// Read-only view of one bucket. Infallible: reads need no guard.
    pub fn reader(&self, storage: Arc<dyn Storage>, bucket: &rstore_domain::Bucket) -> Reader {
        Reader {
            storage,
            bucket: bucket.name.clone(),
        }
    }

    /// Mutation view of one bucket. `Err(Forbidden)` when the bucket is
    /// read-only — the guard is the door, not a convention on callers.
    pub fn writer(
        &self,
        storage: Arc<dyn Storage>,
        bucket: &rstore_domain::Bucket,
    ) -> Result<Writer> {
        if bucket.config.read_only {
            return Err(Error::forbidden(format!(
                "bucket `{}` is read-only; mutations are refused",
                bucket.name
            )));
        }
        Ok(Writer {
            storage,
            bucket: bucket.name.clone(),
        })
    }

    /// Delete view of one bucket. `Err(Forbidden)` unless deletions are
    /// allowed *and* the bucket is not read-only; the two flags are
    /// independent on purpose.
    pub fn deleter(
        &self,
        storage: Arc<dyn Storage>,
        bucket: &rstore_domain::Bucket,
    ) -> Result<Deleter> {
        if bucket.config.read_only {
            return Err(Error::forbidden(format!(
                "bucket `{}` is read-only; deletions are refused",
                bucket.name
            )));
        }
        if !bucket.config.allow_delete {
            return Err(Error::forbidden(format!(
                "bucket `{}` does not allow deletions; enable them with bucket edit",
                bucket.name
            )));
        }
        Ok(Deleter {
            storage,
            bucket: bucket.name.clone(),
        })
    }
}

/// Bucket-bound read handle: every call targets the bound bucket.
pub struct Reader {
    storage: Arc<dyn Storage>,
    bucket: String,
}

impl Reader {
    /// The bound bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Head a single object in the bound bucket.
    pub async fn stat(&self, key: &ObjectPath) -> Result<ObjectMetadata> {
        self.storage.stat(&self.bucket, key).await
    }

    /// Read object bytes, optionally a sub-range.
    pub async fn read(&self, key: &ObjectPath, range: Option<ReadRange>) -> Result<Bytes> {
        self.storage.read(&self.bucket, key, range).await
    }

    /// List objects under a prefix of the bound bucket.
    pub async fn list(&self, opts: &ListOptions) -> Result<ListPage> {
        self.storage.list(&self.bucket, opts).await
    }

    /// Presign a GET for a key in the bound bucket; the request's bucket is
    /// forced to the binding so a handle cannot sign for another bucket.
    pub async fn presign(&self, req: &PresignRequest) -> Result<PresignedUrl> {
        let mut req = req.clone();
        req.bucket = self.bucket.clone();
        self.storage.presign(&req).await
    }
}

/// Bucket-bound mutation handle; obtainable only through
/// [`RemoteGateway::writer`], so holding one proves the guard passed.
pub struct Writer {
    storage: Arc<dyn Storage>,
    bucket: String,
}

impl Writer {
    /// The bound bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Head a single object (conflict probes are reads on the target).
    pub async fn stat(&self, key: &ObjectPath) -> Result<ObjectMetadata> {
        self.storage.stat(&self.bucket, key).await
    }

    /// Upload bytes to a key in the bound bucket.
    pub async fn put(
        &self,
        key: &ObjectPath,
        body: Bytes,
        opts: &PutOptions,
    ) -> Result<ObjectMetadata> {
        self.storage.put(&self.bucket, key, body, opts).await
    }

    /// Server-side copy into the bound bucket from a source bucket on the
    /// same connection.
    pub async fn copy_from(
        &self,
        source_bucket: &str,
        source_key: &ObjectPath,
        target_key: &ObjectPath,
        source: &ObjectMetadata,
    ) -> Result<ObjectMetadata> {
        self.storage
            .copy(
                &CopyRequest {
                    source_bucket: source_bucket.to_string(),
                    source_key: source_key.clone(),
                    target_bucket: self.bucket.clone(),
                    target_key: target_key.clone(),
                },
                source,
            )
            .await
    }

    /// Begin a multipart upload in the bound bucket.
    pub async fn create_multipart(
        &self,
        key: &ObjectPath,
        opts: &PutOptions,
    ) -> Result<Box<dyn MultipartSession>> {
        self.storage.create_multipart(&self.bucket, key, opts).await
    }

    /// Adopt an in-flight multipart upload in the bound bucket.
    pub async fn adopt_multipart(
        &self,
        key: &ObjectPath,
        upload_id: &str,
    ) -> Result<Box<dyn MultipartSession>> {
        self.storage
            .adopt_multipart(&self.bucket, key, upload_id)
            .await
    }
}

/// Bucket-bound delete handle; obtainable only through
/// [`RemoteGateway::deleter`], so holding one proves both guards passed.
pub struct Deleter {
    storage: Arc<dyn Storage>,
    bucket: String,
}

impl Deleter {
    /// The bound bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Delete one object; a missing object is an outcome, not an error.
    pub async fn delete(&self, key: &ObjectPath) -> Result<DeleteOutcome> {
        self.storage.delete(&self.bucket, key).await
    }
}

/// Borrow the underlying adapter — the deliberate escape hatch for
/// transfer-crate machinery (engine, relay, move verification) that takes a
/// `&dyn Storage`. Every bypass is this explicit.
macro_rules! raw_storage {
    ($($t:ty),* $(,)?) => {$(
        impl $t {
            /// The decorated adapter behind this handle.
            pub fn storage(&self) -> &Arc<dyn Storage> {
                &self.storage
            }
        }
    )*};
}
raw_storage!(Reader, Writer, Deleter,);

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
