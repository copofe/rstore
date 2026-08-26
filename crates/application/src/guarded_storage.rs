//! The write-guard decorator the remote gateway issues for mutations.
//!
//! [`crate::gateway::RemoteGateway::writer`] and
//! [`RemoteGateway::deleter`] hand out adapters whose write methods check
//! the bucket's guard bits at call time, so proving the guard and holding
//! the execution adapter are the same object: an adapter that travels into
//! the transfer engine, the relay, or the remote copy entry carries the
//! policy with it. Reads always pass — the bits govern mutations only.
//!
//! Issuance refuses broken bits up front, so a guarded batch fails before
//! its first item runs; the decorator re-checks the same table on every
//! call and never assumes issuance validated. The guard wraps outside the
//! retry decorator, so a refusal never burns a retry attempt. Multipart
//! sessions carry no guard of their own: the door —
//! `create_multipart` / `adopt_multipart` — already passed it.
//!
//! Connection diagnostics probe credentials, not bucket policy, and use
//! the unguarded adapter by definition.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Bucket, Error, Result};
use rstore_storage_api::model::{
    CopyRequest, DeleteOutcome, ListOptions, ListPage, ObjectMetadata, PresignRequest,
    PresignedUrl, PutOptions, ReadRange,
};
use rstore_storage_api::{MultipartSession, Storage};

/// `Arc<dyn Storage>` whose write methods enforce the bucket's guard bits.
pub(crate) struct GuardedStorage {
    storage: Arc<dyn Storage>,
    bucket: String,
    read_only: bool,
    allow_delete: bool,
}

impl GuardedStorage {
    /// Issue the mutation adapter for a bucket: refuses read-only buckets
    /// up front (fail-fast) and wraps the given adapter in the guard.
    pub(crate) fn writer(storage: Arc<dyn Storage>, bucket: &Bucket) -> Result<Arc<dyn Storage>> {
        if bucket.config.read_only {
            return Err(Error::forbidden(format!(
                "bucket `{}` is read-only; mutations are refused",
                bucket.name
            )));
        }
        Ok(Self::guard(storage, bucket))
    }

    /// Issue the delete adapter for a bucket: refuses read-only buckets and
    /// buckets without `allow_delete`; the two flags are independent on
    /// purpose.
    pub(crate) fn deleter(storage: Arc<dyn Storage>, bucket: &Bucket) -> Result<Arc<dyn Storage>> {
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
        Ok(Self::guard(storage, bucket))
    }

    fn guard(storage: Arc<dyn Storage>, bucket: &Bucket) -> Arc<dyn Storage> {
        Arc::new(Self {
            storage,
            bucket: bucket.name.clone(),
            read_only: bucket.config.read_only,
            allow_delete: bucket.config.allow_delete,
        })
    }

    fn refuse_mutation(&self) -> Error {
        Error::forbidden(format!(
            "bucket `{}` is read-only; mutations are refused",
            self.bucket
        ))
    }

    fn refuse_delete_read_only(&self) -> Error {
        Error::forbidden(format!(
            "bucket `{}` is read-only; deletions are refused",
            self.bucket
        ))
    }

    fn refuse_delete_disabled(&self) -> Error {
        Error::forbidden(format!(
            "bucket `{}` does not allow deletions; enable them with bucket edit",
            self.bucket
        ))
    }
}

#[async_trait]
impl Storage for GuardedStorage {
    fn name(&self) -> &str {
        self.storage.name()
    }

    async fn list_buckets(&self) -> Result<Vec<rstore_storage_api::model::BucketSummary>> {
        self.storage.list_buckets().await
    }

    async fn list(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage> {
        self.storage.list(bucket, opts).await
    }

    async fn stat(&self, bucket: &str, key: &ObjectPath) -> Result<ObjectMetadata> {
        self.storage.stat(bucket, key).await
    }

    async fn read(
        &self,
        bucket: &str,
        key: &ObjectPath,
        range: Option<ReadRange>,
    ) -> Result<Bytes> {
        self.storage.read(bucket, key, range).await
    }

    async fn put(
        &self,
        bucket: &str,
        key: &ObjectPath,
        body: Bytes,
        opts: &PutOptions,
    ) -> Result<ObjectMetadata> {
        if self.read_only {
            return Err(self.refuse_mutation());
        }
        self.storage.put(bucket, key, body, opts).await
    }

    async fn copy(&self, req: &CopyRequest) -> Result<ObjectMetadata> {
        if self.read_only {
            return Err(self.refuse_mutation());
        }
        self.storage.copy(req).await
    }

    async fn delete(&self, bucket: &str, key: &ObjectPath) -> Result<DeleteOutcome> {
        if self.read_only {
            return Err(self.refuse_delete_read_only());
        }
        if !self.allow_delete {
            return Err(self.refuse_delete_disabled());
        }
        self.storage.delete(bucket, key).await
    }

    async fn create_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        opts: &PutOptions,
    ) -> Result<Box<dyn MultipartSession>> {
        if self.read_only {
            return Err(self.refuse_mutation());
        }
        self.storage.create_multipart(bucket, key, opts).await
    }

    async fn adopt_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        upload_id: &str,
    ) -> Result<Box<dyn MultipartSession>> {
        if self.read_only {
            return Err(self.refuse_mutation());
        }
        self.storage.adopt_multipart(bucket, key, upload_id).await
    }

    async fn presign(&self, req: &PresignRequest) -> Result<PresignedUrl> {
        self.storage.presign(req).await
    }
}
