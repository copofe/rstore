//! Retry decorator around every [`Storage`] operation.
//!
//! Concrete adapters classify errors; this decorator retries exactly those
//! marked retryable, under one policy shared by flat operations and multipart
//! sessions alike (`RetryingSession`).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, Result};
use rstore_storage_api::model::{
    BucketSummary, CopyRequest, DeleteOutcome, ListOptions, ListPage, ObjectMetadata,
    PresignRequest, PresignedUrl, PutOptions, ReadRange, UploadedPart,
};
use rstore_storage_api::{MultipartSession, Storage};

use crate::{CancellationToken, RetryPolicy};

/// One bounded, cancellation-aware retry loop — shared by the flat decorator
/// and the session wrapper so the policy exists once.
async fn retry_with<T, F, Fut>(
    policy: &RetryPolicy,
    cancel: &CancellationToken,
    op: &'static str,
    mut call: F,
) -> Result<T>
where
    T: Send,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<T>> + Send,
{
    let mut attempt = 1;
    loop {
        cancel.check()?;
        match call().await {
            Ok(value) => return Ok(value),
            Err(error) if error.is_retryable() && attempt < policy.max_attempts => {
                let backoff_ms = policy.backoff_ms(attempt - 1);
                tracing::warn!(
                    op,
                    attempt,
                    backoff_ms,
                    error = %error,
                    "retrying storage operation"
                );
                // Race the wait against cancellation: Ctrl+C must not sit
                // out a capped 30 s backoff before the run notices.
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                    _ = cancel.cancelled() => return Err(Error::cancelled()),
                }
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Adds bounded, cancellation-aware retries to an arbitrary storage adapter.
pub struct RetryingStorage {
    inner: Arc<dyn Storage>,
    policy: RetryPolicy,
    cancel: CancellationToken,
}

impl RetryingStorage {
    /// Wraps `inner` with `policy` retries and cooperative cancellation.
    pub fn new(inner: Arc<dyn Storage>, policy: RetryPolicy, cancel: CancellationToken) -> Self {
        Self {
            inner,
            policy,
            cancel,
        }
    }

    async fn retry<T, F, Fut>(&self, op: &'static str, call: F) -> Result<T>
    where
        T: Send,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        retry_with(&self.policy, &self.cancel, op, call).await
    }
}

#[async_trait]
impl Storage for RetryingStorage {
    async fn list_buckets(&self) -> Result<Vec<BucketSummary>> {
        self.retry("list_buckets", || self.inner.list_buckets())
            .await
    }

    async fn list(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage> {
        self.retry("list", || self.inner.list(bucket, opts)).await
    }

    async fn stat(&self, bucket: &str, key: &ObjectPath) -> Result<ObjectMetadata> {
        self.retry("stat", || self.inner.stat(bucket, key)).await
    }

    async fn read(
        &self,
        bucket: &str,
        key: &ObjectPath,
        range: Option<ReadRange>,
    ) -> Result<Bytes> {
        self.retry("read", || self.inner.read(bucket, key, range))
            .await
    }

    async fn put(
        &self,
        bucket: &str,
        key: &ObjectPath,
        body: Bytes,
        opts: &PutOptions,
    ) -> Result<ObjectMetadata> {
        self.retry("put", || self.inner.put(bucket, key, body.clone(), opts))
            .await
    }

    async fn copy(&self, req: &CopyRequest) -> Result<ObjectMetadata> {
        self.retry("copy", || self.inner.copy(req)).await
    }

    async fn delete(&self, bucket: &str, key: &ObjectPath) -> Result<DeleteOutcome> {
        self.retry("delete", || self.inner.delete(bucket, key))
            .await
    }

    async fn create_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        opts: &PutOptions,
    ) -> Result<Box<dyn MultipartSession>> {
        let inner = self
            .retry("create_multipart", || {
                self.inner.create_multipart(bucket, key, opts)
            })
            .await?;
        Ok(Box::new(RetryingSession {
            inner,
            policy: self.policy,
            cancel: self.cancel.clone(),
        }))
    }

    async fn adopt_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        upload_id: &str,
    ) -> Result<Box<dyn MultipartSession>> {
        // Binding a session to an existing id is no provider call; the
        // session operations themselves retry below.
        let inner = self.inner.adopt_multipart(bucket, key, upload_id).await?;
        Ok(Box::new(RetryingSession {
            inner,
            policy: self.policy,
            cancel: self.cancel.clone(),
        }))
    }

    async fn presign(&self, req: &PresignRequest) -> Result<PresignedUrl> {
        self.retry("presign", || self.inner.presign(req)).await
    }
}

/// Session wrapper: every session operation retries under the same policy as
/// the flat operations — part-level retry semantics are unchanged by the
/// session move.
struct RetryingSession {
    inner: Box<dyn MultipartSession>,
    policy: RetryPolicy,
    cancel: CancellationToken,
}

#[async_trait]
impl MultipartSession for RetryingSession {
    fn upload_id(&self) -> &str {
        self.inner.upload_id()
    }

    async fn upload_part(&self, part_number: u32, body: Bytes) -> Result<UploadedPart> {
        retry_with(&self.policy, &self.cancel, "upload_part", || {
            self.inner.upload_part(part_number, body.clone())
        })
        .await
    }

    async fn complete(&self, parts: &[UploadedPart]) -> Result<ObjectMetadata> {
        retry_with(&self.policy, &self.cancel, "complete_multipart", || {
            self.inner.complete(parts)
        })
        .await
    }

    async fn abort(&self) -> Result<()> {
        retry_with(&self.policy, &self.cancel, "abort_multipart", || {
            self.inner.abort()
        })
        .await
    }

    async fn abandon(self: Box<Self>) {
        // Warn-only teardown, never escalating past the original error — the
        // session port's disposition, with the abort itself still retried
        // like any other operation.
        if let Err(error) = retry_with(&self.policy, &self.cancel, "abort_multipart", || {
            self.inner.abort()
        })
        .await
        {
            tracing::warn!(
                %error,
                upload_id = self.inner.upload_id(),
                "could not abort multipart upload"
            );
        }
    }
}
