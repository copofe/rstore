//! Retry decorator for every storage operation (STORE-024 / JOB-009).
//!
//! Keeping retries at the [`Storage`] seam means transfer, copy, listing,
//! indexing, diagnostics and presigning all share the same policy. Concrete
//! adapters classify errors; this decorator only retries those marked
//! retryable.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;
use rstore_storage_api::{
    BucketSummary, CopyRequest, DeleteOutcome, ListOptions, ListPage, ObjectMetadata,
    PresignRequest, PresignedUrl, PutOptions, ReadRange, Storage, UploadedPart,
};

use crate::{CancellationToken, RetryPolicy};

/// Adds bounded, cancellation-aware retries to an arbitrary storage adapter.
pub struct RetryingStorage {
    inner: Arc<dyn Storage>,
    policy: RetryPolicy,
    cancel: CancellationToken,
}

impl RetryingStorage {
    pub fn new(inner: Arc<dyn Storage>, policy: RetryPolicy, cancel: CancellationToken) -> Self {
        Self {
            inner,
            policy,
            cancel,
        }
    }

    async fn retry<T, F, Fut>(&self, op: &'static str, mut call: F) -> Result<T>
    where
        T: Send,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        let mut attempt = 1;
        loop {
            self.cancel.check()?;
            match call().await {
                Ok(value) => return Ok(value),
                Err(error) if error.is_retryable() && attempt < self.policy.max_attempts => {
                    let backoff_ms = self.policy.backoff_ms(attempt - 1);
                    tracing::warn!(
                        op,
                        attempt,
                        backoff_ms,
                        error = %error,
                        "retrying storage operation"
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[async_trait]
impl Storage for RetryingStorage {
    fn name(&self) -> &str {
        self.inner.name()
    }

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

    async fn copy(&self, req: &CopyRequest, source: &ObjectMetadata) -> Result<ObjectMetadata> {
        self.retry("copy", || self.inner.copy(req, source)).await
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
    ) -> Result<String> {
        self.retry("create_multipart", || {
            self.inner.create_multipart(bucket, key, opts)
        })
        .await
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &ObjectPath,
        upload_id: &str,
        part_number: u32,
        body: Bytes,
    ) -> Result<UploadedPart> {
        self.retry("upload_part", || {
            self.inner
                .upload_part(bucket, key, upload_id, part_number, body.clone())
        })
        .await
    }

    async fn complete_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> Result<ObjectMetadata> {
        self.retry("complete_multipart", || {
            self.inner.complete_multipart(bucket, key, upload_id, parts)
        })
        .await
    }

    async fn abort_multipart(&self, bucket: &str, key: &ObjectPath, upload_id: &str) -> Result<()> {
        self.retry("abort_multipart", || {
            self.inner.abort_multipart(bucket, key, upload_id)
        })
        .await
    }

    async fn presign(&self, req: &PresignRequest) -> Result<PresignedUrl> {
        self.retry("presign", || self.inner.presign(req)).await
    }
}

#[cfg(test)]
mod tests {
    use rstore_domain::remote::ObjectPath;
    use rstore_domain::Error;
    use rstore_test_support::{FakeStorage, StorageMethod};

    use super::*;

    fn storage_with_stat_faults(failures: usize, error: impl Fn() -> Error) -> Arc<FakeStorage> {
        let storage = Arc::new(FakeStorage::new());
        storage.seed("bucket", "key", b"value", None);
        for _ in 0..failures {
            storage.fail_next(StorageMethod::Stat, error());
        }
        storage
    }

    fn policy(max_attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        }
    }

    #[tokio::test]
    async fn retries_retryable_failures_until_success() {
        let flaky = storage_with_stat_faults(2, || Error::network("injected transient failure"));
        let storage = RetryingStorage::new(flaky, policy(3), CancellationToken::new());

        let metadata = storage
            .stat("bucket", &ObjectPath::new("key").unwrap())
            .await
            .unwrap();

        assert_eq!(metadata.size, 5);
    }

    #[tokio::test]
    async fn stops_at_the_attempt_limit() {
        let flaky = storage_with_stat_faults(2, || Error::network("injected transient failure"));
        let storage = RetryingStorage::new(flaky, policy(2), CancellationToken::new());

        let error = storage
            .stat("bucket", &ObjectPath::new("key").unwrap())
            .await
            .unwrap_err();

        assert!(error.is_retryable());
    }

    #[tokio::test]
    async fn does_not_retry_permanent_failures() {
        let flaky =
            storage_with_stat_faults(1, || Error::invalid_input("injected permanent failure"));
        let storage = RetryingStorage::new(flaky, policy(3), CancellationToken::new());

        let error = storage
            .stat("bucket", &ObjectPath::new("key").unwrap())
            .await
            .unwrap_err();

        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn cancellation_prevents_the_first_attempt() {
        let flaky = storage_with_stat_faults(0, || Error::network("unused"));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let storage = RetryingStorage::new(flaky, policy(3), cancel);

        let error = storage
            .stat("bucket", &ObjectPath::new("key").unwrap())
            .await
            .unwrap_err();

        assert_eq!(error.code, rstore_domain::ErrorCode::Cancelled);
    }
}
