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

    async fn copy(&self, req: &CopyRequest, source: &ObjectMetadata) -> Result<ObjectMetadata> {
        if self.read_only {
            return Err(self.refuse_mutation());
        }
        self.storage.copy(req, source).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_domain::bucket::{Bucket, BucketConfig};
    use rstore_domain::id::{BucketId, ConnectionId};
    use rstore_storage_api::model::PresignMethod;
    use rstore_test_support::FakeStorage;

    fn now() -> time::OffsetDateTime {
        time::OffsetDateTime::UNIX_EPOCH
    }

    fn bucket_with(read_only: bool, allow_delete: bool) -> Bucket {
        Bucket {
            id: BucketId::new_random(),
            connection_id: ConnectionId::new_random(),
            name: "media".into(),
            config: BucketConfig {
                read_only,
                allow_delete,
                ..BucketConfig::default()
            },
            created_at: now(),
            updated_at: now(),
        }
    }

    fn key(path: &str) -> ObjectPath {
        ObjectPath::new(path).unwrap()
    }

    fn guarded(
        storage: &Arc<FakeStorage>,
        read_only: bool,
        allow_delete: bool,
    ) -> Arc<dyn Storage> {
        GuardedStorage::guard(
            storage.clone() as Arc<dyn Storage>,
            &bucket_with(read_only, allow_delete),
        )
    }

    #[test]
    fn issuance_refuses_read_only_for_writer_and_deleter() {
        let storage: Arc<dyn Storage> = Arc::new(FakeStorage::new());
        let read_only = bucket_with(true, true);

        let err = match GuardedStorage::writer(storage.clone(), &read_only) {
            Err(e) => e,
            Ok(_) => panic!("read-only bucket must refuse a writer"),
        };
        assert_eq!(err.code, rstore_domain::ErrorCode::Forbidden);
        assert_eq!(
            err.message,
            "bucket `media` is read-only; mutations are refused"
        );

        let err = match GuardedStorage::deleter(storage, &read_only) {
            Err(e) => e,
            Ok(_) => panic!("read-only bucket must refuse a deleter"),
        };
        assert_eq!(err.code, rstore_domain::ErrorCode::Forbidden);
    }

    #[test]
    fn issuance_refuses_delete_without_allow_delete() {
        let storage: Arc<dyn Storage> = Arc::new(FakeStorage::new());
        let no_delete = bucket_with(false, false);

        // The two flags are independent: writes issue, deletions refuse.
        assert!(GuardedStorage::writer(storage.clone(), &no_delete).is_ok());
        let err = match GuardedStorage::deleter(storage, &no_delete) {
            Err(e) => e,
            Ok(_) => panic!("no-delete bucket must refuse a deleter"),
        };
        assert_eq!(err.code, rstore_domain::ErrorCode::Forbidden);
    }

    #[tokio::test]
    async fn read_only_refuses_every_write_at_call_time() {
        let fake = Arc::new(FakeStorage::new());
        fake.seed("media", "src.txt", b"data", None);
        let storage = guarded(&fake, true, true);

        let put = storage
            .put(
                "media",
                &key("a.txt"),
                Bytes::from_static(b"x"),
                &PutOptions::default(),
            )
            .await;
        assert_eq!(put.unwrap_err().code, rstore_domain::ErrorCode::Forbidden);

        let source = storage.stat("media", &key("src.txt")).await.unwrap();
        let copy = storage
            .copy(
                &CopyRequest {
                    source_bucket: "media".into(),
                    source_key: key("src.txt"),
                    target_bucket: "media".into(),
                    target_key: key("b.txt"),
                },
                &source,
            )
            .await;
        assert_eq!(copy.unwrap_err().code, rstore_domain::ErrorCode::Forbidden);

        let delete = storage.delete("media", &key("src.txt")).await;
        assert_eq!(
            delete.unwrap_err().code,
            rstore_domain::ErrorCode::Forbidden
        );

        let create = storage
            .create_multipart("media", &key("m.bin"), &PutOptions::default())
            .await;
        assert!(matches!(create, Err(ref e) if e.code == rstore_domain::ErrorCode::Forbidden));

        let adopt = storage
            .adopt_multipart("media", &key("m.bin"), "upload-id")
            .await;
        assert!(matches!(adopt, Err(ref e) if e.code == rstore_domain::ErrorCode::Forbidden));

        // The refusals never reached the adapter: nothing was written.
        let page = storage
            .list(
                "media",
                &ListOptions {
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page.objects.len(), 1, "only the seeded object exists");
    }

    #[tokio::test]
    async fn delete_requires_allow_delete_even_when_writable() {
        let fake = Arc::new(FakeStorage::new());
        fake.seed("media", "a.txt", b"x", None);
        let storage = guarded(&fake, false, false);

        // Writes pass; deletion is refused by its own flag.
        storage
            .put(
                "media",
                &key("b.txt"),
                Bytes::from_static(b"y"),
                &PutOptions::default(),
            )
            .await
            .unwrap();
        let err = storage.delete("media", &key("a.txt")).await.unwrap_err();
        assert_eq!(err.code, rstore_domain::ErrorCode::Forbidden);
        assert_eq!(
            err.message,
            "bucket `media` does not allow deletions; enable them with bucket edit"
        );

        // The write landed; the delete did not.
        storage.stat("media", &key("b.txt")).await.unwrap();
        storage.stat("media", &key("a.txt")).await.unwrap();
    }

    #[tokio::test]
    async fn reads_pass_under_read_only() {
        let fake = Arc::new(FakeStorage::new());
        fake.seed_bucket("media");
        fake.seed("media", "a.txt", b"data", None);
        let storage = guarded(&fake, true, true);

        let buckets = storage.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 1);

        let page = storage
            .list(
                "media",
                &ListOptions {
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page.objects.len(), 1);

        storage.stat("media", &key("a.txt")).await.unwrap();

        let bytes = storage.read("media", &key("a.txt"), None).await.unwrap();
        assert_eq!(bytes.as_ref(), b"data");

        let url = storage
            .presign(&PresignRequest {
                bucket: "media".into(),
                key: "a.txt".into(),
                method: PresignMethod::Get,
                ttl_seconds: 60,
            })
            .await
            .unwrap();
        assert!(!url.url.is_empty());
    }

    #[tokio::test]
    async fn multipart_sessions_ride_once_the_door_passed() {
        let fake = Arc::new(FakeStorage::new());
        fake.seed_bucket("media");
        let storage = guarded(&fake, false, true);

        // The door — create_multipart — passed the guard; the session's own
        // operations carry no guard of their own.
        let session = storage
            .create_multipart("media", &key("m.bin"), &PutOptions::default())
            .await
            .unwrap();
        let part = session
            .upload_part(1, Bytes::from_static(b"chunk"))
            .await
            .unwrap();
        let meta = session.complete(&[part]).await.unwrap();
        assert_eq!(meta.size, 5);
        storage.stat("media", &key("m.bin")).await.unwrap();
    }
}
