//! The [`Storage`] facade over provider capabilities (CORE-004).

use async_trait::async_trait;
use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;

use crate::model::{
    BucketSummary, CopyRequest, DeleteOutcome, ListOptions, ListPage, ObjectMetadata,
    PresignRequest, PresignedUrl, PutOptions, ReadRange, UploadedPart,
};

/// Object-storage adapter contract.
///
/// Every method maps failures into [`rstore_domain::Error`] using stable
/// [`ErrorCode`]s so the CLI can produce stable exit codes. Object keys are
/// the validating [`ObjectPath`] newtype end to end — the empty-key rejection
/// in `ObjectPath::new` is enforced at this interface, not hoped for at every
/// call site. Listing prefixes stay plain strings: a prefix may be empty (the
/// bucket root) and is never an object address.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Human-readable adapter name (e.g. `"s3"`, `"fake"`).
    fn name(&self) -> &str;

    /// List buckets visible to these credentials (STORE-008). Providers or
    /// policies that forbid the call surface a typed error rather than an
    /// empty list.
    async fn list_buckets(&self) -> Result<Vec<BucketSummary>>;

    /// List objects and common prefixes under a bucket (STORE-009).
    async fn list(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage>;

    /// Head/stat a single object (STORE-010).
    async fn stat(&self, bucket: &str, key: &ObjectPath) -> Result<ObjectMetadata>;

    /// Fetch object bytes, optionally a sub-range (STORE-011).
    async fn read(&self, bucket: &str, key: &ObjectPath, range: Option<ReadRange>)
        -> Result<Bytes>;

    /// Upload object bytes (STORE-012).
    async fn put(
        &self,
        bucket: &str,
        key: &ObjectPath,
        body: Bytes,
        opts: &PutOptions,
    ) -> Result<ObjectMetadata>;

    /// Copy an object (STORE-016). `source` is the caller's stat of the
    /// source — adapters with server-side copy need its size to pick a
    /// strategy and derive the target's metadata from it. The default falls
    /// back to read+put through the local device, preserving the source's
    /// content type, when the provider lacks server-side copy (PRD §10.10.3).
    async fn copy(&self, req: &CopyRequest, source: &ObjectMetadata) -> Result<ObjectMetadata> {
        let bytes = self.read(&req.source_bucket, &req.source_key, None).await?;
        self.put(
            &req.target_bucket,
            &req.target_key,
            bytes,
            &PutOptions {
                content_type: source.content_type.clone(),
                ..PutOptions::default()
            },
        )
        .await
    }

    /// Delete a single object (STORE-014).
    async fn delete(&self, bucket: &str, key: &ObjectPath) -> Result<DeleteOutcome>;

    /// Begin a multipart upload, returning the provider upload id.
    async fn create_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        opts: &PutOptions,
    ) -> Result<String>;

    /// Upload one part (1-based `part_number`), returning its ETag.
    async fn upload_part(
        &self,
        bucket: &str,
        key: &ObjectPath,
        upload_id: &str,
        part_number: u32,
        body: Bytes,
    ) -> Result<UploadedPart>;

    /// Finalise the upload from the ordered completed parts.
    async fn complete_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> Result<ObjectMetadata>;

    /// Abort an in-progress upload (provider-side cleanup).
    async fn abort_multipart(&self, bucket: &str, key: &ObjectPath, upload_id: &str) -> Result<()>;

    /// Generate a presigned URL (STORE-017).
    async fn presign(&self, req: &PresignRequest) -> Result<PresignedUrl>;
}
