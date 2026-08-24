//! The [`Storage`] facade over provider capabilities.

use async_trait::async_trait;
use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;

use crate::model::{
    BucketSummary, CopyRequest, DeleteOutcome, ListOptions, ListPage, ObjectMetadata,
    PresignRequest, PresignedUrl, PutOptions, ReadRange,
};
use crate::multipart::MultipartSession;

/// Object-storage adapter contract.
///
/// Every method maps failures into [`rstore_domain::Error`] using stable
/// [`rstore_domain::ErrorCode`]s so the CLI can produce stable exit codes.
/// Object keys are
/// the validating [`ObjectPath`] newtype end to end — the empty-key rejection
/// in `ObjectPath::new` is enforced at this interface, not hoped for at every
/// call site. Listing prefixes stay plain strings: a prefix may be empty (the
/// bucket root) and is never an object address.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Human-readable adapter name (e.g. `"s3"`, `"fake"`).
    fn name(&self) -> &str;

    /// List buckets visible to these credentials. Providers or policies that
    /// forbid the call surface a typed error rather than an empty list.
    async fn list_buckets(&self) -> Result<Vec<BucketSummary>>;

    /// List objects and common prefixes under a bucket.
    async fn list(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage>;

    /// Head/stat a single object.
    async fn stat(&self, bucket: &str, key: &ObjectPath) -> Result<ObjectMetadata>;

    /// Fetch object bytes, optionally a sub-range.
    async fn read(&self, bucket: &str, key: &ObjectPath, range: Option<ReadRange>)
        -> Result<Bytes>;

    /// Upload object bytes.
    async fn put(
        &self,
        bucket: &str,
        key: &ObjectPath,
        body: Bytes,
        opts: &PutOptions,
    ) -> Result<ObjectMetadata>;

    /// Copy an object. `source` is the caller's stat of the source —
    /// adapters with server-side copy need its size to pick a strategy and
    /// derive the target's metadata from it.
    ///
    /// Deliberately no default implementation: the obvious fallback
    /// (`read(None)` into memory, then `put`) silently buffers whole objects
    /// and is an OOM trap on real sizes. Every adapter picks a strategy —
    /// server-side copy, single-shot vs part-copy by size, or an explicit
    /// in-memory shortcut for test doubles.
    async fn copy(&self, req: &CopyRequest, source: &ObjectMetadata) -> Result<ObjectMetadata>;

    /// Delete a single object; a missing object is an outcome, not an error.
    async fn delete(&self, bucket: &str, key: &ObjectPath) -> Result<DeleteOutcome>;

    /// Begin a multipart upload. The returned session owns the part / copy /
    /// complete / abort lifetime — the protocol lives behind the session
    /// trait, not as flat methods here, so every consumer (engine, relay,
    /// server-side part copy) shares one state machine and one set of abort
    /// dispositions.
    async fn create_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        opts: &PutOptions,
    ) -> Result<Box<dyn MultipartSession>>;

    /// Bind a session to an already in-flight upload (a resumed checkpoint's
    /// id). No provider call: adapters construct the same session type
    /// around the given id.
    async fn adopt_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        upload_id: &str,
    ) -> Result<Box<dyn MultipartSession>>;

    /// Generate a presigned URL.
    async fn presign(&self, req: &PresignRequest) -> Result<PresignedUrl>;
}
