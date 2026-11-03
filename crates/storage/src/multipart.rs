//! The provider multipart protocol.
//!
//! [`MultipartSession`] is the provider-side lifetime: one session type owns
//! part/complete/abort and the abort dispositions, reached through the session
//! factories on [`crate::storage::Storage`] (`create_multipart` and
//! `adopt_multipart`). Sessions never touch checkpoints; those persist in the
//! engine's store.

use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, Result};

use crate::model::{ObjectMetadata, ReadRange, UploadedPart};

/// One in-flight provider multipart upload.
///
/// Failure policy — three named dispositions, one home:
/// - [`MultipartSession::abandon`] — best-effort, warn-only teardown for
///   callers with no resume path; consumes the session.
/// - [`MultipartSession::abort`] — strict: a failed abort surfaces, so the
///   stale-checkpoint path keeps its checkpoint and retries next run instead
///   of leaking the provider upload.
/// - [`MultipartSession::complete`] never aborts: a completion failure
///   leaves the provider upload in place so a checkpointed caller resumes;
///   callers without checkpoints abandon the session themselves.
///
/// The part source is a capability, not a method every adapter must carry:
/// [`MultipartSession::copy_part`] defaults to `CapabilityUnsupported`.
/// Adapters with server-side part copy (S3's `UploadPartCopy`) override it;
/// callers without the capability fall back to reading the source range and
/// pushing it through [`MultipartSession::upload_part`].
#[async_trait::async_trait]
pub trait MultipartSession: Send + Sync {
    /// The provider upload id — for checkpoint bookkeeping.
    fn upload_id(&self) -> &str;

    /// Upload one part (1-based `part_number`) from local bytes.
    async fn upload_part(&self, part_number: u32, body: Bytes) -> Result<UploadedPart>;

    /// Copy one part provider-side from `range` of the source object — no
    /// object bytes transit the local device.
    ///
    /// # Errors
    /// Defaults to `ErrorCode::CapabilityUnsupported`; adapters without
    /// server-side part copy keep the default. Implementations may require a
    /// bounded `range` (an open-ended end is not expressible on S3).
    async fn copy_part(
        &self,
        _part_number: u32,
        _source_bucket: &str,
        _source_key: &ObjectPath,
        _range: ReadRange,
    ) -> Result<UploadedPart> {
        Err(Error::unsupported("copy_part"))
    }

    /// Finalise the upload from the ordered completed parts. Never aborts on
    /// failure — a completion failure leaves the provider upload in place.
    async fn complete(&self, parts: &[UploadedPart]) -> Result<ObjectMetadata>;

    /// Strict abort: a failed abort *is* the error.
    async fn abort(&self) -> Result<()>;

    /// Best-effort teardown: warn-only, never escalating past the original
    /// error. Consumes the session.
    async fn abandon(self: Box<Self>) {
        if let Err(error) = self.abort().await {
            tracing::warn!(
                %error,
                upload_id = self.upload_id(),
                "could not abort multipart upload"
            );
        }
    }
}
