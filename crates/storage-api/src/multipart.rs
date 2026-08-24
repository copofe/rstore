//! The provider multipart protocol.
//!
//! Two ports live here. [`MultipartSession`] is the provider-side lifetime:
//! one session type owns part/complete/abort and the abort dispositions,
//! reached through the session factories on [`crate::storage::Storage`]
//! (`create_multipart`
//! and `adopt_multipart`) — the protocol has one home below every consumer.
//! [`MultipartStore`] is where transfer checkpoints persist between runs;
//! the SQLite adapter (`rstore-persistence`) and the engine
//! (`rstore-transfer`) both depend on this crate and never on each other.
//! Sessions never touch checkpoints.

use bytes::Bytes;
use rstore_domain::id::ConnectionId;
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
    /// The provider upload id — for checkpoint bookkeeping
    /// ([`MultipartStore::started`]).
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

/// Where multipart checkpoints persist between runs (implemented over SQLite
/// by the persistence layer; kept abstract so consumers stay substitutable —
/// two adapters make the seam real: SQLite in production, the no-op in tests).
///
/// All methods key on `(connection, bucket, key)`; the connection is the
/// typed local identity, the bucket the provider-facing name.
#[async_trait::async_trait]
pub trait MultipartStore: Send + Sync {
    /// The pending upload for (connection, bucket, key), with its completed
    /// parts — `None` when nothing is in flight. The connection is the typed
    /// local identity (checkpoints survive across runs and must never attach
    /// to a different endpoint by string coincidence); the bucket stays the
    /// provider-facing name the remote session calls use.
    async fn pending(
        &self,
        connection: ConnectionId,
        bucket: &str,
        key: &str,
    ) -> Result<Option<PendingUpload>>;
    /// Record the upload id for a new multipart upload.
    async fn started(
        &self,
        connection: ConnectionId,
        bucket: &str,
        key: &str,
        upload_id: &str,
        fingerprint: &FileFingerprint,
    ) -> Result<()>;
    /// Persist a completed part.
    async fn part_done(
        &self,
        connection: ConnectionId,
        bucket: &str,
        key: &str,
        part: &UploadedPart,
    ) -> Result<()>;
    /// Drop the checkpoint (upload completed or aborted).
    async fn cleared(&self, connection: ConnectionId, bucket: &str, key: &str) -> Result<()>;
}

/// A no-op checkpoint store (tests / adapters without resumable uploads).
///
/// `pending` always reports `None`, so consumers never attempt a resume.
#[derive(Debug, Default)]
pub struct NoopMultipartStore;

#[async_trait::async_trait]
impl MultipartStore for NoopMultipartStore {
    async fn pending(
        &self,
        _connection: ConnectionId,
        _bucket: &str,
        _key: &str,
    ) -> Result<Option<PendingUpload>> {
        Ok(None)
    }
    async fn started(
        &self,
        _connection: ConnectionId,
        _bucket: &str,
        _key: &str,
        _upload_id: &str,
        _fingerprint: &FileFingerprint,
    ) -> Result<()> {
        Ok(())
    }
    async fn part_done(
        &self,
        _connection: ConnectionId,
        _bucket: &str,
        _key: &str,
        _part: &UploadedPart,
    ) -> Result<()> {
        Ok(())
    }
    async fn cleared(&self, _connection: ConnectionId, _bucket: &str, _key: &str) -> Result<()> {
        Ok(())
    }
}

/// Identity of the local source used to start a multipart upload. A pending
/// provider upload is reusable only while both values still match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileFingerprint {
    pub size: u64,
    /// Nanoseconds since the Unix epoch when the platform exposes mtime.
    pub modified_ns: Option<i64>,
}

impl FileFingerprint {
    /// Derivation lives beside the type: the (size, mtime) pair of a local
    /// file's metadata, in the one spelling every consumer compares against.
    pub fn of(metadata: &std::fs::Metadata) -> Self {
        let modified_ns = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|duration| i64::try_from(duration.as_nanos()).ok());
        Self {
            size: metadata.len(),
            modified_ns,
        }
    }
}

/// A resumable upload checkpoint: the provider upload id, the parts already
/// on the provider, and the source fingerprint the upload was started with.
#[derive(Debug, Clone)]
pub struct PendingUpload {
    pub upload_id: String,
    pub parts: Vec<UploadedPart>,
    pub fingerprint: FileFingerprint,
}
