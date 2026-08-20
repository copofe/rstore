//! Durable multipart checkpoints (STORE-012/013, UP-014/015).
//!
//! The port where transfer checkpoints persist between runs. It lives at the
//! storage seam — beside the multipart methods on [`Storage`] and the
//! [`UploadedPart`] vocabulary they share — so the SQLite adapter
//! (`rstore-persistence`) and the engine (`rstore-transfer`) both depend on
//! this crate and never on each other. The provider-side session lifetime and
//! its abort dispositions stay with the engine.

use rstore_domain::Result;

use crate::model::UploadedPart;

/// Where multipart checkpoints persist between runs (implemented over SQLite
/// by the persistence layer; kept abstract so consumers stay substitutable —
/// two adapters make the seam real: SQLite in production, the no-op in tests).
#[async_trait::async_trait]
pub trait MultipartStore: Send + Sync {
    /// The pending upload for (connection, bucket, key), with its completed
    /// parts — `None` when nothing is in flight.
    async fn pending(
        &self,
        connection: &str,
        bucket: &str,
        key: &str,
    ) -> Result<Option<PendingUpload>>;
    /// Record the upload id for a new multipart upload.
    async fn started(
        &self,
        connection: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        fingerprint: &FileFingerprint,
    ) -> Result<()>;
    /// Persist a completed part.
    async fn part_done(
        &self,
        connection: &str,
        bucket: &str,
        key: &str,
        part: &UploadedPart,
    ) -> Result<()>;
    /// Drop the checkpoint (upload completed or aborted).
    async fn cleared(&self, connection: &str, bucket: &str, key: &str) -> Result<()>;
}

/// A no-op checkpoint store (tests / adapters without resumable uploads).
#[derive(Debug, Default)]
pub struct NoopMultipartStore;

#[async_trait::async_trait]
impl MultipartStore for NoopMultipartStore {
    async fn pending(
        &self,
        _connection: &str,
        _bucket: &str,
        _key: &str,
    ) -> Result<Option<PendingUpload>> {
        Ok(None)
    }
    async fn started(
        &self,
        _connection: &str,
        _bucket: &str,
        _key: &str,
        _upload_id: &str,
        _fingerprint: &FileFingerprint,
    ) -> Result<()> {
        Ok(())
    }
    async fn part_done(
        &self,
        _connection: &str,
        _bucket: &str,
        _key: &str,
        _part: &UploadedPart,
    ) -> Result<()> {
        Ok(())
    }
    async fn cleared(&self, _connection: &str, _bucket: &str, _key: &str) -> Result<()> {
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

/// A resumable upload checkpoint.
#[derive(Debug, Clone)]
pub struct PendingUpload {
    pub upload_id: String,
    pub parts: Vec<UploadedPart>,
    pub fingerprint: FileFingerprint,
}
