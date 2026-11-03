//! Records the store hands to use cases that are not domain entities:
//! summaries with counts, index-walk commits, and multipart checkpoints.

use rstore_domain::id::{BucketId, IndexRunId};
use rstore_domain::remote::RemoteObject;
use rstore_domain::workspace::{IndexRunState, Tag, Workspace};
use rstore_storage::UploadedPart;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// A workspace row with an application-facing summary, not a SQLite row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSummary {
    pub workspace: Workspace,
    pub bucket_count: u64,
}

/// A tag row with an application-facing association count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagSummary {
    pub tag: Tag,
    pub object_count: u64,
}

/// A resumable index walk checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexCheckpoint {
    pub continuation_token: Option<String>,
    pub updated_at: OffsetDateTime,
}

/// One atomic index-page publication request.
#[derive(Debug, Clone)]
pub struct IndexPageCommit {
    pub run: IndexRunId,
    pub bucket: BucketId,
    pub objects: Vec<RemoteObject>,
    pub continuation_token: Option<String>,
    pub at: OffsetDateTime,
    pub objects_seen: u64,
    pub error_count: u64,
}

/// One atomic index-run completion request.
#[derive(Debug, Clone)]
pub struct IndexRunCommit {
    pub run: IndexRunId,
    pub state: IndexRunState,
    pub finished_at: Option<OffsetDateTime>,
    pub objects_seen: u64,
    pub error_count: u64,
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
