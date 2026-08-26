//! Remote object model and bucket-relative path value object.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::Error;
use crate::id::{BucketId, ConnectionId, RemoteObjectId};
use crate::workspace::ObjectState;

/// A normalised object path relative to a bucket root.
///
/// Validation contract: the constructor rejects empty input (including a bare
/// `/`), strips all leading slashes, and stores the remainder verbatim — no
/// other normalisation happens, and no bucket name is embedded. The S3
/// `prefix`/`delimiter` distinction is an adapter-internal concern, so the
/// stored string is directly usable as an S3 object key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectPath(String);

impl ObjectPath {
    /// Create a path, normalising leading slashes and rejecting empty paths.
    ///
    /// # Errors
    /// Returns [`Error::invalid_input`] when the raw value is empty or
    /// consists only of leading slashes.
    pub fn new(path: impl Into<String>) -> Result<Self, Error> {
        let raw = path.into();
        if raw.is_empty() {
            return Err(Error::invalid_input("object path is empty"));
        }
        let trimmed = raw.trim_start_matches('/');
        if trimmed.is_empty() {
            return Err(Error::invalid_input("object path is empty"));
        }
        Ok(Self(trimmed.to_owned()))
    }

    /// The bucket-relative key usable directly as the S3 object key.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parent directory path, or `None` at root level.
    pub fn parent(&self) -> Option<String> {
        self.0.rfind('/').map(|i| self.0[..i].to_string())
    }

    /// Trailing file name component.
    pub fn file_name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or(&self.0)
    }

    /// Lexical file extension without the leading dot, if any. A leading-dot
    /// name like `.gitignore` has no extension: its whole name is the stem.
    pub fn extension(&self) -> Option<&str> {
        self.file_name()
            .rsplit_once('.')
            .filter(|(stem, _)| !stem.is_empty())
            .map(|(_, ext)| ext)
    }
}

/// A single remote object as known to the catalog or returned live by a stat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteObject {
    pub id: RemoteObjectId,
    pub connection_id: ConnectionId,
    pub bucket_id: BucketId,
    pub path: ObjectPath,
    pub size: u64,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub last_modified: OffsetDateTime,
    pub storage_class: Option<String>,
    /// Optional version id for versioning-enabled buckets.
    pub version_id: Option<String>,
    /// When the index last confirmed this object remotely.
    #[serde(with = "time::serde::rfc3339")]
    pub confirmed_at: OffsetDateTime,
    /// Catalog freshness state.
    pub state: ObjectState,
}

impl RemoteObject {
    /// Best-effort classification for filtering: the stored content type
    /// wins; without one, the extension classifies directly.
    pub fn type_hint(&self) -> FileType {
        match self.content_type.as_deref() {
            Some(ct) => FileType::from_mime(ct),
            None => self
                .path
                .extension()
                .map(ext_to_file_type)
                .unwrap_or(FileType::Other),
        }
    }
}

fn ext_to_file_type(ext: &str) -> FileType {
    match ext.to_ascii_lowercase().as_str() {
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "svg" | "bmp" => FileType::Image,
        "mp4" | "mov" | "avi" | "mkv" | "webm" => FileType::Video,
        "mp3" | "wav" | "flac" | "ogg" | "m4a" => FileType::Audio,
        "pdf" | "doc" | "docx" | "ppt" | "pptx" | "xls" | "xlsx" | "txt" | "md" => {
            FileType::Document
        }
        "zip" | "tar" | "gz" | "7z" | "rar" => FileType::Archive,
        _ => FileType::Other,
    }
}

/// Coarse file classification used by filters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    Image,
    Video,
    Audio,
    Document,
    Archive,
    #[default]
    Other,
}

impl FileType {
    /// Persistence name of the classification (`remote_objects.file_type`).
    /// This list — not any SQL — is the one place extensions and MIME
    /// prefixes map to kinds.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Video => "video",
            Self::Audio => "audio",
            Self::Document => "document",
            Self::Archive => "archive",
            Self::Other => "other",
        }
    }

    /// Classify a MIME type; unrecognised types fall back to [`FileType::Other`].
    pub fn from_mime(mime: &str) -> Self {
        if mime.starts_with("image/") || mime == "image" {
            Self::Image
        } else if mime.starts_with("video/") || mime == "video" {
            Self::Video
        } else if mime.starts_with("audio/") || mime == "audio" {
            Self::Audio
        } else if mime.contains("pdf") || mime.contains("document") || mime == "application/pdf" {
            Self::Document
        } else if mime.contains("zip") || mime.contains("archive") || mime == "archive" {
            Self::Archive
        } else {
            Self::Other
        }
    }
}
