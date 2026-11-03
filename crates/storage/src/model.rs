//! Request/response value types shared by every [`crate::storage::Storage`]
//! implementation.
//!
//! The metadata fabrication policy has one owner per shape:
//! [`ObjectMetadata::after_put`] for single writes, [`ObjectMetadata::from_parts`]
//! for completed multipart uploads, and [`PresignedUrl::expiring`] for
//! presign expiry — adapters must not invent their own clocks or bases.

use rstore_domain::remote::ObjectPath;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// A half-open byte range `[start, end)` for ranged reads. `end: None`
/// reads to end of object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadRange {
    pub start: u64,
    pub end: Option<u64>,
}

impl ReadRange {
    /// `bytes=start-` (to EOF).
    pub fn from(start: u64) -> Self {
        Self { start, end: None }
    }
    /// `bytes=start-end` (inclusive end, per HTTP semantics).
    pub fn bounded(start: u64, end_inclusive: u64) -> Self {
        Self {
            start,
            end: Some(end_inclusive + 1),
        }
    }
    /// A `[start, end)` window — the natural spelling for chunk math
    /// (`offset..offset + want`), so callers stop hand-writing the struct
    /// and guessing the end's exclusivity.
    pub fn exclusive(start: u64, end_exclusive: u64) -> Self {
        Self {
            start,
            end: Some(end_exclusive),
        }
    }
}

/// Metadata for a single object, as reported by stat/list/write paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub last_modified: OffsetDateTime,
    pub storage_class: Option<String>,
    pub version_id: Option<String>,
}

impl ObjectMetadata {
    /// Metadata for an object the adapter just wrote. The fabrication policy,
    /// one owner: the write happened at `now`, the size is the body's, and
    /// the content type echoes what was sent (the provider stores exactly
    /// that). The storage contract pins the `last_modified` basis so every
    /// adapter agrees.
    pub fn after_put(
        key: &str,
        size: u64,
        etag: Option<String>,
        opts: &PutOptions,
        now: OffsetDateTime,
    ) -> Self {
        Self {
            key: key.to_string(),
            size,
            etag,
            content_type: opts.content_type.clone(),
            last_modified: now,
            storage_class: None,
            version_id: None,
        }
    }

    /// Metadata for a completed multipart upload: the size is the part sum,
    /// the completion result carries the new ETag, and the object was
    /// assembled at `now`.
    pub fn from_parts(
        key: &str,
        etag: Option<String>,
        parts: &[UploadedPart],
        now: OffsetDateTime,
    ) -> Self {
        Self {
            key: key.to_string(),
            size: parts.iter().map(|p| p.size).sum(),
            etag,
            content_type: None,
            last_modified: now,
            storage_class: None,
            version_id: None,
        }
    }
}

/// One entry in a listing (a leaf object). Folders are derived by the caller
/// from common prefixes and are never returned as objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub metadata: ObjectMetadata,
}

/// A bucket visible on the remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketSummary {
    pub name: String,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub created_at: Option<OffsetDateTime>,
}

/// A page of results from [`crate::storage::Storage::list`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPage {
    pub objects: Vec<ObjectEntry>,
    /// Common-prefix "folders" under the queried prefix.
    pub common_prefixes: Vec<String>,
    /// Opaque continuation token for the next page.
    pub continuation_token: Option<String>,
    pub is_truncated: bool,
}

/// Options for [`crate::storage::Storage::list`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListOptions {
    /// Restrict listing to this key prefix (`None` = bucket root).
    pub prefix: Option<String>,
    /// Delimiter used to fold "folders" (default `/`).
    pub delimiter: Option<String>,
    /// Server-side continuation token from a previous page.
    pub continuation_token: Option<String>,
    /// Maximum keys per page (provider may cap this).
    pub max_keys: Option<u32>,
}

/// Options for [`crate::storage::Storage::put`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PutOptions {
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    /// User-defined metadata pairs stored alongside the object.
    pub metadata: Vec<(String, String)>,
}

/// Content-type guess from a local file's extension.
///
/// The put-side metadata concern lives beside [`PutOptions`]: a tiny fixed
/// table keeps the crate dependency-free and callers can always override
/// explicitly. Unknown extensions map to `application/octet-stream`.
pub fn guess_content_type(path: &std::path::Path) -> String {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    let mime = match ext.as_str() {
        "txt" | "md" | "log" => "text/plain",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" => "text/javascript",
        "json" => "application/json",
        "xml" => "application/xml",
        "csv" => "text/csv",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "mp4" => "video/mp4",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        _ => "application/octet-stream",
    };
    mime.to_string()
}

/// A copy request. The caller supplies the source's metadata alongside the
/// locators — server-side copy needs the source size to choose its strategy
/// (single-action versus part copy), and the target's metadata is derived
/// from it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyRequest {
    pub source_bucket: String,
    pub source_key: ObjectPath,
    pub target_bucket: String,
    pub target_key: ObjectPath,
}

/// Outcome of a single-object delete.
///
/// `deleted: false` means the provider reports the object was already gone —
/// not an error.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeleteOutcome {
    pub deleted: bool,
    pub version_id: Option<String>,
}

/// A presigned URL request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignRequest {
    pub bucket: String,
    pub key: String,
    /// Method to sign (`GET` or `PUT`).
    pub method: PresignMethod,
    /// Time-to-live in seconds.
    pub ttl_seconds: u64,
}

/// HTTP method a presigned URL is valid for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PresignMethod {
    Get,
    Put,
}

/// A generated presigned URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignedUrl {
    pub url: String,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    pub method: PresignMethod,
}

impl PresignedUrl {
    /// A URL expiring at `now + ttl` — the one expiry basis; the storage
    /// contract pins it so adapters cannot drift to a different clock anchor.
    pub fn expiring(
        url: String,
        method: PresignMethod,
        ttl_seconds: u64,
        now: OffsetDateTime,
    ) -> Self {
        Self {
            url,
            expires_at: now + time::Duration::seconds(ttl_seconds as i64),
            method,
        }
    }
}

/// One uploaded part of a multipart upload: its number and the provider ETag
/// required to complete the upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadedPart {
    /// 1-based part number.
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
}
