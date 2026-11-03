//! Plan validation and expansion for uploads and downloads: turns a user
//! plan into concrete [`EngineItem`]s before any storage endpoint is built,
//! so local validation errors never pay the cost of adapter construction.

use std::path::{Path, PathBuf};

use crate::transfer::{DownloadRequest, UploadRequest};
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{ConnectionId, Error, Result};
use rstore_storage::Storage;

use crate::connection::ConnectionRef;

/// A pending upload: local sources into one bucket under a key prefix.
#[derive(Debug, Clone)]
pub struct UploadPlan {
    pub connection: ConnectionRef,
    pub bucket: String,

    /// Key prefix applied to every uploaded object.
    pub prefix: String,
    /// Local files or directories (directories only with `recursive`).
    pub sources: Vec<PathBuf>,
    pub conflict: ConflictPolicy,
    pub recursive: bool,
}

/// A pending download: remote objects under a prefix onto a local target.
#[derive(Debug, Clone)]
pub struct DownloadPlan {
    pub connection: ConnectionRef,
    pub bucket: String,

    /// Object key or directory-style prefix (trailing `/`) to fetch.
    pub prefix: String,
    /// Local file (single object) or directory (expanded tree).
    pub target: PathBuf,
    pub conflict: ConflictPolicy,
    pub recursive: bool,
}

/// One executable unit handed to the transfer engine.
#[derive(Clone)]
pub(crate) enum EngineItem {
    Upload(UploadRequest),
    Download {
        request: DownloadRequest,
        source: String,
    },
}

impl EngineItem {
    fn labels(&self) -> (String, String, String) {
        match self {
            Self::Upload(request) => {
                let source = request.source.display().to_string();
                let target = format!("{}/{}", request.bucket, request.key.as_str());
                (source.clone(), source, target)
            }
            Self::Download { request, source } => {
                let target = request.target.display().to_string();
                (format!("{source} -> {target}"), source.clone(), target)
            }
        }
    }

    /// Wraps the item as a job-run record carrying its display labels.
    pub(crate) fn into_run_item(self) -> crate::job_run::RunItem<Self> {
        let (job_label, source, target) = self.labels();
        crate::job_run::RunItem::new(job_label, source, target, self)
    }

    /// Human-readable dry-run description of the pending operation.
    pub(crate) fn preview(&self) -> String {
        match self {
            Self::Upload(request) => format!(
                "would upload to {}/{}",
                request.bucket,
                request.key.as_str()
            ),
            Self::Download { request, source } => {
                format!("would download {source} to {}", request.target.display())
            }
        }
    }
}

/// Upload pairs resolved against a concrete connection and bucket.
pub(crate) struct PreparedUpload {
    pairs: Vec<(PathBuf, ObjectPath)>,
    conflict: ConflictPolicy,
}

/// Maps local sources to (path, object key) pairs without touching storage.
///
/// Recursive directory sources are walked relative to the source root so each
/// file keeps its nested structure under the plan prefix.
///
/// # Errors
/// Returns `not_found` for missing paths or directories without `recursive`,
/// and `invalid_input` when expansion yields no uploadable file.
pub fn expand_upload(plan: &UploadPlan) -> Result<Vec<(PathBuf, ObjectPath)>> {
    let mut raw = Vec::new();
    for source in &plan.sources {
        if plan.recursive && source.is_dir() {
            let mut tree = Vec::new();
            collect_files(source, source, &mut tree)?;
            raw.extend(
                tree.into_iter()
                    .map(|(path, key)| (path, join_key(&plan.prefix, &key))),
            );
        } else if source.is_file() {
            let name = source
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .ok_or_else(|| Error::invalid_input("upload source has no file name"))?;
            raw.push((source.clone(), join_key(&plan.prefix, &name)));
        } else {
            return Err(Error::not_found(format!(
                "local path `{}` does not exist",
                source.display()
            )));
        }
    }
    raw.into_iter()
        .map(|(path, key)| Ok((path, ObjectPath::new(key)?)))
        .collect()
}

/// Expands and validates an upload plan entirely on local inputs.
///
/// Cheap enough to run before endpoint resolution; rejects empty plans here
/// so no storage adapter is ever built for them.
pub(crate) fn prepare_upload(plan: &UploadPlan) -> Result<PreparedUpload> {
    let pairs = expand_upload(plan)?;
    if pairs.is_empty() {
        return Err(Error::invalid_input("nothing to upload"));
    }
    Ok(PreparedUpload {
        pairs,
        conflict: plan.conflict,
    })
}

/// Binds prepared upload pairs to a resolved endpoint as engine items.
pub(crate) fn upload_items(
    prepared: PreparedUpload,
    connection: ConnectionId,
    bucket: &str,
) -> Vec<EngineItem> {
    prepared
        .pairs
        .into_iter()
        .map(|(source, key)| {
            EngineItem::Upload(UploadRequest {
                bucket: bucket.to_string(),
                key,
                source,
                content_type: None,
                conflict: prepared.conflict,
                connection,
            })
        })
        .collect()
}

/// Download entries resolved against a concrete endpoint.
pub(crate) struct PreparedDownload {
    key: String,
    expand: bool,
    requested: String,
    target: PathBuf,
    conflict: ConflictPolicy,
    mirror_into_directory: bool,
}

/// Validates a download plan against local rules only, never storage.
///
/// Directory references require `recursive` up front, and an unslashed prefix
/// is checked as a valid exact key so malformed input fails before the
/// endpoint is resolved. An existing-directory target (or recursion) mirrors
/// the remote tree beneath it; otherwise every entry lands on `target`.
pub(crate) fn prepare_download(plan: &DownloadPlan) -> Result<PreparedDownload> {
    // Directory syntax must be rejected locally; storage is not consulted yet.
    crate::expand::require_recursive(&plan.prefix, &plan.prefix, plan.recursive)?;
    let expand = crate::expand::is_directory_ref(&plan.prefix);
    if !expand {
        ObjectPath::new(&plan.prefix)?;
    }
    Ok(PreparedDownload {
        key: plan.prefix.clone(),
        expand,
        requested: plan.prefix.clone(),
        target: plan.target.clone(),
        conflict: plan.conflict,
        mirror_into_directory: plan.target.is_dir() || plan.recursive,
    })
}

/// Lists the objects behind a prepared download and turns them into items.
///
/// # Errors
/// Returns `not_found` when the prefix matches no stored objects.
pub(crate) async fn download_items(
    prepared: PreparedDownload,
    connection: ConnectionId,
    bucket: &str,
    storage: &dyn Storage,
) -> Result<Vec<EngineItem>> {
    let entries =
        crate::expand::source_entries(storage, bucket, &prepared.key, prepared.expand).await?;
    if entries.is_empty() {
        return Err(Error::not_found(format!(
            "no objects under `{}`",
            prepared.requested
        )));
    }
    Ok(entries
        .into_iter()
        .map(|entry| {
            let source = format!("{connection}/{bucket}/{}", entry.path.as_str());
            let target = if prepared.mirror_into_directory {
                prepared.target.join(&entry.relative)
            } else {
                prepared.target.clone()
            };
            EngineItem::Download {
                request: DownloadRequest {
                    bucket: bucket.to_string(),
                    key: entry.path,
                    target,
                    conflict: prepared.conflict,
                },
                source,
            }
        })
        .collect())
}

/// Recursively collects files under `root`, recording keys relative to it.
fn collect_files(root: &Path, current: &Path, out: &mut Vec<(PathBuf, String)>) -> Result<()> {
    let entries = std::fs::read_dir(current)
        .map_err(|error| Error::database("read directory").with_source(error))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| Error::database("read directory entry").with_source(error))?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, out)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| Error::invalid_input("path outside root").with_source(error))?;
            out.push((path.clone(), relative.to_string_lossy().to_string()));
        }
    }
    Ok(())
}

/// Joins a name onto a prefix, inserting `/` unless the prefix already ends
/// with one (or is empty).
fn join_key(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else if prefix.ends_with('/') {
        format!("{prefix}{name}")
    } else {
        format!("{prefix}/{name}")
    }
}
