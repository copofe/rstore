//! Stateless transfer planning.
//!
//! This module validates plans before adapter construction, expands local
//! upload trees or remote download prefixes, mirrors paths, and constructs
//! typed engine items. Transfer execution remains in `transfer`.

use std::path::{Path, PathBuf};

use rstore_domain::policy::ConflictPolicy;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{ConnectionId, Error, Result};
use rstore_storage_api::Storage;
use rstore_transfer::{DownloadRequest, UploadRequest};

use crate::connection::ConnectionRef;

/// An upload target: local path(s) plus a `conn/bucket/prefix/` base.
#[derive(Debug, Clone)]
pub struct UploadPlan {
    pub connection: ConnectionRef,
    pub bucket: String,
    /// Key prefix under the bucket ("" = root).
    pub prefix: String,
    pub sources: Vec<PathBuf>,
    pub conflict: ConflictPolicy,
    pub recursive: bool,
}

/// A download target: `conn/bucket/key…` plus a local base.
#[derive(Debug, Clone)]
pub struct DownloadPlan {
    pub connection: ConnectionRef,
    pub bucket: String,
    /// Remote prefix to fetch (recursively lists when `recursive`).
    pub prefix: String,
    pub target: PathBuf,
    pub conflict: ConflictPolicy,
    pub recursive: bool,
}

/// One typed item ready for the transfer engine.
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

    pub(crate) fn into_run_item(self) -> crate::job_run::RunItem<Self> {
        let (job_label, source, target) = self.labels();
        crate::job_run::RunItem::new(job_label, source, target, self)
    }

    /// The dry-run narration: what a real run would do with this item — the
    /// same "would \<verb\> \<target\>" grammar cp/rm use.
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

/// Validated, expanded upload input. It is produced before resolving a remote
/// endpoint, so local input failures never construct an adapter.
pub(crate) struct PreparedUpload {
    pairs: Vec<(PathBuf, ObjectPath)>,
    conflict: ConflictPolicy,
}

/// Expand local sources while preserving their tree beneath `plan.prefix`.
/// This function needs only the local filesystem and is the direct planning
/// test surface.
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

pub(crate) fn upload_items(
    prepared: PreparedUpload,
    connection: ConnectionId,
    bucket: &str,
) -> Vec<EngineItem> {
    let connection = connection.to_string();
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
                connection: connection.clone(),
            })
        })
        .collect()
}

/// Locally validated download input: the source key plus whether it pairs
/// as a directory (expansion) or an exact object. Validation runs before
/// resolving or constructing a remote adapter.
pub(crate) struct PreparedDownload {
    key: String,
    expand: bool,
    requested: String,
    target: PathBuf,
    conflict: ConflictPolicy,
    mirror_into_directory: bool,
}

pub(crate) fn prepare_download(plan: &DownloadPlan) -> Result<PreparedDownload> {
    // The shared validation arm; download preparation re-runs it locally so
    // bad syntax never constructs an adapter.
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

/// Expand a validated download against one already-resolved adapter and build
/// typed engine items with canonical source labels.
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

fn join_key(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else if prefix.ends_with('/') {
        format!("{prefix}{name}")
    } else {
        format!("{prefix}/{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_test_support::FakeStorage;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rstore-xfer-plan-{}-{name}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn upload_plan(sources: Vec<PathBuf>, prefix: &str, recursive: bool) -> UploadPlan {
        UploadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: prefix.into(),
            sources,
            conflict: ConflictPolicy::Overwrite,
            recursive,
        }
    }

    fn download_plan(prefix: &str, target: PathBuf, recursive: bool) -> DownloadPlan {
        DownloadPlan {
            connection: ConnectionRef::Name("nas".into()),
            bucket: "media".into(),
            prefix: prefix.into(),
            target,
            conflict: ConflictPolicy::Overwrite,
            recursive,
        }
    }

    #[test]
    fn recursive_upload_preserves_structure() {
        let dir = temp_dir("recursive");
        std::fs::create_dir_all(dir.join("nested/deeper")).unwrap();
        std::fs::write(dir.join("top.txt"), b"t").unwrap();
        std::fs::write(dir.join("nested/mid.log"), b"m").unwrap();
        std::fs::write(dir.join("nested/deeper/leaf.txt"), b"l").unwrap();

        let pairs = expand_upload(&upload_plan(vec![dir], "base/", true)).unwrap();
        let mut keys: Vec<String> = pairs
            .iter()
            .map(|(_, key)| key.as_str().to_string())
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "base/nested/deeper/leaf.txt".to_string(),
                "base/nested/mid.log".to_string(),
                "base/top.txt".to_string(),
            ]
        );
    }

    #[test]
    fn multiple_recursive_sources_apply_prefix_once() {
        let first = temp_dir("multi-first");
        let second = temp_dir("multi-second");
        std::fs::write(first.join("first.txt"), b"1").unwrap();
        std::fs::write(second.join("second.txt"), b"2").unwrap();

        let pairs = expand_upload(&upload_plan(vec![first, second], "base/", true)).unwrap();
        let mut keys: Vec<String> = pairs
            .iter()
            .map(|(_, key)| key.as_str().to_string())
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["base/first.txt".to_string(), "base/second.txt".to_string()]
        );
    }

    #[test]
    fn missing_upload_source_is_not_found() {
        let error = expand_upload(&upload_plan(
            vec![temp_dir("missing").join("absent.txt")],
            "",
            false,
        ))
        .unwrap_err();
        assert_eq!(error.code, rstore_domain::ErrorCode::NotFound);
    }

    #[test]
    fn upload_directory_without_recursive_is_not_found() {
        let dir = temp_dir("directory-without-recursive");
        std::fs::write(dir.join("x.txt"), b"x").unwrap();
        let error = expand_upload(&upload_plan(vec![dir], "", false)).unwrap_err();
        assert_eq!(error.code, rstore_domain::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn recursive_download_strips_prefix_and_mirrors_tree() {
        let storage = FakeStorage::new();
        storage.seed("media", "docs/a.txt", b"a", None);
        storage.seed("media", "docs/nested/b.txt", b"b", None);
        storage.seed("media", "other/c.txt", b"c", None);
        let target = temp_dir("download-mirror");
        let prepared = prepare_download(&download_plan("docs/", target.clone(), true)).unwrap();

        let items = download_items(prepared, ConnectionId::new(), "media", &storage)
            .await
            .unwrap();
        let mut targets = items
            .iter()
            .map(|item| match item {
                EngineItem::Download { request, .. } => format!(
                    "{}=>{}",
                    request.key.as_str(),
                    request.target.strip_prefix(&target).unwrap().display()
                ),
                EngineItem::Upload(_) => unreachable!(),
            })
            .collect::<Vec<_>>();
        targets.sort();
        assert_eq!(
            targets,
            vec![
                "docs/a.txt=>a.txt".to_string(),
                "docs/nested/b.txt=>nested/b.txt".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn slashed_download_prefix_isolates_sibling_keys() {
        let storage = FakeStorage::new();
        storage.seed("media", "docs/a.txt", b"a", None);
        storage.seed("media", "docs-old/b.txt", b"b", None);
        let prepared =
            prepare_download(&download_plan("docs/", temp_dir("download-sibling"), true)).unwrap();

        let items = download_items(prepared, ConnectionId::new(), "media", &storage)
            .await
            .unwrap();
        let keys = items
            .iter()
            .map(|item| match item {
                EngineItem::Download { request, .. } => request.key.as_str(),
                EngineItem::Upload(_) => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["docs/a.txt"]);
    }

    #[test]
    fn directory_download_requires_recursive_before_storage_is_needed() {
        let error = prepare_download(&download_plan(
            "docs/",
            temp_dir("download-needs-recursive"),
            false,
        ))
        .err()
        .expect("directory syntax should fail locally");
        assert_eq!(error.code, rstore_domain::ErrorCode::InvalidInput);
    }

    #[tokio::test]
    async fn unslashed_download_key_stays_exact() {
        let storage = FakeStorage::new();
        storage.seed("media", "img/logo.png", b"x", None);
        storage.seed("media", "img/logo.png2", b"y", None);
        let prepared = prepare_download(&download_plan(
            "img/logo.png",
            temp_dir("download-exact"),
            false,
        ))
        .unwrap();

        let items = download_items(prepared, ConnectionId::new(), "media", &storage)
            .await
            .unwrap();
        let keys = items
            .iter()
            .map(|item| match item {
                EngineItem::Download { request, .. } => request.key.as_str(),
                EngineItem::Upload(_) => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["img/logo.png"]);
    }
}
