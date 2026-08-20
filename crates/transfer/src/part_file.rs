//! Download staging (DOWN-004/009): the `.part` file policy.
//!
//! One module owns where staged bytes live, when a staged file is safe to
//! resume, and how it promotes to the final name. Resume is validated
//! against the remote object's identity — size **and** etag, recorded in a
//! sidecar when staging starts — so a stale `.part` from a different object
//! restarts instead of reusing bytes from another object. The
//! A length-only check could not catch same-sized objects.

use std::path::{Path, PathBuf};

use rstore_domain::{Error, Result};
use rstore_storage_api::model::ObjectMetadata;
use tokio::io::AsyncWriteExt;

/// A staged download opened for appending. Dropping it leaves the staged
/// bytes for the next run; [`complete`](StagedDownload::complete) promotes
/// them to the final name.
pub(crate) struct StagedDownload {
    file: tokio::fs::File,
    path: PathBuf,
    identity_path: PathBuf,
    /// Bytes already staged for this object.
    pub(crate) offset: u64,
}

/// The staged path for `target`: `.part` appended after the full file name
/// (`movie.mp4` → `movie.mp4.part`, `Makefile` → `Makefile.part`). Unlike
/// `Path::with_extension`, this never mangles extension-less names.
pub(crate) fn staged_path(target: &Path) -> PathBuf {
    let mut name = target
        .file_name()
        .map_or_else(|| target.as_os_str().to_owned(), std::ffi::OsStr::to_owned)
        .to_os_string();
    name.push(".part");
    target.with_file_name(name)
}
/// The sidecar recording which object the staging file belongs to:
/// `<staged>.rmeta`, two lines (`size`, `etag`).
fn identity_path(target: &Path) -> PathBuf {
    let staged = staged_path(target).into_os_string();
    let mut identity = std::ffi::OsString::with_capacity(staged.len() + 6);
    identity.push(staged);
    identity.push(".rmeta");
    PathBuf::from(identity)
}

/// Truncate-then-record: if the run dies between the two, the next run sees
/// staged bytes with no (or a stale) sidecar and restarts — never unrelated bytes
/// promoted as fresh.
fn write_identity(path: &Path, meta: &ObjectMetadata) -> Result<()> {
    let etag = meta.etag.as_deref().unwrap_or("-");
    std::fs::write(path, format!("{}\n{etag}\n", meta.size))
        .map_err(|e| Error::io("write .part identity").with_source(e))
}

fn identity_matches(path: &Path, meta: &ObjectMetadata) -> bool {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return false;
    };
    let mut lines = raw.lines();
    let size = lines.next().and_then(|line| line.parse::<u64>().ok());
    let etag = lines.next();
    size == Some(meta.size) && etag == Some(meta.etag.as_deref().unwrap_or("-"))
}

/// Open the staged `.part` for `target` against the remote `meta`.
///
/// Resume continues from the staged length only when the recorded identity
/// still matches the object (size + etag) and the staging file is not longer
/// than the object. Anything else — a missing sidecar or a crash before it was
/// written, a changed object, an over-long staging file — restarts from zero.
pub(crate) async fn stage(target: &Path, meta: &ObjectMetadata) -> Result<StagedDownload> {
    let path = staged_path(target);
    let identity = identity_path(target);

    let staged_len = tokio::fs::metadata(&path).await.map(|m| m.len()).ok();
    let resume =
        identity_matches(&identity, meta) && staged_len.is_some_and(|len| len <= meta.size);
    let offset = if resume { staged_len.unwrap_or(0) } else { 0 };

    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(offset > 0)
        .write(true)
        .open(&path)
        .await
        .map_err(|e| Error::io("open .part file").with_source(e))?;
    if offset == 0 {
        file.set_len(0)
            .await
            .map_err(|e| Error::io("truncate .part file").with_source(e))?;
        write_identity(&identity, meta)?;
    }
    Ok(StagedDownload {
        file,
        path,
        identity_path: identity,
        offset,
    })
}

impl StagedDownload {
    /// Stage one chunk and report the staged offset after the write. The
    /// explicit flush matters: tokio's `File` buffers in userspace, and the
    /// staged length must be observable by the next `stage` the moment this
    /// returns (a background close would race it).
    pub(crate) async fn append(&mut self, body: &[u8]) -> Result<u64> {
        self.file
            .write_all(body)
            .await
            .map_err(|e| Error::io("write .part file").with_source(e))?;
        self.file
            .flush()
            .await
            .map_err(|e| Error::io("flush .part file").with_source(e))?;
        self.offset += body.len() as u64;
        Ok(self.offset)
    }

    /// Promote the staged bytes to `target` atomically (DOWN-009): the final
    /// name appears only when fully written. The sidecar removal is
    /// best-effort — a leftover is inert because identity is re-checked
    /// per run.
    pub(crate) async fn complete(self, target: &Path) -> Result<()> {
        tokio::fs::rename(&self.path, target)
            .await
            .map_err(|e| Error::io("finalise download").with_source(e))?;
        let _ = tokio::fs::remove_file(&self.identity_path).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rstore-part-file-test-{}-{name}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn meta(size: u64, etag: Option<&str>) -> ObjectMetadata {
        ObjectMetadata {
            key: "doc.bin".into(),
            size,
            etag: etag.map(str::to_string),
            content_type: None,
            last_modified: time::OffsetDateTime::UNIX_EPOCH,
            storage_class: None,
            version_id: None,
        }
    }

    #[test]
    fn staged_path_appends_after_the_full_name() {
        assert_eq!(
            staged_path(Path::new("/tmp/movie.mp4")),
            Path::new("/tmp/movie.mp4.part")
        );
        assert_eq!(
            staged_path(Path::new("/tmp/archive.tar.gz")),
            Path::new("/tmp/archive.tar.gz.part")
        );
        assert_eq!(
            staged_path(Path::new("/tmp/Makefile")),
            Path::new("/tmp/Makefile.part")
        );
        assert_eq!(
            staged_path(Path::new("/tmp/.hidden")),
            Path::new("/tmp/.hidden.part")
        );
    }

    #[tokio::test]
    async fn resume_continues_from_the_staged_length() {
        let dir = temp_dir("resume");
        let target = dir.join("doc.bin");
        let m = meta(10, Some("\"etag-a\""));

        let mut staged = stage(&target, &m).await.unwrap();
        assert_eq!(staged.offset, 0);
        staged.append(b"01234").await.unwrap();
        drop(staged);

        let staged = stage(&target, &m).await.unwrap();
        assert_eq!(staged.offset, 5, "same object resumes");
    }

    #[tokio::test]
    async fn same_sized_different_object_restarts() {
        let dir = temp_dir("etag");
        let target = dir.join("doc.bin");

        let mut staged = stage(&target, &meta(10, Some("\"etag-a\""))).await.unwrap();
        staged.append(b"01234").await.unwrap();
        drop(staged);

        // Same size, different etag: the old length-only check resumed here
        // and spliced the two objects together on rename.
        let staged = stage(&target, &meta(10, Some("\"etag-b\""))).await.unwrap();
        assert_eq!(staged.offset, 0, "changed object restarts");
        assert_eq!(
            std::fs::read(staged_path(&target)).unwrap().len(),
            0,
            "staging file is truncated"
        );
    }

    #[tokio::test]
    async fn over_long_staging_restarts() {
        let dir = temp_dir("overlong");
        let target = dir.join("doc.bin");
        std::fs::write(identity_path(&target), "4\n\"etag-a\"\n").unwrap();
        std::fs::write(staged_path(&target), b"garbage-bytes").unwrap();

        let staged = stage(&target, &meta(4, Some("\"etag-a\""))).await.unwrap();
        assert_eq!(staged.offset, 0, "longer than the object restarts");
    }

    #[tokio::test]
    async fn missing_sidecar_restarts_conservatively() {
        let dir = temp_dir("nosidecar");
        let target = dir.join("doc.bin");
        // A `.part` without an identity sidecar, or a crash before the sidecar
        // is written.
        std::fs::write(staged_path(&target), b"untrusted").unwrap();

        let staged = stage(&target, &meta(9, Some("\"etag-a\""))).await.unwrap();
        assert_eq!(staged.offset, 0, "unattributed bytes are never resumed");
    }

    #[tokio::test]
    async fn etagless_objects_fall_back_to_size_identity() {
        let dir = temp_dir("etagless");
        let target = dir.join("doc.bin");
        let mut staged = stage(&target, &meta(8, None)).await.unwrap();
        staged.append(b"0123").await.unwrap();
        drop(staged);

        let staged = stage(&target, &meta(8, None)).await.unwrap();
        assert_eq!(staged.offset, 4);
        // A size change still restarts.
        let staged = stage(&target, &meta(5, None)).await.unwrap();
        assert_eq!(staged.offset, 0);
    }

    #[tokio::test]
    async fn complete_promotes_and_cleans_up() {
        let dir = temp_dir("complete");
        let target = dir.join("doc.bin");
        let mut staged = stage(&target, &meta(3, Some("\"etag-a\""))).await.unwrap();
        staged.append(b"xyz").await.unwrap();
        staged.complete(&target).await.unwrap();

        assert_eq!(std::fs::read(&target).unwrap(), b"xyz");
        assert!(!staged_path(&target).exists(), "no .part residue");
        assert!(!identity_path(&target).exists(), "no sidecar residue");
    }
}
