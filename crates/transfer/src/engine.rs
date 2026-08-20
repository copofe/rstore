//! Transfer execution engine (CORE-006 / UP- / DOWN- / STORE-013).
//!
//! One engine instance runs a whole job's items against one [`Storage`]
//! adapter. Design points:
//!
//! - **Uploads**: files at or above the multipart threshold stream part by
//!   part through [`Storage::upload_part`]; the (upload_id, completed parts)
//!   checkpoint goes to the [`MultipartStore`] after every part, so an
//!   interrupted upload resumes without re-sending finished parts (UP-014/015).
//! - **Downloads**: ranged chunked reads append to a `<name>.part` file and
//!   rename atomically on completion (DOWN-004/009); an existing `.part`
//!   resumes from its length.
//! - **Retries**: the application composition root wraps the adapter in
//!   [`crate::RetryingStorage`], so this engine issues each storage operation
//!   once and remains focused on transfer state.
//! - Progress flows through [`ProgressSink`] to stderr only (JOB-005/016).

use std::path::PathBuf;

use rstore_domain::policy::ConflictPolicy;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, Result};

use rstore_storage_api::model::{ObjectMetadata, PutOptions};
use rstore_storage_api::{guess_content_type, FileFingerprint, MultipartStore, Storage};

use crate::cancel::CancellationToken;
use crate::conflict::{decide_target, path_probe, stat_probe, TargetDecision};
use crate::multipart;
use crate::progress::{Progress, ProgressSink};
use crate::pump::{pump, ChunkPlan, ChunkSink, RangedSource};

/// Tuning knobs (UP-006/007 thresholds are configurable).
#[derive(Debug, Clone, Copy)]
pub struct TransferConfig {
    /// Files at or above this size use multipart (default 8 MiB).
    pub multipart_threshold: u64,
    /// Part size for multipart uploads (default 8 MiB, S3 minimum 5 MiB).
    pub part_size: u64,
    /// Download chunk size for ranged reads (default 1 MiB).
    pub download_chunk: u64,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            multipart_threshold: 8 * 1024 * 1024,
            part_size: 8 * 1024 * 1024,
            download_chunk: 1024 * 1024,
        }
    }
}

/// One upload request (local file → remote object).
#[derive(Debug, Clone)]
pub struct UploadRequest {
    pub bucket: String,
    pub key: ObjectPath,
    pub source: PathBuf,
    pub content_type: Option<String>,
    pub conflict: ConflictPolicy,
    /// Connection identity for the multipart checkpoint.
    pub connection: String,
}

/// One download request (remote object → local file).
#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub bucket: String,
    pub key: ObjectPath,
    pub target: PathBuf,
    pub conflict: ConflictPolicy,
}

/// Outcome of one item.
#[derive(Debug, Clone, Default)]
pub struct ItemOutcome {
    pub bytes: u64,
    /// Metadata returned by the adapter for the object that was transferred.
    /// Skipped/blocked items carry none.
    pub metadata: Option<ObjectMetadata>,
    pub skipped: bool,
    /// The conflict policy left this item undone (unresolved `ask`, or rename
    /// found no free name) — a per-item outcome, not an error.
    pub blocked: bool,
    /// Human detail for skipped/blocked items (the conflicting target or the
    /// block reason).
    pub detail: Option<String>,
}

impl ItemOutcome {
    /// A completed transfer of `bytes`, with the adapter's metadata.
    pub fn transferred(bytes: u64, metadata: Option<ObjectMetadata>) -> Self {
        Self {
            bytes,
            metadata,
            ..Self::default()
        }
    }

    /// The conflict policy left the existing target alone.
    pub fn skipped(detail: String) -> Self {
        Self {
            skipped: true,
            detail: Some(detail),
            ..Self::default()
        }
    }

    /// The conflict policy could not resolve (unresolved `ask`, or rename
    /// found no free name).
    pub fn blocked(detail: String) -> Self {
        Self {
            blocked: true,
            detail: Some(detail),
            ..Self::default()
        }
    }
}

/// The engine: stateless between calls except the checkpoint store.
pub struct TransferEngine {
    storage: std::sync::Arc<dyn Storage>,
    checkpoints: std::sync::Arc<dyn rstore_storage_api::MultipartStore>,
    config: TransferConfig,
}

impl TransferEngine {
    pub fn new(
        storage: std::sync::Arc<dyn Storage>,
        checkpoints: std::sync::Arc<dyn MultipartStore>,
        config: TransferConfig,
    ) -> Self {
        Self {
            storage,
            checkpoints,
            config,
        }
    }

    /// Upload one file, honouring conflict policy, resume and cancellation.
    pub async fn upload(
        &self,
        req: &UploadRequest,
        progress: &dyn ProgressSink,
        cancel: &CancellationToken,
    ) -> Result<ItemOutcome> {
        cancel.check()?;
        let source_metadata = std::fs::metadata(&req.source).map_err(|e| {
            Error::not_found(format!("local file `{}`", req.source.display())).with_source(e)
        })?;
        let size = source_metadata.len();
        let fingerprint = FileFingerprint::of(&source_metadata);

        // Conflict policy against an existing remote object (UP-010..013):
        // the one decision table, its one interpretation and its one
        // reaction — rename re-entry included.
        let key = match decide_target(
            req.key.as_str(),
            req.conflict,
            stat_probe(&self.storage, &req.bucket).as_ref(),
            |name| ObjectPath::new(name.to_owned()),
        )
        .await?
        {
            TargetDecision::Finish(outcome) => return Ok(outcome),
            TargetDecision::Write(key) => key,
        };

        let opts = PutOptions {
            content_type: req
                .content_type
                .clone()
                .or_else(|| guess_content_type(&req.source)),
            ..Default::default()
        };

        let metadata = if size < self.config.multipart_threshold {
            self.upload_small(req, &key, size, &opts, progress).await?
        } else {
            self.upload_multipart(req, &key, fingerprint, &opts, progress, cancel)
                .await?
        };
        Ok(ItemOutcome::transferred(
            metadata.size.max(size),
            Some(metadata),
        ))
    }

    async fn upload_small(
        &self,
        req: &UploadRequest,
        key: &ObjectPath,
        size: u64,
        opts: &PutOptions,
        progress: &dyn ProgressSink,
    ) -> Result<ObjectMetadata> {
        let body = bytes::Bytes::from(
            tokio::fs::read(&req.source)
                .await
                .map_err(|e| Error::io("read upload source").with_source(e))?,
        );
        let meta = self.storage.put(&req.bucket, key, body, opts).await?;
        progress.report(Progress::Bytes {
            done: size,
            total: size,
        });
        Ok(meta)
    }

    async fn upload_multipart(
        &self,
        req: &UploadRequest,
        key: &ObjectPath,
        fingerprint: FileFingerprint,
        opts: &PutOptions,
        progress: &dyn ProgressSink,
        cancel: &CancellationToken,
    ) -> Result<ObjectMetadata> {
        let size = fingerprint.size;
        let (connection, bucket) = (&req.connection, &req.bucket);
        let file = tokio::fs::File::open(&req.source)
            .await
            .map_err(|e| Error::io("open upload source").with_source(e))?;
        let mut reader = tokio::io::BufReader::with_capacity(self.config.part_size as usize, file);

        // The one resume policy: adopt a matching checkpoint, strictly retire
        // a stale one, or begin fresh (UP-014/015).
        let (session, mut done_parts) = multipart::resume_or_begin(
            self.storage.as_ref(),
            self.checkpoints.as_ref(),
            connection,
            bucket,
            key,
            &fingerprint,
            opts,
        )
        .await?;
        let done_numbers: Vec<u32> = done_parts.iter().map(|p| p.part_number).collect();
        let done_bytes: u64 = done_parts.iter().map(|p| p.size).sum();

        // One counter — bytes already safely stored. `pending` owns the skip
        // decision and the part numbering, so the plan position that once
        // drifted from the progress counter (truncating a resumed upload by
        // exactly the resumed bytes) no longer exists.
        let mut progress_bytes = done_bytes;
        for (part_number, range) in
            ChunkPlan::new(size, self.config.part_size).pending(&done_numbers)
        {
            let want = (range.end.unwrap_or(range.start) - range.start) as usize;
            cancel.check()?;
            // Seek to the part's own offset: correctness never assumes the
            // checkpointed parts form a prefix.
            tokio::io::AsyncSeekExt::seek(&mut reader, std::io::SeekFrom::Start(range.start))
                .await
                .map_err(|e| Error::io("seek upload source").with_source(e))?;
            let mut buf = vec![0u8; want];
            tokio::io::AsyncReadExt::read_exact(&mut reader, &mut buf)
                .await
                .map_err(|e| Error::io("read upload part").with_source(e))?;
            let part = session
                .upload_part(part_number, bytes::Bytes::from(buf))
                .await?;
            // Checkpoint after every part: an interrupt loses at most one.
            if let Err(error) = self
                .checkpoints
                .part_done(connection, bucket, key.as_str(), &part)
                .await
            {
                multipart::abandon_with_checkpoint(
                    self.checkpoints.as_ref(),
                    connection,
                    bucket,
                    key.as_str(),
                    session,
                )
                .await;
                return Err(error);
            }
            done_parts.push(part);
            progress_bytes += want as u64;
            progress.report(Progress::Bytes {
                done: progress_bytes,
                total: size,
            });
        }

        done_parts.sort_by_key(|p| p.part_number);
        // A completion failure leaves the provider upload in place — the
        // checkpoint survives, so the next run resumes instead of restarting.
        let metadata = session.complete(&done_parts).await?;
        if let Err(error) = self
            .checkpoints
            .cleared(connection, bucket, key.as_str())
            .await
        {
            // Completion is already a remote fact; do not misreport a
            // successful upload as failed because local cleanup lagged.
            tracing::warn!(error = %error, "could not clear completed multipart checkpoint");
        }
        progress.report(Progress::Bytes {
            done: size,
            total: size,
        });
        Ok(metadata)
    }

    /// Download one object with `.part` staging and byte-exact resume
    /// (DOWN-001/004/009).
    pub async fn download(
        &self,
        req: &DownloadRequest,
        progress: &dyn ProgressSink,
        cancel: &CancellationToken,
    ) -> Result<ItemOutcome> {
        cancel.check()?;
        let meta = self.storage.stat(&req.bucket, &req.key).await?;

        // Local conflict policy mirrors the upload set (DOWN-005/006): the
        // one decision, interpretation and reaction.
        let target = match decide_target(
            &req.target.to_string_lossy(),
            req.conflict,
            path_probe().as_ref(),
            |name| Ok(std::path::PathBuf::from(name.to_owned())),
        )
        .await?
        {
            TargetDecision::Finish(outcome) => return Ok(outcome),
            TargetDecision::Write(target) => target,
        };

        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::io("create download dir").with_source(e))?;
        }

        // Staging policy (DOWN-004/009): identity-checked resume, atomic
        // promotion — one module owns the `.part` lifetime.
        let mut staged = crate::part_file::stage(&target, &meta).await?;

        struct StagedSink<'a>(&'a mut crate::part_file::StagedDownload);
        #[async_trait::async_trait]
        impl ChunkSink for StagedSink<'_> {
            async fn accept(&mut self, _index: usize, body: bytes::Bytes) -> Result<()> {
                self.0.append(&body).await.map(|_| ())
            }
        }
        pump(
            &RangedSource::new(self.storage.as_ref(), &req.bucket, &req.key),
            ChunkPlan::resumed(meta.size, self.config.download_chunk, staged.offset),
            cancel,
            progress,
            &mut StagedSink(&mut staged),
        )
        .await?;

        staged.complete(&target).await?;
        progress.report(Progress::Bytes {
            done: meta.size,
            total: meta.size,
        });
        Ok(ItemOutcome::transferred(meta.size, Some(meta)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_domain::remote::ObjectPath;

    fn key(s: &str) -> ObjectPath {
        ObjectPath::new(s).unwrap()
    }
    use rstore_domain::ErrorCode;
    use rstore_storage_api::model::{ReadRange, UploadedPart};
    use rstore_storage_api::{NoopMultipartStore, PendingUpload};
    use rstore_test_support::FakeStorage;
    use std::sync::Arc;

    fn engine(storage: Arc<FakeStorage>) -> TransferEngine {
        TransferEngine::new(
            storage,
            Arc::new(NoopMultipartStore),
            TransferConfig {
                // Tiny thresholds so tests exercise multipart without big files.
                multipart_threshold: 10,
                part_size: 10,
                download_chunk: 4,
            },
        )
    }

    fn temp_file(name: &str, bytes: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rstore-transfer-test-{}-{name}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[tokio::test]
    async fn small_upload_and_download_round_trip() {
        let storage = Arc::new(FakeStorage::new());
        let engine = engine(storage.clone());
        let source = temp_file("small.txt", b"hello rstore");
        let cancel = CancellationToken::new();

        let req = UploadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("docs/small.txt").unwrap(),
            source: source.clone(),
            content_type: None,
            conflict: ConflictPolicy::Overwrite,
            connection: "conn_nas".into(),
        };
        let out = engine
            .upload(&req, &rstore_transfer_sink(), &cancel)
            .await
            .unwrap();
        assert_eq!(out.bytes, 12);
        assert!(!out.skipped);
        let metadata = out.metadata.expect("successful upload metadata");
        assert_eq!(metadata.key, "docs/small.txt");
        assert_eq!(metadata.size, 12);

        // Download it back and compare bytes.
        let target = source.with_extension("downloaded");
        let dl = DownloadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("docs/small.txt").unwrap(),
            target: target.clone(),
            conflict: ConflictPolicy::Overwrite,
        };
        engine
            .download(&dl, &rstore_transfer_sink(), &cancel)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"hello rstore");
        // No `.part` residue after atomic completion (DOWN-009).
        assert!(!target.with_extension("downloaded.part").exists());
    }

    /// A leftover `.part` nobody can attribute (or a crash before the
    /// identity sidecar is written) is never resumed — the download
    /// restarts instead of splicing untrusted bytes into the file.
    #[tokio::test]
    async fn unattributed_stale_part_restarts_cleanly() {
        let storage = Arc::new(FakeStorage::new());
        let engine = engine(storage.clone());
        storage.seed("media", "doc.bin", b"abcdef", None);
        let target = std::env::temp_dir().join(format!(
            "rstore-transfer-test-{}-stale/doc.bin",
            std::process::id()
        ));
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(
            crate::part_file::staged_path(&target),
            b"untrusted-leftovers-longer-than-the-object",
        )
        .unwrap();

        let dl = DownloadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("doc.bin").unwrap(),
            target: target.clone(),
            conflict: ConflictPolicy::Overwrite,
        };
        engine
            .download(&dl, &rstore_transfer_sink(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"abcdef");
    }

    /// A download interrupted mid-flight (staged bytes + identity sidecar on
    /// disk) resumes and produces the byte-exact object.
    #[tokio::test]
    async fn interrupted_download_resumes_byte_exact() {
        let storage = Arc::new(FakeStorage::new());
        let engine = engine(storage.clone());
        let payload: Vec<u8> = (0..24u8).collect();
        storage.seed("media", "doc.bin", &payload, None);
        let target = std::env::temp_dir().join(format!(
            "rstore-transfer-test-{}-resume/doc.bin",
            std::process::id()
        ));
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();

        // Simulate a run interrupted after the first two chunks (chunk = 4).
        let meta = storage.stat("media", &key("doc.bin")).await.unwrap();
        let mut staged = crate::part_file::stage(&target, &meta).await.unwrap();
        let head = storage
            .read("media", &key("doc.bin"), Some(ReadRange::exclusive(0, 8)))
            .await
            .unwrap();
        staged.append(&head).await.unwrap();
        drop(staged);

        let dl = DownloadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("doc.bin").unwrap(),
            target: target.clone(),
            conflict: ConflictPolicy::Overwrite,
        };
        engine
            .download(&dl, &rstore_transfer_sink(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), payload);
        assert!(!crate::part_file::staged_path(&target).exists());
    }

    /// An unresolved `ask` must block the item instead of overwriting it.
    /// The shared decision table applies this rule to every transfer.
    #[tokio::test]
    async fn unresolved_ask_blocks_instead_of_overwriting() {
        let storage = Arc::new(FakeStorage::new());
        let engine = engine(storage.clone());
        storage.seed("media", "doc.txt", b"original", None);
        let source = temp_file("doc.txt", b"new");
        let cancel = CancellationToken::new();
        let req = UploadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("doc.txt").unwrap(),
            source,
            content_type: None,
            conflict: ConflictPolicy::Ask,
            connection: "conn_nas".into(),
        };
        let out = engine
            .upload(&req, &rstore_transfer_sink(), &cancel)
            .await
            .unwrap();
        assert!(out.blocked);
        assert!(out.detail.as_deref().unwrap().contains("ask"));
        assert_eq!(out.bytes, 0);
        // The existing target is untouched.
        let meta = storage.stat("media", &key("doc.txt")).await.unwrap();
        assert_eq!(meta.size, 8);
    }

    #[tokio::test]
    async fn multipart_upload_assembles_object() {
        let storage = Arc::new(FakeStorage::new());
        let engine = engine(storage.clone());
        // 25 bytes over threshold 10 → 3 parts of ≤10 bytes.
        let payload: Vec<u8> = (0..25u8).collect();
        let source = temp_file("big.bin", &payload);
        let cancel = CancellationToken::new();

        let req = UploadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("big.bin").unwrap(),
            source,
            content_type: None,
            conflict: ConflictPolicy::Overwrite,
            connection: "conn_nas".into(),
        };
        let out = engine
            .upload(&req, &rstore_transfer_sink(), &cancel)
            .await
            .unwrap();
        assert_eq!(out.bytes, 25);

        let meta = storage.stat("media", &key("big.bin")).await.unwrap();
        assert_eq!(meta.size, 25);
        let body = storage.read("media", &key("big.bin"), None).await.unwrap();
        assert_eq!(&body[..], &payload[..]);
    }

    /// A resumed upload must produce a byte-exact object even when checkpoints
    /// already contain completed parts.
    #[tokio::test]
    async fn multipart_resume_produces_byte_exact_object() {
        use std::sync::Mutex;

        // 40 bytes, parts of 10 → 4 parts. Simulate a run interrupted after
        // the first 2 parts (checkpointed in a store backed by real state).
        type StoreState = Option<(String, Vec<UploadedPart>, FileFingerprint)>;
        #[derive(Default, Clone)]
        struct SharedStore {
            state: std::sync::Arc<Mutex<StoreState>>,
        }

        #[async_trait::async_trait]
        impl MultipartStore for SharedStore {
            async fn pending(&self, _c: &str, _b: &str, _k: &str) -> Result<Option<PendingUpload>> {
                let guard = self.state.lock().unwrap();
                Ok(guard
                    .as_ref()
                    .map(|(id, parts, fingerprint)| PendingUpload {
                        upload_id: id.clone(),
                        parts: parts.clone(),
                        fingerprint: *fingerprint,
                    }))
            }
            async fn started(
                &self,
                _c: &str,
                _b: &str,
                _k: &str,
                upload_id: &str,
                fingerprint: &FileFingerprint,
            ) -> Result<()> {
                *self.state.lock().unwrap() =
                    Some((upload_id.to_string(), Vec::new(), *fingerprint));
                Ok(())
            }
            async fn part_done(
                &self,
                _c: &str,
                _b: &str,
                _k: &str,
                part: &UploadedPart,
            ) -> Result<()> {
                self.state
                    .lock()
                    .unwrap()
                    .as_mut()
                    .expect("upload started")
                    .1
                    .push(part.clone());
                Ok(())
            }
            async fn cleared(&self, _c: &str, _b: &str, _k: &str) -> Result<()> {
                *self.state.lock().unwrap() = None;
                Ok(())
            }
        }

        let payload: Vec<u8> = (0..40u8).collect();
        let source = temp_file("resume.bin", &payload);
        let fingerprint = FileFingerprint::of(&std::fs::metadata(&source).unwrap());
        let store = SharedStore::default();
        let storage = Arc::new(FakeStorage::new());
        let cancel = CancellationToken::new();

        // Seed the checkpoint AND the fake provider's staged parts as a
        // previous run would have left them (server keeps uploaded parts).
        store.state.lock().unwrap().replace((
            "upload-1".into(),
            vec![
                UploadedPart {
                    part_number: 1,
                    etag: "etag-1".into(),
                    size: 10,
                },
                UploadedPart {
                    part_number: 2,
                    etag: "etag-2".into(),
                    size: 10,
                },
            ],
            fingerprint,
        ));
        storage.seed_part("media", "resume.bin", "upload-1", 1, &payload[0..10]);
        storage.seed_part("media", "resume.bin", "upload-1", 2, &payload[10..20]);

        let engine = TransferEngine::new(
            storage.clone(),
            Arc::new(store.clone()),
            TransferConfig {
                multipart_threshold: 10,
                part_size: 10,
                download_chunk: 4,
            },
        );
        let req = UploadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("resume.bin").unwrap(),
            source,
            content_type: None,
            conflict: ConflictPolicy::Overwrite,
            connection: "conn_nas".into(),
        };
        engine
            .upload(&req, &rstore_transfer_sink(), &cancel)
            .await
            .unwrap();

        let body = storage
            .read("media", &key("resume.bin"), None)
            .await
            .unwrap();
        assert_eq!(body.len(), 40, "assembled size must be exact");
        assert_eq!(&body[..], &payload[..], "resumed bytes must be byte-exact");
        assert!(
            store.state.lock().unwrap().is_none(),
            "checkpoint cleared after completion"
        );
    }

    #[tokio::test]
    async fn changed_source_aborts_stale_upload_before_starting_fresh() {
        use std::sync::Mutex;

        struct StaleCheckpoint {
            pending: Mutex<Option<PendingUpload>>,
        }

        #[async_trait::async_trait]
        impl MultipartStore for StaleCheckpoint {
            async fn pending(
                &self,
                _connection: &str,
                _bucket: &str,
                _key: &str,
            ) -> Result<Option<PendingUpload>> {
                Ok(self.pending.lock().unwrap().clone())
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
                *self.pending.lock().unwrap() = None;
                Ok(())
            }
        }

        let payload: Vec<u8> = (0..30u8).collect();
        let source = temp_file("changed-source.bin", &payload);
        let checkpoint = StaleCheckpoint {
            pending: Mutex::new(Some(PendingUpload {
                upload_id: "old-upload".into(),
                parts: vec![UploadedPart {
                    part_number: 1,
                    etag: "old-etag".into(),
                    size: 10,
                }],
                fingerprint: FileFingerprint {
                    size: 10,
                    modified_ns: None,
                },
            })),
        };
        let storage = Arc::new(FakeStorage::new());
        storage.seed_part(
            "media",
            "changed-source.bin",
            "old-upload",
            1,
            b"old-source",
        );
        let engine = TransferEngine::new(
            storage.clone(),
            Arc::new(checkpoint),
            TransferConfig {
                multipart_threshold: 10,
                part_size: 10,
                ..TransferConfig::default()
            },
        );
        let request = UploadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("changed-source.bin").unwrap(),
            source,
            content_type: None,
            conflict: ConflictPolicy::Overwrite,
            connection: "conn_nas".into(),
        };

        engine
            .upload(&request, &rstore_transfer_sink(), &CancellationToken::new())
            .await
            .unwrap();

        assert!(!storage.has_upload("media", "changed-source.bin", "old-upload"));
        assert_eq!(
            storage
                .read("media", &key("changed-source.bin"), None)
                .await
                .unwrap(),
            bytes::Bytes::from(payload)
        );
    }

    #[tokio::test]
    async fn checkpoint_start_failure_aborts_provider_upload() {
        struct FailingCheckpoint;

        #[async_trait::async_trait]
        impl MultipartStore for FailingCheckpoint {
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
                Err(Error::database("injected checkpoint failure"))
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

        let storage = Arc::new(FakeStorage::new());
        let engine = TransferEngine::new(
            storage.clone(),
            Arc::new(FailingCheckpoint),
            TransferConfig {
                multipart_threshold: 10,
                part_size: 10,
                ..TransferConfig::default()
            },
        );
        let request = UploadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("checkpoint-failure.bin").unwrap(),
            source: temp_file("checkpoint-failure.bin", &[7; 20]),
            content_type: None,
            conflict: ConflictPolicy::Overwrite,
            connection: "conn_nas".into(),
        };

        let error = engine
            .upload(&request, &rstore_transfer_sink(), &CancellationToken::new())
            .await
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Database);
        assert_eq!(storage.active_upload_count(), 0);
    }

    #[tokio::test]
    async fn skip_conflict_leaves_target_untouched() {
        let storage = Arc::new(FakeStorage::new());
        let engine = engine(storage.clone());
        let source = temp_file("s.txt", b"one");
        let cancel = CancellationToken::new();

        storage
            .put(
                "media",
                &key("s.txt"),
                bytes::Bytes::from_static(b"existing"),
                &PutOptions::default(),
            )
            .await
            .unwrap();

        let req = UploadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("s.txt").unwrap(),
            source,
            content_type: None,
            conflict: ConflictPolicy::Skip,
            connection: "conn_nas".into(),
        };
        let out = engine
            .upload(&req, &rstore_transfer_sink(), &cancel)
            .await
            .unwrap();
        assert!(out.skipped);
        assert_eq!(
            storage.stat("media", &key("s.txt")).await.unwrap().size,
            8,
            "existing object untouched"
        );
    }

    #[tokio::test]
    async fn cancellation_before_start_fails_cancelled() {
        let storage = Arc::new(FakeStorage::new());
        let engine = engine(storage);
        let source = temp_file("c.txt", b"x");
        let cancel = CancellationToken::new();
        cancel.cancel();
        let req = UploadRequest {
            bucket: "media".into(),
            key: ObjectPath::new("c.txt").unwrap(),
            source,
            content_type: None,
            conflict: ConflictPolicy::Overwrite,
            connection: "conn_nas".into(),
        };
        let err = engine
            .upload(&req, &rstore_transfer_sink(), &cancel)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Cancelled);
    }

    fn rstore_transfer_sink() -> crate::progress::NullProgressSink {
        crate::progress::NullProgressSink
    }
}
