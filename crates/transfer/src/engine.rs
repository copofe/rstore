//! Transfer execution: resumable uploads and downloads against one
//! [`Storage`] adapter.
//!
//! Stateless between calls except the checkpoint store — multipart uploads
//! checkpoint after every part, downloads stage through a `.part` file and
//! rename atomically on completion; retries live at the storage seam
//! ([`crate::RetryingStorage`]), never inside the engine.

use std::path::PathBuf;

use rstore_domain::id::ConnectionId;
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, Result};

use rstore_storage_api::model::{ObjectMetadata, PutOptions};
use rstore_storage_api::{guess_content_type, FileFingerprint, MultipartStore, Storage};

use crate::conflict::{decide_target, path_probe, stat_probe, TargetDecision};
use crate::multipart;
use crate::progress::Progress;
use crate::pump::{pump, ChunkPlan, ChunkSink, RangedSource};
use crate::run::{ItemOutcome, RunContext};

/// Tuning knobs for transfer sizes and thresholds.
/// One upload request (local file → remote object).
#[derive(Debug, Clone)]
pub struct UploadRequest {
    pub bucket: String,
    pub key: ObjectPath,
    pub source: PathBuf,
    pub content_type: Option<String>,
    pub conflict: ConflictPolicy,
    /// Typed connection identity for the multipart checkpoint.
    pub connection: ConnectionId,
}

/// One download request (remote object → local file).
#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub bucket: String,
    pub key: ObjectPath,
    pub target: PathBuf,
    pub conflict: ConflictPolicy,
}

/// The engine: stateless between calls except the checkpoint store.
pub struct TransferEngine {
    storage: std::sync::Arc<dyn Storage>,
    checkpoints: std::sync::Arc<dyn rstore_storage_api::MultipartStore>,
}

impl TransferEngine {
    /// Creates an engine bound to one storage adapter and its checkpoint
    /// store; tuning rides the per-run [`RunContext`].
    pub fn new(
        storage: std::sync::Arc<dyn Storage>,
        checkpoints: std::sync::Arc<dyn MultipartStore>,
    ) -> Self {
        Self {
            storage,
            checkpoints,
        }
    }

    /// Upload one file, honouring conflict policy, resume and cancellation.
    pub async fn upload(&self, req: &UploadRequest, ctx: &RunContext<'_>) -> Result<ItemOutcome> {
        let cancel = ctx.cancel();
        cancel.check()?;
        let source_metadata = std::fs::metadata(&req.source).map_err(|e| {
            Error::not_found(format!("local file `{}`", req.source.display())).with_source(e)
        })?;
        let size = source_metadata.len();
        let fingerprint = FileFingerprint::of(&source_metadata);

        // Conflict policy against an existing remote object: the decision
        // table, its interpretation and its reaction (rename re-entry
        // included) live in this crate — never at call sites.
        let key = match decide_target(
            req.key.as_str(),
            req.conflict,
            stat_probe(&self.storage, &req.bucket).as_ref(),
            |name| ObjectPath::new(name.to_owned()),
        )
        .await?
        {
            TargetDecision::Finish(outcome) => return Ok(*outcome),
            TargetDecision::Write(key) => key,
        };

        let opts = PutOptions {
            content_type: Some(
                req.content_type
                    .clone()
                    .unwrap_or_else(|| guess_content_type(&req.source)),
            ),
            ..Default::default()
        };

        let metadata = if size < ctx.config().multipart_threshold {
            self.upload_small(req, &key, size, &opts, ctx).await?
        } else {
            self.upload_multipart(req, &key, fingerprint, &opts, ctx)
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
        ctx: &RunContext<'_>,
    ) -> Result<ObjectMetadata> {
        let progress = ctx.progress();
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
        ctx: &RunContext<'_>,
    ) -> Result<ObjectMetadata> {
        let (cancel, progress) = (ctx.cancel(), ctx.progress());
        let size = fingerprint.size;
        let (connection, bucket) = (req.connection, req.bucket.as_str());
        let file = tokio::fs::File::open(&req.source)
            .await
            .map_err(|e| Error::io("open upload source").with_source(e))?;
        let mut reader = tokio::io::BufReader::with_capacity(ctx.config().part_size as usize, file);

        // The one resume policy: adopt a matching checkpoint, strictly retire
        // a stale one, or begin fresh.
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

        // One counter — bytes already safely stored. `pending` owns both the
        // skip decision and the part numbering, so the progress counter can
        // never drift from the plan positions.
        let mut progress_bytes = done_bytes;
        for (part_number, range) in
            ChunkPlan::new(size, ctx.config().part_size).pending(&done_numbers)
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
        // The per-part loop already reported the terminal count; emit here
        // only when resume skipped every pending part.
        if progress_bytes < size {
            progress.report(Progress::Bytes {
                done: size,
                total: size,
            });
        }
        Ok(metadata)
    }

    /// Download one object with `.part` staging and byte-exact resume.
    pub async fn download(
        &self,
        req: &DownloadRequest,
        ctx: &RunContext<'_>,
    ) -> Result<ItemOutcome> {
        let (cancel, progress) = (ctx.cancel(), ctx.progress());
        cancel.check()?;
        let meta = self.storage.stat(&req.bucket, &req.key).await?;

        // Local conflict policy mirrors the upload set: the one decision,
        // interpretation and reaction.
        let target = match decide_target(
            &req.target.to_string_lossy(),
            req.conflict,
            path_probe().as_ref(),
            |name| Ok(std::path::PathBuf::from(name.to_owned())),
        )
        .await?
        {
            TargetDecision::Finish(outcome) => return Ok(*outcome),
            TargetDecision::Write(target) => target,
        };

        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::io("create download dir").with_source(e))?;
        }

        // Staging policy: identity-checked resume, atomic promotion — one
        // module owns the `.part` lifetime.
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
            ChunkPlan::resumed(meta.size, ctx.config().download_chunk, staged.offset),
            ctx,
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
