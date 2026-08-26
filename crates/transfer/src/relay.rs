//! Constant-memory object relay across storage adapters.
//!
//! On any error after the target multipart upload exists, the upload is
//! abandoned warn-only — the relay keeps no checkpoint, so there is nothing
//! to resume — and the original error returns. Chunk policy and progress
//! ride the engine's [`TransferConfig`] and [`ProgressSink`] seams.

use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;
use rstore_storage_api::{MultipartSession, ObjectMetadata, PutOptions, Storage, UploadedPart};

use crate::progress::Progress;
use crate::pump::{pump, ChunkPlan, ChunkSink, RangedSource};

/// The locators and source metadata for one cross-adapter relay.
pub(crate) struct RelayRequest<'a> {
    source_bucket: &'a str,
    source_key: &'a ObjectPath,
    target_bucket: &'a str,
    target_key: &'a ObjectPath,
    metadata: &'a ObjectMetadata,
}

impl<'a> RelayRequest<'a> {
    pub(crate) fn new(
        source_bucket: &'a str,
        source_key: &'a ObjectPath,
        target_bucket: &'a str,
        target_key: &'a ObjectPath,
        metadata: &'a ObjectMetadata,
    ) -> Self {
        Self {
            source_bucket,
            source_key,
            target_bucket,
            target_key,
            metadata,
        }
    }
}

/// Relay one object without buffering it whole in memory.
pub(crate) async fn relay(
    source: &dyn Storage,
    target: &dyn Storage,
    request: &RelayRequest<'_>,
    ctx: &crate::run::RunContext<'_>,
) -> Result<ObjectMetadata> {
    let (cancel, config, progress) = (ctx.cancel(), ctx.config(), ctx.progress());
    cancel.check()?;
    let opts = PutOptions {
        content_type: request.metadata.content_type.clone(),
        ..PutOptions::default()
    };
    let size = request.metadata.size;
    if size < config.multipart_threshold {
        let body = source
            .read(request.source_bucket, request.source_key, None)
            .await?;
        cancel.check()?;
        let metadata = target
            .put(request.target_bucket, request.target_key, body, &opts)
            .await?;
        progress.report(Progress::Bytes {
            done: size,
            total: size,
        });
        return Ok(metadata);
    }

    let session = target
        .create_multipart(request.target_bucket, request.target_key, &opts)
        .await?;
    let result = relay_parts(source, session.as_ref(), request, ctx).await;
    if result.is_err() {
        // No checkpoint exists to resume from — clean up the provider
        // upload, warn-only; the original error stays the answer.
        session.abandon().await;
    }
    result
}

/// One cross-adapter part pump: read a chunk from the source, upload it as
/// the next part. The relay-specific residue is exactly this sink — the walk,
/// cancellation and progress are the shared [`pump`].
struct PartUploader<'s> {
    session: &'s dyn MultipartSession,
    parts: Vec<UploadedPart>,
}

#[async_trait::async_trait]
impl ChunkSink for PartUploader<'_> {
    async fn accept(&mut self, index: usize, body: bytes::Bytes) -> Result<()> {
        let part = self.session.upload_part(index as u32 + 1, body).await?;
        self.parts.push(part);
        Ok(())
    }
}

async fn relay_parts(
    source: &dyn Storage,
    session: &dyn MultipartSession,
    request: &RelayRequest<'_>,
    ctx: &crate::run::RunContext<'_>,
) -> Result<ObjectMetadata> {
    let (cancel, config) = (ctx.cancel(), ctx.config());
    let mut uploader = PartUploader {
        session,
        parts: Vec::new(),
    };
    pump(
        &RangedSource::new(source, request.source_bucket, request.source_key),
        ChunkPlan::new(request.metadata.size, config.part_size),
        ctx,
        &mut uploader,
    )
    .await?;
    cancel.check()?;
    session.complete(&uploader.parts).await
}
