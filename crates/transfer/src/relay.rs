//! Constant-memory object relay across storage adapters (MOVE-006).
//!
//! The provider multipart lifetime runs through
//! [`MultipartSession`](crate::multipart::MultipartSession): once an upload
//! is created, every read, upload, completion, or cancellation error takes
//! the same best-effort `abandon` path before the original error returns —
//! the relay has no checkpoint to resume from.
//!
//! Chunk policy and progress ride the engine's seams: [`TransferConfig`]
//! decides read+put versus multipart and the part size, and a
//! [`ProgressSink`] receives per-part byte progress like any transfer.

use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;
use rstore_storage_api::{ObjectMetadata, PutOptions, Storage, UploadedPart};

use crate::multipart::MultipartSession;
use crate::progress::{Progress, ProgressSink};
use crate::pump::{pump, ChunkPlan, ChunkSink, RangedSource};
use crate::{CancellationToken, TransferConfig};

/// The locators and source metadata for one cross-adapter relay.
pub struct RelayRequest<'a> {
    source_bucket: &'a str,
    source_key: &'a ObjectPath,
    target_bucket: &'a str,
    target_key: &'a ObjectPath,
    metadata: &'a ObjectMetadata,
}

impl<'a> RelayRequest<'a> {
    pub fn new(
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
pub async fn relay(
    source: &dyn Storage,
    target: &dyn Storage,
    request: &RelayRequest<'_>,
    cancel: &CancellationToken,
    config: &TransferConfig,
    progress: &dyn ProgressSink,
) -> Result<ObjectMetadata> {
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

    let session =
        MultipartSession::create(target, request.target_bucket, request.target_key, &opts).await?;
    let result = relay_parts(source, &session, request, cancel, config, progress).await;
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
struct PartUploader<'a, 's> {
    session: &'s MultipartSession<'a>,
    parts: Vec<UploadedPart>,
}

#[async_trait::async_trait]
impl ChunkSink for PartUploader<'_, '_> {
    async fn accept(&mut self, index: usize, body: bytes::Bytes) -> Result<()> {
        let part = self.session.upload_part(index as u32 + 1, body).await?;
        self.parts.push(part);
        Ok(())
    }
}

async fn relay_parts(
    source: &dyn Storage,
    session: &MultipartSession<'_>,
    request: &RelayRequest<'_>,
    cancel: &CancellationToken,
    config: &TransferConfig,
    progress: &dyn ProgressSink,
) -> Result<ObjectMetadata> {
    let mut uploader = PartUploader {
        session,
        parts: Vec::new(),
    };
    pump(
        &RangedSource::new(source, request.source_bucket, request.source_key),
        ChunkPlan::new(request.metadata.size, config.part_size),
        cancel,
        progress,
        &mut uploader,
    )
    .await?;
    cancel.check()?;
    session.complete(&uploader.parts).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstore_domain::{Error, ErrorCode};
    use rstore_test_support::{FakeStorage, StorageMethod};

    use super::*;
    use rstore_domain::remote::ObjectPath;

    async fn source_fixture() -> (Arc<FakeStorage>, ObjectMetadata) {
        let source = Arc::new(FakeStorage::new());
        source.seed("source", "large.bin", b"abcdefghij", None);
        let metadata = source
            .stat("source", &ObjectPath::new("large.bin").unwrap())
            .await
            .expect("seeded source metadata");
        (source, metadata)
    }

    fn request<'a>(
        source: &'a ObjectPath,
        target: &'a ObjectPath,
        metadata: &'a ObjectMetadata,
    ) -> RelayRequest<'a> {
        RelayRequest::new("source", source, "target", target, metadata)
    }

    /// A tiny config so the multipart path runs on bytes, not 8 MiB.
    fn tiny_config() -> TransferConfig {
        TransferConfig {
            multipart_threshold: 4,
            part_size: 4,
            download_chunk: 4,
        }
    }

    async fn relay_tiny(
        source: &dyn Storage,
        target: &dyn Storage,
        request: &RelayRequest<'_>,
        cancel: &CancellationToken,
    ) -> rstore_domain::Result<ObjectMetadata> {
        relay(
            source,
            target,
            request,
            cancel,
            &tiny_config(),
            &crate::NullProgressSink,
        )
        .await
    }

    /// The chunk boundary table: every size around a small part size relays
    /// byte-exact (k·part, k·part±1, under one part).
    #[tokio::test]
    async fn part_boundaries_relay_byte_exact() {
        for size in [1u8, 3, 4, 5, 7, 8, 9, 12] {
            let source = Arc::new(FakeStorage::new());
            let payload: Vec<u8> = (0..size).collect();
            source.seed("source", "obj.bin", &payload, None);
            let metadata = source
                .stat("source", &ObjectPath::new("obj.bin").unwrap())
                .await
                .unwrap();
            let target = Arc::new(FakeStorage::new());

            relay_tiny(
                source.as_ref(),
                target.as_ref(),
                &RelayRequest::new(
                    "source",
                    &ObjectPath::new("obj.bin").unwrap(),
                    "target",
                    &ObjectPath::new("obj.bin").unwrap(),
                    &metadata,
                ),
                &CancellationToken::new(),
            )
            .await
            .unwrap_or_else(|e| panic!("size {size}: {e}"));

            let landed = target
                .read("target", &ObjectPath::new("obj.bin").unwrap(), None)
                .await
                .unwrap();
            assert_eq!(&landed[..], &payload[..], "size {size}");
        }
    }

    #[derive(Default)]
    struct RecordingSink(std::sync::Mutex<Vec<(u64, u64)>>);

    impl crate::ProgressSink for RecordingSink {
        fn report(&self, progress: crate::Progress) {
            if let crate::Progress::Bytes { done, total } = progress {
                self.0.lock().unwrap().push((done, total));
            }
        }
    }

    /// A relayed multipart copy reports byte progress per part like any
    /// transfer — the cross-connection path is no longer a silent one.
    #[tokio::test]
    async fn multipart_relay_reports_per_part_progress() {
        let (source, metadata) = source_fixture().await;
        let target = Arc::new(FakeStorage::new());
        let sink = RecordingSink::default();

        relay(
            source.as_ref(),
            target.as_ref(),
            &request(
                &ObjectPath::new("large.bin").unwrap(),
                &ObjectPath::new("copy.bin").unwrap(),
                &metadata,
            ),
            &CancellationToken::new(),
            &tiny_config(),
            &sink,
        )
        .await
        .unwrap();

        let reports = sink.0.lock().unwrap().clone();
        assert_eq!(reports, vec![(4, 10), (8, 10), (10, 10)]);
    }

    #[tokio::test]
    async fn read_failure_aborts_created_multipart_upload() {
        let (source, metadata) = source_fixture().await;
        source.fail_next(
            StorageMethod::Read,
            Error::network("injected relay read failure"),
        );
        let target = Arc::new(FakeStorage::new());

        let error = relay_tiny(
            source.as_ref(),
            target.as_ref(),
            &request(
                &ObjectPath::new("large.bin").unwrap(),
                &ObjectPath::new("copy.bin").unwrap(),
                &metadata,
            ),
            &CancellationToken::new(),
        )
        .await
        .expect_err("relay should fail while reading");

        assert_eq!(error.code, ErrorCode::NetworkUnreachable);
        assert_eq!(target.active_upload_count(), 0);
    }

    #[tokio::test]
    async fn part_failure_aborts_created_multipart_upload() {
        let (source, metadata) = source_fixture().await;
        let target = Arc::new(FakeStorage::new());
        target.fail_next(
            StorageMethod::UploadPart,
            Error::network("injected relay part failure"),
        );

        let error = relay_tiny(
            source.as_ref(),
            target.as_ref(),
            &request(
                &ObjectPath::new("large.bin").unwrap(),
                &ObjectPath::new("copy.bin").unwrap(),
                &metadata,
            ),
            &CancellationToken::new(),
        )
        .await
        .expect_err("relay should fail while uploading a part");

        assert_eq!(error.code, ErrorCode::NetworkUnreachable);
        assert_eq!(target.active_upload_count(), 0);
    }

    #[tokio::test]
    async fn completion_failure_aborts_created_multipart_upload() {
        let (source, metadata) = source_fixture().await;
        let target = Arc::new(FakeStorage::new());
        target.fail_next(
            StorageMethod::CompleteMultipart,
            Error::network("injected relay completion failure"),
        );

        let error = relay_tiny(
            source.as_ref(),
            target.as_ref(),
            &request(
                &ObjectPath::new("large.bin").unwrap(),
                &ObjectPath::new("copy.bin").unwrap(),
                &metadata,
            ),
            &CancellationToken::new(),
        )
        .await
        .expect_err("relay should fail while completing");

        assert_eq!(error.code, ErrorCode::NetworkUnreachable);
        assert_eq!(target.active_upload_count(), 0);
    }

    #[tokio::test]
    async fn cancellation_after_read_aborts_created_multipart_upload() {
        let (source, metadata) = source_fixture().await;
        let cancel = CancellationToken::new();
        source.after_read({
            let cancel = cancel.clone();
            move || cancel.cancel()
        });
        let target = Arc::new(FakeStorage::new());

        let error = relay_tiny(
            source.as_ref(),
            target.as_ref(),
            &request(
                &ObjectPath::new("large.bin").unwrap(),
                &ObjectPath::new("copy.bin").unwrap(),
                &metadata,
            ),
            &cancel,
        )
        .await
        .expect_err("relay should observe cancellation after reading");

        assert_eq!(error.code, ErrorCode::Cancelled);
        assert_eq!(target.active_upload_count(), 0);
    }
}
