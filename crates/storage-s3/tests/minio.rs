//! MinIO integration test (QA-005).
//!
//! Gated behind `RSTORE_MINIO_ENDPOINT` so it only runs when a MinIO instance is
//! reachable. Run locally, e.g.:
//!
//! ```sh
//! docker run -d --rm -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
//!   -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
//! RSTORE_MINIO_ENDPOINT=http://localhost:9000 \
//!   cargo test -p rstore-storage-s3 --test minio -- --ignored --nocapture
//! ```

use rstore_domain::connection::{AddressingStyle, ConnectionConfig, Region};
use rstore_domain::Secret;
use rstore_storage_api::StorageCredentials;
use rstore_storage_s3::{build_client, S3Storage};
use time::OffsetDateTime;

struct Minio {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

fn load_minio() -> Option<Minio> {
    let endpoint = std::env::var("RSTORE_MINIO_ENDPOINT").ok()?;
    Some(Minio {
        endpoint,
        access_key: std::env::var("RSTORE_MINIO_ACCESS_KEY")
            .unwrap_or_else(|_| "minioadmin".to_string()),
        secret_key: std::env::var("RSTORE_MINIO_SECRET_KEY")
            .unwrap_or_else(|_| "minioadmin".to_string()),
    })
}

fn make_storage(m: &Minio, addressing: AddressingStyle) -> S3Storage {
    let config = ConnectionConfig {
        id: rstore_domain::ConnectionId::new(),
        name: "minio-test".into(),
        endpoint: m.endpoint.clone(),
        public_base_url: None,
        cdn_base_url: None,
        region: Region::Auto,
        addressing_style: addressing,
        tls_verify: true,
        access_key: None,
        secret_key: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
        last_tested_at: None,
        last_test_status: None,
    };
    let creds = StorageCredentials {
        access_key: Secret::new(m.access_key.clone()),
        secret_key: Secret::new(m.secret_key.clone()),
    };
    S3Storage::new(build_client(&config, &creds))
}

async fn ensure_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    let _ = client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| panic!("create_bucket failed: {e:?}"));
}

/// The raw SDK client for bucket-admin calls, built through the same
/// `build_client` path as the adapter under test.
fn creds_for(m: &Minio) -> aws_sdk_s3::Client {
    let config = ConnectionConfig {
        id: rstore_domain::ConnectionId::new(),
        name: "minio-admin".into(),
        endpoint: m.endpoint.clone(),
        public_base_url: None,
        cdn_base_url: None,
        region: Region::Auto,
        addressing_style: AddressingStyle::Path,
        tls_verify: true,
        access_key: None,
        secret_key: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
        last_tested_at: None,
        last_test_status: None,
    };
    build_client(
        &config,
        &StorageCredentials {
            access_key: Secret::new(m.access_key.clone()),
            secret_key: Secret::new(m.secret_key.clone()),
        },
    )
}

#[tokio::test]
#[ignore = "requires a live MinIO at RSTORE_MINIO_ENDPOINT"]
async fn minio_satisfies_storage_contract() {
    let Some(minio) = load_minio() else {
        eprintln!("RSTORE_MINIO_ENDPOINT not set; skipping MinIO integration test");
        return;
    };

    let admin = creds_for(&minio);
    let bucket = format!(
        "rstore-it-{}",
        OffsetDateTime::now_utc().unix_timestamp_nanos() % 1_000_000_000
    );
    ensure_bucket(&admin, &bucket).await;

    let storage = make_storage(&minio, AddressingStyle::Path);
    rstore_test_support::storage_contract(&storage, &bucket).await;

    // The part-copy path (STORE-016, objects beyond the atomic limit) with
    // the limit lowered so a small object exercises `UploadPartCopy`: the
    // copied bytes are identical and the size survives the part assembly.
    use rstore_domain::remote::ObjectPath;
    use rstore_storage_api::model::{CopyRequest, PutOptions};
    use rstore_storage_api::Storage as _;
    // S3 requires non-final parts of at least 5 MiB, so the limit stays at
    // the real minimum and the source exceeds it by two parts plus a tail.
    const PART: u64 = 5 * 1024 * 1024;
    let small_limit = S3Storage::with_single_copy_limit(creds_for(&minio), PART);
    let source = ObjectPath::new("contract/parted-source.bin").unwrap();
    let target = ObjectPath::new("contract/parted-target.bin").unwrap();
    let payload: Vec<u8> = (0..(PART * 2 + 1024) as usize)
        .map(|i| (i % 251) as u8)
        .collect();
    small_limit
        .put(
            &bucket,
            &source,
            bytes::Bytes::from(payload.clone()),
            &PutOptions::default(),
        )
        .await
        .expect("parted-copy source put");
    let source_meta = small_limit
        .stat(&bucket, &source)
        .await
        .expect("parted-copy source stat");
    let copied = small_limit
        .copy(
            &CopyRequest {
                source_bucket: bucket.clone(),
                source_key: source.clone(),
                target_bucket: bucket.clone(),
                target_key: target.clone(),
            },
            &source_meta,
        )
        .await
        .expect("parted copy");
    assert_eq!(
        copied.size as usize,
        payload.len(),
        "parts must reassemble the source size"
    );
    assert_eq!(
        small_limit
            .read(&bucket, &target, None)
            .await
            .expect("parted-copy read")
            .to_vec(),
        payload,
        "part copy must be byte-exact"
    );
    for key in [&source, &target] {
        let _ = small_limit.delete(&bucket, key).await;
    }

    // Cleanup.
    let _ = admin.delete_bucket().bucket(&bucket).send().await;
}

/// The README interrupt-resume claim, automated: a `TransferEngine` upload
/// over the real adapter is cancelled after the first part, leaves no
/// complete object behind, and a second run finishes byte-exact from the
/// checkpointed parts. This is the only test that composes engine + S3
/// adapter + SQLite checkpoints — the world the offline fake tests and the
/// contract suite above never cover together.
#[tokio::test]
#[ignore = "requires a live MinIO at RSTORE_MINIO_ENDPOINT"]
async fn minio_engine_resumes_an_interrupted_upload_byte_exact() {
    use rstore_domain::ConflictPolicy;
    use rstore_persistence::Database;
    use rstore_storage_api::Storage as _;
    use rstore_transfer::{
        CancellationToken, Progress, ProgressSink, TransferConfig, TransferEngine, UploadRequest,
    };
    use std::sync::Arc;

    const PART: u64 = 5 * 1024 * 1024;

    /// Cancels the run once a part boundary is crossed: deterministic
    /// mid-flight interruption. `done` is the engine's cumulative counter,
    /// so a single comparison suffices.
    struct CancelAfterFirstPart {
        threshold: u64,
        token: CancellationToken,
    }

    impl ProgressSink for CancelAfterFirstPart {
        fn report(&self, progress: Progress) {
            if let Progress::Bytes { done, .. } = progress {
                if done >= self.threshold {
                    self.token.cancel();
                }
            }
        }
    }

    let Some(minio) = load_minio() else {
        eprintln!("RSTORE_MINIO_ENDPOINT not set; skipping MinIO integration test");
        return;
    };
    let admin = creds_for(&minio);
    let bucket = format!(
        "rstore-engine-{}",
        OffsetDateTime::now_utc().unix_timestamp_nanos() % 1_000_000_000
    );
    ensure_bucket(&admin, &bucket).await;

    let storage = Arc::new(make_storage(&minio, AddressingStyle::Path));
    // The database handle must outlive the checkpoint store (the store owns
    // only cloned pools).
    let checkpoints_db = Database::open_in_memory().await.unwrap();
    let checkpoints = Arc::new(checkpoints_db.multipart(Arc::new(rstore_domain::SystemClock)));
    let engine = TransferEngine::new(
        storage.clone(),
        checkpoints.clone(),
        TransferConfig {
            multipart_threshold: PART,
            part_size: PART,
            download_chunk: 1024 * 1024,
        },
    );

    // Source: two full parts plus a tail so completion needs all three.
    let payload: Vec<u8> = (0..(PART * 2 + 1024) as usize)
        .map(|i| (i % 251) as u8)
        .collect();
    let source_dir = tempfile::tempdir().unwrap();
    let source = source_dir.path().join("resume-source.bin");
    std::fs::write(&source, &payload).unwrap();

    let key = rstore_domain::remote::ObjectPath::new("engine/resume.bin").unwrap();
    let make_request = || UploadRequest {
        bucket: bucket.clone(),
        key: key.clone(),
        source: source.clone(),
        content_type: None,
        conflict: ConflictPolicy::Overwrite,
        connection: "conn_minio".into(),
    };

    // Run 1: cancelled right after the first part lands on the provider.
    let token = CancellationToken::new();
    let sink = CancelAfterFirstPart {
        threshold: PART,
        token: token.clone(),
    };
    let error = engine
        .upload(&make_request(), &sink, &token)
        .await
        .expect_err("run must be interrupted by the injected cancel");
    assert_eq!(error.code, rstore_domain::ErrorCode::Cancelled);
    assert!(
        storage.stat(&bucket, &key).await.is_err(),
        "an interrupted upload must not leave a complete object"
    );

    // Run 2: same request, fresh run resumes from SQLite checkpoints and
    // finishes byte-exact.
    let outcome = engine
        .upload(
            &make_request(),
            &rstore_transfer::NullProgressSink,
            &CancellationToken::new(),
        )
        .await
        .expect("resumed run must complete");
    assert_eq!(outcome.bytes as usize, payload.len());
    let body = storage.read(&bucket, &key, None).await.unwrap();
    assert_eq!(&body[..], &payload[..], "resumed bytes must be byte-exact");

    let _ = storage.delete(&bucket, &key).await;
    let _ = admin.delete_bucket().bucket(&bucket).send().await;
}
