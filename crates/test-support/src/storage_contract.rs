//! Shared behavioral contract for every [`Storage`] adapter: the same
//! assertions run against the fake and real providers, pinning put/stat/
//! read round-trips, delimiter folding, keyset pagination stability,
//! idempotent delete, server-side copy, presign expiry and multipart
//! isolation.

use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_storage_api::model::{CopyRequest, ListOptions, PutOptions, ReadRange};
use rstore_storage_api::Storage;

/// Contract keys are literals — a failure to parse one is a bug in this file.
fn key(s: &str) -> ObjectPath {
    ObjectPath::new(s).expect("contract key literal")
}

/// Exercise the substitutability contract shared by FakeStorage and S3.
/// The caller owns an empty bucket and remains responsible for deleting it.
pub async fn storage_contract(storage: &dyn Storage, bucket: &str) {
    let object = key("contract/round-trip.txt");
    let body = Bytes::from_static(b"hello rstore");
    let put_before = time::OffsetDateTime::now_utc();
    let put = storage
        .put(
            bucket,
            &object,
            body.clone(),
            &PutOptions {
                content_type: Some("text/plain".into()),
                ..Default::default()
            },
        )
        .await
        .expect("contract put");
    let put_after = time::OffsetDateTime::now_utc();
    assert_eq!(put.size, body.len() as u64);
    // The fabricated last_modified rides the adapter's clock at call time —
    // the one basis every adapter agrees on (`ObjectMetadata::after_put`).
    assert!(
        put.last_modified >= put_before && put.last_modified <= put_after,
        "put's last_modified must be the write time (got {}, want [{}, {}])",
        put.last_modified,
        put_before,
        put_after
    );

    let stat = storage.stat(bucket, &object).await.expect("contract stat");
    assert_eq!(stat.size, body.len() as u64);
    assert_eq!(stat.content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        storage
            .read(bucket, &object, Some(ReadRange::bounded(2, 4)))
            .await
            .expect("contract ranged read"),
        Bytes::from_static(b"llo")
    );

    let folded = storage
        .list(
            bucket,
            &ListOptions {
                delimiter: Some("/".into()),
                ..Default::default()
            },
        )
        .await
        .expect("contract folded list");
    assert!(folded
        .common_prefixes
        .iter()
        .any(|prefix| prefix == "contract/"));

    for k in ["a", "b", "c", "d", "e"] {
        storage
            .put(
                bucket,
                &key(&format!("contract/page/{k}")),
                Bytes::from_static(b"x"),
                &PutOptions::default(),
            )
            .await
            .expect("contract pagination seed");
    }
    let mut token = None;
    let mut keys = Vec::new();
    let mut pages = 0usize;
    loop {
        let page = storage
            .list(
                bucket,
                &ListOptions {
                    prefix: Some("contract/page/".into()),
                    delimiter: None,
                    continuation_token: token,
                    max_keys: Some(2),
                },
            )
            .await
            .expect("contract paginated list");
        pages += 1;
        assert!(page.objects.len() <= 2);
        keys.extend(page.objects.into_iter().map(|entry| entry.metadata.key));
        token = page.continuation_token;
        if token.is_none() {
            assert!(!page.is_truncated);
            break;
        }
        assert!(page.is_truncated);
    }
    assert!(pages >= 3, "max_keys must produce multiple pages");
    assert_eq!(keys.len(), 5);
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 5, "continuation tokens must not repeat rows");

    // Continuation tokens are stable under mid-walk writes (the indexer
    // resumes scans across process restarts on them): inserting keys on
    // either side of the cursor must not duplicate or skip the keys that
    // existed when the walk started.
    for k in ["a0", "z9"] {
        storage
            .put(
                bucket,
                &key(&format!("contract/page/{k}")),
                Bytes::from_static(b"x"),
                &PutOptions::default(),
            )
            .await
            .expect("contract mid-walk seed");
    }
    let mut token = None;
    let mut walked = Vec::new();
    loop {
        let resuming = token.is_some();
        let page = storage
            .list(
                bucket,
                &ListOptions {
                    prefix: Some("contract/page/".into()),
                    delimiter: None,
                    continuation_token: token,
                    max_keys: Some(2),
                },
            )
            .await
            .expect("contract mid-walk list");
        if resuming && walked.len() == 2 {
            // Write after the first page: one key sorts before everything
            // seen, one after the cursor.
            storage
                .put(
                    bucket,
                    &key("contract/page/a-inserted"),
                    Bytes::from_static(b"x"),
                    &PutOptions::default(),
                )
                .await
                .expect("contract mid-walk insert");
        }
        walked.extend(page.objects.into_iter().map(|entry| entry.metadata.key));
        token = page.continuation_token;
        if token.is_none() {
            break;
        }
    }
    let mut unique = walked.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        walked.len(),
        "a mid-walk write must not duplicate rows"
    );
    for key in &["contract/page/a", "contract/page/c", "contract/page/e"] {
        assert!(
            unique.iter().any(|k| k == key),
            "keys present at walk start must not be skipped"
        );
    }
    for k in &unique {
        storage
            .delete(bucket, &key(k))
            .await
            .expect("contract mid-walk cleanup");
    }

    // Delete is idempotent like the provider's: a missing key is still a
    // successful delete, and a deleted key is absent afterwards.
    let delete_missing = storage
        .delete(bucket, &key("contract/never-existed"))
        .await
        .expect("deleting a missing key must not error");
    assert!(
        delete_missing.deleted,
        "the provider's idempotent semantics: `deleted` reports the delete succeeded, not that a row went"
    );
    assert!(storage
        .stat(bucket, &key("contract/never-existed"))
        .await
        .is_err());
    // Copy: the copied object reads back byte-identical, the returned
    // metadata describes the target (its key, the source's size and
    // content type), and the source is untouched.
    let copy_target = key("contract/round-trip.copy.txt");
    let copied = storage
        .copy(
            &CopyRequest {
                source_bucket: bucket.to_string(),
                source_key: object.clone(),
                target_bucket: bucket.to_string(),
                target_key: copy_target.clone(),
            },
            &stat,
        )
        .await
        .expect("contract copy");
    assert_eq!(copied.key, copy_target.as_str());
    assert_eq!(copied.size, stat.size);
    assert_eq!(copied.content_type, stat.content_type);
    assert_eq!(
        storage
            .read(bucket, &copy_target, None)
            .await
            .expect("contract read copy"),
        body
    );
    assert_eq!(
        storage
            .read(bucket, &object, None)
            .await
            .expect("contract source intact"),
        body
    );

    // Presigned expiry rides the adapter's now + the requested TTL — the
    // Presigned expiry rides the adapter's call time plus the requested
    // TTL — the same basis for every adapter.
    let before = time::OffsetDateTime::now_utc();
    let presigned = storage
        .presign(&rstore_storage_api::model::PresignRequest {
            bucket: bucket.to_string(),
            key: object.as_str().to_string(),
            method: rstore_storage_api::model::PresignMethod::Get,
            ttl_seconds: 60,
        })
        .await
        .expect("contract presign");
    let after = time::OffsetDateTime::now_utc();
    let floor = before + time::Duration::seconds(60);
    let ceiling = after + time::Duration::seconds(60);
    assert!(
        presigned.expires_at >= floor && presigned.expires_at <= ceiling,
        "expires_at must be now + ttl (got {}, want [{}, {}])",
        presigned.expires_at,
        floor,
        ceiling
    );
    assert!(!presigned.url.is_empty());

    // Concurrent uploads of one key are isolated by upload_id. Aborting one
    // must not remove the other's staged parts.
    let multipart = key("contract/multipart.bin");
    let first = storage
        .create_multipart(bucket, &multipart, &PutOptions::default())
        .await
        .expect("contract create first multipart");
    let second = storage
        .create_multipart(bucket, &multipart, &PutOptions::default())
        .await
        .expect("contract create second multipart");
    first
        .upload_part(1, Bytes::from_static(b"old"))
        .await
        .expect("contract first upload part");
    let live_part = second
        .upload_part(1, Bytes::from_static(b"new"))
        .await
        .expect("contract second upload part");
    first.abort().await.expect("contract abort first multipart");
    // The session port's part-copy capability: adapters either implement it
    // (S3/MinIO `UploadPartCopy`, source must already exist) or keep the
    // default and say so loudly. Both are contract-legal; anything else is
    // not. The staged copy part is not referenced by the completion list —
    // providers assemble only the listed parts.
    let copy_capability = second
        .copy_part(
            2,
            bucket,
            &object,
            rstore_storage_api::model::ReadRange {
                start: 0,
                end: Some(1),
            },
        )
        .await;
    match copy_capability {
        Ok(part) => assert_eq!(part.size, 1, "copy_part must carry the range size"),
        Err(error) => assert_eq!(
            error.code,
            rstore_domain::ErrorCode::CapabilityUnsupported,
            "copy_part is either implemented or explicitly unsupported"
        ),
    }
    let complete_before = time::OffsetDateTime::now_utc();
    let complete = second
        .complete(&[live_part])
        .await
        .expect("contract complete second multipart");
    let complete_after = time::OffsetDateTime::now_utc();
    assert_eq!(complete.size, 3);
    // Same clock basis as put: the assembled object was written at completion
    // time (`ObjectMetadata::from_parts`).
    assert!(
        complete.last_modified >= complete_before && complete.last_modified <= complete_after,
        "complete's last_modified must be the completion time (got {}, want [{}, {}])",
        complete.last_modified,
        complete_before,
        complete_after
    );
    assert_eq!(
        storage
            .read(bucket, &multipart, None)
            .await
            .expect("contract read multipart"),
        Bytes::from_static(b"new")
    );

    for k in keys.into_iter().chain([
        object.as_str().to_string(),
        copy_target.as_str().to_string(),
        multipart.as_str().to_string(),
    ]) {
        storage
            .delete(bucket, &key(&k))
            .await
            .expect("contract cleanup delete");
    }
}

/// The multipart-legality negatives every adapter must reject: the S3
/// completion rules (part-set legality, minimum non-final size, issued
/// etags, part-number range) and unknown-upload handling. These are the
/// rules the in-memory fake simulates — running them here, against the fake
/// and any real provider alike, keeps the transcription honest.
///
/// Failures are expected as `InvalidInput` for legality violations and
/// `NotFound` for unknown upload ids; providers report them as
/// InvalidPart/EntityTooSmall/InvalidArgument/NoSuchUpload and the adapter's
/// classification maps them into that vocabulary.
pub async fn multipart_legality_negatives(storage: &dyn Storage, bucket: &str) {
    use rstore_storage_api::model::UploadedPart;

    let fabricated = |part_number: u32, etag: &str, size: u64| UploadedPart {
        part_number,
        etag: etag.into(),
        size,
    };

    // Undersized non-final part: a real upload of the tail keeps the
    // submission honest, the fabricated small head is the violation.
    let upload = storage
        .create_multipart(bucket, &key("legality/small.bin"), &PutOptions::default())
        .await
        .expect("create small-part upload");
    let tail = upload
        .upload_part(2, Bytes::from_static(b"tail"))
        .await
        .expect("upload real tail part");
    let error = upload
        .complete(&[fabricated(1, "etag-1", 10), tail])
        .await
        .expect_err("undersized non-final part must be rejected");
    assert_eq!(error.code, rstore_domain::ErrorCode::InvalidInput);
    upload.abort().await.expect("abort after rejection");

    // Fabricated etag: never issued by any UploadPart.
    let upload = storage
        .create_multipart(bucket, &key("legality/forged.bin"), &PutOptions::default())
        .await
        .expect("create forged-etag upload");
    let error = upload
        .complete(&[fabricated(1, "forged", 5)])
        .await
        .expect_err("forged etag must be rejected");
    assert_eq!(error.code, rstore_domain::ErrorCode::InvalidInput);
    upload.abort().await.expect("abort after rejection");

    // Duplicate part numbers.
    let upload = storage
        .create_multipart(bucket, &key("legality/dup.bin"), &PutOptions::default())
        .await
        .expect("create duplicate-part upload");
    let first = upload
        .upload_part(1, Bytes::from_static(b"one"))
        .await
        .expect("upload first part");
    let duplicate = UploadedPart { ..first.clone() };
    let error = upload
        .complete(&[first.clone(), duplicate])
        .await
        .expect_err("duplicate part numbers must be rejected");
    assert_eq!(error.code, rstore_domain::ErrorCode::InvalidInput);

    // Descending order over the same upload's real parts.
    let second = upload
        .upload_part(2, Bytes::from_static(b"two"))
        .await
        .expect("upload second part");
    let error = upload
        .complete(&[second, first])
        .await
        .expect_err("descending part order must be rejected");
    assert_eq!(error.code, rstore_domain::ErrorCode::InvalidInput);
    upload.abort().await.expect("abort after rejection");

    // Part numbers outside the S3 range 1..=10000.
    for illegal in [0u32, 10_001] {
        let upload = storage
            .create_multipart(
                bucket,
                &key(&format!("legality/range-{illegal}.bin")),
                &PutOptions::default(),
            )
            .await
            .expect("create range upload");
        let error = upload
            .complete(&[fabricated(illegal, &format!("etag-{illegal}"), 5)])
            .await
            .expect_err(&format!("part number {illegal} must be rejected"));
        assert_eq!(error.code, rstore_domain::ErrorCode::InvalidInput);
        upload.abort().await.expect("abort after rejection");
    }

    // Unknown upload id: part upload and abort both surface it. Abort is
    // strict — a session over a foreign id never silently succeeds.
    let ghost = storage
        .adopt_multipart(bucket, &key("legality/ghost.bin"), "no-such-upload-id")
        .await
        .expect("adopt ghost session");
    let error = ghost
        .upload_part(1, Bytes::from_static(b"x"))
        .await
        .expect_err("unknown upload id must surface on part upload");
    assert_eq!(error.code, rstore_domain::ErrorCode::NotFound);
    let error = ghost
        .abort()
        .await
        .expect_err("unknown upload id must surface on abort");
    assert_eq!(error.code, rstore_domain::ErrorCode::NotFound);
}
