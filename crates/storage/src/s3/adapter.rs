//! The S3-compatible [`Storage`] implementation.
//!
//! `list`, `stat`, `read`, `put`, `delete`, server-side `copy` and
//! `presign` are implemented; `copy` is verified against MinIO through the
//! shared storage contract.

use crate::model::{
    BucketSummary, CopyRequest, DeleteOutcome, ListOptions, ListPage, ObjectEntry, ObjectMetadata,
    PresignMethod, PresignRequest, PresignedUrl, PutOptions, ReadRange, UploadedPart,
};
use crate::{MultipartSession, Storage};
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::DateTime as SdkDateTime;
use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, ErrorCode, Result};

use std::num::NonZeroU64;

use crate::s3::multipart::S3MultipartSession;
use time::OffsetDateTime;

use crate::s3::error::map_sdk;

/// An S3-compatible object store backed by the AWS SDK.
#[derive(Clone)]
pub struct S3Storage {
    client: aws_sdk_s3::Client,
    /// Objects at or below this size copy in one atomic action; larger ones
    /// copy by parts. Fixed at S3's 5 GiB limit in production; the MinIO
    /// gate lowers it to exercise the part-copy path on small objects.
    single_copy_limit: u64,
}

impl S3Storage {
    /// Wrap an already-configured SDK client.
    pub fn new(client: aws_sdk_s3::Client) -> Self {
        Self {
            client,
            single_copy_limit: Self::S3_ATOMIC_COPY_LIMIT,
        }
    }

    /// The provider-fixed atomic copy limit, lowered by tests only. `NonZero`
    /// makes a zero limit unrepresentable instead of a runtime panic.
    pub fn with_single_copy_limit(client: aws_sdk_s3::Client, limit: NonZeroU64) -> Self {
        Self {
            client,
            single_copy_limit: limit.get(),
        }
    }

    /// S3's atomic single-action copy limit (5 GiB).
    const S3_ATOMIC_COPY_LIMIT: u64 = 5 * 1024 * 1024 * 1024;
}

impl S3Storage {
    async fn copy_single(
        &self,
        req: &CopyRequest,
        copy_source: &str,
        source: &ObjectMetadata,
    ) -> Result<ObjectMetadata> {
        let out = self
            .client
            .copy_object()
            .bucket(&req.target_bucket)
            .key(req.target_key.as_str())
            .copy_source(copy_source)
            .send()
            .await
            .map_err(map_sdk)?;
        let result = out.copy_object_result();
        Ok(ObjectMetadata {
            key: req.target_key.as_str().to_string(),
            size: source.size,
            etag: result.and_then(|r| r.e_tag().map(str::to_string)),
            // A server-side copy preserves the source's content type; the
            // provider's copy result does not restate it.
            content_type: source.content_type.clone(),
            last_modified: match result.and_then(|r| r.last_modified()) {
                Some(dt) => to_offset(dt)?,
                // The copy just happened; the provider did not restate when.
                None => OffsetDateTime::now_utc(),
            },
            storage_class: source.storage_class.clone(),
            version_id: out.version_id().map(str::to_string),
        })
    }

    /// Beyond the atomic limit: one multipart session of provider-side part
    /// copies (`UploadPartCopy` per range), then complete — the same session
    /// protocol every other multipart upload uses. A failed part-copy
    /// abandons the session (warn-only): server-side parts are cheap to redo,
    /// so there is nothing worth resuming.
    async fn copy_by_parts(
        &self,
        req: &CopyRequest,
        source: &ObjectMetadata,
    ) -> Result<ObjectMetadata> {
        let session = self
            .create_multipart(
                &req.target_bucket,
                &req.target_key,
                &PutOptions {
                    content_type: source.content_type.clone(),
                    ..PutOptions::default()
                },
            )
            .await?;
        let result =
            copy_parts_loop(session.as_ref(), req, source.size, self.single_copy_limit).await;
        match result {
            Ok(parts) => session.complete(&parts).await,
            Err(error) => {
                session.abandon().await;
                Err(error)
            }
        }
    }
}

/// The part-copy walk, expressed against the session port: offset-driven
/// bounded ranges of the source, one `copy_part` each.
async fn copy_parts_loop(
    session: &dyn MultipartSession,
    req: &CopyRequest,
    size: u64,
    part_size: u64,
) -> Result<Vec<UploadedPart>> {
    let mut parts = Vec::new();
    let mut offset = 0u64;
    let mut part_number = 1u32;
    while offset < size {
        let end = (offset + part_size).min(size);
        parts.push(
            session
                .copy_part(
                    part_number,
                    &req.source_bucket,
                    &req.source_key,
                    ReadRange {
                        start: offset,
                        end: Some(end),
                    },
                )
                .await?,
        );
        offset = end;
        part_number += 1;
    }
    Ok(parts)
}

#[async_trait]
impl Storage for S3Storage {
    async fn list_buckets(&self) -> Result<Vec<BucketSummary>> {
        let out = self.client.list_buckets().send().await.map_err(map_sdk)?;
        let mut buckets = Vec::new();
        for b in out.buckets() {
            let Some(name) = b.name() else { continue };
            let created_at = match b.creation_date() {
                Some(dt) => Some(to_offset(dt)?),
                None => None,
            };
            buckets.push(BucketSummary {
                name: name.to_string(),
                created_at,
            });
        }
        Ok(buckets)
    }

    async fn list(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage> {
        let mut req = self.client.list_objects_v2().bucket(bucket);

        if let Some(prefix) = &opts.prefix {
            if !prefix.is_empty() {
                req = req.prefix(prefix);
            }
        }
        // `delimiter: None` means a flat recursive listing (no folding);
        // callers wanting folder folding pass `Some("/")` explicitly.
        if let Some(delimiter) = &opts.delimiter {
            if !delimiter.is_empty() {
                req = req.delimiter(delimiter);
            }
        }
        if let Some(token) = &opts.continuation_token {
            req = req.continuation_token(token);
        }
        if let Some(max) = opts.max_keys {
            req = req.max_keys(max as i32);
        }

        let out = req.send().await.map_err(map_sdk)?;

        let objects = out
            .contents()
            .iter()
            .filter(|o| o.key().is_some())
            .map(object_entry)
            .collect::<Result<Vec<_>>>()?;

        let common_prefixes = out
            .common_prefixes()
            .iter()
            .filter_map(|p| p.prefix().map(str::to_string))
            .collect();

        Ok(ListPage {
            objects,
            common_prefixes,
            continuation_token: out.next_continuation_token().map(str::to_string),
            is_truncated: out.is_truncated().unwrap_or(false),
        })
    }

    async fn stat(&self, bucket: &str, key: &ObjectPath) -> Result<ObjectMetadata> {
        let out = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key.as_str())
            .send()
            .await
            .map_err(map_sdk)?;

        Ok(ObjectMetadata {
            key: key.as_str().to_string(),
            size: out.content_length().unwrap_or_default() as u64,
            etag: out.e_tag().map(str::to_string),
            content_type: out.content_type().map(str::to_string),
            // A provider that omits LastModified (real S3 never does) reads
            // as the epoch rather than failing the whole stat.
            last_modified: out
                .last_modified()
                .map(to_offset)
                .transpose()?
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
            storage_class: out.storage_class.as_ref().map(|s| s.as_str().to_string()),
            version_id: out.version_id().map(str::to_string),
        })
    }

    async fn read(
        &self,
        bucket: &str,
        key: &ObjectPath,
        range: Option<ReadRange>,
    ) -> Result<Bytes> {
        let mut req = self.client.get_object().bucket(bucket).key(key.as_str());
        if let Some(r) = range {
            req = req.range(range_header(r));
        }
        let out = req.send().await.map_err(map_sdk)?;
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| {
                Error::wrap(
                    ErrorCode::NetworkUnreachable,
                    "failed reading object body",
                    e,
                )
            })?
            .into_bytes();
        Ok(bytes)
    }

    async fn put(
        &self,
        bucket: &str,
        key: &ObjectPath,
        body: Bytes,
        opts: &PutOptions,
    ) -> Result<ObjectMetadata> {
        let size = body.len() as u64;
        let mut req = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key.as_str())
            .body(ByteStream::from(body));
        if let Some(ct) = &opts.content_type {
            req = req.content_type(ct);
        }
        if let Some(cc) = &opts.cache_control {
            req = req.cache_control(cc);
        }
        let out = req.send().await.map_err(map_sdk)?;

        let mut meta = ObjectMetadata::after_put(
            key.as_str(),
            size,
            out.e_tag().map(str::to_string),
            opts,
            OffsetDateTime::now_utc(),
        );
        meta.version_id = out.version_id().map(str::to_string);
        Ok(meta)
    }

    /// Server-side copy: one `CopyObject` action up to S3's 5 GiB atomic
    /// limit, a part-copy session beyond it — no object bytes transit the
    /// local device either way. The source is stated here (the port's
    /// callers pass locators only) and the target's metadata is derived
    /// from that stat plus the provider's copy result (which carries the
    /// new ETag and modification time but no size).
    async fn copy(&self, req: &CopyRequest) -> Result<ObjectMetadata> {
        let source = self.stat(&req.source_bucket, &req.source_key).await?;
        let copy_source = copy_source_header(&req.source_bucket, req.source_key.as_str());
        if source.size <= self.single_copy_limit {
            self.copy_single(req, &copy_source, &source).await
        } else {
            self.copy_by_parts(req, &source).await
        }
    }

    async fn delete(&self, bucket: &str, key: &ObjectPath) -> Result<DeleteOutcome> {
        let out = self
            .client
            .delete_object()
            .bucket(bucket)
            .key(key.as_str())
            .send()
            .await
            .map_err(map_sdk)?;
        Ok(DeleteOutcome {
            deleted: true,
            version_id: out.version_id().map(str::to_string),
        })
    }

    async fn create_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        opts: &PutOptions,
    ) -> Result<Box<dyn MultipartSession>> {
        let mut req = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key.as_str());
        if let Some(ct) = &opts.content_type {
            req = req.content_type(ct);
        }
        if let Some(cc) = &opts.cache_control {
            req = req.cache_control(cc);
        }
        let out = req.send().await.map_err(map_sdk)?;
        let upload_id = out
            .upload_id
            .ok_or_else(|| Error::unknown("provider returned no upload id"))?;
        Ok(Box::new(S3MultipartSession::new(
            self.client.clone(),
            bucket,
            key,
            upload_id,
        )))
    }

    async fn adopt_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        upload_id: &str,
    ) -> Result<Box<dyn MultipartSession>> {
        Ok(Box::new(S3MultipartSession::new(
            self.client.clone(),
            bucket,
            key,
            upload_id.to_owned(),
        )))
    }

    /// Generate a presigned URL locally: the request itself is never sent,
    /// so no credentials touch the network and the signature query stays
    /// out of logs.
    async fn presign(&self, req: &PresignRequest) -> Result<PresignedUrl> {
        use aws_sdk_s3::presigning::PresigningConfig;
        let config = PresigningConfig::expires_in(std::time::Duration::from_secs(req.ttl_seconds))
            .map_err(|e| {
                Error::new(
                    ErrorCode::InvalidInput,
                    format!("invalid presign TTL {}s", req.ttl_seconds),
                )
                .with_source(e)
            })?;
        let url = match req.method {
            PresignMethod::Get => self
                .client
                .get_object()
                .bucket(&req.bucket)
                .key(&req.key)
                .presigned(config)
                .await
                .map_err(map_sdk)?
                .uri()
                .to_string(),
            PresignMethod::Put => self
                .client
                .put_object()
                .bucket(&req.bucket)
                .key(&req.key)
                .presigned(config)
                .await
                .map_err(map_sdk)?
                .uri()
                .to_string(),
        };
        Ok(PresignedUrl::expiring(
            url,
            req.method,
            req.ttl_seconds,
            OffsetDateTime::now_utc(),
        ))
    }
}

fn object_entry(o: &aws_sdk_s3::types::Object) -> Result<ObjectEntry> {
    Ok(ObjectEntry {
        metadata: ObjectMetadata {
            key: o.key().unwrap_or_default().to_string(),
            size: o.size().unwrap_or_default() as u64,
            etag: o.e_tag().map(str::to_string),
            // LIST does not return Content-Type; a stat fills it in.
            content_type: None,
            last_modified: o
                .last_modified()
                .map(to_offset)
                .transpose()?
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
            storage_class: o.storage_class.as_ref().map(|s| s.as_str().to_string()),
            version_id: None,
        },
    })
}

/// Provider timestamps convert loudly: an out-of-range value is corrupt
/// provider data, not "the epoch" — silently mapping it there would poison
/// modified-sorting and retention math.
fn to_offset(dt: &SdkDateTime) -> Result<OffsetDateTime> {
    let secs = dt.secs();
    let nanos = dt.subsec_nanos() as i128;
    OffsetDateTime::from_unix_timestamp_nanos(secs as i128 * 1_000_000_000 + nanos)
        .map_err(|e| Error::unknown("provider timestamp out of range").with_source(e))
}

/// The `CopySource` header value: `/{bucket}/{key}` percent-encoded as S3
/// requires (slashes stay literal — they are the path; everything outside
/// the unreserved set encodes, including UTF-8 bytes beyond ASCII).
pub(crate) fn copy_source_header(bucket: &str, key: &str) -> String {
    fn encode_into(out: &mut String, part: &str) {
        for byte in part.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                    out.push(byte as char);
                }
                _ => out.push_str(&format!("%{byte:02X}")),
            }
        }
    }
    let mut value = String::with_capacity(bucket.len() + key.len() + 2);
    value.push('/');
    encode_into(&mut value, bucket);
    value.push('/');
    encode_into(&mut value, key);
    value
}

fn range_header(r: ReadRange) -> String {
    match r.end {
        Some(end) => format!("bytes={}-{}", r.start, end.saturating_sub(1)),
        None => format!("bytes={}-", r.start),
    }
}
