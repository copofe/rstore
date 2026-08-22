//! The S3 multipart session: one home for the provider-side protocol
//! (STORE-012/013). Part upload, server-side part copy, completion and the
//! abort dispositions all live behind
//! [`rstore_storage_api::MultipartSession`]; the adapter never re-rolls the
//! protocol for its own copy path.

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, Result};
use rstore_storage_api::model::{ObjectMetadata, ReadRange, UploadedPart};
use rstore_storage_api::MultipartSession;
use time::OffsetDateTime;

use crate::adapter::copy_source_header;
use crate::error::map_sdk;

/// One in-flight S3 multipart upload: the SDK client plus the
/// (bucket, key, upload id) that names it.
pub(crate) struct S3MultipartSession {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    upload_id: String,
}

impl S3MultipartSession {
    pub(crate) fn new(
        client: aws_sdk_s3::Client,
        bucket: &str,
        key: &ObjectPath,
        upload_id: String,
    ) -> Self {
        Self {
            client,
            bucket: bucket.to_string(),
            key: key.as_str().to_string(),
            upload_id,
        }
    }
}

#[async_trait]
impl MultipartSession for S3MultipartSession {
    fn upload_id(&self) -> &str {
        &self.upload_id
    }

    async fn upload_part(&self, part_number: u32, body: Bytes) -> Result<UploadedPart> {
        let size = body.len() as u64;
        let out = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(part_number as i32)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(map_sdk)?;
        let etag = out
            .e_tag()
            .map(str::to_string)
            .ok_or_else(|| Error::unknown("provider returned no part etag"))?;
        Ok(UploadedPart {
            part_number,
            etag,
            size,
        })
    }

    /// Server-side part copy (`UploadPartCopy`): the part's bytes are copied
    /// by the provider — no object bytes transit the local device.
    async fn copy_part(
        &self,
        part_number: u32,
        source_bucket: &str,
        source_key: &ObjectPath,
        range: ReadRange,
    ) -> Result<UploadedPart> {
        // S3's UploadPartCopy requires an explicit CopySourceRange; an
        // open-ended range is not expressible.
        let Some(end) = range.end else {
            return Err(Error::invalid_input(
                "copy_part requires a bounded range (end must be set)",
            ));
        };
        let out = self
            .client
            .upload_part_copy()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(part_number as i32)
            .copy_source(copy_source_header(source_bucket, source_key.as_str()))
            .copy_source_range(format!("bytes={}-{}", range.start, end - 1))
            .send()
            .await
            .map_err(map_sdk)?;
        let etag = out
            .copy_part_result()
            .and_then(|r| r.e_tag().map(str::to_string))
            .ok_or_else(|| Error::unknown("provider returned no copy-part etag"))?;
        Ok(UploadedPart {
            part_number,
            etag,
            size: end - range.start,
        })
    }

    async fn complete(&self, parts: &[UploadedPart]) -> Result<ObjectMetadata> {
        let completed: Vec<aws_sdk_s3::types::CompletedPart> = parts
            .iter()
            .map(|p| {
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(p.part_number as i32)
                    .e_tag(&p.etag)
                    .build()
            })
            .collect();
        let out = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(completed))
                    .build(),
            )
            .send()
            .await
            .map_err(map_sdk)?;
        let mut meta = ObjectMetadata::from_parts(
            &self.key,
            out.e_tag().map(str::to_string),
            parts,
            OffsetDateTime::now_utc(),
        );
        meta.version_id = out.version_id().map(str::to_string);
        Ok(meta)
    }

    async fn abort(&self) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .send()
            .await
            .map_err(map_sdk)?;
        Ok(())
    }
}
