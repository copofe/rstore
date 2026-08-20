//! The provider multipart lifetime (STORE-): one session type owns
//! create/part/complete and the abort dispositions, shared by the engine's
//! resumable upload and the cross-connection relay. Durable checkpoints live
//! behind [`rstore_storage_api::MultipartStore`] at the storage seam — the
//! session never touches them.
//!
//! Failure policy — three named dispositions, one home:
//! - [`MultipartSession::abandon`] — best-effort, warn-only: callers with no
//!   resume path (the relay) or after a checkpoint write failed.
//! - [`MultipartSession::abort`] — strict: a failed abort surfaces, so the
//!   stale-checkpoint path keeps its checkpoint and retries next run instead
//!   of leaking the provider upload.
//! - [`MultipartSession::complete`] never aborts: a completion failure
//!   leaves the provider upload in place so a checkpointed caller resumes;
//!   callers without checkpoints abandon it themselves.

use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;
use rstore_storage_api::{FileFingerprint, MultipartStore};
use rstore_storage_api::{ObjectMetadata, PutOptions, Storage, UploadedPart};

/// One in-flight provider multipart upload: the target adapter plus the
/// (bucket, key, upload id) that names it.
pub struct MultipartSession<'a> {
    target: &'a dyn Storage,
    bucket: String,
    key: String,
    upload_id: String,
}

impl<'a> MultipartSession<'a> {
    /// Start a new provider upload.
    pub async fn create(
        target: &'a dyn Storage,
        bucket: &str,
        key: &ObjectPath,
        opts: &PutOptions,
    ) -> Result<Self> {
        let upload_id = target.create_multipart(bucket, key, opts).await?;
        Ok(Self {
            target,
            bucket: bucket.to_string(),
            key: key.as_str().to_string(),
            upload_id,
        })
    }

    /// Wrap an already in-flight upload (a resumed checkpoint's id).
    pub fn adopt(
        target: &'a dyn Storage,
        bucket: &str,
        key: &ObjectPath,
        upload_id: String,
    ) -> Self {
        Self {
            target,
            bucket: bucket.to_string(),
            key: key.as_str().to_string(),
            upload_id,
        }
    }

    /// The provider upload id — for checkpoint bookkeeping
    /// ([`MultipartStore::started`]).
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Upload one part.
    pub async fn upload_part(&self, part_number: u32, body: Bytes) -> Result<UploadedPart> {
        let key = rstore_domain::remote::ObjectPath::new(&self.key)
            .expect("session key was validated at construction");
        self.target
            .upload_part(&self.bucket, &key, &self.upload_id, part_number, body)
            .await
    }

    /// Complete the upload. Never aborts on failure: a checkpointed caller
    /// resumes on the next run; a caller without checkpoints (the relay)
    /// abandons the session itself.
    pub async fn complete(&self, parts: &[UploadedPart]) -> Result<ObjectMetadata> {
        let key = rstore_domain::remote::ObjectPath::new(&self.key)
            .expect("session key was validated at construction");
        self.target
            .complete_multipart(&self.bucket, &key, &self.upload_id, parts)
            .await
    }

    /// Strict abort: a failed abort *is* the error. For stale checkpoints —
    /// the checkpoint stays behind so the next run retries the abort
    /// instead of leaking the provider upload.
    pub async fn abort(&self) -> Result<()> {
        let key = rstore_domain::remote::ObjectPath::new(&self.key)
            .expect("session key was validated at construction");
        self.target
            .abort_multipart(&self.bucket, &key, &self.upload_id)
            .await
    }

    /// Best-effort abort: warn-only, never escalating past the original
    /// error. For callers with no resume path.
    pub async fn abandon(self) {
        if let Err(error) = self.abort().await {
            tracing::warn!(
                error = %error,
                upload_id = %self.upload_id,
                "could not abort multipart upload"
            );
        }
    }
}

/// The one resume policy for checkpointed multipart uploads (UP-014/015).
/// The dispositions, in code rather than comments:
///
/// - **matching checkpoint** → adopt the provider upload and its parts;
/// - **stale checkpoint** (the source changed under it) → strict abort, then
///   strict clear, then start fresh — a failed step keeps the checkpoint so
///   the next run retries instead of leaking the provider upload;
/// - **no checkpoint** → start fresh.
pub(crate) async fn resume_or_begin<'a>(
    storage: &'a dyn Storage,
    store: &dyn MultipartStore,
    connection: &str,
    bucket: &str,
    key: &rstore_domain::remote::ObjectPath,
    fingerprint: &FileFingerprint,
    opts: &PutOptions,
) -> Result<(MultipartSession<'a>, Vec<UploadedPart>)> {
    match store.pending(connection, bucket, key.as_str()).await? {
        Some(pending) if pending.fingerprint != *fingerprint => {
            MultipartSession::adopt(storage, bucket, key, pending.upload_id)
                .abort()
                .await?;
            store.cleared(connection, bucket, key.as_str()).await?;
            begin(storage, store, connection, bucket, key, fingerprint, opts).await
        }
        Some(pending) => Ok((
            MultipartSession::adopt(storage, bucket, key, pending.upload_id),
            pending.parts,
        )),
        None => begin(storage, store, connection, bucket, key, fingerprint, opts).await,
    }
}

/// Create a fresh provider upload and record its checkpoint. A checkpoint
/// that cannot be persisted makes the upload unsafe to resume — abandon
/// both (warn-only) and surface the checkpoint error.
async fn begin<'a>(
    storage: &'a dyn Storage,
    store: &dyn MultipartStore,
    connection: &str,
    bucket: &str,
    key: &rstore_domain::remote::ObjectPath,
    fingerprint: &FileFingerprint,
    opts: &PutOptions,
) -> Result<(MultipartSession<'a>, Vec<UploadedPart>)> {
    let session = MultipartSession::create(storage, bucket, key, opts).await?;
    if let Err(error) = store
        .started(
            connection,
            bucket,
            key.as_str(),
            session.upload_id(),
            fingerprint,
        )
        .await
    {
        abandon_with_checkpoint(store, connection, bucket, key.as_str(), session).await;
        return Err(error);
    }
    Ok((session, Vec::new()))
}

/// A checkpoint that cannot be persisted safely is not resumable: abandon the
/// provider upload (the session's warn-only policy) and clear local state
/// (warn-only) while preserving the caller's original error.
pub(crate) async fn abandon_with_checkpoint(
    store: &dyn MultipartStore,
    connection: &str,
    bucket: &str,
    key: &str,
    session: MultipartSession<'_>,
) {
    session.abandon().await;
    if let Err(error) = store.cleared(connection, bucket, key).await {
        tracing::warn!(error = %error, "could not clear abandoned multipart checkpoint");
    }
}
