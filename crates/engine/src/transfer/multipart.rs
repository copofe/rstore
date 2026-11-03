//! The one resume policy for checkpointed multipart uploads.
//!
//! The provider-side session lifetime — parts, completion, and the three
//! dispositions (abandon warn-only, abort strict, complete never aborts) —
//! lives at the storage seam (`rstore_storage::MultipartSession`); this
//! module only decides which session a run uses. Durable checkpoints stay
//! in the store's `MultipartStore`; sessions never touch them.

use crate::db::{FileFingerprint, MultipartStore};
use rstore_domain::id::ConnectionId;
use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;
use rstore_storage::{MultipartSession, PutOptions, Storage, UploadedPart};

/// The one resume policy, in code rather than comments:
///
/// - **matching checkpoint** → adopt the provider upload and its parts;
/// - **stale checkpoint** (the source changed under it) → strict abort, then
///   strict clear, then start fresh — a failed step keeps the checkpoint so
///   the next run retries instead of leaking the provider upload;
/// - **no checkpoint** → start fresh.
pub(crate) async fn resume_or_begin(
    storage: &dyn Storage,
    store: &MultipartStore,
    connection: ConnectionId,
    bucket: &str,
    key: &ObjectPath,
    fingerprint: &FileFingerprint,
    opts: &PutOptions,
) -> Result<(Box<dyn MultipartSession>, Vec<UploadedPart>)> {
    match store.pending(connection, bucket, key.as_str()).await? {
        Some(pending) if pending.fingerprint != *fingerprint => {
            storage
                .adopt_multipart(bucket, key, &pending.upload_id)
                .await?
                .abort()
                .await?;
            store.cleared(connection, bucket, key.as_str()).await?;
            begin(storage, store, connection, bucket, key, fingerprint, opts).await
        }
        Some(pending) => Ok((
            storage
                .adopt_multipart(bucket, key, &pending.upload_id)
                .await?,
            pending.parts,
        )),
        None => begin(storage, store, connection, bucket, key, fingerprint, opts).await,
    }
}

/// Create a fresh provider upload and record its checkpoint. A checkpoint
/// that cannot be persisted makes the upload unsafe to resume — abandon
/// both (warn-only) and surface the checkpoint error.
async fn begin(
    storage: &dyn Storage,
    store: &MultipartStore,
    connection: ConnectionId,
    bucket: &str,
    key: &ObjectPath,
    fingerprint: &FileFingerprint,
    opts: &PutOptions,
) -> Result<(Box<dyn MultipartSession>, Vec<UploadedPart>)> {
    let session = storage.create_multipart(bucket, key, opts).await?;
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
    store: &MultipartStore,
    connection: ConnectionId,
    bucket: &str,
    key: &str,
    session: Box<dyn MultipartSession>,
) {
    session.abandon().await;
    if let Err(error) = store.cleared(connection, bucket, key).await {
        tracing::warn!(error = %error, "could not clear abandoned multipart checkpoint");
    }
}
