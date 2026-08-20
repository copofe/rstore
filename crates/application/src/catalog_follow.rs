//! Catalog projections for live storage observations.
//!
//! Read paths such as indexing and `remote ls --update-catalog` propagate their
//! catalog errors. Completed remote mutations are settled by
//! [`crate::mutation::MutationCoordinator`], which owns the fact, projection
//! and repair sequence.

use rstore_catalog::RemoteObjectRepository;
use rstore_domain::id::RemoteObjectId;
use rstore_domain::remote::{ObjectPath, RemoteObject};
use rstore_domain::{BucketId, ConnectionId, Result};
use rstore_storage_api::model::{ObjectEntry, ObjectMetadata};
use time::OffsetDateTime;

fn confirmed_object(
    connection: ConnectionId,
    bucket: BucketId,
    metadata: &ObjectMetadata,
    at: OffsetDateTime,
) -> Result<RemoteObject> {
    Ok(RemoteObject {
        id: RemoteObjectId::new(),
        connection_id: connection,
        bucket_id: bucket,
        path: ObjectPath::new(&metadata.key)?,
        size: metadata.size,
        etag: metadata.etag.clone(),
        content_type: metadata.content_type.clone(),
        last_modified: metadata.last_modified,
        storage_class: metadata.storage_class.clone(),
        version_id: metadata.version_id.clone(),
        confirmed_at: at,
        state: rstore_domain::ObjectState::Confirmed,
    })
}

pub(crate) fn confirmed_objects<'a>(
    connection: ConnectionId,
    bucket: BucketId,
    entries: impl IntoIterator<Item = &'a ObjectEntry>,
    at: OffsetDateTime,
) -> Result<Vec<RemoteObject>> {
    entries
        .into_iter()
        .map(|entry| confirmed_object(connection, bucket, &entry.metadata, at))
        .collect()
}

/// Persist one storage-confirmed object. Indexing and explicit catalog update
/// commands propagate this result because the catalog write is their work.
pub(crate) async fn upsert_confirmed(
    catalog: &dyn RemoteObjectRepository,
    connection: ConnectionId,
    bucket: BucketId,
    metadata: &ObjectMetadata,
    at: OffsetDateTime,
) -> Result<()> {
    let object = confirmed_object(connection, bucket, metadata, at)?;
    catalog.upsert(&object).await
}

/// Upsert a page of listed objects as confirmed rows — the shared loop
/// behind the indexer's scan and `remote ls`'s catalog update
/// (IDX-002..004). Cataloguing reads propagate failures. Returns how many
/// rows were written.
pub(crate) async fn sync_page<'a>(
    catalog: &dyn RemoteObjectRepository,
    connection: ConnectionId,
    bucket: BucketId,
    entries: impl IntoIterator<Item = &'a ObjectEntry>,
    at: OffsetDateTime,
) -> Result<u64> {
    let objects = confirmed_objects(connection, bucket, entries, at)?;
    catalog.upsert_many(&objects).await?;
    Ok(objects.len() as u64)
}
