//! Catalog write helpers shared by confirmation flows (listing sync,
//! transfer receipts): build confirmed catalog rows from storage metadata
//! and upsert them.

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
        id: RemoteObjectId::new_random(),
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

/// Build confirmed catalog rows for one listing page.
///
/// Errors when any entry's key violates [`ObjectPath`] rules; the whole
/// page fails rather than being silently partial.
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

/// Upsert a single object into the catalog in [`RemoteObject`] confirmed
/// state, stamped at `at`. Errors on path validation or repository failure.
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

/// Upsert one listing page and report how many rows were written.
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
