//! Catalog cache writes from provider facts: the row a confirmed object
//! becomes, and the write-through after remote operations.
//!
//! The catalog is a cache of remote state and the provider stays the source
//! of truth. A confirmed mutation or listing updates the cache best-effort: a
//! failed cache write never turns a succeeded remote operation into a
//! failure, it comes back as a note for the item's detail, and the next
//! listing or index refresh corrects the row.
//!
//! What the cache keeps: every object of an indexed bucket, plus any object a
//! workspace tags. A write into an unindexed bucket only refreshes a row that
//! already exists; a delete drops the row and the object's tags.

use std::sync::Arc;

use rstore_domain::bucket::Bucket;
use rstore_domain::id::RemoteObjectId;
use rstore_domain::remote::{ObjectPath, RemoteObject};
use rstore_domain::{Clock, ObjectState, Result};
use rstore_storage::model::{ObjectEntry, ObjectMetadata};
use time::OffsetDateTime;

use crate::db::{CatalogRepository, TagRepository};
use crate::events::{EngineEvent, EventBus};

/// The cache row for an object the provider confirmed at `at`.
///
/// Errors when the key violates [`ObjectPath`] rules.
pub(crate) fn confirmed_object(
    bucket: &Bucket,
    metadata: &ObjectMetadata,
    at: OffsetDateTime,
) -> Result<RemoteObject> {
    Ok(RemoteObject {
        id: RemoteObjectId::new_random(),
        connection_id: bucket.connection_id,
        bucket_id: bucket.id,
        path: ObjectPath::new(&metadata.key)?,
        size: metadata.size,
        etag: metadata.etag.clone(),
        content_type: metadata.content_type.clone(),
        last_modified: metadata.last_modified,
        storage_class: metadata.storage_class.clone(),
        version_id: metadata.version_id.clone(),
        confirmed_at: at,
        state: ObjectState::Confirmed,
    })
}

/// Applies confirmed remote mutations to the catalog cache.
#[derive(Clone)]
pub(crate) struct WriteThrough {
    catalog: Arc<CatalogRepository>,
    tags: Arc<TagRepository>,
    clock: Arc<dyn Clock>,
    bus: EventBus,
}

impl WriteThrough {
    pub(crate) fn new(
        catalog: Arc<CatalogRepository>,
        tags: Arc<TagRepository>,
        clock: Arc<dyn Clock>,
        bus: EventBus,
    ) -> Self {
        Self {
            catalog,
            tags,
            clock,
            bus,
        }
    }

    /// An object landed (upload or copy target). Returns a note when the
    /// cache could not follow.
    pub(crate) async fn written(
        &self,
        bucket: &Bucket,
        metadata: &ObjectMetadata,
    ) -> Option<String> {
        let result = self.write(bucket, metadata).await;
        self.settled(bucket, result)
    }

    /// A listing page of an indexed bucket was read: refresh its rows.
    /// Returns whether the cache followed; the listing stands either way.
    pub(crate) async fn listed(&self, bucket: &Bucket, entries: &[ObjectEntry]) -> bool {
        let at = self.clock.now();
        let result = async {
            let objects = entries
                .iter()
                .map(|entry| confirmed_object(bucket, &entry.metadata, at))
                .collect::<Result<Vec<_>>>()?;
            self.catalog.upsert_many(&objects).await
        }
        .await;
        self.settled(bucket, result).is_none()
    }

    /// An object was deleted by rstore: drop its row and its tags. Returns a
    /// note when the cache could not follow.
    pub(crate) async fn deleted(&self, bucket: &Bucket, path: &ObjectPath) -> Option<String> {
        let result = async {
            self.tags.forget_object(bucket.id, path).await?;
            self.catalog.forget(bucket.id, path).await
        }
        .await;
        self.settled(bucket, result)
    }

    /// Mirror the source's tags onto a copy target. Call it before
    /// [`Self::written`] so a tagged target in an unindexed bucket gets its
    /// row. Returns a note on failure.
    pub(crate) async fn tags_copied(
        &self,
        from: (&Bucket, &ObjectPath),
        to: (&Bucket, &ObjectPath),
    ) -> Option<String> {
        let result = self
            .tags
            .copy_associations((from.0.id, from.1), (to.0.id, to.1), self.clock.now())
            .await
            .map(|_| ());
        self.note(result)
    }

    async fn write(&self, bucket: &Bucket, metadata: &ObjectMetadata) -> Result<()> {
        let path = ObjectPath::new(&metadata.key)?;
        let tracked = bucket.config.index_enabled
            || self.tags.is_tagged(bucket.id, &path).await?
            || self
                .catalog
                .find_by_locator(bucket.connection_id, bucket.id, &path)
                .await?
                .is_some();
        if !tracked {
            return Ok(());
        }
        self.catalog
            .upsert(&confirmed_object(bucket, metadata, self.clock.now())?)
            .await
    }

    /// Announce a cache change, or turn a failed cache write into a note.
    fn settled(&self, bucket: &Bucket, result: Result<()>) -> Option<String> {
        if result.is_ok() {
            self.bus
                .publish(EngineEvent::CatalogChanged { bucket: bucket.id });
        }
        self.note(result)
    }

    fn note(&self, result: Result<()>) -> Option<String> {
        result.err().map(|error| {
            tracing::warn!(%error, "catalog cache did not follow a remote change");
            format!("catalog not updated: {error}")
        })
    }
}
