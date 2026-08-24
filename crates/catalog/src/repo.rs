//! Repository and query contracts over the catalog.
//!
//! An [`InMemoryCatalog`] reference implementation lives here so service and
//! contract tests can run without SQLite. Its semantics are pinned by the
//! shared `catalog_contract` suite in `rstore-test-support`, which the SQLite
//! repositories also run — the two adapters must not drift.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use rstore_domain::remote::RemoteObject;
use rstore_domain::Result;
use rstore_domain::{BucketId, ConnectionId, RemoteObjectId, TagId, WorkspaceId};

use crate::order::{cursor_of, order_key};
use crate::query::{
    CatalogPage, CatalogPageItem, FileFilter, FileSort, Page, PageRequest, Scope, SortOrder,
};

/// Write/read access to the indexed remote objects of the catalog.
///
/// Implementations live behind this port so the indexer never sees SQLite.
#[async_trait]
pub trait RemoteObjectRepository: Send + Sync {
    /// Insert or replace a row keyed by object id.
    async fn upsert(&self, object: &RemoteObject) -> Result<()>;

    /// Publish a page's object rows as one adapter-owned write unit. The
    /// default keeps simple adapters substitutable; durable adapters should
    /// override it with a transaction.
    async fn upsert_many(&self, objects: &[RemoteObject]) -> Result<()> {
        for object in objects {
            self.upsert(object).await?;
        }
        Ok(())
    }

    /// Fetch one row by id; `None` when absent or tombstoned away.
    async fn get(&self, id: RemoteObjectId) -> Result<Option<RemoteObject>>;

    /// Remove a row outright (no tombstone is kept).
    async fn delete(&self, id: RemoteObjectId) -> Result<()>;

    /// Number of rows currently indexed for `bucket`.
    async fn count_in_bucket(&self, bucket: BucketId) -> Result<u64>;

    /// Record a deletion: drop the catalog row (if any) and keep a tombstone
    /// carrying the locator for later index reconciliation.
    async fn tombstone(
        &self,
        connection: rstore_domain::ConnectionId,
        bucket: BucketId,
        path: &rstore_domain::remote::ObjectPath,
        at: time::OffsetDateTime,
    ) -> Result<()>;

    /// Look up a full object row by its remote locator.
    async fn find_by_locator(
        &self,
        connection: rstore_domain::ConnectionId,
        bucket: BucketId,
        path: &rstore_domain::remote::ObjectPath,
    ) -> Result<Option<RemoteObject>>;

    /// Rows in a bucket still marked stale (status surface).
    async fn count_stale(&self, bucket: BucketId) -> Result<u64>;

    /// Reconciliation step: rows last confirmed before `before` flip to stale
    /// (the refresh re-confirmed everything still remote). Returns how many
    /// rows flipped.
    async fn mark_stale_older_than(
        &self,
        bucket: BucketId,
        before: time::OffsetDateTime,
    ) -> Result<u64>;

    /// Mark every row in a bucket as stale before a rebuild. Existing rows
    /// remain visible until the replacement scan commits.
    async fn mark_all_stale_in_bucket(&self, bucket: BucketId) -> Result<u64>;

    /// Remove rows left stale by a completed rebuild.
    async fn delete_stale_in_bucket(&self, bucket: BucketId) -> Result<u64>;

    /// Remove every catalog row of a bucket. Tag associations cascade; tag
    /// definitions survive.
    async fn delete_all_in_bucket(&self, bucket: BucketId) -> Result<u64>;

    /// Tombstones past retention: count only (dry-run support). Adapters
    /// without tombstone records report zero.
    async fn count_expired_tombstones(
        &self,
        _now: time::OffsetDateTime,
        _retention_secs: u64,
    ) -> Result<u64> {
        Ok(0)
    }

    /// Purge tombstones past retention, returning the number removed. A row
    /// expires at its own `expires_at` when set, else
    /// `created_at + retention`.
    async fn purge_expired_tombstones(
        &self,
        _now: time::OffsetDateTime,
        _retention_secs: u64,
    ) -> Result<u64> {
        Ok(0)
    }
}

/// Read-side search over the catalog: filtered, sorted keyset pagination.
#[async_trait]
pub trait CatalogQuery: Send + Sync {
    /// One page of matching objects in walk order.
    async fn search(
        &self,
        scope: Scope,
        filter: &FileFilter,
        sort: FileSort,
        page: &PageRequest,
    ) -> Result<Page<RemoteObject>>;

    /// Number of rows matching the filter, ignoring the cursor.
    async fn count(&self, scope: Scope, filter: &FileFilter) -> Result<u64>;

    /// Assemble the complete mixed files page in the catalog adapter.
    async fn page(
        &self,
        scope: Scope,
        filter: &FileFilter,
        sort: FileSort,
        page: &PageRequest,
    ) -> Result<CatalogPage>;
}

/// In-memory catalog: reference implementation and test fixture.
///
/// Its observable behavior must match the persisted adapters; contract tests
/// pin the overlap.
pub struct InMemoryCatalog {
    objects: Mutex<HashMap<RemoteObjectId, RemoteObject>>,
    /// Workspace-scoped tag associations.
    tags: Mutex<HashMap<(WorkspaceId, RemoteObjectId), Vec<TagId>>>,
    /// Workspace bucket membership — query scoping mirrors the SQL adapters'
    /// `workspace_buckets` join.
    members: Mutex<HashMap<WorkspaceId, HashSet<BucketId>>>,
    connection_names: Mutex<HashMap<ConnectionId, String>>,
    bucket_names: Mutex<HashMap<BucketId, String>>,
    tag_names: Mutex<HashMap<TagId, String>>,
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryCatalog {
    /// An empty catalog with no names, memberships, or tags registered.
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            tags: Mutex::new(HashMap::new()),
            members: Mutex::new(HashMap::new()),
            connection_names: Mutex::new(HashMap::new()),
            bucket_names: Mutex::new(HashMap::new()),
            tag_names: Mutex::new(HashMap::new()),
        }
    }

    /// Register the display name used by the assembled page read model.
    pub fn set_connection_name(&self, connection: ConnectionId, name: impl Into<String>) {
        self.connection_names
            .lock()
            .expect("catalog poisoned")
            .insert(connection, name.into());
    }

    /// Register the display name used by the assembled page read model.
    pub fn set_bucket_name(&self, bucket: BucketId, name: impl Into<String>) {
        self.bucket_names
            .lock()
            .expect("catalog poisoned")
            .insert(bucket, name.into());
    }

    /// Register the display name used by the assembled page read model.
    pub fn set_tag_name(&self, tag: TagId, name: impl Into<String>) {
        self.tag_names
            .lock()
            .expect("catalog poisoned")
            .insert(tag, name.into());
    }

    /// Register a bucket as visible inside a workspace (the equivalent of
    /// `workspace bucket add` on the SQL side).
    pub fn add_bucket(&self, workspace: WorkspaceId, bucket: BucketId) {
        self.members
            .lock()
            .expect("catalog poisoned")
            .entry(workspace)
            .or_default()
            .insert(bucket);
    }

    /// Attach a tag to an object within a workspace.
    pub fn tag(&self, workspace: WorkspaceId, object: RemoteObjectId, tag: TagId) {
        self.tags
            .lock()
            .expect("catalog poisoned")
            .entry((workspace, object))
            .or_default()
            .push(tag);
    }

    fn page_item(&self, workspace: WorkspaceId, object: RemoteObject) -> CatalogPageItem {
        let bucket_name = self
            .bucket_names
            .lock()
            .expect("catalog poisoned")
            .get(&object.bucket_id)
            .cloned()
            .unwrap_or_else(|| object.bucket_id.to_string());
        let connection_name = self
            .connection_names
            .lock()
            .expect("catalog poisoned")
            .get(&object.connection_id)
            .cloned()
            .unwrap_or_else(|| object.connection_id.to_string());
        let tag_ids = self
            .tags
            .lock()
            .expect("catalog poisoned")
            .get(&(workspace, object.id))
            .cloned()
            .unwrap_or_default();
        let mut tags = tag_ids
            .into_iter()
            .map(|tag| {
                self.tag_names
                    .lock()
                    .expect("catalog poisoned")
                    .get(&tag)
                    .cloned()
                    .unwrap_or_else(|| tag.to_string())
            })
            .collect::<Vec<_>>();
        tags.sort_by_key(|tag| tag.to_lowercase());
        CatalogPageItem {
            object,
            bucket_name,
            connection_name,
            tags,
        }
    }

    fn is_member(&self, workspace: WorkspaceId, bucket: BucketId) -> bool {
        self.members
            .lock()
            .expect("catalog poisoned")
            .get(&workspace)
            .map(|buckets| buckets.contains(&bucket))
            .unwrap_or(false)
    }

    fn matches_filter(
        &self,
        workspace: WorkspaceId,
        obj: &RemoteObject,
        filter: &FileFilter,
    ) -> bool {
        // `state: None` hides deleted rows; `Some` is an exact match.
        let state_ok = match filter.state {
            None => obj.state != rstore_domain::ObjectState::Deleted,
            Some(want) => obj.state == want,
        };
        if !state_ok {
            return false;
        }
        // Workspace scoping: only member buckets are visible.
        if !self.is_member(workspace, obj.bucket_id) {
            return false;
        }
        if let Some(q) = &filter.name_contains {
            if !obj
                .path
                .file_name()
                .to_lowercase()
                .contains(&q.to_lowercase())
            {
                return false;
            }
        }
        if let Some(q) = &filter.path_contains {
            if !obj.path.as_str().to_lowercase().contains(&q.to_lowercase()) {
                return false;
            }
        }
        if !filter.buckets.is_empty() && !filter.buckets.contains(&obj.bucket_id) {
            return false;
        }
        if !filter.connections.is_empty() && !filter.connections.contains(&obj.connection_id) {
            return false;
        }
        if let Some(ft) = filter.file_type {
            if obj.type_hint() != ft {
                return false;
            }
        }
        if let Some(ext) = &filter.extension {
            if obj.path.extension().map(str::to_ascii_lowercase).as_deref()
                != Some(ext.to_ascii_lowercase().as_str())
            {
                return false;
            }
        }
        if let Some(min) = filter.min_size {
            if obj.size < min {
                return false;
            }
        }
        if let Some(max) = filter.max_size {
            if obj.size > max {
                return false;
            }
        }
        if let Some(after) = filter.modified_after {
            if obj.last_modified < after {
                return false;
            }
        }
        if let Some(before) = filter.modified_before {
            if obj.last_modified > before {
                return false;
            }
        }

        let tags = self
            .tags
            .lock()
            .expect("catalog poisoned")
            .get(&(workspace, obj.id))
            .cloned()
            .unwrap_or_default();

        if filter.untagged {
            return tags.is_empty();
        }
        if !filter.tags.is_empty() {
            let hits = filter.tags.iter().filter(|t| tags.contains(t)).count();
            if filter.tag_mode_or {
                if hits == 0 {
                    return false;
                }
            } else if hits != filter.tags.len() {
                return false;
            }
        }
        true
    }
}

#[async_trait]
impl RemoteObjectRepository for InMemoryCatalog {
    async fn upsert(&self, object: &RemoteObject) -> Result<()> {
        self.objects
            .lock()
            .expect("catalog poisoned")
            .insert(object.id, object.clone());
        Ok(())
    }

    async fn upsert_many(&self, objects: &[RemoteObject]) -> Result<()> {
        let mut stored = self.objects.lock().expect("catalog poisoned");
        for object in objects {
            stored.insert(object.id, object.clone());
        }
        Ok(())
    }

    async fn get(&self, id: RemoteObjectId) -> Result<Option<RemoteObject>> {
        Ok(self
            .objects
            .lock()
            .expect("catalog poisoned")
            .get(&id)
            .cloned())
    }

    async fn delete(&self, id: RemoteObjectId) -> Result<()> {
        self.objects.lock().expect("catalog poisoned").remove(&id);
        Ok(())
    }

    async fn count_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        Ok(self
            .objects
            .lock()
            .expect("catalog poisoned")
            .values()
            .filter(|o| o.bucket_id == bucket)
            .count() as u64)
    }

    /// Reference implementation: the row is dropped and no tombstone record is
    /// kept — reconciliation markers only matter for the persisted catalog.
    async fn tombstone(
        &self,
        connection: rstore_domain::ConnectionId,
        bucket: BucketId,
        path: &rstore_domain::remote::ObjectPath,
        _at: time::OffsetDateTime,
    ) -> Result<()> {
        let mut objects = self.objects.lock().expect("catalog poisoned");
        let matched = objects
            .iter()
            .filter(|(_, o)| {
                o.connection_id == connection
                    && o.bucket_id == bucket
                    && o.path.as_str() == path.as_str()
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in matched {
            objects.remove(&id);
        }
        Ok(())
    }

    async fn find_by_locator(
        &self,
        connection: rstore_domain::ConnectionId,
        bucket: BucketId,
        path: &rstore_domain::remote::ObjectPath,
    ) -> Result<Option<RemoteObject>> {
        Ok(self
            .objects
            .lock()
            .expect("catalog poisoned")
            .values()
            .find(|o| {
                o.connection_id == connection
                    && o.bucket_id == bucket
                    && o.path.as_str() == path.as_str()
            })
            .cloned())
    }

    async fn count_stale(&self, bucket: BucketId) -> Result<u64> {
        Ok(self
            .objects
            .lock()
            .expect("catalog poisoned")
            .values()
            .filter(|o| o.bucket_id == bucket && o.state == rstore_domain::ObjectState::Stale)
            .count() as u64)
    }

    async fn mark_stale_older_than(
        &self,
        bucket: BucketId,
        before: time::OffsetDateTime,
    ) -> Result<u64> {
        let mut objects = self.objects.lock().expect("catalog poisoned");
        let mut flipped = 0u64;
        for o in objects.values_mut() {
            if o.bucket_id == bucket
                && o.state == rstore_domain::ObjectState::Confirmed
                && o.confirmed_at < before
            {
                o.state = rstore_domain::ObjectState::Stale;
                flipped += 1;
            }
        }
        Ok(flipped)
    }

    async fn mark_all_stale_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        let mut objects = self.objects.lock().expect("catalog poisoned");
        let mut marked = 0;
        for object in objects.values_mut().filter(|o| o.bucket_id == bucket) {
            if object.state != rstore_domain::ObjectState::Stale {
                object.state = rstore_domain::ObjectState::Stale;
                marked += 1;
            }
        }
        Ok(marked)
    }

    async fn delete_stale_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        let mut objects = self.objects.lock().expect("catalog poisoned");
        let before = objects.len();
        objects.retain(|_, object| {
            !(object.bucket_id == bucket && object.state == rstore_domain::ObjectState::Stale)
        });
        Ok((before - objects.len()) as u64)
    }

    async fn delete_all_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        let mut objects = self.objects.lock().expect("catalog poisoned");
        let before = objects.len();
        objects.retain(|_, o| o.bucket_id != bucket);
        Ok((before - objects.len()) as u64)
    }
}

#[async_trait]
impl CatalogQuery for InMemoryCatalog {
    async fn search(
        &self,
        scope: Scope,
        filter: &FileFilter,
        sort: FileSort,
        page: &PageRequest,
    ) -> Result<Page<RemoteObject>> {
        let mut matched: Vec<RemoteObject> = self
            .objects
            .lock()
            .expect("catalog poisoned")
            .values()
            .filter(|o| self.matches_filter(scope.workspace, o, filter))
            .cloned()
            .collect();

        // Deterministic total order: (sort value, path, id), the tie-break in
        // the same direction as the sort so a keyset cursor can resume the
        // walk without skipping or repeating rows.
        matched.sort_by_cached_key(|o| order_key(o, sort.field));
        if sort.order == SortOrder::Desc {
            matched.reverse();
        }

        if let Some(cursor) = &page.after {
            let cursor_key = crate::order::decode(cursor, sort.field)?;
            // Rows strictly past the cursor in walk direction.
            matched.retain(|o| {
                let ord = order_key(o, sort.field).cmp(&cursor_key);
                if sort.order == SortOrder::Asc {
                    ord == Ordering::Greater
                } else {
                    ord == Ordering::Less
                }
            });
        }

        let limit = page.limit.max(1) as usize;
        let has_more = matched.len() > limit;
        matched.truncate(limit);
        let next_cursor = if has_more {
            matched.last().map(|o| cursor_of(o, sort.field))
        } else {
            None
        };

        Ok(Page {
            items: matched,
            next_cursor,
        })
    }

    async fn count(&self, scope: Scope, filter: &FileFilter) -> Result<u64> {
        let n = self
            .objects
            .lock()
            .expect("catalog poisoned")
            .values()
            .filter(|o| self.matches_filter(scope.workspace, o, filter))
            .count();
        Ok(n as u64)
    }

    async fn page(
        &self,
        scope: Scope,
        filter: &FileFilter,
        sort: FileSort,
        page: &PageRequest,
    ) -> Result<CatalogPage> {
        let total = self.count(scope, filter).await?;
        let result = self.search(scope, filter, sort, page).await?;
        Ok(CatalogPage {
            items: result
                .items
                .into_iter()
                .map(|object| self.page_item(scope.workspace, object))
                .collect(),
            total,
            next_cursor: result.next_cursor,
        })
    }
}
