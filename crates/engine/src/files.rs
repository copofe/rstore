//! Mixed file list and search over the workspace catalog.
//!
//! Queries run against the local cache — offline-safe and staleness-flagged. The cache holds
//! every object of an indexed bucket plus every tagged object, so a tag query is complete across
//! the workspace while a name or attribute search covers indexed buckets only; the page names the
//! buckets it could not search.

use std::sync::Arc;

use crate::catalog::{CatalogPageItem, FileFilter, FileSort, PageRequest, Scope};
use rstore_domain::remote::RemoteObject;
use rstore_domain::{Error, Result};

use crate::workspace::WorkspaceRef;

/// One row of the mixed list: the object plus its resolved source identity and the workspace's
/// tag names.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FileEntry {
    pub uri: String,
    pub name: String,
    pub directory: String,
    pub bucket: String,
    pub connection: String,
    pub size: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub last_modified: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub confirmed_at: time::OffsetDateTime,
    pub state: rstore_domain::ObjectState,
    pub content_type: Option<String>,
    pub tags: Vec<String>,
}

/// A page plus the total match count for the filter.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FilesPage {
    pub items: Vec<FileEntry>,
    pub total: u64,
    pub next_cursor: Option<String>,
    /// `connection/bucket` labels of in-scope buckets the search could not cover because they
    /// are not indexed. Empty for tag queries, which the cache answers completely.
    pub unindexed: Vec<String>,
}

/// Mixed-list queries over the indexed catalog.
pub struct FilesService {
    catalog: Arc<crate::db::CatalogRepository>,
    workspaces: Arc<crate::db::WorkspaceRepository>,
    tags: Arc<crate::db::TagRepository>,
    connections: Arc<crate::db::ConnectionRepository>,
    buckets: Arc<crate::db::BucketRepository>,
    gateway: crate::gateway::RemoteGateway,
}

impl FilesService {
    pub fn new(
        catalog: Arc<crate::db::CatalogRepository>,
        workspaces: Arc<crate::db::WorkspaceRepository>,
        tags: Arc<crate::db::TagRepository>,
        connections: Arc<crate::db::ConnectionRepository>,
        buckets: Arc<crate::db::BucketRepository>,
        gateway: crate::gateway::RemoteGateway,
    ) -> Self {
        Self {
            catalog,
            workspaces,
            tags,
            connections,
            buckets,
            gateway,
        }
    }

    async fn workspace_id(&self, workspace: &WorkspaceRef) -> Result<rstore_domain::WorkspaceId> {
        Ok(
            crate::workspace::resolve_workspace(self.workspaces.as_ref(), workspace)
                .await?
                .id,
        )
    }

    async fn entry_for(
        &self,
        workspace: rstore_domain::WorkspaceId,
        object: &RemoteObject,
    ) -> Result<FileEntry> {
        let bucket = self
            .buckets
            .get(object.bucket_id)
            .await?
            .ok_or_else(|| Error::database("catalog row references an unregistered bucket"))?;
        let connection = self
            .connections
            .get(object.connection_id)
            .await?
            .map(|c| c.name)
            .unwrap_or_else(|| object.connection_id.to_string());
        let tags = self
            .tags
            .tags_for_object(workspace, object.bucket_id, &object.path)
            .await?;
        Ok(Self::from_parts(
            // Canonical address display: `conn/bucket/key`, the CLI's short remote-reference
            // shape.
            &format!("{}/{}/{}", connection, bucket.name, object.path.as_str()),
            bucket.name,
            connection,
            object,
            tags,
        ))
    }

    /// The one 12-field mapping from catalog primitives; both lookup paths go through it so
    /// the views cannot drift.
    fn from_parts(
        uri: &str,
        bucket: String,
        connection: String,
        object: &RemoteObject,
        tags: Vec<String>,
    ) -> FileEntry {
        FileEntry {
            uri: uri.to_string(),
            name: object.path.file_name().to_string(),
            directory: object.path.parent().unwrap_or_default(),
            bucket,
            connection,
            size: object.size,
            last_modified: object.last_modified,
            confirmed_at: object.confirmed_at,
            state: object.state,
            content_type: object.content_type.clone(),
            tags,
        }
    }

    /// The in-scope buckets a non-tag search cannot cover.
    async fn unindexed_in_scope(
        &self,
        workspace: rstore_domain::WorkspaceId,
        filter: &FileFilter,
    ) -> Result<Vec<String>> {
        if !filter.tags.is_empty() && !filter.untagged {
            return Ok(Vec::new());
        }
        let members = self.workspaces.buckets(workspace).await?;
        let mut labels = Vec::new();
        for bucket in self.buckets.list().await? {
            let in_scope = members.contains(&bucket.id)
                && (filter.buckets.is_empty() || filter.buckets.contains(&bucket.id))
                && (filter.connections.is_empty()
                    || filter.connections.contains(&bucket.connection_id));
            if in_scope && !bucket.config.index_enabled {
                labels.push(self.gateway.bucket_label(&bucket).await);
            }
        }
        Ok(labels)
    }

    fn entry_from_page(item: CatalogPageItem) -> FileEntry {
        let object = item.object;
        Self::from_parts(
            &format!(
                "{}/{}/{}",
                item.connection_name,
                item.bucket_name,
                object.path.as_str()
            ),
            item.bucket_name,
            item.connection_name,
            &object,
            item.tags,
        )
    }
}

impl FilesService {
    /// `find` — one filtered, sorted, paginated query.
    pub async fn list(
        &self,
        workspace: &WorkspaceRef,
        filter: &FileFilter,
        sort: FileSort,
        page: &PageRequest,
    ) -> Result<FilesPage> {
        let workspace_id = self.workspace_id(workspace).await?;
        let scope = Scope {
            workspace: workspace_id,
        };
        let result = self.catalog.page(scope, filter, sort, page).await?;
        let items = result
            .items
            .into_iter()
            .map(Self::entry_from_page)
            .collect();
        Ok(FilesPage {
            items,
            total: result.total,
            next_cursor: result.next_cursor,
            unindexed: self.unindexed_in_scope(workspace_id, filter).await?,
        })
    }

    /// `stat --offline` — one object with tags and staleness state.
    pub async fn show(
        &self,
        workspace: &WorkspaceRef,
        uri: &crate::reference::RemoteUri,
    ) -> Result<FileEntry> {
        let workspace_id = self.workspace_id(workspace).await?;
        let object = self.cached_object(uri, workspace_id).await?;
        self.entry_for(workspace_id, &object).await
    }

    /// The cached row behind a reference, which must belong to the workspace.
    ///
    /// A not-found error covers both an uncached object and one outside the workspace on
    /// purpose, so callers cannot tell a missing object from one they must not see.
    async fn cached_object(
        &self,
        uri: &crate::reference::RemoteUri,
        workspace: rstore_domain::WorkspaceId,
    ) -> Result<RemoteObject> {
        let (config, bucket) = self.gateway.resolve_bucket(uri).await?;
        let object = self
            .catalog
            .find_by_locator(config.id, bucket.id, &uri.object_path()?)
            .await?
            .ok_or_else(|| {
                Error::not_found(format!(
                    "`{uri}` is not in the catalog; run `index run` first"
                ))
            })?;
        if !self
            .workspaces
            .buckets(workspace)
            .await?
            .contains(&bucket.id)
        {
            return Err(Error::not_found(format!(
                "`{uri}` is not part of this workspace"
            )));
        }
        Ok(object)
    }
}
