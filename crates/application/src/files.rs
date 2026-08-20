//! Mixed file list and search over the workspace catalog (MIX-/IDX-015..017).
//!
//! All queries run through the [`CatalogQuery`] seam against the local
//! index — offline-safe and explicitly staleness-flagged (IDX-017). The
//! catalog adapter assembles list pages, while `show` enriches one object.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{BucketStore, ConnectionStore, TagStore, WorkspaceStore};
use rstore_catalog::{CatalogPageItem, CatalogQuery, FileFilter, FileSort, PageRequest, Scope};
use rstore_domain::remote::RemoteObject;
use rstore_domain::{Error, Result};

use crate::workspace::WorkspaceRef;

/// One row of the mixed list (MIX-003..010): the object plus its resolved
/// source identity and the workspace's tag names.
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

/// A page plus the total match count for the filter (MIX-024).
#[derive(Debug, Clone, serde::Serialize)]
pub struct FilesPage {
    pub items: Vec<FileEntry>,
    pub total: u64,
    pub next_cursor: Option<String>,
}

/// Mixed-list queries (MIX-).
#[async_trait]
pub trait FilesService: Send + Sync {
    /// `files list` / `files search` — one filtered, sorted, paginated query.
    async fn list(
        &self,
        workspace: &WorkspaceRef,
        filter: &FileFilter,
        sort: FileSort,
        page: &PageRequest,
    ) -> Result<FilesPage>;
    /// `files show` — one object with tags and staleness (IDX-015/016).
    async fn show(&self, workspace: &WorkspaceRef, uri: &str) -> Result<FileEntry>;
}

pub struct DefaultFilesService {
    catalog: Arc<dyn CatalogQuery>,
    workspaces: Arc<dyn WorkspaceStore>,
    tags: Arc<dyn TagStore>,
    connections: Arc<dyn ConnectionStore>,
    buckets: Arc<dyn BucketStore>,
    locator: crate::locator::CatalogLocator,
}

impl DefaultFilesService {
    pub fn new(
        catalog: Arc<dyn CatalogQuery>,
        workspaces: Arc<dyn WorkspaceStore>,
        tags: Arc<dyn TagStore>,
        connections: Arc<dyn ConnectionStore>,
        buckets: Arc<dyn BucketStore>,
        locator: crate::locator::CatalogLocator,
    ) -> Self {
        Self {
            catalog,
            workspaces,
            tags,
            connections,
            buckets,
            locator,
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
        let tags = self.tags.tags_for_object(workspace, object.id).await?;
        Ok(FileEntry {
            // Canonical address display: `conn/bucket/key`, the CLI's short
            // Canonical remote-reference shape.
            uri: format!("{}/{}/{}", connection, bucket.name, object.path.as_str()),
            name: object.path.file_name().to_string(),
            directory: object.path.parent().unwrap_or_default(),
            bucket: bucket.name,
            connection,
            size: object.size,
            last_modified: object.last_modified,
            confirmed_at: object.confirmed_at,
            state: object.state,
            content_type: object.content_type.clone(),
            tags,
        })
    }

    fn entry_from_page(item: CatalogPageItem) -> FileEntry {
        let object = item.object;
        FileEntry {
            // Canonical address display: `conn/bucket/key`, the CLI's short
            // canonical remote-reference shape.
            uri: format!(
                "{}/{}/{}",
                item.connection_name,
                item.bucket_name,
                object.path.as_str()
            ),
            name: object.path.file_name().to_string(),
            directory: object.path.parent().unwrap_or_default(),
            bucket: item.bucket_name,
            connection: item.connection_name,
            size: object.size,
            last_modified: object.last_modified,
            confirmed_at: object.confirmed_at,
            state: object.state,
            content_type: object.content_type,
            tags: item.tags,
        }
    }
}

#[async_trait]
impl FilesService for DefaultFilesService {
    async fn list(
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
        })
    }

    async fn show(&self, workspace: &WorkspaceRef, uri: &str) -> Result<FileEntry> {
        let workspace_id = self.workspace_id(workspace).await?;
        let object = self.locator.resolve(uri, workspace_id).await?;
        self.entry_for(workspace_id, &object).await
    }
}
