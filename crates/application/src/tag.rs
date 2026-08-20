//! Tag service (TAG-).
//!
//! Tags are workspace-scoped and keyed to the stable object id, so they
//! survive product-side renames (TAG-018) and same-path overwrites
//! (TAG-021). Attach validates that the object's bucket belongs to the
//! workspace (WSP-013 isolation).

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{TagStore, WorkspaceStore};
use rstore_domain::id::{TagId, WorkspaceId};
use rstore_domain::workspace::Tag;
use rstore_domain::RemoteObjectId;
use rstore_domain::{Clock, Error, Result};

use crate::workspace::WorkspaceRef;

/// Input for `tag create` (TAG-001..004).
#[derive(Debug, Clone)]
pub struct CreateTagInput {
    pub workspace: WorkspaceRef,
    pub name: String,
    pub color: Option<String>,
    pub description: Option<String>,
}

/// Editable fields (TAG-006).
#[derive(Debug, Clone, Default)]
pub struct UpdateTagInput {
    pub name: Option<String>,
    pub color: Option<String>,
    /// `Some("")` clears the description.
    pub description: Option<String>,
}

/// Reference a tag by id or (workspace-scoped) name.
#[derive(Debug, Clone)]
pub enum TagRef {
    Id(TagId),
    Name(String),
}

impl TagRef {
    pub fn parse(s: &str) -> Self {
        if s.starts_with("tag_") {
            if let Ok(id) = s.parse() {
                return TagRef::Id(id);
            }
        }
        TagRef::Name(s.to_string())
    }
}

/// A tag plus its live association count (TAG-005) — the application's view
/// type; storage shapes never cross this seam.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TagWithCount {
    pub tag: Tag,
    pub object_count: u64,
}

/// Outcome of `tag merge` (TAG-008).
#[derive(Debug, Clone, serde::Serialize)]
pub struct MergeReport {
    pub source: Tag,
    pub target: Tag,
    /// Associations moved (deduplicated against existing ones).
    pub moved: u64,
}

/// Per-reference outcome for a batch tag mutation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TagBatchOutcome {
    pub uri: String,
    pub error: Option<String>,
}

impl TagBatchOutcome {
    pub fn succeeded(&self) -> bool {
        self.error.is_none()
    }
}

/// Tag lifecycle and association management (TAG-001..012).
#[async_trait]
pub trait TagService: Send + Sync {
    async fn create(&self, input: CreateTagInput) -> Result<Tag>;
    async fn list(&self, workspace: &WorkspaceRef) -> Result<Vec<TagWithCount>>;
    /// Resolve a reference to the tag itself (validation/confirmation paths).
    async fn resolve(&self, workspace: &WorkspaceRef, tag: &TagRef) -> Result<Tag>;
    async fn edit(
        &self,
        workspace: &WorkspaceRef,
        tag: &TagRef,
        input: UpdateTagInput,
    ) -> Result<Tag>;
    async fn remove(&self, workspace: &WorkspaceRef, tag: &TagRef) -> Result<()>;
    /// Merge `source` into `target`: associations move and deduplicate, the
    /// source tag disappears (TAG-008). Returns the number of associations
    /// moved.
    async fn merge(
        &self,
        workspace: &WorkspaceRef,
        source: &TagRef,
        target: &TagRef,
    ) -> Result<MergeReport>;
    /// Attach tags to one object located by a remote reference (TAG-009/011).
    /// `replace` swaps the whole tag set (TAG-013).
    async fn attach(
        &self,
        workspace: &WorkspaceRef,
        uri: &str,
        tags: &[String],
        replace: bool,
    ) -> Result<()>;
    /// Attach tags to a batch, resolving shared workspace/tag inputs once.
    async fn attach_many(
        &self,
        workspace: &WorkspaceRef,
        uris: &[String],
        tags: &[String],
        replace: bool,
    ) -> Result<Vec<TagBatchOutcome>>;
    /// Detach tags from one object (TAG-010/012); empty `tags` = all.
    async fn detach(&self, workspace: &WorkspaceRef, uri: &str, tags: &[String]) -> Result<()>;
    /// Detach tags from a batch, resolving shared workspace/tag inputs once.
    async fn detach_many(
        &self,
        workspace: &WorkspaceRef,
        uris: &[String],
        tags: &[String],
    ) -> Result<Vec<TagBatchOutcome>>;
    /// Tag names for one object (MIX-007 / files show).
    async fn tags_for(&self, workspace: &WorkspaceRef, uri: &str) -> Result<Vec<String>>;
}

pub struct DefaultTagService {
    tags: Arc<dyn TagStore>,
    workspaces: Arc<dyn WorkspaceStore>,
    locator: crate::locator::CatalogLocator,
    clock: Arc<dyn Clock>,
}

impl DefaultTagService {
    pub fn new(
        tags: Arc<dyn TagStore>,
        workspaces: Arc<dyn WorkspaceStore>,
        locator: crate::locator::CatalogLocator,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            tags,
            workspaces,
            locator,
            clock,
        }
    }

    async fn resolve_workspace(
        &self,
        workspace: &WorkspaceRef,
    ) -> Result<rstore_domain::Workspace> {
        crate::workspace::resolve_workspace(self.workspaces.as_ref(), workspace).await
    }

    async fn find_tag(&self, workspace_id: WorkspaceId, tag: &TagRef) -> Result<Tag> {
        let found = match tag {
            TagRef::Id(id) => {
                let t = self.tags.get_by_id(*id).await?;
                if t.as_ref().map(|t| t.workspace_id) == Some(workspace_id) {
                    t
                } else {
                    None
                }
            }
            TagRef::Name(name) => self.tags.get(workspace_id, name).await?,
        };
        found.ok_or_else(|| {
            Error::not_found(match tag {
                TagRef::Id(id) => format!("tag `{id}` not found in this workspace"),
                TagRef::Name(n) => format!("tag `{n}` not found in this workspace"),
            })
        })
    }

    /// Resolve `<connection>/<bucket>/<key>` to a catalog row id.
    /// Workspace membership (WSP-013) is enforced by the locator.
    async fn resolve_object(&self, workspace: WorkspaceId, uri: &str) -> Result<RemoteObjectId> {
        Ok(self.locator.resolve(uri, workspace).await?.id)
    }

    async fn resolve_many(&self, workspace: WorkspaceId, names: &[String]) -> Result<Vec<TagId>> {
        let mut ids = Vec::with_capacity(names.len());
        for name in names {
            ids.push(
                self.find_tag(workspace, &TagRef::Name(name.clone()))
                    .await?
                    .id,
            );
        }
        Ok(ids)
    }

    async fn attach_resolved(
        &self,
        workspace: WorkspaceId,
        uri: &str,
        tags: &[TagId],
        replace: bool,
    ) -> Result<()> {
        let object = self.resolve_object(workspace, uri).await?;
        if replace {
            self.tags.clear_object(workspace, object).await?;
        }
        let now = self.clock.now();
        for tag in tags {
            self.tags.attach(workspace, object, *tag, now).await?;
        }
        Ok(())
    }

    async fn detach_resolved(
        &self,
        workspace: WorkspaceId,
        uri: &str,
        tags: &[TagId],
    ) -> Result<()> {
        let object = self.resolve_object(workspace, uri).await?;
        if tags.is_empty() {
            return self.tags.clear_object(workspace, object).await;
        }
        for tag in tags {
            self.tags.detach(workspace, object, *tag).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl TagService for DefaultTagService {
    async fn create(&self, input: CreateTagInput) -> Result<Tag> {
        let workspace = self.resolve_workspace(&input.workspace).await?;
        let name = input.name.trim();
        if name.is_empty() {
            return Err(Error::invalid_input("tag name is empty"));
        }
        let mut tag = Tag::new(TagId::new(), workspace.id, name, self.clock.now());
        tag.color = input.color.unwrap_or_else(|| "gray".to_string());
        tag.description = input.description;
        self.tags.insert(&tag).await?;
        Ok(tag)
    }

    async fn list(&self, workspace: &WorkspaceRef) -> Result<Vec<TagWithCount>> {
        let workspace = self.resolve_workspace(workspace).await?;
        Ok(self
            .tags
            .list(workspace.id)
            .await?
            .into_iter()
            .map(|row| TagWithCount {
                tag: row.tag,
                object_count: row.object_count,
            })
            .collect())
    }

    async fn resolve(&self, workspace: &WorkspaceRef, tag: &TagRef) -> Result<Tag> {
        let workspace = self.resolve_workspace(workspace).await?;
        self.find_tag(workspace.id, tag).await
    }

    async fn edit(
        &self,
        workspace: &WorkspaceRef,
        tag: &TagRef,
        input: UpdateTagInput,
    ) -> Result<Tag> {
        let workspace = self.resolve_workspace(workspace).await?;
        let existing = self.find_tag(workspace.id, tag).await?;
        self.tags
            .update(
                existing.id,
                input.name.as_deref(),
                input.color.as_deref(),
                input.description.as_deref(),
            )
            .await?;
        self.tags
            .get_by_id(existing.id)
            .await?
            .ok_or_else(|| Error::not_found("tag disappeared during edit"))
    }

    async fn remove(&self, workspace: &WorkspaceRef, tag: &TagRef) -> Result<()> {
        let workspace = self.resolve_workspace(workspace).await?;
        let existing = self.find_tag(workspace.id, tag).await?;
        self.tags.delete(existing.id).await?;
        Ok(())
    }

    async fn merge(
        &self,
        workspace: &WorkspaceRef,
        source: &TagRef,
        target: &TagRef,
    ) -> Result<MergeReport> {
        let workspace = self.resolve_workspace(workspace).await?;
        let source = self.find_tag(workspace.id, source).await?;
        let target = self.find_tag(workspace.id, target).await?;
        if source.id == target.id {
            return Err(Error::invalid_input("a tag cannot merge into itself"));
        }
        let moved = self
            .tags
            .merge_associations(workspace.id, source.id, target.id, self.clock.now())
            .await?;
        self.tags.delete(source.id).await?;
        Ok(MergeReport {
            source,
            target,
            moved,
        })
    }

    async fn attach(
        &self,
        workspace: &WorkspaceRef,
        uri: &str,
        tags: &[String],
        replace: bool,
    ) -> Result<()> {
        let workspace = self.resolve_workspace(workspace).await?;
        let resolved = self.resolve_many(workspace.id, tags).await?;
        self.attach_resolved(workspace.id, uri, &resolved, replace)
            .await
    }

    async fn attach_many(
        &self,
        workspace: &WorkspaceRef,
        uris: &[String],
        tags: &[String],
        replace: bool,
    ) -> Result<Vec<TagBatchOutcome>> {
        let workspace = self.resolve_workspace(workspace).await?;
        let tags = self.resolve_many(workspace.id, tags).await?;
        let mut outcomes = Vec::with_capacity(uris.len());
        for uri in uris {
            let error = self
                .attach_resolved(workspace.id, uri, &tags, replace)
                .await
                .err()
                .map(|error| error.to_string());
            outcomes.push(TagBatchOutcome {
                uri: uri.clone(),
                error,
            });
        }
        Ok(outcomes)
    }

    async fn detach(&self, workspace: &WorkspaceRef, uri: &str, tags: &[String]) -> Result<()> {
        let workspace = self.resolve_workspace(workspace).await?;
        let resolved = self.resolve_many(workspace.id, tags).await?;
        self.detach_resolved(workspace.id, uri, &resolved).await
    }

    async fn detach_many(
        &self,
        workspace: &WorkspaceRef,
        uris: &[String],
        tags: &[String],
    ) -> Result<Vec<TagBatchOutcome>> {
        let workspace = self.resolve_workspace(workspace).await?;
        let tags = self.resolve_many(workspace.id, tags).await?;
        let mut outcomes = Vec::with_capacity(uris.len());
        for uri in uris {
            let error = self
                .detach_resolved(workspace.id, uri, &tags)
                .await
                .err()
                .map(|error| error.to_string());
            outcomes.push(TagBatchOutcome {
                uri: uri.clone(),
                error,
            });
        }
        Ok(outcomes)
    }

    async fn tags_for(&self, workspace: &WorkspaceRef, uri: &str) -> Result<Vec<String>> {
        let workspace = self.resolve_workspace(workspace).await?;
        let object = self.resolve_object(workspace.id, uri).await?;
        self.tags.tags_for_object(workspace.id, object).await
    }
}
