//! Tag service.
//!
//! Tags are workspace-scoped and keyed to the stable object id, so they survive product-side
//! renames and same-path overwrites. Attach validates that the object's bucket belongs to the
//! workspace (workspace isolation) and creates tags it does not find.

use std::sync::Arc;

use crate::batch_outcome::{BatchOutcome, ItemOutcome};
use crate::reference::RemoteUri;
use crate::workspace::WorkspaceRef;
use async_trait::async_trait;
use rstore_application_ports::{TagStore, WorkspaceStore};
use rstore_domain::id::{TagId, WorkspaceId};
use rstore_domain::workspace::Tag;
use rstore_domain::RemoteObjectId;
use rstore_domain::{Clock, Error, Result};

/// Input for creating a tag.
#[derive(Debug, Clone)]
pub struct CreateTagInput {
    pub workspace: WorkspaceRef,
    pub name: String,
    pub color: Option<String>,
    pub description: Option<String>,
}

/// Editable tag fields.
///
/// `description` follows the `Option<Option<T>>` clearing convention: outer `None` keeps the
/// value, inner `None` clears it.
#[derive(Debug, Clone, Default)]
pub struct UpdateTagInput {
    pub name: Option<String>,
    pub color: Option<String>,
    /// The outer `None` keeps the description; the inner `None` clears it.
    pub description: Option<Option<String>>,
}

/// Reference a tag by id or (workspace-scoped) name.
#[derive(Debug, Clone)]
pub enum TagRef {
    Id(TagId),
    Name(String),
}

impl TagRef {
    /// Parse a user-supplied string: a `tag_…` id, otherwise a name.
    pub fn parse(s: &str) -> Self {
        if s.starts_with("tag_") {
            if let Ok(id) = s.parse() {
                return TagRef::Id(id);
            }
        }
        TagRef::Name(s.to_string())
    }

    /// Human-readable form for messages — same shape as the other refs.
    pub fn display(&self) -> String {
        match self {
            TagRef::Id(id) => id.to_string(),
            TagRef::Name(n) => n.clone(),
        }
    }
}

/// A tag plus its live association count — the application's view type; storage shapes never
/// cross this seam.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TagWithCount {
    pub tag: Tag,
    pub object_count: u64,
}

/// Outcome of a batch attach: per-object results plus the tags the attach created.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AttachReport {
    /// Tags that did not exist in the workspace and were created by this attach.
    pub created: Vec<Tag>,
    pub outcome: BatchOutcome<()>,
}

/// Outcome of merging one tag into another.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MergeReport {
    pub source: Tag,
    pub target: Tag,
    /// Associations moved (deduplicated against existing ones).
    pub moved: u64,
}

/// Tag lifecycle and association management.
#[async_trait]
pub trait TagService: Send + Sync {
    async fn create(&self, input: CreateTagInput) -> Result<Tag>;
    async fn list(&self, workspace: &WorkspaceRef) -> Result<Vec<TagWithCount>>;
    /// Resolve a reference to the tag itself (validation/confirmation paths).
    async fn resolve(&self, workspace: &WorkspaceRef, tag: &TagRef) -> Result<Tag>;
    async fn update(
        &self,
        workspace: &WorkspaceRef,
        tag: &TagRef,
        input: UpdateTagInput,
    ) -> Result<Tag>;
    async fn remove(&self, workspace: &WorkspaceRef, tag: &TagRef) -> Result<()>;
    /// Merge `source` into `target`: associations move and deduplicate, and the source tag
    /// disappears. Returns both tags plus the number of associations moved.
    async fn merge(
        &self,
        workspace: &WorkspaceRef,
        source: &TagRef,
        target: &TagRef,
    ) -> Result<MergeReport>;
    /// Attach tags to a batch of objects located by remote references, resolving shared
    /// workspace/tag inputs once; `replace` swaps each object's whole tag set instead of
    /// extending it. Tag names missing from the workspace are created (default color) once
    /// the first object resolves — a batch that tags nothing creates nothing — and reported
    /// in [`AttachReport::created`].
    ///
    /// Failures are per-item: each reference gets an outcome with the error text instead of
    /// aborting the whole batch.
    async fn attach_many(
        &self,
        workspace: &WorkspaceRef,
        uris: &[RemoteUri],
        tags: &[String],
        replace: bool,
    ) -> Result<AttachReport>;
    /// Detach tags from a batch, resolving shared workspace/tag inputs once (per-item
    /// outcomes, as in [`Self::attach_many`]); empty `tags` detaches all of them. Unknown tag
    /// names are an error, never created.
    async fn detach_many(
        &self,
        workspace: &WorkspaceRef,
        uris: &[RemoteUri],
        tags: &[String],
    ) -> Result<BatchOutcome<()>>;
    /// Tag names for one object.
    async fn tags_for(&self, workspace: &WorkspaceRef, uri: &RemoteUri) -> Result<Vec<String>>;
}

/// Default tag implementation over the tag store and catalog locator.
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

    /// Resolve within one workspace: an id is only accepted when it belongs to that
    /// workspace, so cross-workspace ids read as not-found rather than leaking.
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
    ///
    /// Workspace membership is enforced by the locator.
    async fn resolve_object(
        &self,
        workspace: WorkspaceId,
        uri: &RemoteUri,
    ) -> Result<RemoteObjectId> {
        Ok(self.locator.resolve(uri, workspace).await?.id)
    }

    /// Build and persist a new tag; the one home of the name rule and the default color.
    async fn insert_tag(
        &self,
        workspace: WorkspaceId,
        name: &str,
        color: Option<String>,
        description: Option<String>,
    ) -> Result<Tag> {
        let name = name.trim();
        if name.is_empty() {
            return Err(Error::invalid_input("tag name is empty"));
        }
        let mut tag = Tag::new(TagId::new_random(), workspace, name, self.clock.now());
        tag.color = color.unwrap_or_else(|| "gray".to_string());
        tag.description = description;
        self.tags.insert(&tag).await?;
        Ok(tag)
    }

    /// Split names into the workspace's existing tag ids and the names it lacks. Names are
    /// trimmed and deduplicated first, so `a, a` names one tag.
    async fn partition_tags(
        &self,
        workspace: WorkspaceId,
        names: &[String],
    ) -> Result<(Vec<TagId>, Vec<String>)> {
        let mut ids = Vec::with_capacity(names.len());
        let mut missing = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for name in names.iter().map(|name| name.trim()) {
            if !seen.insert(name) {
                continue;
            }
            match self.tags.get(workspace, name).await? {
                Some(existing) => ids.push(existing.id),
                None => missing.push(name.to_string()),
            }
        }
        Ok((ids, missing))
    }

    // Resolve names to ids through the store's by-name lookup — no per-name `TagRef`
    // allocation.
    async fn resolve_many(&self, workspace: WorkspaceId, names: &[String]) -> Result<Vec<TagId>> {
        let mut ids = Vec::with_capacity(names.len());
        for name in names {
            let found = self.tags.get(workspace, name).await?;
            ids.push(
                found
                    .ok_or_else(|| {
                        Error::not_found(format!("tag `{name}` not found in this workspace"))
                    })?
                    .id,
            );
        }
        Ok(ids)
    }

    /// Attach to one object. Any name still in `missing` is created first — only after the
    /// object resolves — and moves into `tags` and `created`.
    async fn attach_one(
        &self,
        workspace: WorkspaceId,
        uri: &RemoteUri,
        tags: &mut Vec<TagId>,
        missing: &mut Vec<String>,
        created: &mut Vec<Tag>,
        replace: bool,
    ) -> Result<()> {
        let object = self.resolve_object(workspace, uri).await?;
        while let Some(name) = missing.first() {
            let tag = self.insert_tag(workspace, name, None, None).await?;
            missing.remove(0);
            tags.push(tag.id);
            created.push(tag);
        }
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
        uri: &RemoteUri,
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
        self.insert_tag(workspace.id, &input.name, input.color, input.description)
            .await
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

    async fn update(
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
                input.description.as_ref().map(|d| d.as_deref()),
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

    async fn attach_many(
        &self,
        workspace: &WorkspaceRef,
        uris: &[RemoteUri],
        tags: &[String],
        replace: bool,
    ) -> Result<AttachReport> {
        let workspace = self.resolve_workspace(workspace).await?;
        let (mut tags, mut missing) = self.partition_tags(workspace.id, tags).await?;
        let mut created = Vec::new();
        let mut items = Vec::with_capacity(uris.len());
        for uri in uris {
            items.push(
                match self
                    .attach_one(
                        workspace.id,
                        uri,
                        &mut tags,
                        &mut missing,
                        &mut created,
                        replace,
                    )
                    .await
                {
                    Ok(()) => ItemOutcome::succeeded(uri.to_string(), None),
                    Err(e) => ItemOutcome::failed(uri.to_string(), &e),
                },
            );
        }
        Ok(AttachReport {
            created,
            outcome: BatchOutcome::new(items),
        })
    }

    async fn detach_many(
        &self,
        workspace: &WorkspaceRef,
        uris: &[RemoteUri],
        tags: &[String],
    ) -> Result<BatchOutcome<()>> {
        let workspace = self.resolve_workspace(workspace).await?;
        let tags = self.resolve_many(workspace.id, tags).await?;
        let mut items = Vec::with_capacity(uris.len());
        for uri in uris {
            items.push(match self.detach_resolved(workspace.id, uri, &tags).await {
                Ok(()) => ItemOutcome::succeeded(uri.to_string(), None),
                Err(e) => ItemOutcome::failed(uri.to_string(), &e),
            });
        }
        Ok(BatchOutcome::new(items))
    }

    async fn tags_for(&self, workspace: &WorkspaceRef, uri: &RemoteUri) -> Result<Vec<String>> {
        let workspace = self.resolve_workspace(workspace).await?;
        let object = self.resolve_object(workspace.id, uri).await?;
        self.tags.tags_for_object(workspace.id, object).await
    }
}
