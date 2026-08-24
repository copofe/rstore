//! Workspace management service.
//!
//! A workspace is a local grouping of registered buckets; creating, editing, or removing one
//! never touches remote state — removing a workspace removes neither buckets nor remote data.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{IgnoreRuleStore, WorkspaceStore};
use rstore_domain::id::{BucketId, WorkspaceId};
use rstore_domain::workspace::{IgnoreRule, IgnoreRuleId, Workspace};
use rstore_domain::{Clock, Error, Result};

use crate::bucket::{BucketRef, BucketService};

/// Reference a workspace by id or name. There is never an implicit default workspace.
#[derive(Debug, Clone)]
pub enum WorkspaceRef {
    Id(WorkspaceId),
    Name(String),
}

impl WorkspaceRef {
    /// Parse: `wsp_…` id, else a name.
    pub fn parse(s: &str) -> Self {
        if s.starts_with("wsp_") {
            if let Ok(id) = s.parse() {
                return WorkspaceRef::Id(id);
            }
        }
        WorkspaceRef::Name(s.to_string())
    }

    /// Human-readable form for messages.
    pub fn display(&self) -> String {
        match self {
            WorkspaceRef::Id(id) => id.to_string(),
            WorkspaceRef::Name(n) => n.clone(),
        }
    }
}

/// Input for creating a workspace.
#[derive(Debug, Clone)]
pub struct CreateWorkspaceInput {
    pub name: String,
    pub description: Option<String>,
}

/// Editable workspace fields.
///
/// `description` follows the `Option<Option<T>>` clearing convention: outer `None` keeps the
/// value, the inner `None` clears it.
#[derive(Debug, Clone, Default)]
pub struct UpdateWorkspaceInput {
    pub name: Option<String>,
    /// The outer `None` keeps the description; the inner `None` clears it.
    pub description: Option<Option<String>>,
}

/// A workspace plus its bucket-count summary — the application's view type; storage shapes
/// never cross this seam.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkspaceWithCount {
    pub workspace: Workspace,
    pub bucket_count: u64,
}

/// A workspace with its bucket membership resolved.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkspaceDetail {
    pub workspace: Workspace,
    pub buckets: Vec<BucketId>,
    pub object_count: u64,
}

/// Resolve a [`WorkspaceRef`] against the registry.
///
/// The one not-found message for a workspace reference, shared by every service.
pub(crate) async fn resolve_workspace(
    workspaces: &dyn WorkspaceStore,
    reference: &WorkspaceRef,
) -> Result<Workspace> {
    let found = match reference {
        WorkspaceRef::Id(id) => workspaces.get(*id).await?,
        WorkspaceRef::Name(name) => workspaces.get_by_name(name).await?,
    };
    found.ok_or_else(|| Error::not_found(format!("workspace `{}` not found", reference.display())))
}

/// Workspace lifecycle and bucket membership.
#[async_trait]
pub trait WorkspaceService: Send + Sync {
    async fn create(&self, input: CreateWorkspaceInput) -> Result<Workspace>;
    async fn list(&self) -> Result<Vec<WorkspaceWithCount>>;
    async fn resolve(&self, reference: &WorkspaceRef) -> Result<Workspace>;
    async fn show(&self, reference: &WorkspaceRef) -> Result<WorkspaceDetail>;
    async fn update(
        &self,
        reference: &WorkspaceRef,
        input: UpdateWorkspaceInput,
    ) -> Result<Workspace>;
    async fn remove(&self, reference: &WorkspaceRef) -> Result<()>;
    async fn add_bucket(&self, reference: &WorkspaceRef, bucket: &BucketRef) -> Result<BucketId>;
    async fn remove_bucket(&self, reference: &WorkspaceRef, bucket: &BucketRef) -> Result<()>;
    /// Ignore rules operate bucket-globally: every workspace that contains the bucket sees
    /// them.
    async fn ignore_add(&self, bucket: &BucketRef, directory: &str) -> Result<IgnoreRule>;
    async fn ignore_list(&self, bucket: &BucketRef) -> Result<Vec<IgnoreRule>>;
    async fn ignore_remove(&self, bucket: &BucketRef, directory: &str) -> Result<()>;
}

/// Default workspace implementation over the registry stores.
pub struct DefaultWorkspaceService {
    workspaces: Arc<dyn WorkspaceStore>,
    buckets: Arc<dyn BucketService>,
    ignores: Arc<dyn IgnoreRuleStore>,
    clock: Arc<dyn Clock>,
}

impl DefaultWorkspaceService {
    pub fn new(
        workspaces: Arc<dyn WorkspaceStore>,
        buckets: Arc<dyn BucketService>,
        ignores: Arc<dyn IgnoreRuleStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            workspaces,
            buckets,
            ignores,
            clock,
        }
    }

    async fn find(&self, reference: &WorkspaceRef) -> Result<Workspace> {
        resolve_workspace(self.workspaces.as_ref(), reference).await
    }

    async fn bucket_id(&self, bucket: &BucketRef) -> Result<BucketId> {
        Ok(self.buckets.resolve(bucket).await?.id)
    }
}

#[async_trait]
impl WorkspaceService for DefaultWorkspaceService {
    async fn create(&self, input: CreateWorkspaceInput) -> Result<Workspace> {
        let name = input.name.trim();
        if name.is_empty() {
            return Err(Error::invalid_input("workspace name is empty"));
        }
        let mut workspace = Workspace::new(WorkspaceId::new_random(), name, self.clock.now());
        workspace.description = input.description;
        self.workspaces.insert(&workspace).await?;
        Ok(workspace)
    }

    async fn list(&self) -> Result<Vec<WorkspaceWithCount>> {
        Ok(self
            .workspaces
            .list()
            .await?
            .into_iter()
            .map(|row| WorkspaceWithCount {
                workspace: row.workspace,
                bucket_count: row.bucket_count,
            })
            .collect())
    }

    async fn resolve(&self, reference: &WorkspaceRef) -> Result<Workspace> {
        self.find(reference).await
    }

    async fn show(&self, reference: &WorkspaceRef) -> Result<WorkspaceDetail> {
        let workspace = self.find(reference).await?;
        let buckets = self.workspaces.buckets(workspace.id).await?;
        let counts = self
            .workspaces
            .object_counts_by_bucket(workspace.id)
            .await?;
        let object_count = buckets
            .iter()
            .filter_map(|b| counts.iter().find(|(id, _)| id == b).map(|(_, n)| n))
            .sum();
        Ok(WorkspaceDetail {
            workspace,
            buckets,
            object_count,
        })
    }

    async fn update(
        &self,
        reference: &WorkspaceRef,
        input: UpdateWorkspaceInput,
    ) -> Result<Workspace> {
        let workspace = self.find(reference).await?;
        self.workspaces
            .update(
                workspace.id,
                input.name.as_deref(),
                input.description.as_ref().map(|d| d.as_deref()),
                self.clock.now(),
            )
            .await?;
        self.find(&WorkspaceRef::Id(workspace.id)).await
    }

    async fn remove(&self, reference: &WorkspaceRef) -> Result<()> {
        let workspace = self.find(reference).await?;
        self.workspaces.delete(workspace.id).await?;
        // Tag associations cascade with the workspace row (workspace isolation ends with the
        // workspace itself); bucket rows are untouched.
        Ok(())
    }

    async fn add_bucket(&self, reference: &WorkspaceRef, bucket: &BucketRef) -> Result<BucketId> {
        let workspace = self.find(reference).await?;
        let bucket_id = self.bucket_id(bucket).await?;
        self.workspaces
            .add_bucket(workspace.id, bucket_id, self.clock.now())
            .await?;
        Ok(bucket_id)
    }

    async fn remove_bucket(&self, reference: &WorkspaceRef, bucket: &BucketRef) -> Result<()> {
        let workspace = self.find(reference).await?;
        let bucket_id = self.bucket_id(bucket).await?;
        let removed = self
            .workspaces
            .remove_bucket(workspace.id, bucket_id)
            .await?;
        if !removed {
            return Err(Error::not_found(format!(
                "bucket `{}` is not part of workspace `{}`",
                crate::bucket::BucketRef::display(bucket),
                workspace.name
            )));
        }
        // Tag relations enter the retention window: kept in the remote_object_tags rows until
        // the catalog retention job purges them.
        Ok(())
    }

    async fn ignore_add(&self, bucket: &BucketRef, directory: &str) -> Result<IgnoreRule> {
        let bucket_id = self.bucket_id(bucket).await?;
        let directory = IgnoreRule::normalise_directory(directory)?;
        // Reject a rule that duplicates or nests inside an existing one (and vice versa) so
        // pruning stays deterministic.
        for existing in self.ignores.list(bucket_id).await? {
            if existing.directory == directory
                || directory.starts_with(&existing.directory)
                || existing.directory.starts_with(&directory)
            {
                return Err(Error::conflict(format!(
                    "rule `{}` overlaps existing ignore rule `{}`",
                    directory, existing.directory
                )));
            }
        }
        let rule = IgnoreRule {
            id: IgnoreRuleId(0),
            bucket_id,
            directory,
            enabled: true,
            created_at: self.clock.now(),
        };
        self.ignores.insert(&rule).await?;
        Ok(rule)
    }

    async fn ignore_list(&self, bucket: &BucketRef) -> Result<Vec<IgnoreRule>> {
        let bucket_id = self.bucket_id(bucket).await?;
        self.ignores.list(bucket_id).await
    }

    async fn ignore_remove(&self, bucket: &BucketRef, directory: &str) -> Result<()> {
        let bucket_id = self.bucket_id(bucket).await?;
        let directory = IgnoreRule::normalise_directory(directory)?;
        let removed = self.ignores.delete(bucket_id, &directory).await?;
        if !removed {
            return Err(Error::not_found(format!(
                "no ignore rule for `{directory}`"
            )));
        }
        // A later index start re-scans the freed subtree; tags on those objects survive
        // because their rows were never deleted.
        Ok(())
    }
}
