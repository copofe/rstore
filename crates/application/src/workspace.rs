//! Workspace management service.
//!
//! A workspace is a local grouping of registered buckets; creating, editing, or removing one
//! never touches remote state — removing a workspace removes neither buckets nor remote data.
//! One workspace may be selected as current; commands that omit a workspace reference target
//! it ([`WorkspaceService::target`]).

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::{IgnoreRuleStore, SettingsStore, WorkspaceStore};
use rstore_domain::id::{BucketId, WorkspaceId};
use rstore_domain::workspace::{IgnoreRule, IgnoreRuleId, Workspace};
use rstore_domain::{Clock, Error, Result};

use crate::bucket::{BucketRef, BucketService};

/// Settings key holding the current workspace's id.
const CURRENT_WORKSPACE_KEY: &str = "workspace.current";

/// Name of the workspace created when the first bucket is enrolled into none.
const DEFAULT_WORKSPACE_NAME: &str = "default";

/// Reference a workspace by id or name.
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
    /// Whether this is the selected current workspace.
    pub current: bool,
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
    /// Create a workspace; it becomes current when no workspace is selected yet.
    async fn create(&self, input: CreateWorkspaceInput) -> Result<Workspace>;
    async fn list(&self) -> Result<Vec<WorkspaceWithCount>>;
    async fn resolve(&self, reference: &WorkspaceRef) -> Result<Workspace>;
    /// The selected current workspace, if any.
    ///
    /// # Errors
    /// `not_found` when the selection names a workspace that no longer exists.
    async fn current(&self) -> Result<Option<Workspace>>;
    /// Select `reference` as the current workspace.
    async fn select(&self, reference: &WorkspaceRef) -> Result<Workspace>;
    /// The workspace a command targets: the explicit reference, else the current workspace,
    /// else the only workspace when exactly one exists.
    ///
    /// # Errors
    /// `invalid_input` when no reference is given and none of the fallbacks applies.
    async fn target(&self, explicit: Option<&WorkspaceRef>) -> Result<Workspace>;
    /// Add a registered bucket to the workspace a registration targets — the explicit
    /// reference, else the current or only workspace. When no workspace exists yet, `default`
    /// is created and selected. Returns `None`, leaving the bucket unenrolled, when several
    /// workspaces exist and none is selected.
    async fn enroll(
        &self,
        bucket: &BucketRef,
        explicit: Option<&WorkspaceRef>,
    ) -> Result<Option<Workspace>>;
    async fn show(&self, reference: &WorkspaceRef) -> Result<WorkspaceDetail>;
    async fn update(
        &self,
        reference: &WorkspaceRef,
        input: UpdateWorkspaceInput,
    ) -> Result<Workspace>;
    /// Delete a workspace, clearing the current selection when it pointed here.
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
    settings: Arc<dyn SettingsStore>,
    clock: Arc<dyn Clock>,
}

/// What an omitted workspace reference resolves to.
enum Implicit {
    Found(Workspace),
    /// No workspace exists.
    Empty,
    /// This many workspaces exist and none is selected.
    Ambiguous(usize),
}

impl DefaultWorkspaceService {
    pub fn new(
        workspaces: Arc<dyn WorkspaceStore>,
        buckets: Arc<dyn BucketService>,
        ignores: Arc<dyn IgnoreRuleStore>,
        settings: Arc<dyn SettingsStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            workspaces,
            buckets,
            ignores,
            settings,
            clock,
        }
    }

    async fn find(&self, reference: &WorkspaceRef) -> Result<Workspace> {
        resolve_workspace(self.workspaces.as_ref(), reference).await
    }

    /// The stored selection, without checking that the workspace still exists.
    async fn current_id(&self) -> Result<Option<WorkspaceId>> {
        self.settings
            .get(CURRENT_WORKSPACE_KEY)
            .await?
            .map(|raw| {
                raw.parse::<WorkspaceId>().map_err(|error| {
                    Error::database(format!(
                        "setting `{CURRENT_WORKSPACE_KEY}` holds `{raw}`, not a workspace id"
                    ))
                    .with_source(error)
                })
            })
            .transpose()
    }

    async fn set_current(&self, id: WorkspaceId) -> Result<()> {
        self.settings
            .set(CURRENT_WORKSPACE_KEY, &id.to_string())
            .await
    }

    /// Resolve an omitted reference: the current workspace, else the only one.
    async fn implicit(&self) -> Result<Implicit> {
        if let Some(current) = self.current().await? {
            return Ok(Implicit::Found(current));
        }
        let all = self.workspaces.list().await?;
        let count = all.len();
        Ok(match all.into_iter().next() {
            None => Implicit::Empty,
            Some(only) if count == 1 => Implicit::Found(only.workspace),
            Some(_) => Implicit::Ambiguous(count),
        })
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
        if self.current_id().await?.is_none() {
            self.set_current(workspace.id).await?;
        }
        Ok(workspace)
    }

    async fn list(&self) -> Result<Vec<WorkspaceWithCount>> {
        let current = self.current_id().await?;
        Ok(self
            .workspaces
            .list()
            .await?
            .into_iter()
            .map(|row| WorkspaceWithCount {
                current: current == Some(row.workspace.id),
                workspace: row.workspace,
                bucket_count: row.bucket_count,
            })
            .collect())
    }

    async fn resolve(&self, reference: &WorkspaceRef) -> Result<Workspace> {
        self.find(reference).await
    }

    async fn current(&self) -> Result<Option<Workspace>> {
        let Some(id) = self.current_id().await? else {
            return Ok(None);
        };
        self.workspaces
            .get(id)
            .await?
            .ok_or_else(|| {
                Error::not_found(format!(
                    "the current workspace `{id}` no longer exists; select another with \
                     `workspace use <name>`"
                ))
            })
            .map(Some)
    }

    async fn select(&self, reference: &WorkspaceRef) -> Result<Workspace> {
        let workspace = self.find(reference).await?;
        self.set_current(workspace.id).await?;
        Ok(workspace)
    }

    async fn target(&self, explicit: Option<&WorkspaceRef>) -> Result<Workspace> {
        if let Some(reference) = explicit {
            return self.find(reference).await;
        }
        match self.implicit().await? {
            Implicit::Found(workspace) => Ok(workspace),
            Implicit::Empty => Err(Error::invalid_input(
                "no workspace exists yet; create one with `workspace create <name>`",
            )),
            Implicit::Ambiguous(count) => Err(Error::invalid_input(format!(
                "{count} workspaces exist and none is selected; select one with \
                 `workspace use <name>` or pass `--workspace`"
            ))),
        }
    }

    async fn enroll(
        &self,
        bucket: &BucketRef,
        explicit: Option<&WorkspaceRef>,
    ) -> Result<Option<Workspace>> {
        // Resolve the bucket first so a bad reference never creates `default`.
        let bucket_id = self.bucket_id(bucket).await?;
        let workspace = match explicit {
            Some(reference) => self.find(reference).await?,
            None => match self.implicit().await? {
                Implicit::Found(workspace) => workspace,
                Implicit::Empty => {
                    self.create(CreateWorkspaceInput {
                        name: DEFAULT_WORKSPACE_NAME.to_string(),
                        description: None,
                    })
                    .await?
                }
                Implicit::Ambiguous(_) => return Ok(None),
            },
        };
        self.workspaces
            .add_bucket(workspace.id, bucket_id, self.clock.now())
            .await?;
        Ok(Some(workspace))
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
        // Clear the selection before deleting: an interruption between the two writes leaves an
        // unselected workspace, never a selection pointing at a deleted one.
        if self.current_id().await? == Some(workspace.id) {
            self.settings.delete(CURRENT_WORKSPACE_KEY).await?;
        }
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
