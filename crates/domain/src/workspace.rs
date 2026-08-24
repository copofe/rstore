//! Workspace, tag, ignore-rule and index-run domain models.
//!
//! A workspace is a *local grouping* of registered buckets: it never touches
//! remote state. Tags are workspace-scoped and keyed to the stable
//! [`crate::RemoteObjectId`], so they survive remote renames and same-path
//! overwrites.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::id::{BucketId, IndexRunId, TagId, WorkspaceId};

/// A named grouping of buckets with its own tag namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: WorkspaceId,
    /// Unique, case-sensitive; uniqueness is checked case-insensitively too,
    /// so two workspaces may not differ by case alone.
    pub name: String,
    pub description: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl Workspace {
    /// Create a workspace stamped with the given instant.
    pub fn new(id: WorkspaceId, name: impl Into<String>, at: OffsetDateTime) -> Self {
        Self {
            id,
            name: name.into(),
            description: None,
            created_at: at,
            updated_at: at,
        }
    }
}

/// A bucket-in-workspace membership row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceBucket {
    pub workspace_id: WorkspaceId,
    pub bucket_id: BucketId,
    #[serde(with = "time::serde::rfc3339::option")]
    pub added_at: Option<OffsetDateTime>,
}

/// Stable identity of one ignore-rule row (SQLite autoincrement).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IgnoreRuleId(pub i64);

impl std::fmt::Display for IgnoreRuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ign_{}", self.0)
    }
}

/// One ignore rule: a bucket-relative directory prefix.
///
/// The directory is stored normalised with a trailing `/` so prefix matching
/// (`path.starts_with(dir)`) is exact; glob characters are rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IgnoreRule {
    pub id: IgnoreRuleId,
    pub bucket_id: BucketId,
    pub directory: String,
    pub enabled: bool,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl IgnoreRule {
    /// Normalise a user-supplied directory: strip leading `/`, ensure the
    /// trailing `/` that prefix matching relies on, reject globs and empty
    /// input.
    ///
    /// # Errors
    /// Returns [`Error::invalid_input`] for glob characters (`*`, `?`, `[`)
    /// and for input that normalises to empty.
    pub fn normalise_directory(input: &str) -> Result<String> {
        if input.contains('*') || input.contains('?') || input.contains('[') {
            return Err(Error::invalid_input(
                "ignore rules do not support glob patterns; use an exact directory (IGN-015)",
            ));
        }
        let trimmed = input.trim().trim_matches('/');
        if trimmed.is_empty() {
            return Err(Error::invalid_input("ignore directory is empty"));
        }
        Ok(format!("{trimmed}/"))
    }

    /// Does `path` fall inside this ignored subtree? Exact prefix pruning:
    /// the stored directory always ends in `/`, so sibling names sharing a
    /// stem never match.
    pub fn matches(&self, path: &str) -> bool {
        path.starts_with(&self.directory)
    }
}

/// A workspace-scoped tag definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub id: TagId,
    pub workspace_id: WorkspaceId,
    pub name: String,
    /// Case-folded name; uniqueness is enforced on this column.
    pub normalized_name: String,
    pub color: String,
    pub description: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl Tag {
    /// Create a tag with the default colour `"gray"` (free-form otherwise;
    /// no palette validation lives in the domain) and its case-folded
    /// uniqueness key.
    pub fn new(
        id: TagId,
        workspace_id: WorkspaceId,
        name: impl Into<String>,
        at: OffsetDateTime,
    ) -> Self {
        let name = name.into();
        Self {
            id,
            workspace_id,
            normalized_name: normalize_tag_name(&name),
            name,
            color: "gray".to_string(),
            description: None,
            created_at: at,
        }
    }
}

/// Case-fold a user-supplied tag name into its uniqueness key: trim
/// surrounding whitespace first, then lowercase. Two names whose keys are
/// equal are the same tag within a workspace.
pub fn normalize_tag_name(name: &str) -> String {
    name.trim().to_lowercase()
}

/// One scan/reconcile run over a bucket's index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRun {
    pub id: IndexRunId,
    pub bucket_id: BucketId,
    pub state: IndexRunState,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,
    pub objects_seen: u64,
    pub error_count: u64,
}

/// Index run lifecycle. Closed vocabulary stored as text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexRunState {
    Running,
    Paused,
    Cancelled,
    Succeeded,
    Failed,
}

impl IndexRunState {
    /// Canonical token stored in the `state` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Cancelled => "cancelled",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    /// Decode the canonical token.
    ///
    /// # Errors
    /// Returns [`Error::database`] for an unknown token.
    pub fn parse(token: &str) -> Result<Self> {
        Ok(match token {
            "running" => Self::Running,
            "paused" => Self::Paused,
            "cancelled" => Self::Cancelled,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            other => {
                return Err(Error::database("decode index run state").with_context("value", other))
            }
        })
    }

    /// Apply a lifecycle transition, rejecting state jumps that would make a
    /// persisted index run lie about whether scanning completed.
    ///
    /// # Errors
    /// Returns [`Error::conflict`] for any edge outside
    /// `Running → {Paused, Cancelled, Succeeded, Failed}`,
    /// `Paused → {Running, Cancelled}`, and self-transitions.
    pub fn transition(self, target: Self) -> Result<Self> {
        use IndexRunState::{Cancelled, Failed, Paused, Running, Succeeded};
        let allowed = matches!(
            (self, target),
            (Running, Paused | Cancelled | Succeeded | Failed) | (Paused, Running | Cancelled)
        ) || self == target;
        if allowed {
            Ok(target)
        } else {
            Err(Error::conflict(format!(
                "illegal index run transition: {self:?} -> {target:?}"
            )))
        }
    }
}

/// Catalog-side object freshness: when the index last confirmed the object
/// against the remote, and whether it has since gone stale or been deleted
/// (tombstoned).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectState {
    #[default]
    Confirmed,
    /// A refresh marked it missing remotely but inside the retention window:
    /// still listed, flagged stale.
    Stale,
    Deleted,
}

impl ObjectState {
    /// Canonical token stored in the `state` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Stale => "stale",
            Self::Deleted => "deleted",
        }
    }

    /// Decode the canonical token.
    ///
    /// # Errors
    /// Returns [`Error::database`] for an unknown token.
    pub fn parse(token: &str) -> Result<Self> {
        Ok(match token {
            "confirmed" => Self::Confirmed,
            "stale" => Self::Stale,
            "deleted" => Self::Deleted,
            other => {
                return Err(Error::database("decode object state").with_context("value", other))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignore_directory_normalises() {
        assert_eq!(
            IgnoreRule::normalise_directory("/photos/raw/").unwrap(),
            "photos/raw/"
        );
        assert_eq!(
            IgnoreRule::normalise_directory("backup").unwrap(),
            "backup/"
        );
    }

    #[test]
    fn ignore_directory_rejects_globs_and_empty() {
        assert!(IgnoreRule::normalise_directory("*.tmp").is_err());
        assert!(IgnoreRule::normalise_directory("/").is_err());
    }

    #[test]
    fn ignore_rule_prefix_matches_subtree_only() {
        let rule = IgnoreRule {
            id: IgnoreRuleId(1),
            bucket_id: BucketId::new_random(),
            directory: "photos/raw/".into(),
            enabled: true,
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        assert!(rule.matches("photos/raw/a.nef"));
        assert!(rule.matches("photos/raw/sub/b.nef"));
        assert!(!rule.matches("photos/raw.nef"));
        assert!(!rule.matches("photos/keep/c.png"));
    }

    #[test]
    fn tag_names_fold_case() {
        assert_eq!(normalize_tag_name("  Urgent "), "urgent");
    }

    #[test]
    fn index_run_transitions_are_guarded() {
        assert_eq!(
            IndexRunState::Running
                .transition(IndexRunState::Paused)
                .unwrap(),
            IndexRunState::Paused
        );
        assert_eq!(
            IndexRunState::Running
                .transition(IndexRunState::Succeeded)
                .unwrap(),
            IndexRunState::Succeeded
        );
        assert!(IndexRunState::Paused
            .transition(IndexRunState::Succeeded)
            .is_err());
        assert!(IndexRunState::Succeeded
            .transition(IndexRunState::Running)
            .is_err());
    }
}
