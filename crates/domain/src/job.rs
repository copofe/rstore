//! Job / transfer-task domain model (JOB-001..003, CORE-006).
//!
//! A [`Job`] is created the moment a long operation starts and is immediately
//! persisted (JOB-001). [`JobState`] is a guarded state machine: illegal
//! transitions are rejected (JOB-003).

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{Error, ErrorCode};
use crate::id::JobId;

/// What a job does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Upload,
    Download,
    Copy,
    Move,
    Delete,
    IndexScan,
    Verify,
}

impl JobKind {
    /// Canonical token stored in the `kind` column.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Download => "download",
            Self::Copy => "copy",
            Self::Move => "move",
            Self::Delete => "delete",
            Self::IndexScan => "index_scan",
            Self::Verify => "verify",
        }
    }

    pub fn parse(token: &str) -> Result<Self, Error> {
        match token {
            "upload" => Ok(Self::Upload),
            "download" => Ok(Self::Download),
            "copy" => Ok(Self::Copy),
            "move" => Ok(Self::Move),
            "delete" => Ok(Self::Delete),
            "index_scan" => Ok(Self::IndexScan),
            "verify" => Ok(Self::Verify),
            other => Err(Error::database(format!("unknown job kind `{other}`"))),
        }
    }
}

/// The lifecycle of a job (JOB-003 / PRD §10.11.2).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Created but not yet executing (JOB-004).
    #[default]
    Pending,
    /// Actively executing (JOB-005).
    Running,
    /// User-paused, resumable (JOB-006).
    Paused,
    /// Backing off before another attempt (JOB-009).
    Retrying,
    /// Finished with some items failing (JOB-011).
    PartialSuccess,
    /// All items succeeded.
    Succeeded,
    /// Unrecoverable failure.
    Failed,
    /// Cancelled by the user (JOB-008).
    Cancelled,
    /// Process died mid-run; resumable on next start (JOB-015).
    Interrupted,
}

impl JobState {
    /// Whether the state is terminal (no further transitions).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::PartialSuccess
        )
    }

    /// Whether the state is actively consuming resources.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Running | Self::Retrying)
    }

    /// Canonical token stored in the `state` column.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Retrying => "retrying",
            Self::PartialSuccess => "partial_success",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn parse(token: &str) -> Result<Self, Error> {
        match token {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "retrying" => Ok(Self::Retrying),
            "partial_success" => Ok(Self::PartialSuccess),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(Error::database(format!("unknown job state `{other}`"))),
        }
    }

    /// Apply a target state, rejecting illegal transitions (JOB-003).
    pub fn transition(self, target: JobState) -> Result<JobState, Error> {
        use JobState::*;
        let allowed = match (self, target) {
            // Bootstrap and run-control transitions.
            (Pending, Running) => true,
            (Pending, Cancelled) => true,
            // Startup recovery owns the gap between persistence and the first
            // Running transition too: a process can die immediately after
            // creating the pending row.
            (Pending, Interrupted) => true,
            (Running, Paused) => true,
            (Running, Retrying) => true,
            (Running, Succeeded) => true,
            (Running, Failed) => true,
            (Running, PartialSuccess) => true,
            (Running, Cancelled) => true,
            (Running, Interrupted) => true,
            (Paused, Running) => true,
            (Paused, Cancelled) => true,
            (Retrying, Running) => true,
            (Retrying, Failed) => true,
            (Retrying, Cancelled) => true,
            (Retrying, Interrupted) => true,
            (Interrupted, Running) => true,
            (Interrupted, Cancelled) => true,
            // JOB-010: `task retry` re-runs a finished-but-unsuccessful job.
            (Failed, Running) => true,
            (PartialSuccess, Running) => true,
            (s, t) if s == t => true,
            _ => false,
        };
        if allowed {
            Ok(target)
        } else {
            Err(Error::new(
                ErrorCode::Conflict,
                format!("illegal job transition: {self:?} -> {target:?}"),
            ))
        }
    }
}

/// Per-file progress / outcome record inside a job (JOB-002).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Cancelled,
}

impl ItemState {
    /// Canonical token stored in the item `state` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(token: &str) -> Result<Self, Error> {
        match token {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "skipped" => Ok(Self::Skipped),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(Error::database(format!("unknown item state `{other}`"))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobItem {
    pub remote_uri: String,
    pub state: ItemState,
    pub bytes_total: u64,
    pub bytes_done: u64,
    pub error_code: Option<String>,
}

/// A transfer / index job record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub kind: JobKind,
    pub state: JobState,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    pub items: Vec<JobItem>,
    pub error: Option<String>,
}

impl Job {
    pub fn new(kind: JobKind, id: JobId, now: OffsetDateTime) -> Self {
        Self {
            id,
            kind,
            state: JobState::Pending,
            created_at: now,
            updated_at: now,
            items: Vec::new(),
            error: None,
        }
    }

    /// Move to `target`, updating `updated_at` (JOB-003).
    pub fn transition(&mut self, target: JobState, now: OffsetDateTime) -> Result<(), Error> {
        self.state = self.state.transition(target)?;
        self.updated_at = now;
        Ok(())
    }

    /// Items that count as finished for progress display: succeeded and
    /// skipped both settle an item (the same definition transfer reports
    /// use). The task list renders this instead of re-deriving it.
    pub fn items_done(&self) -> usize {
        self.items
            .iter()
            .filter(|item| matches!(item.state, ItemState::Succeeded | ItemState::Skipped))
            .count()
    }
}

/// Persists jobs and their items (JOB-001/002/015). Implemented over SQLite
/// by `rstore-persistence`; consumers hold `Arc<dyn …>` so job services stay
/// testable without a database. The state machine and its persistence
/// contract belong together.
#[async_trait::async_trait]
pub trait JobRepository: Send + Sync {
    /// Insert a new job (created ⇒ persisted immediately).
    async fn create(&self, job: &Job) -> crate::Result<()>;
    async fn get(&self, id: JobId) -> crate::Result<Option<Job>>;
    /// Apply a state transition and persist (JOB-003).
    async fn transition(
        &self,
        id: JobId,
        target: JobState,
        at: OffsetDateTime,
    ) -> crate::Result<Job>;
    /// Atomically replace terminal items and apply a validated terminal state
    /// transition. A report must never outrun this local commit.
    async fn commit_terminal(
        &self,
        id: JobId,
        items: &[JobItem],
        target: JobState,
        at: OffsetDateTime,
    ) -> crate::Result<Job>;
    async fn list(&self) -> crate::Result<Vec<Job>>;
    /// Replace a job's item rows wholesale (checkpoints / interruptions).
    async fn replace_items(&self, id: JobId, items: &[JobItem]) -> crate::Result<()>;
    async fn delete(&self, id: JobId) -> crate::Result<bool>;
    /// JOB-015: jobs orphaned by a dead process (pending/running/retrying)
    /// transition to `Interrupted`; returns how many. Called once at startup
    /// (`Application::open`) — there is no daemon, no job survives its owner.
    async fn mark_stale_jobs_interrupted(&self, at: OffsetDateTime) -> crate::Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::JobId;

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    #[test]
    fn legal_transitions() {
        let mut job = Job::new(JobKind::Upload, JobId::new(), now());
        job.transition(JobState::Running, now()).unwrap();
        job.transition(JobState::Paused, now()).unwrap();
        job.transition(JobState::Running, now()).unwrap();
        job.transition(JobState::Succeeded, now()).unwrap();
        assert!(job.state.is_terminal());
    }

    #[test]
    fn illegal_transition_rejected() {
        let mut job = Job::new(JobKind::Delete, JobId::new(), now());
        // Cannot pause a pending job that has not started.
        assert!(job.transition(JobState::Paused, now()).is_err());
        // Cannot skip Running and jump straight to a terminal state.
        assert!(job.transition(JobState::Succeeded, now()).is_err());
        // Legal path into a terminal state.
        job.transition(JobState::Running, now()).unwrap();
        job.transition(JobState::Succeeded, now()).unwrap();
        assert!(job.state.is_terminal());
        // Cannot resurrect a terminal job.
        assert!(job.transition(JobState::Running, now()).is_err());
    }

    #[test]
    fn interrupted_is_resumable() {
        let mut job = Job::new(JobKind::Upload, JobId::new(), now());
        job.transition(JobState::Running, now()).unwrap();
        job.transition(JobState::Interrupted, now()).unwrap();
        job.transition(JobState::Running, now()).unwrap();
    }

    #[test]
    fn every_non_terminal_startup_state_can_be_interrupted() {
        assert_eq!(
            JobState::Pending.transition(JobState::Interrupted).unwrap(),
            JobState::Interrupted
        );
        assert_eq!(
            JobState::Retrying
                .transition(JobState::Interrupted)
                .unwrap(),
            JobState::Interrupted
        );
    }

    #[test]
    fn items_done_counts_only_succeeded_and_skipped_items() {
        let mut job = Job::new(JobKind::Upload, JobId::new(), now());
        job.items = [
            ItemState::Pending,
            ItemState::Running,
            ItemState::Succeeded,
            ItemState::Failed,
            ItemState::Skipped,
            ItemState::Cancelled,
        ]
        .into_iter()
        .map(|state| JobItem {
            remote_uri: String::new(),
            state,
            bytes_total: 0,
            bytes_done: 0,
            error_code: None,
        })
        .collect();

        assert_eq!(job.items_done(), 2);
    }
}
