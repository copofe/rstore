//! Job / transfer-task domain model.
//!
//! A [`Job`] is created the moment a long operation starts and is immediately
//! persisted. [`JobState`] is a guarded state machine: illegal transitions
//! are rejected.

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

    /// Decode the canonical token.
    ///
    /// # Errors
    /// Returns [`Error::database`] for an unknown token: a vocabulary miss in
    /// the `kind` column is internal corruption, not user input.
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

/// The lifecycle of a job.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Created but not yet executing.
    #[default]
    Pending,
    /// Actively executing.
    Running,
    /// User-paused, resumable.
    Paused,
    /// Backing off before another attempt.
    Retrying,
    /// Finished with some items failing.
    PartialSuccess,
    /// All items succeeded.
    Succeeded,
    /// Unrecoverable failure.
    Failed,
    /// Cancelled by the user.
    Cancelled,
    /// Process died mid-run; resumable on next start.
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

    /// Whether the run ended via Ctrl+C — the exit-130 trigger.
    pub fn is_interrupted(self) -> bool {
        self == Self::Interrupted
    }

    /// Decode the canonical token.
    ///
    /// # Errors
    /// Returns [`Error::database`] for an unknown token: a vocabulary miss in
    /// the `state` column is internal corruption, not user input.
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

    /// Apply a target state, rejecting illegal transitions.
    ///
    /// # Errors
    /// Returns [`Error::conflict`] when `(self, target)` is not an allowed
    /// edge of the transition table; terminal states can only be re-entered
    /// via retry into `Running`.
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
            // `task retry` re-runs a finished-but-unsuccessful job.
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

/// Per-file progress / outcome record inside a job.
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

    /// Decode the canonical token.
    ///
    /// # Errors
    /// Returns [`Error::database`] for an unknown token.
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

/// One transferred object's progress and outcome within a [`Job`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobItem {
    /// Canonical narration label of the transferred object
    /// (`conn/bucket/key` or the local path spelling a dry run reports).
    pub label: String,
    pub state: ItemState,
    pub bytes_total: u64,
    pub bytes_done: u64,
    /// Truncated failure detail of a failed item (`job_run` caps length);
    /// the stable exit-code class lives on the report, not per item.
    pub error: Option<String>,
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
    /// Create a pending job stamped with the given instant.
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

    /// Move to `target`, updating `updated_at`.
    ///
    /// # Errors
    /// Returns [`Error::conflict`] when the underlying state machine rejects
    /// the transition; the job state is left unchanged in that case.
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

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
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
        let mut job = Job::new(JobKind::Upload, JobId::new_random(), now());
        job.transition(JobState::Running, now()).unwrap();
        job.transition(JobState::Paused, now()).unwrap();
        job.transition(JobState::Running, now()).unwrap();
        job.transition(JobState::Succeeded, now()).unwrap();
        assert!(job.state.is_terminal());
    }

    #[test]
    fn illegal_transition_rejected() {
        let mut job = Job::new(JobKind::Delete, JobId::new_random(), now());
        assert!(job.transition(JobState::Paused, now()).is_err());
        assert!(job.transition(JobState::Succeeded, now()).is_err());
        job.transition(JobState::Running, now()).unwrap();
        job.transition(JobState::Succeeded, now()).unwrap();
        assert!(job.state.is_terminal());
        assert!(job.transition(JobState::Running, now()).is_err());
    }

    #[test]
    fn interrupted_is_resumable() {
        let mut job = Job::new(JobKind::Upload, JobId::new_random(), now());
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
        let mut job = Job::new(JobKind::Upload, JobId::new_random(), now());
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
            label: String::new(),
            state,
            bytes_total: 0,
            bytes_done: 0,
            error: None,
        })
        .collect();

        assert_eq!(job.items_done(), 2);
    }
}
