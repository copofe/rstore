//! Job inspection and control: pause, resume, retry, cancel, and startup
//! recovery of orphaned runs.
//!
//! Foreground job execution and persisted state transitions for the runtime.
//! Cooperative cancellation is shared by CLI and future desktop callers.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::JobRepository;
use rstore_domain::job::Job;
use rstore_domain::job::JobState;
use rstore_domain::{Clock, Error, JobId, Result};

/// A reference to a job: `job_…` id or its ordinal position in the list
/// (`1`, `2`, … newest-first) for convenient typing.
#[derive(Debug, Clone)]
pub enum JobRef {
    Id(JobId),
    Ordinal(u32),
}

impl JobRef {
    pub fn parse(s: &str) -> Result<Self> {
        if s.starts_with("job_") {
            return s
                .parse()
                .map(JobRef::Id)
                .map_err(|e| Error::invalid_input(format!("invalid job id `{s}`")).with_source(e));
        }
        s.parse::<u32>().map(JobRef::Ordinal).map_err(|e| {
            Error::invalid_input(format!("invalid job reference `{s}`")).with_source(e)
        })
    }
}

/// Job control operations behind `rstore task …`.
#[async_trait]
pub trait JobService: Send + Sync {
    /// All jobs, newest first.
    async fn list(&self) -> Result<Vec<Job>>;
    /// Resolve a reference (`job_…` id or 1-based ordinal, newest first).
    async fn resolve(&self, reference: &JobRef) -> Result<Job>;
    /// Cancel a job in any non-terminal state.
    async fn cancel(&self, reference: &JobRef) -> Result<Job>;
    /// At startup, mark jobs orphaned by a dead process as `interrupted`.
    /// Called once from [`crate::Application::initialize`].
    async fn mark_stale_interrupted(&self) -> Result<u64>;
}

/// The default implementation over the [`JobRepository`] seam (SQLite in
/// production; an in-memory store in tests).
pub struct DefaultJobService {
    repo: Arc<dyn JobRepository>,
    clock: Arc<dyn Clock>,
}

impl DefaultJobService {
    pub fn new(repo: Arc<dyn JobRepository>, clock: Arc<dyn Clock>) -> Self {
        Self { repo, clock }
    }
}

fn not_found(reference: &JobRef) -> Error {
    match reference {
        JobRef::Id(id) => Error::not_found(format!("job {id} not found")),
        JobRef::Ordinal(n) => Error::not_found(format!("job #{n} not found")),
    }
}

#[async_trait]
impl JobService for DefaultJobService {
    async fn list(&self) -> Result<Vec<Job>> {
        self.repo.list().await
    }

    async fn resolve(&self, reference: &JobRef) -> Result<Job> {
        match reference {
            JobRef::Id(id) => self
                .repo
                .get(*id)
                .await?
                .ok_or_else(|| not_found(reference)),
            JobRef::Ordinal(n) => {
                let all = self.repo.list().await?;
                all.into_iter()
                    .nth((*n as usize).saturating_sub(1))
                    .ok_or_else(|| not_found(reference))
            }
        }
    }

    async fn cancel(&self, reference: &JobRef) -> Result<Job> {
        let job = self.resolve(reference).await?;
        if job.state.is_terminal() {
            return Err(Error::conflict(format!(
                "job already finished as `{}`",
                job.state.as_str()
            )));
        }
        self.repo
            .transition(job.id, JobState::Cancelled, self.clock.now())
            .await
    }
    async fn mark_stale_interrupted(&self) -> Result<u64> {
        self.repo
            .mark_stale_jobs_interrupted(self.clock.now())
            .await
    }
}
