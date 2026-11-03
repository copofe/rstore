//! Job inspection and control: list, show, cancel, and startup recovery of
//! orphaned runs.
//!
//! Cancel reaches a job wherever it runs: a job of this process stops at its
//! next checkpoint, a job another process owns gets a cancel request its
//! heartbeat picks up, and a job nobody owns any more is cancelled outright.

use std::sync::Arc;

use rstore_domain::job::Job;
use rstore_domain::job::JobState;
use rstore_domain::{Error, JobId, Result};
use serde::Serialize;

use crate::job_run::JobEnv;

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

/// What a cancel did.
#[derive(Debug, Clone, Serialize)]
pub struct CancelReport {
    pub job: Job,
    /// `true` when the job is still running and will stop at its next
    /// checkpoint; `false` when it was cancelled on the spot.
    pub requested: bool,
}

/// Job control operations behind `rstore task …`.
pub struct JobService {
    repo: Arc<crate::db::JobRepository>,
    env: JobEnv,
}

impl JobService {
    pub(crate) fn new(env: JobEnv) -> Self {
        Self {
            repo: env.jobs.clone(),
            env,
        }
    }
}

fn not_found(reference: &JobRef) -> Error {
    match reference {
        JobRef::Id(id) => Error::not_found(format!("job {id} not found")),
        JobRef::Ordinal(n) => Error::not_found(format!("job #{n} not found")),
    }
}

impl JobService {
    /// All jobs, newest first.
    pub async fn list(&self) -> Result<Vec<Job>> {
        self.repo.list().await
    }

    /// Resolve a reference (`job_…` id or 1-based ordinal, newest first).
    pub async fn resolve(&self, reference: &JobRef) -> Result<Job> {
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

    /// Cancel a job in any non-terminal state, wherever it runs.
    pub async fn cancel(&self, reference: &JobRef) -> Result<CancelReport> {
        let job = self.resolve(reference).await?;
        if job.state.is_terminal() {
            return Err(Error::conflict(format!(
                "job already finished as `{}`",
                job.state.as_str()
            )));
        }
        if self.env.live.cancel(job.id) {
            return Ok(CancelReport {
                job,
                requested: true,
            });
        }
        let now = self.env.clock.now();
        let leased = self
            .repo
            .lease_expires_at(job.id)
            .await?
            .is_some_and(|until| until >= now);
        if leased && job.state == JobState::Running {
            self.repo.request_cancel(job.id).await?;
            return Ok(CancelReport {
                job,
                requested: true,
            });
        }
        let job = self
            .repo
            .transition(job.id, JobState::Cancelled, now)
            .await?;
        Ok(CancelReport {
            job,
            requested: false,
        })
    }

    /// At startup, mark jobs whose owner stopped renewing their lease as
    /// `interrupted`. Called once from [`crate::Application::initialize`].
    pub async fn mark_stale_interrupted(&self) -> Result<u64> {
        self.repo
            .mark_stale_jobs_interrupted(self.env.clock.now())
            .await
    }
}
