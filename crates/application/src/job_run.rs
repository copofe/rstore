//! The shared job-run seam for batch remote operations.
//!
//! Every application module that persists a run as a [`Job`] and reports per-item
//! outcomes (copy/move, upload/download) drives it through [`run_items`]:
//! one item identity, one create-job path, one outcome→`ItemState`/counts mapping with the
//! 200-char error truncation, one cancellation/interrupt path, and one
//! terminal-state rule — `failed` and `blocked` both count toward failure
//! (`failed>0 || blocked>0` → `PartialSuccess` when anything succeeded,
//! else `Failed`). The rule lives only here so new batch commands cannot
//! fork it. Callers provide one [`RunItem`] per operation; its persisted and
//! reported identity cannot drift from the work item it describes.

use std::future::Future;

use rstore_domain::job::{ItemState, Job, JobItem, JobKind, JobState};
use rstore_domain::JobRepository;
use rstore_domain::{Clock, JobId, Result};
use rstore_transfer::{CancellationToken, Progress, ProgressSink};

use crate::transfer_report::{Outcome, TransferReport, TransferReportItem};

/// Cap on the per-item `error_code` persisted into the jobs table; long
/// adapter errors are truncated, not stored whole.
const ERROR_CODE_CAP: usize = 200;

/// One operation and the identity Job-run persists and reports for it.
/// Keeping these values together keeps persisted identity and work-item
/// ordering in one value.
pub struct RunItem<T> {
    job_label: String,
    source: String,
    target: String,
    work: T,
}

impl<T> RunItem<T> {
    pub fn new(
        job_label: impl Into<String>,
        source: impl Into<String>,
        target: impl Into<String>,
        work: T,
    ) -> Self {
        Self {
            job_label: job_label.into(),
            source: source.into(),
            target: target.into(),
            work,
        }
    }
}

/// What one item did when the callback ran it.
pub enum Step {
    /// The item finished with this outcome and transferred byte count
    /// (0 where not counted). Its identity stays on [`RunItem`].
    Done {
        outcome: Outcome,
        detail: Option<String>,
        bytes: u64,
    },
    /// The item was abandoned mid-flight (e.g. the engine saw `Cancelled`);
    /// it stays pending and the run persists as interrupted.
    Interrupted,
}

impl Step {
    /// A completed step that does not count bytes.
    pub fn done(outcome: Outcome, detail: Option<String>) -> Self {
        Self::Done {
            outcome,
            detail,
            bytes: 0,
        }
    }

    /// A failed item — the one Err→Failed translation, so the failure detail
    /// format cannot fork per caller.
    pub fn failed(e: &rstore_domain::Error) -> Self {
        Self::done(Outcome::Failed, Some(e.to_string()))
    }

    /// A failed item narrated by a message rather than an error value — the
    /// per-item failure arms that compose their own detail.
    pub fn failed_msg(detail: impl Into<String>) -> Self {
        Self::done(Outcome::Failed, Some(detail.into()))
    }

    /// A dry-run preview (MOVE-012 / DEL-003): the run performed no work;
    /// `detail` is the "would \<verb\> \<target\>" narration. The one
    /// spelling of the preview policy, so the batch families cannot drift —
    /// and a family without a narration is visible at the call site.
    pub fn preview(detail: impl Into<String>) -> Self {
        Self::done(Outcome::Succeeded, Some(detail.into()))
    }
}

/// Create the job, pre-populate its items as `Pending`, and transition it to
/// `Running`. The persisted rows are derived from the same [`RunItem`] values
/// the run later executes.
async fn create_job<T>(
    jobs: &dyn JobRepository,
    clock: &dyn Clock,
    kind: JobKind,
    items: &[RunItem<T>],
) -> Result<Job> {
    let now = clock.now();
    let mut job = Job::new(kind, JobId::new(), now);
    job.items = items
        .iter()
        .map(|item| JobItem {
            remote_uri: item.job_label.clone(),
            state: ItemState::Pending,
            bytes_total: 0,
            bytes_done: 0,
            error_code: None,
        })
        .collect();
    jobs.create(&job).await?;
    jobs.transition(job.id, JobState::Running, now).await?;
    Ok(job)
}

/// Run `items` one by one against `per_item`, bookkeeping the job items and
/// the [`TransferReport`] as it goes, then persist the terminal state.
///
/// Cancellation is checked before every item; a callback that abandoned its
/// item mid-flight returns [`Step::Interrupted`]. Both enter the one
/// interrupt path (JOB-014/015): remaining pending items become `cancelled`
/// and the job persists as `interrupted`.
#[allow(clippy::too_many_arguments)]
pub async fn run_items<T, F, Fut>(
    jobs: &dyn JobRepository,
    clock: &dyn Clock,
    kind: JobKind,
    items: Vec<RunItem<T>>,
    dry_run: bool,
    progress: &dyn ProgressSink,
    cancel: &CancellationToken,
    mut per_item: F,
) -> Result<TransferReport>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = Step>,
{
    let mut job = create_job(jobs, clock, kind, &items).await?;
    let mut report = TransferReport {
        job_id: job.id,
        kind: kind.as_str(),
        items: Vec::new(),
        succeeded: 0,
        skipped: 0,
        failed: 0,
        blocked: 0,
        state: "running",
        dry_run,
    };

    let total = job.items.len() as u64;

    for (index, item) in items.into_iter().enumerate() {
        if cancel.is_cancelled() || cancel.check().is_err() {
            // Persist the checkpoint state; the caller reports exit 130.
            return interrupt(jobs, clock, job, index, report).await;
        }

        let RunItem {
            source,
            target,
            work,
            ..
        } = item;
        let Step::Done {
            outcome,
            detail,
            bytes,
        } = per_item(work).await
        else {
            return interrupt(jobs, clock, job, index, report).await;
        };

        match outcome {
            Outcome::Succeeded => {
                report.succeeded += 1;
                job.items[index].state = ItemState::Succeeded;
            }
            Outcome::Skipped => {
                report.skipped += 1;
                job.items[index].state = ItemState::Skipped;
            }
            Outcome::Failed => {
                report.failed += 1;
                job.items[index].state = ItemState::Failed;
            }
            Outcome::Blocked => {
                report.blocked += 1;
                job.items[index].state = ItemState::Failed;
            }
        }
        if let Some(d) = &detail {
            job.items[index].error_code = Some(d.chars().take(ERROR_CODE_CAP).collect());
        }
        job.items[index].bytes_done = bytes;
        job.items[index].bytes_total = bytes;
        report.items.push(TransferReportItem {
            source,
            target,
            kind: kind.as_str(),
            outcome,
            detail,
        });
        let settled = (report.succeeded + report.skipped + report.failed + report.blocked) as u64;
        progress.report(Progress::Items {
            done: settled,
            total,
        });
    }

    // Terminal state (JOB-011 semantics): `failed` and `blocked` both count
    // toward failure; anything succeeded alongside → PartialSuccess.
    let final_state = if report.failed > 0 || report.blocked > 0 {
        if report.succeeded > 0 {
            JobState::PartialSuccess
        } else {
            JobState::Failed
        }
    } else {
        JobState::Succeeded
    };
    let committed = jobs
        .commit_terminal(job.id, &job.items, final_state, clock.now())
        .await?;
    report.state = committed.state.as_str();
    Ok(report)
}

/// Persist an interrupted job: remaining pending items become `cancelled`,
/// the job transitions to `interrupted` (JOB-014/015).
async fn interrupt(
    jobs: &dyn JobRepository,
    clock: &dyn Clock,
    mut job: Job,
    index: usize,
    mut report: TransferReport,
) -> Result<TransferReport> {
    for pending in job.items.iter_mut().skip(index) {
        if pending.state == ItemState::Pending {
            pending.state = ItemState::Cancelled;
        }
    }
    let committed = jobs
        .commit_terminal(job.id, &job.items, JobState::Interrupted, clock.now())
        .await?;
    report.state = committed.state.as_str();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use rstore_domain::{Error, JobId};
    use rstore_test_support::TestClock;
    use rstore_transfer::NullProgressSink;

    use super::*;

    #[derive(Default)]
    struct MemoryJobs {
        jobs: Mutex<HashMap<JobId, Job>>,
        fail_terminal: Mutex<bool>,
    }

    impl MemoryJobs {
        fn fail_terminal_commits(&self) {
            *self.fail_terminal.lock().expect("jobs poisoned") = true;
        }
    }

    #[async_trait]
    impl JobRepository for MemoryJobs {
        async fn create(&self, job: &Job) -> Result<()> {
            self.jobs
                .lock()
                .expect("jobs poisoned")
                .insert(job.id, job.clone());
            Ok(())
        }

        async fn get(&self, id: JobId) -> Result<Option<Job>> {
            Ok(self.jobs.lock().expect("jobs poisoned").get(&id).cloned())
        }

        async fn transition(
            &self,
            id: JobId,
            target: JobState,
            at: time::OffsetDateTime,
        ) -> Result<Job> {
            let mut jobs = self.jobs.lock().expect("jobs poisoned");
            let job = jobs
                .get_mut(&id)
                .ok_or_else(|| Error::not_found(format!("job {id} not found")))?;
            job.state = job.state.transition(target)?;
            job.updated_at = at;
            Ok(job.clone())
        }

        async fn commit_terminal(
            &self,
            id: JobId,
            items: &[JobItem],
            target: JobState,
            at: time::OffsetDateTime,
        ) -> Result<Job> {
            if *self.fail_terminal.lock().expect("jobs poisoned") {
                return Err(Error::database("injected terminal commit failure"));
            }
            if !target.is_terminal() && target != JobState::Interrupted {
                return Err(Error::invalid_input(
                    "settled commit needs a terminal or interrupted state",
                ));
            }
            let mut jobs = self.jobs.lock().expect("jobs poisoned");
            let job = jobs
                .get_mut(&id)
                .ok_or_else(|| Error::not_found(format!("job {id} not found")))?;
            job.state = job.state.transition(target)?;
            job.updated_at = at;
            job.items = items.to_vec();
            Ok(job.clone())
        }

        async fn list(&self) -> Result<Vec<Job>> {
            Ok(self
                .jobs
                .lock()
                .expect("jobs poisoned")
                .values()
                .cloned()
                .collect())
        }

        async fn replace_items(&self, id: JobId, items: &[JobItem]) -> Result<()> {
            let mut jobs = self.jobs.lock().expect("jobs poisoned");
            let job = jobs
                .get_mut(&id)
                .ok_or_else(|| Error::not_found(format!("job {id} not found")))?;
            job.items = items.to_vec();
            Ok(())
        }

        async fn delete(&self, id: JobId) -> Result<bool> {
            Ok(self
                .jobs
                .lock()
                .expect("jobs poisoned")
                .remove(&id)
                .is_some())
        }

        async fn mark_stale_jobs_interrupted(&self, at: time::OffsetDateTime) -> Result<u64> {
            let mut jobs = self.jobs.lock().expect("jobs poisoned");
            let mut stale = 0u64;
            for job in jobs.values_mut() {
                if job.state.is_active() {
                    job.state = job.state.transition(JobState::Interrupted)?;
                    job.updated_at = at;
                    stale += 1;
                }
            }
            Ok(stale)
        }
    }

    #[tokio::test]
    async fn one_item_identity_drives_persistence_and_report() {
        let jobs = MemoryJobs::default();
        let report = run_items(
            &jobs,
            &TestClock::default(),
            JobKind::Upload,
            vec![RunItem::new("persisted", "source", "target", 7_u64)],
            false,
            &NullProgressSink,
            &CancellationToken::new(),
            |bytes| async move {
                Step::Done {
                    outcome: Outcome::Succeeded,
                    detail: None,
                    bytes,
                }
            },
        )
        .await
        .unwrap();

        let job = jobs.get(report.job_id).await.unwrap().unwrap();
        assert_eq!(job.items.len(), 1);
        assert_eq!(job.items[0].remote_uri, "persisted");
        assert_eq!(job.items[0].state, ItemState::Succeeded);
        assert_eq!(job.items[0].bytes_done, 7);
        assert_eq!(report.items.len(), 1);
        assert_eq!(report.items[0].source, "source");
        assert_eq!(report.items[0].target, "target");
        assert_eq!(report.state, "succeeded");
    }

    #[tokio::test]
    async fn mixed_outcomes_share_one_terminal_state_rule() {
        let jobs = MemoryJobs::default();
        let report = run_items(
            &jobs,
            &TestClock::default(),
            JobKind::Copy,
            vec![
                RunItem::new("a", "a", "a-copy", Outcome::Succeeded),
                RunItem::new("b", "b", "b-copy", Outcome::Blocked),
            ],
            false,
            &NullProgressSink,
            &CancellationToken::new(),
            |outcome| async move {
                Step::Done {
                    outcome,
                    detail: (outcome == Outcome::Blocked).then(|| "x".repeat(250)),
                    bytes: 0,
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(report.succeeded, 1);
        assert_eq!(report.blocked, 1);
        assert_eq!(report.state, "partial_success");
        let job = jobs.get(report.job_id).await.unwrap().unwrap();
        assert_eq!(job.state, JobState::PartialSuccess);
        assert_eq!(job.items[0].state, ItemState::Succeeded);
        assert_eq!(job.items[1].state, ItemState::Failed);
        assert_eq!(job.items[1].error_code.as_deref().unwrap().len(), 200);
    }

    #[tokio::test]
    async fn terminal_commit_failure_is_returned_and_does_not_advance_job() {
        let jobs = MemoryJobs::default();
        jobs.fail_terminal_commits();
        let error = run_items(
            &jobs,
            &TestClock::default(),
            JobKind::Upload,
            vec![RunItem::new("a", "a", "a", ())],
            false,
            &NullProgressSink,
            &CancellationToken::new(),
            |_| async { Step::done(Outcome::Succeeded, None) },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, rstore_domain::ErrorCode::Database);
        let job = jobs.list().await.unwrap().pop().expect("created job");
        assert_eq!(job.state, JobState::Running);
        assert_eq!(job.items[0].state, ItemState::Pending);
    }

    #[tokio::test]
    async fn cancellation_interrupts_remaining_items() {
        let jobs = MemoryJobs::default();
        let cancel = CancellationToken::new();
        let cancel_from_work = cancel.clone();
        let report = run_items(
            &jobs,
            &TestClock::default(),
            JobKind::Download,
            vec![
                RunItem::new("a", "a", "local-a", ()),
                RunItem::new("b", "b", "local-b", ()),
            ],
            false,
            &NullProgressSink,
            &cancel,
            move |_| {
                let cancel = cancel_from_work.clone();
                async move {
                    cancel.cancel();
                    Step::done(Outcome::Succeeded, None)
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(report.succeeded, 1);
        assert_eq!(report.items.len(), 1);
        assert_eq!(report.state, "interrupted");
        let job = jobs.get(report.job_id).await.unwrap().unwrap();
        assert_eq!(job.state, JobState::Interrupted);
        assert_eq!(job.items[0].state, ItemState::Succeeded);
        assert_eq!(job.items[1].state, ItemState::Cancelled);
    }
}
