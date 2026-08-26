//! Persisted transfer-run loop shared by upload, download, copy, and removal
//! jobs.
//!
//! Drives a batch of [`RunItem`]s through a caller-supplied execution closure
//! and a settlement hook, mirroring every settled item into the durable job
//! record and a [`TransferReport`]. The loop owns the verdict vocabulary
//! ([`ItemVerdict`] → [`Step`] → report item), the one
//! settle-on-success enforcement point (a successful remote mutation is
//! settled exactly here, before it can be counted), and the terminal-state
//! commit; interruption (cancel token, a cancelled engine result, or an
//! `Step::Interrupted` step) marks remaining pending items cancelled and
//! commits `Interrupted`.

use std::future::Future;

use crate::batch_outcome::{BlockReason, Disposition, Failure, SkipReason};
use crate::transfer_report::{TransferReport, TransferReportItem};
use rstore_application_ports::JobRepository;
use rstore_domain::job::{ItemState, Job, JobItem, JobKind, JobState};
use rstore_domain::{Clock, ErrorCode, JobId, Result};
use rstore_storage_api::model::ObjectMetadata;
use rstore_transfer::{CancellationToken, ItemOutcome, Progress, ProgressSink};

/// Maximum characters of a per-item error detail persisted on the job record.
const ERROR_CODE_CAP: usize = 200;

/// One schedulable unit inside a transfer run: display labels plus the
/// caller-defined payload handed to the per-item closure.
pub struct RunItem<T> {
    job_label: String,
    source: String,
    target: String,
    work: T,
}

impl<T> RunItem<T> {
    /// Builds an item with its job-record label and report source/target paths.
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

/// Result of one per-item closure invocation.
pub enum Step {
    /// The item finished; carry its disposition, optional detail, and byte
    /// count.
    Done {
        disposition: Disposition,
        detail: Option<String>,
        bytes: u64,
    },

    /// The run must stop here; remaining items are marked cancelled.
    Interrupted,
}

impl Step {
    /// A completed item with no transferred bytes recorded.
    pub fn done(disposition: Disposition, detail: Option<String>) -> Self {
        Self::Done {
            disposition,
            detail,
            bytes: 0,
        }
    }

    /// A succeeded item with no detail.
    pub fn succeeded() -> Self {
        Self::done(Disposition::Succeeded, None)
    }

    /// A failed item whose detail is rendered from a domain error.
    pub fn failed(e: &rstore_domain::Error) -> Self {
        Self::Done {
            disposition: Disposition::Failed(Failure::from_error(e)),
            detail: Some(e.to_string()),
            bytes: 0,
        }
    }

    /// A failed item with a free-form detail message.
    pub fn failed_msg(detail: impl Into<String>) -> Self {
        Self::Done {
            disposition: Disposition::Failed(Failure {
                code: rstore_domain::ErrorCode::Unknown,
                message: String::new(),
            }),
            detail: Some(detail.into()),
            bytes: 0,
        }
    }

    /// A dry-run/preview item: fully resolved, deliberately not executed.
    pub fn preview(detail: impl Into<String>) -> Self {
        Self::done(
            Disposition::Skipped(SkipReason::DryRun),
            Some(detail.into()),
        )
    }
}

/// What one executed item reports back to the run loop: its verdict plus the
/// adapter metadata a settlement may need. Engine-driven flows (upload,
/// copy) carry the transferred object's metadata; native flows (removal)
/// carry `None`.
pub struct ItemVerdict {
    pub step: Step,
    pub metadata: Option<ObjectMetadata>,
}

impl ItemVerdict {
    /// A loop-native verdict with no engine metadata.
    pub fn from_step(step: Step) -> Self {
        Self {
            step,
            metadata: None,
        }
    }

    /// A dry-run/preview verdict.
    pub fn preview(detail: impl Into<String>) -> Self {
        Self::from_step(Step::preview(detail))
    }

    /// Maps one engine outcome onto the run vocabulary: previews, conflict
    /// skips, and blocks keep their detail; transfers carry bytes and the
    /// adapter's metadata for the settlement hook.
    pub fn from_outcome(outcome: &ItemOutcome) -> Self {
        let disposition = if outcome.dry_run {
            Disposition::Skipped(SkipReason::DryRun)
        } else if outcome.skipped {
            Disposition::Skipped(SkipReason::Conflict)
        } else if outcome.blocked {
            Disposition::Blocked(BlockReason::Unresolved)
        } else {
            Disposition::Succeeded
        };
        Self {
            step: Step::Done {
                disposition,
                detail: outcome.detail.clone(),
                bytes: outcome.bytes,
            },
            metadata: outcome.metadata.clone(),
        }
    }

    /// Maps one engine result: a cancelled item interrupts the run instead
    /// of counting as a failure — the one cancellation interpretation for
    /// every batch operation.
    pub fn from_engine_result(outcome: Result<ItemOutcome>) -> Self {
        match outcome {
            Ok(outcome) => Self::from_outcome(&outcome),
            Err(e) if e.code == ErrorCode::Cancelled => Self::from_step(Step::Interrupted),
            Err(e) => Self::from_step(Step::failed(&e)),
        }
    }
}

/// Persists a fresh job in `Pending` state and transitions it to `Running`.
async fn create_job<T>(
    jobs: &dyn JobRepository,
    clock: &dyn Clock,
    kind: JobKind,
    items: &[RunItem<T>],
) -> Result<Job> {
    let now = clock.now();
    let mut job = Job::new(kind, JobId::new_random(), now);
    job.items = items
        .iter()
        .map(|item| JobItem {
            label: item.job_label.clone(),
            state: ItemState::Pending,
            bytes_total: 0,
            bytes_done: 0,
            error: None,
        })
        .collect();
    jobs.create(&job).await?;
    jobs.transition(job.id, JobState::Running, now).await?;
    Ok(job)
}

#[allow(clippy::too_many_arguments)]
/// Runs every item through `per_item`, settles every success through
/// `settle`, persists per-item state, and commits a single terminal state:
/// `Succeeded`, `PartialSuccess` (some failed/blocked), or `Failed` (none
/// succeeded). Returns the resulting [`TransferReport`].
///
/// `settle` is the enforcement point for the settlement rule: it runs only
/// for items `per_item` reported as succeeded, receives the item and its
/// verdict (engine metadata included), and may revise the verdict — a move
/// whose post-copy verification fails comes back as a failure. Flows without
/// a settlement pass the verdict through unchanged.
///
/// # Errors
/// Propagates repository failures; a failing terminal commit leaves the job
/// record in `Running` so a later sweep can mark stale runs interrupted.
pub async fn run_items<T, F, Fut, S, SFut>(
    jobs: &dyn JobRepository,
    clock: &dyn Clock,
    kind: JobKind,
    items: Vec<RunItem<T>>,
    dry_run: bool,
    progress: &dyn ProgressSink,
    cancel: &CancellationToken,
    mut per_item: F,
    mut settle: S,
) -> Result<TransferReport>
where
    T: Clone,
    F: FnMut(T) -> Fut,
    Fut: Future<Output = ItemVerdict>,
    S: FnMut(T, ItemVerdict) -> SFut,
    SFut: Future<Output = Step>,
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
        state: JobState::Running,
        dry_run,
    };

    let total = job.items.len() as u64;

    for (index, item) in items.iter().enumerate() {
        if cancel.is_cancelled() || cancel.check().is_err() {
            return interrupt(jobs, clock, job, index, report).await;
        }

        let verdict = per_item(item.work.clone()).await;
        let succeeded = matches!(
            verdict.step,
            Step::Done {
                disposition: Disposition::Succeeded,
                ..
            }
        );
        let settled = match verdict.step {
            Step::Interrupted => return interrupt(jobs, clock, job, index, report).await,
            _ if succeeded => settle(item.work.clone(), verdict).await,
            done => done,
        };
        let (disposition, detail, bytes) = match settled {
            Step::Done {
                disposition,
                detail,
                bytes,
            } => (disposition, detail, bytes),
            Step::Interrupted => return interrupt(jobs, clock, job, index, report).await,
        };

        match &disposition {
            Disposition::Succeeded => {
                report.succeeded += 1;
                job.items[index].state = ItemState::Succeeded;
            }
            Disposition::Skipped(_) => {
                report.skipped += 1;
                job.items[index].state = ItemState::Skipped;
            }
            Disposition::Failed(_) => {
                report.failed += 1;
                job.items[index].state = ItemState::Failed;
            }
            Disposition::Blocked(_) => {
                report.blocked += 1;
                // Blocked items surface as failed on the durable record while
                // keeping their distinct counter in the report.
                job.items[index].state = ItemState::Failed;
            }
        }
        if let Some(d) = &detail {
            // Cap persisted details so oversized provider errors cannot bloat
            // the job record.
            job.items[index].error = Some(d.chars().take(ERROR_CODE_CAP).collect());
        }
        job.items[index].bytes_done = bytes;
        job.items[index].bytes_total = bytes;
        report.items.push(TransferReportItem {
            source: item.source.clone(),
            target: item.target.clone(),
            kind: kind.as_str(),
            disposition,
            detail,
        });
        let settled = (report.succeeded + report.skipped + report.failed + report.blocked) as u64;
        progress.report(Progress::Items {
            done: settled,
            total,
        });
    }

    // Single terminal commit: partial success only when at least one item
    // landed alongside failures/blocks.
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
    report.state = committed.state;
    Ok(report)
}

/// Marks all not-yet-settled items cancelled and commits `Interrupted`.
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
    report.state = committed.state;
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
    use crate::batch_outcome::BlockReason;

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
                ItemVerdict::from_step(Step::Done {
                    disposition: Disposition::Succeeded,
                    detail: None,
                    bytes,
                })
            },
            |_, verdict| async move { verdict.step },
        )
        .await
        .unwrap();

        let job = jobs.get(report.job_id).await.unwrap().unwrap();
        assert_eq!(job.items.len(), 1);
        assert_eq!(job.items[0].label, "persisted");
        assert_eq!(job.items[0].state, ItemState::Succeeded);
        assert_eq!(job.items[0].bytes_done, 7);
        assert_eq!(report.items.len(), 1);
        assert_eq!(report.items[0].source, "source");
        assert_eq!(report.items[0].target, "target");
        assert_eq!(report.state, JobState::Succeeded);
    }

    #[tokio::test]
    async fn mixed_outcomes_share_one_terminal_state_rule() {
        let jobs = MemoryJobs::default();
        let report = run_items(
            &jobs,
            &TestClock::default(),
            JobKind::Copy,
            vec![
                RunItem::new("a", "a", "a-copy", Disposition::Succeeded),
                RunItem::new(
                    "b",
                    "b",
                    "b-copy",
                    Disposition::Blocked(BlockReason::Unresolved),
                ),
            ],
            false,
            &NullProgressSink,
            &CancellationToken::new(),
            |disposition| async move {
                ItemVerdict::from_step(Step::Done {
                    detail: (!disposition.is_success()).then(|| "x".repeat(250)),
                    disposition,
                    bytes: 0,
                })
            },
            |_, verdict| async move { verdict.step },
        )
        .await
        .unwrap();

        assert_eq!(report.succeeded, 1);
        assert_eq!(report.blocked, 1);
        assert_eq!(report.state, JobState::PartialSuccess);
        let job = jobs.get(report.job_id).await.unwrap().unwrap();
        assert_eq!(job.state, JobState::PartialSuccess);
        assert_eq!(job.items[0].state, ItemState::Succeeded);
        assert_eq!(job.items[1].state, ItemState::Failed);
        assert_eq!(job.items[1].error.as_deref().unwrap().len(), 200);
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
            |_| async { ItemVerdict::from_step(Step::succeeded()) },
            |_, verdict| async move { verdict.step },
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
                    ItemVerdict::from_step(Step::succeeded())
                }
            },
            |_, verdict| async move { verdict.step },
        )
        .await
        .unwrap();

        assert_eq!(report.succeeded, 1);
        assert_eq!(report.items.len(), 1);
        assert_eq!(report.state, JobState::Interrupted);
        let job = jobs.get(report.job_id).await.unwrap().unwrap();
        assert_eq!(job.state, JobState::Interrupted);
        assert_eq!(job.items[0].state, ItemState::Succeeded);
        assert_eq!(job.items[1].state, ItemState::Cancelled);
    }

    #[tokio::test]
    async fn settle_hook_may_revise_a_succeeded_item() {
        let jobs = MemoryJobs::default();
        let report = run_items(
            &jobs,
            &TestClock::default(),
            JobKind::Move,
            vec![
                RunItem::new("kept", "kept", "kept-copy", false),
                RunItem::new("revised", "revised", "revised-copy", true),
            ],
            false,
            &NullProgressSink,
            &CancellationToken::new(),
            |_revise| async { ItemVerdict::from_step(Step::succeeded()) },
            |revise, verdict| async move {
                if revise {
                    // A failed move verification revises the verdict before
                    // counting.
                    Step::failed_msg("verify failed after copy")
                } else {
                    verdict.step
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(report.succeeded, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(report.state, JobState::PartialSuccess);
        let job = jobs.get(report.job_id).await.unwrap().unwrap();
        assert_eq!(job.items[0].state, ItemState::Succeeded);
        assert_eq!(job.items[1].state, ItemState::Failed);
        assert_eq!(
            job.items[1].error.as_deref(),
            Some("verify failed after copy")
        );
    }

    #[tokio::test]
    async fn settle_hook_runs_only_for_succeeded_items() {
        let jobs = MemoryJobs::default();
        let settled = std::sync::atomic::AtomicUsize::new(0);
        let report = run_items(
            &jobs,
            &TestClock::default(),
            JobKind::Delete,
            vec![
                RunItem::new("a", "a", "a", Disposition::Succeeded),
                RunItem::new("b", "b", "b", Disposition::Skipped(SkipReason::Conflict)),
                RunItem::new(
                    "c",
                    "c",
                    "c",
                    Disposition::Failed(Failure::from_error(&Error::not_found("x"))),
                ),
            ],
            false,
            &NullProgressSink,
            &CancellationToken::new(),
            |disposition| async move { ItemVerdict::from_step(Step::done(disposition, None)) },
            |_, verdict| {
                settled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move { verdict.step }
            },
        )
        .await
        .unwrap();

        assert_eq!(report.succeeded, 1);
        assert_eq!(report.skipped, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(
            settled.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only the succeeded item settles"
        );
    }

    #[tokio::test]
    async fn cancelled_engine_result_interrupts_the_run() {
        let jobs = MemoryJobs::default();
        let report = run_items(
            &jobs,
            &TestClock::default(),
            JobKind::Copy,
            vec![
                RunItem::new("a", "a", "a-copy", ()),
                RunItem::new("b", "b", "b-copy", ()),
            ],
            false,
            &NullProgressSink,
            &CancellationToken::new(),
            |_| async { ItemVerdict::from_engine_result(Err(Error::cancelled())) },
            |_, verdict| async move { verdict.step },
        )
        .await
        .unwrap();

        assert_eq!(report.state, JobState::Interrupted);
        let job = jobs.get(report.job_id).await.unwrap().unwrap();
        assert_eq!(job.items[0].state, ItemState::Cancelled);
        assert_eq!(job.items[1].state, ItemState::Cancelled);
    }

    #[tokio::test]
    async fn dry_run_outcome_counts_as_a_deliberate_skip() {
        let jobs = MemoryJobs::default();
        let report = run_items(
            &jobs,
            &TestClock::default(),
            JobKind::Copy,
            vec![RunItem::new("a", "a", "a-copy", ())],
            true,
            &NullProgressSink,
            &CancellationToken::new(),
            |_| async {
                ItemVerdict::from_outcome(&rstore_transfer::ItemOutcome::preview(
                    "would copy to bucket/key".into(),
                ))
            },
            |_, verdict| async move { verdict.step },
        )
        .await
        .unwrap();

        assert_eq!(report.skipped, 1);
        assert_eq!(report.succeeded, 0);
        assert_eq!(
            report.items[0].detail.as_deref(),
            Some("would copy to bucket/key")
        );
    }
}
