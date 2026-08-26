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

use async_trait::async_trait;

use crate::batch_outcome::{Disposition, Failure, SkipReason};
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

    /// Maps one engine outcome onto the run vocabulary: the disposition
    /// rides over as-is (one vocabulary, no re-translation); transfers
    /// carry bytes and the adapter's metadata for the settlement hook.
    pub fn from_outcome(outcome: &ItemOutcome) -> Self {
        Self {
            step: Step::Done {
                disposition: outcome.disposition.clone(),
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

/// The settlement hook at the run loop's seam: every batch operation
/// supplies one implementation that settles a succeeded item and may revise
/// its verdict — a move whose post-copy verification fails comes back as a
/// failure. Runs only for items `per_item` reported as succeeded, with the
/// engine metadata included when the flow carries one.
///
/// Implementations today: upload/download (upload settlement), copy/move
/// (copy settlement), and removal (delete settlement) — three adapters over
/// one seam.
#[async_trait]
pub trait SettleHook<T>: Send + Sync {
    /// Settles one succeeded item and returns its final step.
    async fn settle(&self, work: T, verdict: ItemVerdict) -> Step;
}

#[allow(clippy::too_many_arguments)]
/// Runs every item through `per_item`, settles every success through the
/// `settle` hook, persists per-item state, and commits a single terminal
/// state: `Succeeded`, `PartialSuccess` (some failed/blocked), or `Failed`
/// (none succeeded). Returns the resulting [`TransferReport`].
///
/// The settle hook is the enforcement point for the settlement rule: it
/// runs only for items `per_item` reported as succeeded, receives the item
/// and its verdict (engine metadata included), and may revise the verdict.
/// Flows without a settlement pass the verdict through unchanged.
///
/// # Errors
/// Propagates repository failures; a failing terminal commit leaves the job
/// record in `Running` so a later sweep can mark stale runs interrupted.
pub async fn run_items<T, F, Fut>(
    jobs: &dyn JobRepository,
    clock: &dyn Clock,
    kind: JobKind,
    items: Vec<RunItem<T>>,
    dry_run: bool,
    progress: &dyn ProgressSink,
    cancel: &CancellationToken,
    mut per_item: F,
    settle: &dyn SettleHook<T>,
) -> Result<TransferReport>
where
    T: Clone,
    F: FnMut(T) -> Fut,
    Fut: Future<Output = ItemVerdict>,
{
    let mut job = create_job(jobs, clock, kind, &items).await?;
    let mut report = TransferReport {
        job_id: job.id,
        kind: kind.as_str(),
        items: Vec::new(),
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
            _ if succeeded => settle.settle(item.work.clone(), verdict).await,
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
                job.items[index].state = ItemState::Succeeded;
            }
            Disposition::Skipped(_) => {
                job.items[index].state = ItemState::Skipped;
            }
            Disposition::Failed(_) => {
                job.items[index].state = ItemState::Failed;
            }
            Disposition::Blocked(_) => {
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
        progress.report(Progress::Items {
            done: report.items.len() as u64,
            total,
        });
    }

    // Single terminal commit: partial success only when at least one item
    // landed alongside failures/blocks. Counts are derived from the item
    // dispositions rather than maintained as a second mutable state.
    let final_state = if report.failed() > 0 || report.blocked() > 0 {
        if report.succeeded() > 0 {
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
