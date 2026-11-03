//! Persisted transfer-run loop shared by upload, download, copy, and removal
//! jobs.
//!
//! Drives a batch of [`RunItem`]s through a caller-supplied execution closure
//! and a settlement hook, mirroring every settled item into the durable job
//! record and a [`TransferReport`]. The loop owns the verdict vocabulary
//! ([`ItemVerdict`] → [`Step`] → report item), the one settle-on-success
//! point (a successful remote mutation is settled exactly here, before it is
//! counted), and the terminal-state commit; interruption (cancel token, a
//! cancelled engine result, or a `Step::Interrupted` step) marks remaining
//! pending items cancelled and commits `Interrupted` — or `Cancelled` when
//! the stop was a cancel request.
//!
//! A running job holds a lease in the store and renews it on a heartbeat, so
//! several processes can share one database: startup recovery interrupts only
//! jobs whose lease lapsed, and a cancel request from another process reaches
//! the owner on its next heartbeat. Jobs this process runs are also in the
//! [`LiveJobs`] registry, where an in-process cancel lands immediately.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;

use crate::batch_outcome::{Disposition, Failure, SkipReason};
use crate::db::JobRepository;
use crate::events::{EngineEvent, EventBus, JobSink};
use crate::transfer::{CancellationToken, ItemOutcome, Progress, ProgressSink};
use crate::transfer_report::{TransferReport, TransferReportItem};
use rstore_domain::job::{ItemState, Job, JobItem, JobKind, JobState};
use rstore_domain::{Clock, ErrorCode, JobId, Result};
use rstore_storage::model::ObjectMetadata;

/// Maximum characters of a per-item error detail persisted on the job record.
const ERROR_CODE_CAP: usize = 200;

/// How long a lease holds without renewal.
const LEASE: Duration = Duration::from_secs(30);

/// Heartbeat period: renew the lease and pick up cancel requests.
const LEASE_RENEW: Duration = Duration::from_secs(10);

/// This process's identity as a lease owner.
fn lease_owner() -> &'static str {
    static OWNER: OnceLock<String> = OnceLock::new();
    OWNER.get_or_init(|| format!("{}-{}", std::process::id(), uuid::Uuid::new_v4().simple()))
}

/// What a job run needs from the engine: the job store, the clock, the event
/// bus, and the in-process registry of live jobs.
#[derive(Clone)]
pub(crate) struct JobEnv {
    pub(crate) jobs: Arc<JobRepository>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) bus: EventBus,
    pub(crate) live: LiveJobs,
}

impl JobEnv {
    /// Allocate the job's id and progress sink before the run starts, so the
    /// engine's byte progress already carries the id. `cancel` becomes the
    /// job's token: Ctrl+C, an in-process cancel, and a cross-process cancel
    /// request all trip it.
    pub(crate) fn begin<'a>(
        &self,
        cancel: &CancellationToken,
        progress: &'a dyn ProgressSink,
    ) -> JobRun<'a> {
        let id = JobId::new_random();
        JobRun {
            env: self.clone(),
            id,
            cancel: cancel.clone(),
            sink: self.bus.job_sink(id, progress),
        }
    }
}

/// One job about to run: its id, token, and progress sink.
pub(crate) struct JobRun<'a> {
    env: JobEnv,
    id: JobId,
    cancel: CancellationToken,
    sink: JobSink<'a>,
}

impl JobRun<'_> {
    /// The job's cancellation token, for the run context.
    pub(crate) fn cancel(&self) -> &CancellationToken {
        &self.cancel
    }

    /// The job's progress sink, for the run context.
    pub(crate) fn progress(&self) -> &dyn ProgressSink {
        &self.sink
    }
}

/// Jobs this process is running, by id.
#[derive(Clone, Default)]
pub(crate) struct LiveJobs {
    jobs: Arc<Mutex<HashMap<JobId, LiveJob>>>,
}

#[derive(Clone)]
struct LiveJob {
    cancel: CancellationToken,
    requested: Arc<AtomicBool>,
}

impl LiveJob {
    fn request_cancel(&self) {
        self.requested.store(true, Ordering::SeqCst);
        self.cancel.cancel();
    }
}

impl LiveJobs {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<JobId, LiveJob>> {
        self.jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Cancel a job this process runs. `false` when it is not running here.
    pub(crate) fn cancel(&self, id: JobId) -> bool {
        match self.lock().get(&id) {
            Some(job) => {
                job.request_cancel();
                true
            }
            None => false,
        }
    }
}

/// Keeps a job live for the duration of its run: registered in [`LiveJobs`]
/// with its lease heartbeat running. Dropping it — on any exit path —
/// unregisters the job and stops the heartbeat.
struct Liveness {
    id: JobId,
    live: LiveJobs,
    requested: Arc<AtomicBool>,
    heartbeat: tokio::task::JoinHandle<()>,
}

impl Liveness {
    fn start(env: &JobEnv, id: JobId, cancel: &CancellationToken) -> Self {
        let job = LiveJob {
            cancel: cancel.clone(),
            requested: Arc::new(AtomicBool::new(false)),
        };
        env.live.lock().insert(id, job.clone());
        let jobs = env.jobs.clone();
        let clock = env.clock.clone();
        let requested = job.requested.clone();
        let heartbeat = tokio::spawn(async move {
            let mut tick = tokio::time::interval(LEASE_RENEW);
            tick.tick().await;
            loop {
                tick.tick().await;
                match jobs
                    .renew_lease(id, lease_owner(), clock.now() + LEASE)
                    .await
                {
                    Ok(true) => job.request_cancel(),
                    Ok(false) => {}
                    Err(error) => tracing::warn!(%error, "job lease renewal failed"),
                }
            }
        });
        Self {
            id,
            live: env.live.clone(),
            requested,
            heartbeat,
        }
    }

    /// Whether the stop came from a cancel request rather than Ctrl+C.
    fn cancel_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }
}

impl Drop for Liveness {
    fn drop(&mut self) {
        self.heartbeat.abort();
        self.live.lock().remove(&self.id);
    }
}

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

    /// Append a follow-up note (a cache write that did not land) to a
    /// finished step's detail; the disposition stays as it was.
    pub fn noted(self, note: Option<String>) -> Self {
        match (self, note) {
            (
                Self::Done {
                    disposition,
                    detail,
                    bytes,
                },
                Some(note),
            ) => Self::Done {
                disposition,
                detail: Some(match detail {
                    Some(detail) => format!("{detail}; {note}"),
                    None => note,
                }),
                bytes,
            },
            (step, _) => step,
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

/// Persists a fresh, leased job in `Pending` state and transitions it to
/// `Running`.
async fn create_job<T>(
    env: &JobEnv,
    id: JobId,
    kind: JobKind,
    items: &[RunItem<T>],
) -> Result<Job> {
    let now = env.clock.now();
    let mut job = Job::new(kind, id, now);
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
    env.jobs.create(&job, lease_owner(), now + LEASE).await?;
    env.jobs.transition(job.id, JobState::Running, now).await?;
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

/// Runs every item through `per_item`, settles every success through the
/// `settle` hook, persists per-item state as it goes, and commits a single
/// terminal state: `Succeeded`, `PartialSuccess` (some failed/blocked), or
/// `Failed` (none succeeded). Returns the resulting [`TransferReport`].
///
/// The settle hook runs only for items `per_item` reported as succeeded,
/// receives the item and its verdict (engine metadata included), and may
/// revise the verdict. Flows without a settlement pass the verdict through.
///
/// # Errors
/// Propagates repository failures; a failing terminal commit leaves the job
/// `Running` until its lease lapses and recovery marks it interrupted.
pub(crate) async fn run_items<T, F, Fut>(
    run: &JobRun<'_>,
    kind: JobKind,
    items: Vec<RunItem<T>>,
    dry_run: bool,
    mut per_item: F,
    settle: &dyn SettleHook<T>,
) -> Result<TransferReport>
where
    T: Clone,
    F: FnMut(T) -> Fut,
    Fut: Future<Output = ItemVerdict>,
{
    let env = &run.env;
    let mut job = create_job(env, run.id, kind, &items).await?;
    let liveness = Liveness::start(env, job.id, &run.cancel);
    let total = job.items.len() as u64;
    env.bus.publish(EngineEvent::JobStarted {
        job: job.id,
        kind,
        items: total,
    });
    let mut report = TransferReport {
        job_id: job.id,
        kind: kind.as_str(),
        items: Vec::new(),
        state: JobState::Running,
        dry_run,
    };

    let outcome = async {
        for (index, item) in items.iter().enumerate() {
            if run.cancel.check().is_err() {
                return interrupt(env, &liveness, job, index, report).await;
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
                Step::Interrupted => return interrupt(env, &liveness, job, index, report).await,
                _ if succeeded => settle.settle(item.work.clone(), verdict).await,
                done => done,
            };
            let (disposition, detail, bytes) = match settled {
                Step::Done {
                    disposition,
                    detail,
                    bytes,
                } => (disposition, detail, bytes),
                Step::Interrupted => return interrupt(env, &liveness, job, index, report).await,
            };

            job.items[index].state = match &disposition {
                Disposition::Succeeded => ItemState::Succeeded,
                Disposition::Skipped(_) => ItemState::Skipped,
                // Blocked items surface as failed on the durable record while
                // keeping their distinct counter in the report.
                Disposition::Failed(_) | Disposition::Blocked(_) => ItemState::Failed,
            };
            if let Some(d) = &detail {
                // Cap persisted details so oversized provider errors cannot
                // bloat the job record.
                job.items[index].error = Some(d.chars().take(ERROR_CODE_CAP).collect());
            }
            job.items[index].bytes_done = bytes;
            job.items[index].bytes_total = bytes;
            // Progress persistence is best-effort; the terminal commit below
            // is the authoritative record.
            if let Err(error) = env.jobs.update_item(job.id, index, &job.items[index]).await {
                tracing::warn!(%error, "could not persist job item progress");
            }
            report.items.push(TransferReportItem {
                source: item.source.clone(),
                target: item.target.clone(),
                kind: kind.as_str(),
                disposition,
                detail,
            });
            run.sink.report(Progress::Items {
                done: report.items.len() as u64,
                total,
            });
        }

        // Single terminal commit: partial success only when at least one item
        // landed alongside failures/blocks. Counts derive from the item
        // dispositions rather than a second mutable state.
        let final_state = if report.failed() > 0 || report.blocked() > 0 {
            if report.succeeded() > 0 {
                JobState::PartialSuccess
            } else {
                JobState::Failed
            }
        } else {
            JobState::Succeeded
        };
        let committed = env
            .jobs
            .commit_terminal(job.id, &job.items, final_state, env.clock.now())
            .await?;
        report.state = committed.state;
        Ok(report)
    }
    .await;

    if let Ok(report) = &outcome {
        env.bus.publish(EngineEvent::JobFinished {
            job: report.job_id,
            state: report.state,
        });
    }
    outcome
}

/// Marks all not-yet-settled items cancelled and commits `Interrupted`, or
/// `Cancelled` when a cancel request stopped the run.
async fn interrupt(
    env: &JobEnv,
    liveness: &Liveness,
    mut job: Job,
    index: usize,
    mut report: TransferReport,
) -> Result<TransferReport> {
    for pending in job.items.iter_mut().skip(index) {
        if pending.state == ItemState::Pending {
            pending.state = ItemState::Cancelled;
        }
    }
    let target = if liveness.cancel_requested() {
        JobState::Cancelled
    } else {
        JobState::Interrupted
    };
    let committed = env
        .jobs
        .commit_terminal(job.id, &job.items, target, env.clock.now())
        .await?;
    report.state = committed.state;
    Ok(report)
}
