//! Engine events: the runtime notification stream entry points subscribe to.
//!
//! Events are best-effort runtime messages, not durable facts: a lagging
//! subscriber loses the oldest ones and nothing is replayed. Durable state
//! lives in the store; a subscriber re-queries it when an event says
//! something changed. The CLI renders job progress from this stream; the
//! desktop drives its views from it.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rstore_domain::id::{BucketId, JobId, WorkspaceId};
use rstore_domain::job::{JobKind, JobState};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::transfer::{Progress, ProgressSink};

/// Buffered events per subscriber before the oldest are dropped.
const CAPACITY: usize = 1024;

/// Minimum spacing of byte-progress events for one job.
const BYTES_INTERVAL: Duration = Duration::from_millis(200);

/// One engine event.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum EngineEvent {
    /// A job row exists and its items are about to run.
    JobStarted {
        job: JobId,
        kind: JobKind,
        items: u64,
    },
    /// Byte progress of the in-flight item, or item progress of the batch.
    JobProgress { job: JobId, progress: Progress },
    /// The job committed its final (or interrupted) state.
    JobFinished { job: JobId, state: JobState },
    /// Cached rows of a bucket changed (write-through, index run, tagging).
    CatalogChanged { bucket: BucketId },
    /// A workspace's tag set or tag associations changed.
    TagsChanged { workspace: WorkspaceId },
}

/// The engine's broadcast channel. Cloning shares the channel.
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<EngineEvent>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self {
            tx: broadcast::channel(CAPACITY).0,
        }
    }
}

impl EventBus {
    /// Subscribe to every event published from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.tx.subscribe()
    }

    /// Publish an event; having no subscriber is not an error.
    pub(crate) fn publish(&self, event: EngineEvent) {
        let _ = self.tx.send(event);
    }

    /// A progress sink for one job: forwards every snapshot to the caller's
    /// sink and publishes it, throttling byte progress.
    pub(crate) fn job_sink<'a>(&self, job: JobId, inner: &'a dyn ProgressSink) -> JobSink<'a> {
        JobSink {
            bus: self.clone(),
            job,
            inner,
            last_bytes: Mutex::new(None),
        }
    }
}

/// See [`EventBus::job_sink`].
pub(crate) struct JobSink<'a> {
    bus: EventBus,
    job: JobId,
    inner: &'a dyn ProgressSink,
    last_bytes: Mutex<Option<Instant>>,
}

impl ProgressSink for JobSink<'_> {
    fn report(&self, progress: Progress) {
        self.inner.report(progress);
        if let Progress::Bytes { done, total } = progress {
            let now = Instant::now();
            let mut last = self
                .last_bytes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let due = done >= total || last.map_or(true, |at| now - at >= BYTES_INTERVAL);
            if !due {
                return;
            }
            *last = Some(now);
        }
        self.bus.publish(EngineEvent::JobProgress {
            job: self.job,
            progress,
        });
    }
}
