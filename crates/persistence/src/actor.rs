//! Bounded single-writer actor for SQLite mutations.
//!
//! One write permit exists at a time; repository code runs its transaction
//! while holding it. The request channel is bounded — a saturated queue fails
//! with `DatabaseBusy` rather than growing without limit.
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use rstore_domain::{Error, Result};

struct Request {
    response: oneshot::Sender<OwnedSemaphorePermit>,
}

#[derive(Clone)]
/// Cloneable handle to the writer actor; clones share the same bounded queue.
pub struct SqliteWriter {
    requests: mpsc::Sender<Request>,
}

#[derive(Debug)]
/// Permit held for the complete duration of one repository write; dropping it releases the writer.
pub struct WritePermit {
    _permit: OwnedSemaphorePermit,
}

impl SqliteWriter {
    /// Create the actor with a bounded queue of `capacity` pending requests (minimum 1).
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (requests, mut receiver) = mpsc::channel::<Request>(capacity);
        let semaphore = std::sync::Arc::new(Semaphore::new(1));
        let actor_semaphore = semaphore.clone();
        tokio::spawn(async move {
            loop {
                let Ok(permit) = actor_semaphore.clone().acquire_owned().await else {
                    break;
                };
                let Some(request) = receiver.recv().await else {
                    break;
                };
                // Caller cancelled before receiving the permit; dropping it here lets the actor advance.
                if request.response.send(permit).is_err() {}
            }
        });
        Self { requests }
    }

    /// Request the next write permit. Fails immediately with `DatabaseBusy` when the queue is full;
    /// otherwise waits for the actor to hand over the single permit.
    pub async fn acquire(&self) -> Result<WritePermit> {
        let (response, receiver) = oneshot::channel();
        self.requests
            .try_send(Request { response })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    Error::database_busy("SQLite writer queue is full")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    Error::database("SQLite writer actor is closed")
                }
            })?;
        let permit = receiver
            .await
            .map_err(|_| Error::database("SQLite writer actor stopped"))?;
        Ok(WritePermit { _permit: permit })
    }
}
