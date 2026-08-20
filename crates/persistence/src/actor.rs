//! Bounded single-writer actor for SQLite mutations.
//!
//! The actor hands out one write permit at a time. Repository code performs
//! its existing SQL transaction while holding that permit, so the migration
//! can preserve repository APIs while enforcing process-local write ordering.
//! The channel is bounded; a saturated queue returns `DatabaseBusy` instead of
//! spawning unbounded tasks or silently waiting forever.

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use rstore_domain::{Error, Result};

struct Request {
    response: oneshot::Sender<OwnedSemaphorePermit>,
}

#[derive(Clone)]
pub struct SqliteWriter {
    requests: mpsc::Sender<Request>,
}

/// Permit held for the complete duration of one repository write.
#[derive(Debug)]
pub struct WritePermit {
    _permit: OwnedSemaphorePermit,
}

impl SqliteWriter {
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
                if request.response.send(permit).is_err() {
                    // The caller cancelled before receiving the permit. The
                    // owned permit is dropped here and the actor can advance.
                }
            }
        });
        Self { requests }
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn serializes_writes_and_rejects_a_full_queue() {
        let writer = SqliteWriter::new(1);
        let first = writer.acquire().await.unwrap();
        let queued = writer.clone();
        let waiter = tokio::spawn(async move { queued.acquire().await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let full = writer.acquire().await.unwrap_err();
        assert_eq!(full.code, rstore_domain::ErrorCode::DatabaseBusy);
        drop(first);
        waiter.await.unwrap().unwrap();
    }
}
