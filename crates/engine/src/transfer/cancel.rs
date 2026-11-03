//! Cooperative cancellation shared by every transfer loop.
//!
//! Transfers poll [`CancellationToken::check`] between parts, chunks and
//! items; waiters resolve event-driven from the signal itself, no polling.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rstore_domain::{Error, Result};
use tokio::sync::Notify;

/// A shared cancellation flag.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl CancellationToken {
    /// Creates a token nobody has cancelled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal cancellation to every holder.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Whether cancellation was signalled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Return `Error::cancelled()` when signalled; `Ok(())` otherwise.
    /// Long-running loops call this at every safe checkpoint.
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::cancelled())
        } else {
            Ok(())
        }
    }

    /// Resolves as soon as cancellation is signalled — event-driven, no
    /// polling. The waiter registers before re-checking the flag, so a
    /// concurrent `cancel` can never fall into the gap between check and
    /// wait.
    pub async fn cancelled(&self) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}
