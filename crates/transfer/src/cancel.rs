//! Cooperative cancellation (CORE-010).
//!
//! Transfers poll the token between parts/items/chunks. The same cooperative
//! cancellation contract is shared by CLI and future desktop callers; the
//! next poll unwinds with `Error::cancelled()` and the caller persists state.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rstore_domain::{Error, Result};

/// A shared cancellation flag.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal cancellation to every holder.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
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

    /// Resolves as soon as cancellation is signalled. Polls at 10 ms: the
    /// only latency this bounds is how fast a backoff sleep unwinds, and a
    /// sleeping retry needs no tighter precision.
    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_the_flag() {
        let token = CancellationToken::new();
        let clone = token.clone();
        assert!(!token.is_cancelled());
        clone.cancel();
        assert!(token.is_cancelled());
        assert!(token.check().is_err());
    }
}
