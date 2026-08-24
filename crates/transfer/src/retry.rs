//! Exponential-backoff retry policy shared by transfer operations.

use serde::{Deserialize, Serialize};

/// Exponential backoff with a cap and a maximum attempt count.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Total attempts allowed, including the first.
    pub max_attempts: u32,
    /// Backoff before the first retry, in milliseconds.
    pub initial_backoff_ms: u64,
    /// Upper bound on any single backoff, in milliseconds.
    pub max_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
        }
    }
}

impl RetryPolicy {
    /// Backoff (in milliseconds) for the `attempt`-th retry (0-indexed), with
    /// exponential growth capped at `max_backoff_ms`.
    pub fn backoff_ms(&self, attempt: u32) -> u64 {
        let pow = attempt.min(20);
        let growth = 2u64.checked_shl(pow).unwrap_or(u64::MAX);
        let raw = self.initial_backoff_ms.saturating_mul(growth / 2);
        raw.min(self.max_backoff_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let p = RetryPolicy::default();
        assert_eq!(p.backoff_ms(0), 500);
        assert_eq!(p.backoff_ms(1), 1_000);
        assert_eq!(p.backoff_ms(20), p.max_backoff_ms);
    }
}
