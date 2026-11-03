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

    /// [`Self::backoff_ms`] with equal jitter: half the capped delay plus a
    /// random share of the other half, so processes sharing one data
    /// directory do not retry against a provider in lockstep.
    pub fn jittered_backoff_ms(&self, attempt: u32) -> u64 {
        let ceiling = self.backoff_ms(attempt);
        let floor = ceiling / 2;
        let spread = u128::from(ceiling - floor) + 1;
        floor + (uuid::Uuid::new_v4().as_u128() % spread) as u64
    }
}
