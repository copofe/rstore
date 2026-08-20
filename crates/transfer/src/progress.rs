//! Progress reporting for transfers (JOB-005/016/018).

use serde::{Deserialize, Serialize};

/// A point-in-time progress snapshot. Two meanings, two arms — the engine
/// reports `Bytes` for one in-flight item, batch loops report `Items`; a
/// single struct with both field pairs let item counts render as bytes
/// ("3 B/10 B"), so the distinction is in the type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Progress {
    /// Byte progress of one in-flight transfer item.
    Bytes { done: u64, total: u64 },
    /// Whole-item progress of a batch (no byte semantics).
    Items { done: u64, total: u64 },
}

impl Progress {
    /// Completion fraction in `[0, 1]` — the shared math for both arms.
    pub fn fraction(&self) -> f64 {
        let (done, total) = match *self {
            Progress::Bytes { done, total } | Progress::Items { done, total } => (done, total),
        };
        if total == 0 {
            0.0
        } else {
            done as f64 / total as f64
        }
    }
}

/// Sink for progress events. Implementations coalesce high-frequency updates so
/// the UI/CLI isn't flooded (PRD §10.13.5). Progress MUST go to stderr, never
/// stdout (CLI-006).
pub trait ProgressSink: Send + Sync {
    fn report(&self, progress: Progress);
}

/// A sink that discards everything (handy for `--quiet` and tests).
#[derive(Debug, Default, Clone, Copy)]
pub struct NullProgressSink;

impl ProgressSink for NullProgressSink {
    fn report(&self, _progress: Progress) {}
}
