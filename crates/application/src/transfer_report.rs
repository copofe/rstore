//! Serializable per-run outcome report for transfer jobs: one row per item
//! plus aggregate counts, suitable for JSON output. Items speak the unified
//! [`Disposition`] vocabulary, and the report aggregates into a [`Severity`]
//! so entry layers never re-derive "did this batch succeed" from counts.

use crate::batch_outcome::{severity_of, Disposition, Severity};
use rstore_domain::job::JobState;
use rstore_domain::JobId;

/// One item's result in a [`TransferReport`].
#[derive(Debug, Clone, serde::Serialize)]
pub struct TransferReportItem {
    pub source: String,
    pub target: String,
    /// Item kind tag (`upload`, `download`, `copy`, `delete`).
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Machine-readable classification; excluded from serialized output.
    #[serde(skip)]
    pub disposition: Disposition,
    /// Guard, conflict, or engine detail for skipped, blocked, and failed
    /// items.
    pub detail: Option<String>,
}

/// Full per-run outcome of a transfer job.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TransferReport {
    pub job_id: JobId,
    pub kind: &'static str,
    pub items: Vec<TransferReportItem>,
    pub succeeded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub blocked: usize,
    pub state: JobState,
    pub dry_run: bool,
}

impl TransferReport {
    /// Whether the run ended by cancellation or crash rather than reaching
    /// a terminal state on its own.
    pub fn is_interrupted(&self) -> bool {
        self.state.is_interrupted()
    }

    /// The aggregate verdict over all items — the single input for exit
    /// policies in the entry layers. The aggregation rule itself is the
    /// shared [`severity_of`](crate::batch_outcome::severity_of).
    pub fn severity(&self) -> Severity {
        severity_of(self.items.iter().map(|i| &i.disposition))
    }
}
