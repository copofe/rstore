//! Shared report shapes for persisted batch operations.
//!
//! Job-run constructs these reports; Transfer and Copy/Move re-export and
//! render them without owning their bookkeeping rules.

use rstore_domain::JobId;

/// Per-item result inside a transfer report.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TransferReportItem {
    pub source: String,
    pub target: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    #[serde(skip)]
    pub outcome: Outcome,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Succeeded,
    Skipped,
    Failed,
    Blocked,
}

impl Outcome {
    /// The one engine [`ItemOutcome`](rstore_transfer::ItemOutcome) → report
    /// outcome mapping, detail riding along — engine runs and copy/move
    /// consume it so the skip/blocked surfacing cannot fork per site.
    pub fn from_engine(item: &rstore_transfer::ItemOutcome) -> (Self, Option<String>) {
        if item.skipped {
            (Self::Skipped, item.detail.clone())
        } else if item.blocked {
            (Self::Blocked, item.detail.clone())
        } else {
            (Self::Succeeded, None)
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TransferReport {
    pub job_id: JobId,
    pub kind: &'static str,
    pub items: Vec<TransferReportItem>,
    pub succeeded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub blocked: usize,
    pub state: &'static str,
    pub dry_run: bool,
}

impl TransferReport {
    /// The run ended via Ctrl+C — the exit-130 trigger (CLI-013). The one
    /// interpretation of the state string, so a spelling change on the
    /// job-run side cannot silently break the exit code.
    pub fn is_interrupted(&self) -> bool {
        self.state == "interrupted"
    }
}
