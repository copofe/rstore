//! Serializable per-run outcome report for transfer jobs: one row per item
//! plus aggregate counts, suitable for JSON output. Items speak the unified
//! [`Disposition`] vocabulary, and the report aggregates into a [`Severity`]
//! so entry layers never re-derive "did this batch succeed" from counts.

use crate::batch_outcome::{count_dispositions, severity_of, Disposition, OutcomeCounts, Severity};
use rstore_domain::job::JobState;
use rstore_domain::JobId;
use serde::ser::{Serialize, SerializeStruct, Serializer};

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
#[derive(Debug, Clone)]
pub struct TransferReport {
    pub job_id: JobId,
    pub kind: &'static str,
    pub items: Vec<TransferReportItem>,
    pub state: JobState,
    pub dry_run: bool,
}

impl TransferReport {
    fn counts(&self) -> OutcomeCounts {
        count_dispositions(self.items.iter().map(|item| &item.disposition))
    }

    /// Items that completed as requested.
    pub fn succeeded(&self) -> usize {
        self.counts().succeeded
    }

    /// Items skipped by policy or blocked before execution.
    pub fn skipped(&self) -> usize {
        self.counts().skipped
    }

    /// Items that failed.
    pub fn failed(&self) -> usize {
        self.counts().failed
    }

    /// Items blocked before execution.
    pub fn blocked(&self) -> usize {
        self.counts().blocked
    }

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

impl Serialize for TransferReport {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let counts = self.counts();
        let mut out = serializer.serialize_struct("TransferReport", 9)?;
        out.serialize_field("job_id", &self.job_id)?;
        out.serialize_field("kind", &self.kind)?;
        out.serialize_field("items", &self.items)?;
        out.serialize_field("succeeded", &counts.succeeded)?;
        out.serialize_field("skipped", &counts.skipped)?;
        out.serialize_field("failed", &counts.failed)?;
        out.serialize_field("blocked", &counts.blocked)?;
        out.serialize_field("state", &self.state)?;
        out.serialize_field("dry_run", &self.dry_run)?;
        out.end()
    }
}
