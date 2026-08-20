//! One renderer for batch per-item results.
//!
//! Every command that reports one line per URI — upload/download, cp/mv, rm —
//! renders through [`BatchLines`]: a padded marker, the item label, an
//! indented detail line. Marker text comes from [`Outcome::marker`] /
//! [`RemoveOutcome::marker`], and the exit-13 partial-failure policy
//! (DEL-011 / PRD §11.3) comes from [`with_partial_failure`], so a new batch
//! command gets the same grammar and exit semantics for free. The JSON
//! envelopes stay per-command — they are view types at the service seam, not
//! one shape.

use rstore_application::{Outcome, RemoveOutcome, TransferReportItem};

use crate::output::HandlerOutput;

use super::HandlerResult;

/// Marker text for a copy/move/transfer item outcome.
pub(super) fn outcome_marker(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Succeeded => "ok",
        Outcome::Skipped => "skipped",
        Outcome::Failed => "failed",
        Outcome::Blocked => "blocked",
    }
}

/// Marker text for an rm item outcome.
pub(super) fn remove_marker(outcome: RemoveOutcome) -> &'static str {
    match outcome {
        RemoveOutcome::Deleted => "deleted",
        RemoveOutcome::WouldDelete => "would-delete",
        RemoveOutcome::GuardBlocked => "blocked",
        RemoveOutcome::Failed => "failed",
    }
}

/// Accumulates `  {marker:<width}} {label}` lines with indented details.
pub(super) struct BatchLines {
    buf: String,
    marker_width: usize,
    detail_indent: usize,
}

impl BatchLines {
    pub fn new(marker_width: usize, detail_indent: usize) -> Self {
        Self {
            buf: String::new(),
            marker_width,
            detail_indent,
        }
    }

    /// One item line; a `detail` (when the command shows it) indents under
    /// the label.
    pub fn push(&mut self, marker: &str, label: &str, detail: Option<&str>) {
        self.buf.push_str(&format!(
            "  {:<width$} {}\n",
            marker,
            label,
            width = self.marker_width
        ));
        if let Some(detail) = detail {
            self.buf.push_str(&format!(
                "{:indent$}{detail}\n",
                "",
                indent = self.detail_indent
            ));
        }
    }

    /// The rendered lines (without any trailing summary).
    pub fn finish(self) -> String {
        self.buf
    }
}

/// Apply the partial-failure exit policy: some items failed and some
/// succeeded → exit 13 while still printing every per-item result (DEL-011).
pub(super) fn with_partial_failure(
    mut out: HandlerOutput,
    failed: usize,
    succeeded: usize,
) -> HandlerOutput {
    if failed > 0 && succeeded > 0 {
        out = out.with_exit_code(13);
    }
    out
}

/// One rendered batch line, in the generic shape every per-item report maps
/// its outcomes onto — the grammar's currency between command and renderer.
pub(super) struct BatchEntry {
    pub marker: String,
    pub label: String,
    pub detail: Option<String>,
}

/// Batch-level facts the grammar needs beyond its lines.
pub(super) struct BatchFacts {
    /// The closing summary line (omitted when the dry-run banner says it all).
    pub summary: Option<String>,
    pub interrupted: bool,
    pub failed: usize,
    pub succeeded: usize,
}

/// One batch-report grammar: the optional dry-run banner, one
/// [`BatchLines`] line per entry, an optional summary line, and the exit
/// policy — 130 with `interrupt_hint` when the run was interrupted, else the
/// partial-failure 13. Transfer-shaped callers go through [`render_batch`];
/// rm and tag map their outcomes onto [`BatchEntry`] directly. (`url` is a
/// deliberate exception: its human output is one pipeable link per line, not
/// a per-item report.)
pub(super) fn render_entries(
    data: serde_json::Value,
    dry_banner: Option<&str>,
    entries: impl IntoIterator<Item = BatchEntry>,
    interrupt_hint: &str,
    facts: &BatchFacts,
) -> HandlerResult {
    let mut human = String::new();
    if let Some(banner) = dry_banner {
        human.push_str(banner);
        human.push('\n');
    }
    let mut lines = BatchLines::new(8, 11);
    for entry in entries {
        lines.push(&entry.marker, &entry.label, entry.detail.as_deref());
    }
    human.push_str(&lines.finish());
    if let Some(summary) = &facts.summary {
        human.push_str(summary);
        human.push('\n');
    }

    let mut out = HandlerOutput::human(human).with_data(data);
    if facts.interrupted {
        out = out
            .with_warning(format!("interrupted (Ctrl+C) — {interrupt_hint}"))
            // CLI-013: SIGINT exits 130 after the envelope is emitted.
            .with_exit_code(130);
    } else {
        out = with_partial_failure(out, facts.failed, facts.succeeded);
    }
    Ok(out)
}

/// The transfer-shaped arm of [`render_entries`]: upload/download, batch
/// download and cp/mv map their [`TransferReportItem`]s and the checkpoint
/// resume hint in.
pub(super) fn render_batch<'a>(
    data: serde_json::Value,
    dry_banner: Option<&str>,
    items: impl IntoIterator<Item = &'a TransferReportItem>,
    summary: String,
    interrupted: bool,
    failed: usize,
    succeeded: usize,
) -> HandlerResult {
    let entries = items.into_iter().map(|item| BatchEntry {
        marker: outcome_marker(item.outcome).to_string(),
        label: format!("{} → {}", item.source, item.target),
        detail: item.detail.clone(),
    });
    render_entries(
        data,
        dry_banner,
        entries,
        "checkpoints saved; re-run to resume",
        &BatchFacts {
            summary: Some(summary),
            interrupted,
            failed,
            succeeded,
        },
    )
}
