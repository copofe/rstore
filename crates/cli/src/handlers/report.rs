//! One renderer for batch per-item results.
//!
//! Every command that reports one line per URI — upload/download, cp/mv,
//! rm, tag attach/detach — renders through [`BatchLines`]: a padded marker,
//! the item label, an indented detail line. Marker text comes from
//! `outcome_marker` / `remove_marker`, and the exit policy lives here too:
//! any failed item exits 13 while every per-item result is still printed,
//! and an interrupted run exits 130 after the envelope is emitted. A new
//! batch command inherits the same grammar and exit semantics for free.
//! The JSON envelopes stay per-command — they are view types at the service
//! seam, not one shape.

use rstore_engine::{Disposition, Severity, SkipReason, TransferReportItem};

use crate::output::HandlerOutput;

use super::HandlerResult;

/// Marker text for a transfer-shaped item (upload/download/cp/mv): the
/// unified disposition renders to the same glyphs as before the vocabulary
/// existed.
pub(super) fn outcome_marker(disposition: &Disposition) -> &'static str {
    match disposition {
        Disposition::Succeeded => "ok",
        Disposition::Skipped(_) => "skipped",
        Disposition::Blocked(_) => "blocked",
        Disposition::Failed(_) => "failed",
    }
}

/// Marker text for an rm item: removals keep their deletion-flavoured
/// success and dry-run glyphs.
pub(super) fn remove_marker(disposition: &Disposition) -> &'static str {
    match disposition {
        Disposition::Succeeded => "deleted",
        Disposition::Skipped(SkipReason::DryRun) => "would-delete",
        Disposition::Skipped(_) | Disposition::Blocked(_) => "blocked",
        Disposition::Failed(_) => "failed",
    }
}

/// Accumulates `  {marker:<width} {label}` lines with indented details.
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

    /// Appends one item line; `detail`, when the command shows it, indents
    /// under the label.
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

/// Applies the severity policy from the unified batch vocabulary: a
/// `Severity::Failed` batch exits 13 while every per-item result is still
/// printed. The single home of "did this batch succeed" for every entry
/// layer — CLI exit codes here, desktop semantics from the same value.
pub(super) fn with_severity(mut out: HandlerOutput, severity: Severity) -> HandlerOutput {
    if severity == Severity::Failed {
        out = out.with_exit_code(crate::output::EXIT_ITEMS_FAILED);
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

pub(super) struct BatchFacts {
    /// The closing summary line (omitted when the dry-run banner says it all).
    pub summary: Option<String>,
    pub interrupted: bool,
    /// The batch verdict from the application core — the only input to the
    /// exit policy.
    pub severity: Severity,
}

/// Renders one batch report: the optional dry-run banner, one [`BatchLines`]
/// line per entry, an optional summary line, and the exit policy — 130 with
/// `interrupt_hint` when the run was interrupted, else the partial-failure 13.
/// Transfer-shaped callers go through [`render_batch`]; rm and tag map their
/// outcomes onto [`BatchEntry`] directly. (`url` is a deliberate exception:
/// its human output is one pipeable link per line, not a per-item report.)
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
            // SIGINT still prints the full envelope before exiting.
            .with_exit_code(crate::output::EXIT_INTERRUPTED);
    } else {
        out = with_severity(out, facts.severity);
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
    severity: Severity,
) -> HandlerResult {
    let entries = items.into_iter().map(|item| BatchEntry {
        marker: outcome_marker(&item.disposition).to_string(),
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
            severity,
        },
    )
}
