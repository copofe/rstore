//! The vocabulary of one transfer run: its configuration
//! ([`TransferConfig`]), its in-flight context ([`RunContext`]), and its
//! per-item conclusion ([`ItemOutcome`]).
//!
//! These live here — not in `engine` — because every entry (upload,
//! download, remote copy, relay) shares them; the engine is one consumer,
//! not their owner.

use crate::transfer::cancel::CancellationToken;
use crate::transfer::progress::ProgressSink;
use rstore_domain::outcome::{BlockReason, Disposition, SkipReason};
use rstore_storage::model::ObjectMetadata;

/// Tuning knobs for transfer runs.
#[derive(Debug, Clone, Copy)]
pub struct TransferConfig {
    /// Files at or above this size use multipart (default 8 MiB).
    pub multipart_threshold: u64,
    /// Part size for multipart uploads (default 8 MiB, S3 minimum 5 MiB).
    pub part_size: u64,
    /// Download chunk size for ranged reads (default 1 MiB).
    pub download_chunk: u64,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            multipart_threshold: 8 * 1024 * 1024,
            part_size: 8 * 1024 * 1024,
            download_chunk: 1024 * 1024,
        }
    }
}

/// The runtime context every transfer entry shares: cancellation, tuning,
/// and progress reporting. Constructed once per run and carried through
/// every layer (engine, copy entry, relay, pump) — no entry re-assembles
/// the trio, and no layer passes on pieces it does not own.
pub struct RunContext<'a> {
    cancel: &'a CancellationToken,
    config: TransferConfig,
    progress: &'a dyn ProgressSink,
}

impl<'a> RunContext<'a> {
    /// Assemble the context for one run.
    pub fn new(
        cancel: &'a CancellationToken,
        config: TransferConfig,
        progress: &'a dyn ProgressSink,
    ) -> Self {
        Self {
            cancel,
            config,
            progress,
        }
    }

    /// The run's cancellation token.
    pub fn cancel(&self) -> &CancellationToken {
        self.cancel
    }

    /// The run's tuning knobs.
    pub fn config(&self) -> &TransferConfig {
        &self.config
    }

    /// The run's progress sink.
    pub fn progress(&self) -> &dyn ProgressSink {
        self.progress
    }
}

/// Conclusion of one executed transfer item: the shared
/// [`Disposition`] classification plus the transfer-specific payload
/// (moved bytes and the landed object's adapter metadata).
#[derive(Debug, Clone)]
pub struct ItemOutcome {
    pub disposition: Disposition,
    /// Human detail for skipped/blocked items (the conflicting target or
    /// the block reason).
    pub detail: Option<String>,
    pub bytes: u64,
    /// Metadata returned by the adapter for the object that was transferred.
    /// Skipped/blocked items carry none.
    pub metadata: Option<ObjectMetadata>,
}

impl ItemOutcome {
    /// A completed transfer of `bytes`, with the adapter's metadata.
    pub fn transferred(bytes: u64, metadata: Option<ObjectMetadata>) -> Self {
        Self {
            disposition: Disposition::Succeeded,
            detail: None,
            bytes,
            metadata,
        }
    }

    /// The conflict policy left the existing target alone.
    pub fn skipped(detail: String) -> Self {
        Self {
            disposition: Disposition::Skipped(SkipReason::Conflict),
            detail: Some(detail),
            bytes: 0,
            metadata: None,
        }
    }

    /// A dry-run preview: fully resolved, deliberately not executed.
    pub fn preview(detail: String) -> Self {
        Self {
            disposition: Disposition::Skipped(SkipReason::DryRun),
            detail: Some(detail),
            bytes: 0,
            metadata: None,
        }
    }

    /// The conflict policy could not resolve (unresolved `ask`, or rename
    /// found no free name).
    pub fn blocked(detail: String) -> Self {
        Self {
            disposition: Disposition::Blocked(BlockReason::Unresolved),
            detail: Some(detail),
            bytes: 0,
            metadata: None,
        }
    }
}
