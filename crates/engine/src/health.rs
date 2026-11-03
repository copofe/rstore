//! The `doctor` health report.
//!
//! Split from connection diagnostics ([`crate::diagnostics`]) so each side is a deep module:
//! diagnostics owns probing a connection; health owns the whole-store snapshot behind
//! doctor.

use std::sync::Arc;

use serde::Serialize;

/// The full health payload behind `doctor`. The CLI renders it and nothing else — it never
/// assembles health fields itself.
#[derive(Debug, Clone, Serialize)]
pub struct HealthReport {
    pub connection_count: usize,
    /// Human-text only; deliberately not part of the JSON envelope.
    #[serde(skip)]
    pub untested_connections: usize,
    pub db_path: String,
    pub storage_adapter: &'static str,
    pub interrupted_jobs: u64,
    /// Cached rows the last full index scan no longer saw.
    pub stale_objects: u64,
}

/// Collects the doctor health report.
///
/// Counting-only by contract: counts stay counts (no entity lists cross this seam), and the
/// database location comes from the store itself.
pub(crate) struct HealthService {
    stats: Arc<crate::db::Database>,
    storage_adapter: &'static str,
}

impl HealthService {
    pub(crate) fn new(stats: Arc<crate::db::Database>, storage_adapter: &'static str) -> Self {
        Self {
            stats,
            storage_adapter,
        }
    }

    /// Gather every health field. Individual probes degrade to zero-count defaults rather than
    /// failing the whole report — a broken probe is itself a finding, and the schema-mismatch
    /// check still renders.
    pub(crate) async fn health(&self) -> HealthReport {
        let stale_objects = self.stats.count_stale_objects().await.unwrap_or(0);
        let connection_count = self.stats.count_connections().await.unwrap_or(0) as usize;
        let untested_connections =
            self.stats.count_untested_connections().await.unwrap_or(0) as usize;
        let interrupted_jobs = self.stats.count_interrupted_jobs().await.unwrap_or(0);

        HealthReport {
            connection_count,
            untested_connections,
            db_path: self.stats.location(),
            storage_adapter: self.storage_adapter,
            interrupted_jobs,
            stale_objects,
        }
    }
}
