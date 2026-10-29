//! The `doctor` health report.
//!
//! Split from connection diagnostics ([`crate::diagnostics`]) so each side is a deep module:
//! diagnostics owns probing a connection; health owns the whole-catalog snapshot behind
//! doctor. The narrow stats port keeps the service's interface as wide as its questions,
//! including where the catalog is stored.

use std::sync::Arc;

use rstore_application_ports::RuntimeStats;
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
    pub tls_default: &'static str,
    pub telemetry: &'static str,
    pub interrupted_jobs: u64,
    pub pending_outbox: u64,
    pub pending_repairs: u64,
}

/// Collects the doctor health report.
///
/// Counting-only by contract: its sole dependency is the [`RuntimeStats`] port, counts stay
/// counts (no entity lists cross this seam), and the catalog location comes from the same port.
pub(crate) struct HealthService {
    stats: Arc<dyn RuntimeStats>,
}

impl HealthService {
    pub(crate) fn new(stats: Arc<dyn RuntimeStats>) -> Self {
        Self { stats }
    }

    /// Gather every health field. Individual probes degrade to zero-count defaults rather than
    /// failing the whole report — a broken probe is itself a finding, and the schema-mismatch
    /// check still renders.
    pub(crate) async fn health(&self, storage_adapter: Option<&'static str>) -> HealthReport {
        let storage_adapter = storage_adapter.unwrap_or("none");
        let pending_outbox = self.stats.count_pending_outbox().await.unwrap_or(0);
        let pending_repairs = self.stats.count_pending_repairs().await.unwrap_or(0);
        let connection_count = self.stats.count_connections().await.unwrap_or(0) as usize;
        let untested_connections =
            self.stats.count_untested_connections().await.unwrap_or(0) as usize;
        let interrupted_jobs = self.stats.count_interrupted_jobs().await.unwrap_or(0);

        HealthReport {
            connection_count,
            untested_connections,
            db_path: self.stats.location(),
            storage_adapter,
            tls_default: "on (SEC-003)",
            telemetry: "disabled (SEC-009)",
            interrupted_jobs,
            pending_outbox,
            pending_repairs,
        }
    }
}
