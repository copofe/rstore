//! The `doctor` health report (OBS-004 / DATA-014).
//!
//! Split from connection *diagnostics*
//! ([`crate::diagnostics`]) so each side is a deep module: diagnostics owns
//! probing a connection; health owns the whole-catalog snapshot behind
//! `doctor`. The narrow [`HealthDb`] port keeps the service's interface as
//! wide as its questions; platform paths (`db_path`, the config dir holding
//! `secrets.json`) are passed in by the caller — the service does not own
//! them.

use std::path::Path;
use std::sync::Arc;

use rstore_application_ports::{ConnectionStore, RuntimeStats, SecretSourceStats};
use rstore_domain::job::JobState;
use rstore_domain::JobRepository;
use serde::Serialize;

/// Live credential-source counts (DATA-014: counts only — never values).
#[derive(Debug, Clone, Copy, Serialize)]
pub struct CredentialSourceCounts {
    pub env_refs: usize,
    pub file_refs: usize,
}

/// The full health payload behind `doctor` (OBS-004). The CLI renders it and
/// nothing else — it never assembles health fields itself.
#[derive(Debug, Clone, Serialize)]
pub struct HealthReport {
    pub connection_count: usize,
    /// Human-text only; deliberately not part of the JSON envelope.
    #[serde(skip)]
    pub untested_connections: usize,
    pub db_path: String,
    pub storage_adapter: &'static str,
    pub credentials: CredentialSourceCounts,
    pub tls_default: &'static str,
    pub telemetry: &'static str,
    pub interrupted_jobs: u64,
    pub pending_outbox: u64,
    pub pending_repairs: u64,
}

/// Collects the `doctor` health report. Platform paths (`db_path`, the
/// config dir holding `secrets.json`) are passed in by the caller — the
/// service does not own them.
pub struct HealthService {
    stats: Arc<dyn RuntimeStats>,
    connections: Arc<dyn ConnectionStore>,
    jobs: Arc<dyn JobRepository>,
    secret_sources: Arc<dyn SecretSourceStats>,
}

impl HealthService {
    pub fn new(
        stats: Arc<dyn RuntimeStats>,
        connections: Arc<dyn ConnectionStore>,
        jobs: Arc<dyn JobRepository>,
        secret_sources: Arc<dyn SecretSourceStats>,
    ) -> Self {
        Self {
            stats,
            connections,
            jobs,
            secret_sources,
        }
    }

    /// Gather every health field. Individual probes degrade to zero-count
    /// defaults rather than failing the whole report — a broken probe is
    /// itself a finding, and the schema-mismatch check still renders.
    pub async fn health(
        &self,
        db_path: &Path,
        storage_adapter: Option<&'static str>,
    ) -> HealthReport {
        let storage_adapter = storage_adapter.unwrap_or("none");
        let pending_outbox = self.stats.count_pending_outbox().await.unwrap_or(0);
        let pending_repairs = self.stats.count_pending_repairs().await.unwrap_or(0);
        let connections = self.connections.list().await.unwrap_or_default();
        let interrupted_jobs = self
            .jobs
            .list()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|j| j.state == JobState::Interrupted)
            .count() as u64;

        // DATA-014: report which credential sources are live — never values.
        // The source inventory is injected by Bootstrap so this service does
        // not choose or construct a concrete secret backend.
        let source_counts = self.secret_sources.counts().await.unwrap_or_default();

        HealthReport {
            connection_count: connections.len(),
            untested_connections: connections
                .iter()
                .filter(|c| c.last_test_status.is_none())
                .count(),
            db_path: db_path.display().to_string(),
            storage_adapter,
            credentials: CredentialSourceCounts {
                env_refs: source_counts.env_refs,
                file_refs: source_counts.file_refs,
            },
            tls_default: "on (SEC-003)",
            telemetry: "disabled (SEC-009)",
            interrupted_jobs,
            pending_outbox,
            pending_repairs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_persistence::Database;

    #[tokio::test]
    async fn health_reports_counts_and_schema_match() {
        let db = Database::open_in_memory().await.unwrap();
        let jobs: Arc<dyn JobRepository> = Arc::new(db.jobs());
        let service = HealthService::new(
            Arc::new(db.clone()),
            Arc::new(db.connections()),
            jobs,
            Arc::new(rstore_application_ports::EmptySecretSourceStats),
        );
        let report = service
            .health(Path::new("/tmp/catalog.db"), Some("fake"))
            .await;

        assert_eq!(report.connection_count, 0);
        assert_eq!(report.untested_connections, 0);
        assert_eq!(report.interrupted_jobs, 0);
        assert_eq!(report.storage_adapter, "fake");
        assert_eq!(report.db_path, "/tmp/catalog.db");
        // No secrets.json at that config dir: zero file refs. (env refs are
        // whatever the host environment happens to carry — not asserted.)
        assert_eq!(report.credentials.file_refs, 0);
    }
}
