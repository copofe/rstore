//! Connection diagnostics.
//!
//! Split from connection lifecycle so each side is a deep module: lifecycle owns CRUD and
//! credential storage; diagnostics owns probing — the test report and its write probe.
//! Probes always run live: a diagnosis reflects the connection as it is now, not as it was
//! when last looked at. The write probe writes only into registered buckets that allow writes
//! and deletes, through the same guard real mutations use.

use std::sync::Arc;

use bytes::Bytes;
use rstore_domain::connection::ConnectionTestStatus;
use rstore_domain::{Clock, ErrorCode, Result};
use rstore_storage::model::PutOptions;
use rstore_storage::Storage;
use serde::Serialize;
use time::OffsetDateTime;

use crate::connection::ConnectionRef;
use crate::gateway::RemoteGateway;

/// One check inside a connection test.
#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

/// The full connection test report.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionTestReport {
    pub connection: String,
    pub checks: Vec<CheckResult>,
    pub write_test: bool,
    pub status: ConnectionTestStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub tested_at: OffsetDateTime,
}

impl ConnectionTestReport {
    fn overall(&self) -> ConnectionTestStatus {
        if self.checks.iter().all(|c| c.passed) {
            ConnectionTestStatus::Ok
        } else {
            ConnectionTestStatus::Failed
        }
    }
}

/// Probes connections; test outcomes persist on the connection row.
pub struct ConnectionDiagnostics {
    repo: Arc<crate::db::ConnectionRepository>,
    gateway: RemoteGateway,
    clock: Arc<dyn Clock>,
}

impl ConnectionDiagnostics {
    pub fn new(
        repo: Arc<crate::db::ConnectionRepository>,
        gateway: RemoteGateway,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            repo,
            gateway,
            clock,
        }
    }

    /// Probe the connection and persist the outcome.
    ///
    /// Checks run in order: stored credentials, endpoint/auth plus ListBuckets, then the
    /// optional write probe — a temporary object put and deleted in each registered bucket
    /// that allows both. An adapter that
    /// cannot even be constructed short-circuits to a failed report; the outcome is recorded
    /// on the connection either way.
    pub async fn test(
        &self,
        reference: &ConnectionRef,
        write_test: bool,
    ) -> Result<ConnectionTestReport> {
        let config = self.gateway.config(reference).await?;
        let now = self.clock.now();
        let mut checks = Vec::new();

        // 1. Credentials present on the connection.
        let creds_ok = config.access_key.is_some() && config.secret_key.is_some();
        checks.push(CheckResult {
            name: "credentials".into(),
            passed: creds_ok,
            detail: if creds_ok {
                "stored on connection".into()
            } else {
                "no credentials stored on connection".into()
            },
        });

        // 2. Adapter + endpoint + auth + ListBuckets in one probe.
        let storage = match self.gateway.storage_for_config(&config).await {
            Ok(storage) => storage,
            Err(e) => {
                checks.push(CheckResult {
                    name: "endpoint/auth".into(),
                    passed: false,
                    detail: e.to_string(),
                });
                let report = ConnectionTestReport {
                    connection: config.name.clone(),
                    checks,
                    write_test,
                    status: ConnectionTestStatus::Failed,
                    tested_at: now,
                };
                self.repo.record_test(config.id, report.status, now).await?;
                return Ok(report);
            }
        };

        match storage.list_buckets().await {
            Ok(buckets) => {
                checks.push(CheckResult {
                    name: "endpoint/auth".into(),
                    passed: true,
                    detail: format!("authenticated against {}", config.endpoint),
                });
                checks.push(CheckResult {
                    name: "list_buckets".into(),
                    passed: true,
                    detail: format!("{} bucket(s) visible", buckets.len()),
                });
            }
            Err(e) => {
                // Only a refused listing proves the endpoint answered and accepted the
                // credentials; every other failure — unreachable, rejected credentials,
                // unknown — fails the endpoint/auth check too.
                let authenticated = e.code == ErrorCode::Forbidden;
                checks.push(CheckResult {
                    name: "endpoint/auth".into(),
                    passed: authenticated,
                    detail: if authenticated {
                        format!(
                            "authenticated against {}; listing is not permitted",
                            config.endpoint
                        )
                    } else {
                        e.to_string()
                    },
                });
                checks.push(CheckResult {
                    name: "list_buckets".into(),
                    passed: false,
                    detail: e.to_string(),
                });
            }
        }

        // 3. Optional write test: a temporary object put and deleted in every registered
        //    bucket that allows both; read-only and delete-forbidden buckets are never touched.
        if write_test {
            let targets = self.gateway.probe_targets(&config, &storage).await?;
            if targets.is_empty() {
                checks.push(CheckResult {
                    name: "write_test".into(),
                    passed: false,
                    detail: "no registered bucket allows writes and deletes; nothing was written"
                        .into(),
                });
            }
            for (bucket, guarded) in &targets {
                checks.push(write_probe(guarded.as_ref(), &bucket.name).await);
            }
        }

        let report = ConnectionTestReport {
            connection: config.name.clone(),
            checks,
            write_test,
            status: ConnectionTestStatus::Ok,
            tested_at: now,
        };
        let status = report.overall();
        let report = ConnectionTestReport { status, ..report };
        self.repo.record_test(config.id, status, now).await?;
        Ok(report)
    }
}

/// A throwaway probe key under the established write-test prefix; a generated key failing to
/// parse is a bug in this format, not input.
fn probe_key(purpose: &str) -> rstore_domain::remote::ObjectPath {
    rstore_domain::remote::ObjectPath::new(format!(
        ".rstore-write-test/{purpose}-{}",
        uuid::Uuid::new_v4().simple()
    ))
    .expect("generated probe key")
}

/// Put and delete one temporary object in `bucket`.
async fn write_probe(storage: &dyn Storage, bucket: &str) -> CheckResult {
    let key = probe_key("write");
    let body = Bytes::from_static(b"rstore connection write test");
    let options = PutOptions {
        content_type: Some("text/plain".into()),
        ..Default::default()
    };
    let outcome = match storage.put(bucket, &key, body, &options).await {
        Ok(_) => storage.delete(bucket, &key).await,
        Err(e) => Err(e),
    };
    match outcome {
        Ok(_) => CheckResult {
            name: "write_test".into(),
            passed: true,
            detail: format!("put+delete succeeded in `{bucket}` (temporary object cleaned up)"),
        },
        Err(e) => CheckResult {
            name: "write_test".into(),
            passed: false,
            detail: format!("`{bucket}`: {e}"),
        },
    }
}
