//! Connection diagnostics.
//!
//! Split from connection lifecycle so each side is a deep module: lifecycle owns CRUD and
//! credential storage; diagnostics owns probing (test report, write probe) and the cached
//! capability report. The diagnostics service's dependencies — gateway and settings — are
//! required, not optional: a build without an adapter cannot diagnose.

use std::sync::Arc;

use bytes::Bytes;
use rstore_application_ports::{ConnectionStore, SettingsStore};
use rstore_domain::connection::ConnectionTestStatus;
use rstore_domain::{Clock, ErrorCode, Result, TypedId};
use rstore_storage_api::capability::Capability;
use rstore_storage_api::model::PutOptions;
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

/// Capability report for `connection show --capabilities`.
#[derive(Debug, Clone, Serialize)]
pub struct CapabilityReport {
    pub connection: String,
    #[serde(with = "time::serde::rfc3339")]
    pub probed_at: OffsetDateTime,
    /// Feature name → four-state capability.
    pub entries: Vec<(String, Capability)>,
}

/// Probes connections and caches the outcomes.
///
/// Test reports persist the outcome on the connection row; capability reports are cached in
/// the settings store for 24 hours before a fresh probe runs.
pub struct ConnectionDiagnostics {
    repo: Arc<dyn ConnectionStore>,
    gateway: RemoteGateway,
    settings: Arc<dyn SettingsStore>,
    clock: Arc<dyn Clock>,
}

impl ConnectionDiagnostics {
    pub fn new(
        repo: Arc<dyn ConnectionStore>,
        gateway: RemoteGateway,
        settings: Arc<dyn SettingsStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            repo,
            gateway,
            settings,
            clock,
        }
    }

    /// Probe the connection and persist the outcome.
    ///
    /// Checks run in order: stored credentials, endpoint/auth plus ListBuckets, then the
    /// optional write probe (a temporary object that must be cleaned up). An adapter that
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

        let buckets = match storage.list_buckets().await {
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
                buckets
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
                Vec::new()
            }
        };

        // 3. Optional write test against the first visible bucket: a temporary object is
        //    created and must be cleaned up.
        if write_test {
            let write = match buckets.first() {
                Some(bucket) => {
                    // A generated probe key — a failure to parse it is a bug in this format,
                    // not user input.
                    let key = rstore_domain::remote::ObjectPath::new(format!(
                        ".rstore-write-test/{}",
                        uuid::Uuid::new_v4().simple()
                    ))
                    .expect("write-probe key");
                    let body = Bytes::from_static(b"rstore connection write test");
                    let put_result = storage
                        .put(
                            &bucket.name,
                            &key,
                            body,
                            &PutOptions {
                                content_type: Some("text/plain".into()),
                                ..Default::default()
                            },
                        )
                        .await;
                    let outcome = match put_result {
                        Ok(_) => storage.delete(&bucket.name, &key).await,
                        Err(e) => Err(e),
                    };
                    match outcome {
                        Ok(_) => CheckResult {
                            name: "write_test".into(),
                            passed: true,
                            detail: format!(
                                "put+delete succeeded in `{}` (temporary object cleaned up)",
                                bucket.name
                            ),
                        },
                        Err(e) => CheckResult {
                            name: "write_test".into(),
                            passed: false,
                            detail: e.to_string(),
                        },
                    }
                }
                None => CheckResult {
                    name: "write_test".into(),
                    passed: false,
                    detail: "no visible bucket to write into".into(),
                },
            };
            checks.push(write);
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

    /// Resolve capabilities for a connection, probing and caching when stale.
    ///
    /// A complete cached probe younger than 24 hours is reused as-is; incomplete or invalid
    /// cache data counts as stale and triggers a fresh probe.
    pub async fn capabilities(&self, reference: &ConnectionRef) -> Result<CapabilityReport> {
        let config = self.gateway.config(reference).await?;
        let cache_key = format!("capabilities/{}", config.id.as_uuid());

        if let Some(cached) = self.settings.get_json(&cache_key).await? {
            if let Some(probed) = ProbedCapabilities::from_cache(&cached) {
                let age = self.clock.now().unix_timestamp() - probed.probed_at.unix_timestamp();
                if (0..24 * 3600).contains(&age) {
                    return Ok(CapabilityReport {
                        connection: config.name.clone(),
                        probed_at: probed.probed_at,
                        entries: probed.entries(),
                    });
                }
            }
        }

        // Probe: every entry is a real round trip against the live adapter — none is inferred
        // from the provider's name. An adapter that cannot even be constructed classifies
        // uniformly.
        let probed = match self.gateway.storage_for_config(&config).await {
            Ok(storage) => probe(storage.as_ref(), self.clock.now()).await,
            Err(e) => ProbedCapabilities::uniform(self.clock.now(), classify(e.code)),
        };
        self.settings
            .set_json(&cache_key, &probed.cache_payload())
            .await?;

        Ok(CapabilityReport {
            connection: config.name.clone(),
            probed_at: probed.probed_at,
            entries: probed.entries(),
        })
    }
}

/// The probed capability set behind [`CapabilityReport`].
///
/// One listing decides `list_buckets` and supplies the bucket the write-shaped probes run
/// against; multipart and copy probe with a throwaway object under `.rstore-write-test/`,
/// presign signs locally.
#[derive(Debug, Clone, Copy)]
struct ProbedCapabilities {
    probed_at: OffsetDateTime,
    list_buckets: Capability,
    multipart_upload: Capability,
    server_side_copy: Capability,
    presign: Capability,
}

impl ProbedCapabilities {
    fn uniform(probed_at: OffsetDateTime, state: Capability) -> Self {
        Self {
            probed_at,
            list_buckets: state,
            multipart_upload: state,
            server_side_copy: state,
            presign: state,
        }
    }

    fn entries(&self) -> Vec<(String, Capability)> {
        vec![
            ("list_buckets".to_string(), self.list_buckets),
            ("multipart_upload".to_string(), self.multipart_upload),
            ("server_side_copy".to_string(), self.server_side_copy),
            ("presign".to_string(), self.presign),
        ]
    }

    fn cache_payload(self) -> serde_json::Value {
        serde_json::json!({
            "probed_at": self.probed_at.unix_timestamp(),
            "list_buckets": self.list_buckets.as_token(),
            "multipart_upload": self.multipart_upload.as_token(),
            "server_side_copy": self.server_side_copy.as_token(),
            "presign": self.presign.as_token(),
        })
    }

    /// `None` when any probed field is missing or invalid; the caller re-probes rather than
    /// returning an incomplete capability report.
    fn from_cache(value: &serde_json::Value) -> Option<Self> {
        let field = |name: &str| value.get(name)?.as_str().map(Capability::parse_token);
        Some(Self {
            probed_at: OffsetDateTime::from_unix_timestamp(value.get("probed_at")?.as_i64()?)
                .ok()?,
            list_buckets: field("list_buckets")?,
            multipart_upload: field("multipart_upload")?,
            server_side_copy: field("server_side_copy")?,
            presign: field("presign")?,
        })
    }
}

/// The one probe error classification: an adapter's honest `CapabilityUnsupported` reads as
/// unsupported, missing permissions as denied, anything else as inconclusive.
fn classify(code: ErrorCode) -> Capability {
    match code {
        ErrorCode::CapabilityUnsupported => Capability::Unsupported,
        ErrorCode::Unauthorized | ErrorCode::Forbidden => Capability::Denied,
        _ => Capability::Unknown,
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

async fn probe(
    storage: &dyn rstore_storage_api::Storage,
    at: OffsetDateTime,
) -> ProbedCapabilities {
    let (list_buckets, bucket) = match storage.list_buckets().await {
        Ok(buckets) => (
            Capability::Supported,
            buckets.first().map(|b| b.name.clone()),
        ),
        Err(e) => (classify(e.code), None),
    };
    let (multipart_upload, server_side_copy) = match &bucket {
        Some(bucket) => (
            probe_multipart(storage, bucket).await,
            probe_copy(storage, bucket).await,
        ),
        // Without a visible bucket the write-shaped probes are inconclusive; presign still
        // signs locally.
        None => (Capability::Unknown, Capability::Unknown),
    };
    let presign = probe_presign(storage).await;
    ProbedCapabilities {
        probed_at: at,
        list_buckets,
        multipart_upload,
        server_side_copy,
        presign,
    }
}

/// Create-and-abandon one multipart upload: the provider accepting the create is the
/// capability; teardown rides the session's own warn-only abandon.
async fn probe_multipart(storage: &dyn rstore_storage_api::Storage, bucket: &str) -> Capability {
    let key = probe_key("multipart");
    match storage
        .create_multipart(bucket, &key, &PutOptions::default())
        .await
    {
        Ok(session) => {
            session.abandon().await;
            Capability::Supported
        }
        Err(e) => classify(e.code),
    }
}

/// Copy one throwaway object within the first visible bucket: put, copy through the adapter's
/// copy path, clean both ends up. Adapters riding the read+put fallback also succeed — the
/// entry reports the copy path this connection delivers; an adapter that honestly lacks
/// server-side copy answers `CapabilityUnsupported` there.
async fn probe_copy(storage: &dyn rstore_storage_api::Storage, bucket: &str) -> Capability {
    let source = probe_key("copy");
    let target = probe_key("copy-target");
    let outcome = match storage
        .put(
            bucket,
            &source,
            Bytes::from_static(b"rstore capability probe"),
            &PutOptions::default(),
        )
        .await
    {
        Ok(_) => {
            storage
                .copy(&rstore_storage_api::model::CopyRequest {
                    source_bucket: bucket.to_string(),
                    source_key: source.clone(),
                    target_bucket: bucket.to_string(),
                    target_key: target.clone(),
                })
                .await
        }
        Err(e) => Err(e),
    };
    if let Err(error) = storage.delete(bucket, &source).await {
        tracing::warn!(
            error = %error,
            bucket,
            key = source.as_str(),
            "capability probe could not clean up source object"
        );
    }
    if let Err(error) = storage.delete(bucket, &target).await {
        tracing::warn!(
            error = %error,
            bucket,
            key = target.as_str(),
            "capability probe could not clean up target object"
        );
    }
    match outcome {
        Ok(_) => Capability::Supported,
        Err(e) => classify(e.code),
    }
}

/// Presign one throwaway GET: signing is local, so no request leaves the process.
async fn probe_presign(storage: &dyn rstore_storage_api::Storage) -> Capability {
    match storage
        .presign(&rstore_storage_api::model::PresignRequest {
            bucket: ".rstore-write-test".to_string(),
            key: "probe".to_string(),
            method: rstore_storage_api::model::PresignMethod::Get,
            ttl_seconds: 60,
        })
        .await
    {
        Ok(_) => Capability::Supported,
        Err(e) => classify(e.code),
    }
}
