//! Connection diagnostics (CON-007..009 / STORE-022).
//!
//! Split from connection *lifecycle* ([`crate::connection`]) so each side is a
//! deep module: lifecycle owns CRUD and credential storage; diagnostics owns
//! probing (test report, write probe) and the cached capability report. The
//! diagnostics service's dependencies — gateway, settings, secrets — are
//! required, not optional: a build without an adapter cannot diagnose.

use std::sync::Arc;

use bytes::Bytes;
use rstore_application_ports::{ConnectionStore, SettingsStore};
use rstore_domain::connection::ConnectionTestStatus;
use rstore_domain::{Clock, ErrorCode, Result, TypedId};
use rstore_secrets::SecretStore;
use rstore_storage_api::capability::Capability;
use rstore_storage_api::model::PutOptions;
use serde::Serialize;
use time::OffsetDateTime;

use crate::connection::ConnectionRef;
use crate::gateway::RemoteGateway;

/// One check inside a connection test (CON-007).
#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

/// The full connection test report (CON-007/008).
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

/// Capability report for `connection show --capabilities` (CON-009).
#[derive(Debug, Clone, Serialize)]
pub struct CapabilityReport {
    pub connection: String,
    #[serde(with = "time::serde::rfc3339")]
    pub probed_at: OffsetDateTime,
    /// Feature name → four-state capability.
    pub entries: Vec<(String, Capability)>,
}

/// Probes connections and caches the outcomes.
pub struct ConnectionDiagnostics {
    repo: Arc<dyn ConnectionStore>,
    secrets: Arc<dyn SecretStore>,
    gateway: RemoteGateway,
    settings: Arc<dyn SettingsStore>,
    clock: Arc<dyn Clock>,
}

impl ConnectionDiagnostics {
    pub fn new(
        repo: Arc<dyn ConnectionStore>,
        secrets: Arc<dyn SecretStore>,
        gateway: RemoteGateway,
        settings: Arc<dyn SettingsStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            repo,
            secrets,
            gateway,
            settings,
            clock,
        }
    }

    /// Whether the credential chain can currently resolve `secret_ref` —
    /// the `connection add` warning heuristic. Only a hint: env vars may
    /// appear later, so an unresolvable ref is a warning, never an error.
    /// Write a raw capability-cache payload (test seam for cache-format
    /// migration).
    #[cfg(test)]
    async fn settings_set_for_test(
        &self,
        id: &rstore_domain::ConnectionId,
        value: serde_json::Value,
    ) {
        self.settings
            .set_json(&format!("capabilities/{}", id.as_uuid()), &value)
            .await
            .unwrap();
    }

    pub async fn secret_ref_resolvable(&self, secret_ref: &str) -> bool {
        rstore_secrets::get_credentials(self.secrets.as_ref(), secret_ref)
            .await
            .unwrap_or(None)
            .is_some()
    }

    /// Probe the connection and persist the outcome (CON-007/008).
    pub async fn test(
        &self,
        reference: &ConnectionRef,
        write_test: bool,
    ) -> Result<ConnectionTestReport> {
        let config = self.gateway.config(reference).await?;
        let now = self.clock.now();
        let mut checks = Vec::new();

        // 1. Credentials present in the store chain.
        let creds_ok = match &config.secret_ref {
            Some(secret_ref) => {
                rstore_secrets::get_credentials(self.secrets.as_ref(), secret_ref.as_str())
                    .await?
                    .is_some()
            }
            None => false,
        };
        checks.push(CheckResult {
            name: "credentials".into(),
            passed: creds_ok,
            detail: if creds_ok {
                format!(
                    "stored under `{}`",
                    config
                        .secret_ref
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                )
            } else {
                "no credentials resolved (env / config file)".into()
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
                checks.push(CheckResult {
                    name: "endpoint/auth".into(),
                    passed: e.code != ErrorCode::NetworkUnreachable,
                    detail: e.to_string(),
                });
                checks.push(CheckResult {
                    name: "list_buckets".into(),
                    passed: false,
                    detail: e.to_string(),
                });
                Vec::new()
            }
        };

        // 3. Optional write test against the first visible bucket (CON-008):
        //    a temporary object is created and must be cleaned up.
        if write_test {
            let write = match buckets.first() {
                Some(bucket) => {
                    // A generated probe key — a failure to parse it is a bug
                    // in this format, not user input.
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

    /// Resolve capabilities for a connection, probing and caching when stale
    /// (CON-009 / STORE-022).
    pub async fn capabilities(&self, reference: &ConnectionRef) -> Result<CapabilityReport> {
        let config = self.gateway.config(reference).await?;
        let cache_key = format!("capabilities/{}", config.id.as_uuid());

        // STORE-022: reuse a complete cached probe from the last 24 hours.
        // Incomplete or invalid cache data is stale and is replaced by a
        // fresh probe.
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

        // Probe: every entry is a real round trip against the live adapter —
        // none is inferred from the provider's name (PRD §10.2.2). An
        // adapter that cannot even be constructed classifies uniformly.
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

/// The probed capability set behind [`CapabilityReport`] (CON-009 /
/// STORE-022). One listing decides `list_buckets` and supplies the bucket
/// the write-shaped probes run against; multipart and copy probe with a
/// throwaway object under `.rstore-write-test/`, presign signs locally.
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

    /// `None` when any probed field is missing or invalid; the caller re-probes
    /// rather than returning an incomplete capability report.
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

/// The one probe error classification: an adapter's honest
/// `CapabilityUnsupported` reads as unsupported, missing permissions as
/// denied, anything else as inconclusive.
fn classify(code: ErrorCode) -> Capability {
    match code {
        ErrorCode::CapabilityUnsupported => Capability::Unsupported,
        ErrorCode::Unauthorized | ErrorCode::Forbidden => Capability::Denied,
        _ => Capability::Unknown,
    }
}

/// A throwaway probe key under the established write-test prefix; a
/// generated key failing to parse is a bug in this format, not input.
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
        // Without a visible bucket the write-shaped probes are inconclusive;
        // presign still signs locally.
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

/// Create-and-abort one multipart upload: the provider accepting the create
/// is the capability; the abort is best-effort cleanup.
async fn probe_multipart(storage: &dyn rstore_storage_api::Storage, bucket: &str) -> Capability {
    let key = probe_key("multipart");
    match storage
        .create_multipart(bucket, &key, &PutOptions::default())
        .await
    {
        Ok(upload_id) => {
            if let Err(error) = storage.abort_multipart(bucket, &key, &upload_id).await {
                tracing::warn!(
                    error = %error,
                    bucket,
                    key = key.as_str(),
                    upload_id,
                    "capability probe could not clean up multipart upload"
                );
            }
            Capability::Supported
        }
        Err(e) => classify(e.code),
    }
}

/// Copy one throwaway object within the first visible bucket (STORE-016):
/// put, copy through the adapter's copy path, clean both ends up. Adapters
/// riding the read+put fallback also succeed — the entry reports the copy
/// path this connection delivers; an adapter that honestly lacks server-side
/// copy answers `CapabilityUnsupported` there.
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
        Ok(meta) => {
            storage
                .copy(
                    &rstore_storage_api::model::CopyRequest {
                        source_bucket: bucket.to_string(),
                        source_key: source.clone(),
                        target_bucket: bucket.to_string(),
                        target_key: target.clone(),
                    },
                    &meta,
                )
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

/// Presign one throwaway GET: signing is local, so no request leaves the
/// process (LINK-005).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{AddConnectionInput, ConnectionService as _, DefaultConnectionService};
    use rstore_domain::SystemClock;
    use rstore_persistence::Database;
    use rstore_secrets::{put_credentials, InMemorySecretStore};
    use rstore_test_support::FakeStorageFactory;

    async fn setup() -> (ConnectionDiagnostics, DefaultConnectionService) {
        let db = Database::open_in_memory().await.unwrap();
        let secrets = Arc::new(InMemorySecretStore::new());
        put_credentials(secrets.as_ref(), "conn_nas", "AK", "SK")
            .await
            .unwrap();
        let factory = Arc::new(FakeStorageFactory::new());
        factory.storage().seed_bucket("media");
        let clock = Arc::new(SystemClock);
        let diag = ConnectionDiagnostics::new(
            Arc::new(db.connections()),
            secrets.clone(),
            RemoteGateway::new(Arc::new(db.connections()), secrets.clone(), Some(factory)),
            Arc::new(db.settings()),
            clock.clone(),
        );
        let lifecycle = DefaultConnectionService::new(Arc::new(db.connections()), clock, secrets);
        (diag, lifecycle)
    }

    #[tokio::test]
    async fn test_reports_checks_and_persists_status() {
        let (diag, lifecycle) = setup().await;
        lifecycle
            .add(AddConnectionInput {
                name: "nas".into(),
                endpoint: "http://localhost:9000".into(),
                region: rstore_domain::Region::Auto,
                addressing_style: rstore_domain::AddressingStyle::Path,
                tls_verify: true,
                secret_ref: Some("conn_nas".into()),
                credentials: None,
            })
            .await
            .unwrap();

        let report = diag
            .test(&ConnectionRef::Name("nas".into()), false)
            .await
            .unwrap();
        assert_eq!(report.status, ConnectionTestStatus::Ok);
        assert!(report.checks.iter().all(|c| c.passed));

        // The outcome is visible through the lifecycle side (CON-010).
        let config = lifecycle
            .resolve(&ConnectionRef::Name("nas".into()))
            .await
            .unwrap();
        assert_eq!(config.last_test_status, Some(ConnectionTestStatus::Ok));

        let caps = diag
            .capabilities(&ConnectionRef::Name("nas".into()))
            .await
            .unwrap();
        // Every entry is a real probe now: the fake's copy path, multipart
        // create+abort and local presign all succeed against `media`.
        for (name, state) in &caps.entries {
            assert_eq!(*state, Capability::Supported, "{name}");
        }
    }

    /// An incomplete cache payload is discarded and replaced by a fresh probe.
    #[tokio::test]
    async fn incomplete_cache_reprobes() {
        let (diag, lifecycle) = setup().await;
        let config = lifecycle
            .add(AddConnectionInput {
                name: "nas".into(),
                endpoint: "http://localhost:9000".into(),
                region: rstore_domain::Region::Auto,
                addressing_style: rstore_domain::AddressingStyle::Path,
                tls_verify: true,
                secret_ref: Some("conn_nas".into()),
                credentials: None,
            })
            .await
            .unwrap();

        // Simulate a malformed cache: fresh timestamp, only two fields.
        diag.settings_set_for_test(
            &config.id,
            serde_json::json!({
                "probed_at": SystemClock.now().unix_timestamp(),
                "list_buckets": "supported",
            }),
        )
        .await;

        let caps = diag
            .capabilities(&ConnectionRef::Name("nas".into()))
            .await
            .unwrap();
        assert!(caps.entries.iter().any(|(name, state)| {
            name == "server_side_copy" && *state == Capability::Supported
        }));
    }

    #[tokio::test]
    async fn capabilities_reuses_fresh_cache_without_rebuilding_adapter() {
        let (diag, lifecycle) = setup().await;
        lifecycle
            .add(AddConnectionInput {
                name: "nas".into(),
                endpoint: "http://localhost:9000".into(),
                region: rstore_domain::Region::Auto,
                addressing_style: rstore_domain::AddressingStyle::Path,
                tls_verify: true,
                secret_ref: Some("conn_nas".into()),
                credentials: None,
            })
            .await
            .unwrap();

        let first = diag
            .capabilities(&ConnectionRef::Name("nas".into()))
            .await
            .unwrap();
        // STORE-022: the second call within 24h must come from the cache, so
        // the gateway never rebuilds the storage adapter.
        let second = diag
            .capabilities(&ConnectionRef::Name("nas".into()))
            .await
            .unwrap();
        assert_eq!(first.entries, second.entries);
        // The cached entry keeps whole seconds only; compare at that grain.
        assert_eq!(
            first.probed_at.unix_timestamp(),
            second.probed_at.unix_timestamp()
        );
    }
}
