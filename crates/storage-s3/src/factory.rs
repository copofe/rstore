//! S3 client factory.
//!
//! [`S3StorageFactory`] implements the [`StorageFactory`] seam: it builds an
//! [`aws_sdk_s3::Client`] from a [`ConnectionConfig`] plus static credentials.
//! Region, endpoint and addressing style are honoured; TLS uses the explicit
//! provider/client policy selected by Bootstrap.

use std::sync::Arc;

use aws_sdk_s3::config::{BehaviorVersion, Credentials};
use aws_smithy_types::retry::RetryConfig;
use rstore_domain::connection::{AddressingStyle, ConnectionConfig, Region};
use rstore_storage_api::{Storage, StorageCredentials, StorageFactory};

use crate::S3Storage;

/// Ensure an endpoint with a URL path ends in `/` so the SDK appends the
/// bucket as a new path segment (`…/s3/<bucket>`, not `…/s3<bucket>`).
/// Host-only endpoints (`http://minio:9000`) pass through unchanged.
fn normalize_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    if trimmed.is_empty() {
        return endpoint.trim_end_matches('/').to_string();
    }
    // Path exists iff there is a `/` after the `://` authority.
    let after_scheme = trimmed
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(trimmed);
    if after_scheme.contains('/') {
        format!("{trimmed}/")
    } else {
        trimmed.to_string()
    }
}

/// Builds a configured S3 client for a connection. This is the one place
/// credential plaintext is exposed to the SDK (`expose()`).
pub(crate) fn build_client(
    config: &ConnectionConfig,
    creds: &StorageCredentials,
) -> aws_sdk_s3::Client {
    let mut builder = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        // Retries belong to one layer: every adapter the gateway hands out is
        // wrapped in `RetryingStorage`, which reads the transient signal from
        // the error classification. SDK retries underneath would multiply
        // the attempts and hide them from that policy.
        .retry_config(RetryConfig::disabled())
        .region(aws_sdk_s3::config::Region::new(region_string(
            &config.region,
        )))
        // An endpoint whose path doesn't end in `/` (e.g. Supabase's
        // `…/storage/v1/s3`) makes the SDK concatenate the bucket straight
        // onto the path (`…/s3<bucket>`), which providers route as 404.
        .endpoint_url(normalize_endpoint(&config.endpoint))
        .credentials_provider(Credentials::new(
            creds.access_key.expose(),
            creds.secret_key.expose(),
            None,
            None,
            "static",
        ));

    builder = match config.addressing_style {
        AddressingStyle::Path => builder.force_path_style(true),
        AddressingStyle::VirtualHost => builder.force_path_style(false),
        AddressingStyle::Auto => builder,
    };

    aws_sdk_s3::Client::from_conf(builder.build())
}

/// The S3-compatible [`StorageFactory`] (wired into the application by the
/// binary — the core never imports this crate directly).
#[derive(Debug, Default, Clone, Copy)]
pub struct S3StorageFactory;

impl S3StorageFactory {
    pub fn new() -> Self {
        Self
    }
}

impl StorageFactory for S3StorageFactory {
    fn name(&self) -> &'static str {
        "s3"
    }

    fn build(
        &self,
        config: &ConnectionConfig,
        creds: &StorageCredentials,
    ) -> rstore_domain::Result<Arc<dyn Storage>> {
        let client = build_client(config, creds);
        Ok(Arc::new(S3Storage::new(client)))
    }
}

fn region_string(region: &Region) -> String {
    match region {
        Region::Auto => "us-east-1".to_string(),
        Region::Specific(r) => r.clone(),
    }
}
