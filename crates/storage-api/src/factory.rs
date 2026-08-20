//! The storage factory seam (STORE-001).
//!
//! The application core calls [`StorageFactory`] to turn a
//! [`ConnectionConfig`] plus resolved credentials into a connected
//! [`Storage`]. The concrete factory (S3) lives in `rstore-storage-s3` and is
//! injected by the binary — this crate stays free of any SDK dependency.

use std::sync::Arc;

use rstore_domain::connection::ConnectionConfig;
use rstore_domain::Result;
use rstore_secrets::Secret;

use crate::Storage;

/// Static credentials for authenticating against an endpoint. The fields are
/// [`Secret`] so the redaction invariant travels the seam by type: the
/// derived `Debug` prints redacted fields (SEC-001), and adapters call
/// `expose()` only where the SDK needs plaintext.
#[derive(Debug, Clone)]
pub struct StorageCredentials {
    pub access_key: Secret<String>,
    pub secret_key: Secret<String>,
}

/// Builds [`Storage`] adapters for connections.
pub trait StorageFactory: Send + Sync {
    /// Adapter identifier (e.g. `"s3"`) surfaced by `doctor`.
    fn name(&self) -> &'static str;

    /// Build a connected adapter for `config`.
    fn build(
        &self,
        config: &ConnectionConfig,
        creds: &StorageCredentials,
    ) -> Result<Arc<dyn Storage>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_credentials_debug_is_redacted() {
        let creds = StorageCredentials {
            access_key: Secret::new("AKIAEXAMPLE".to_string()),
            secret_key: Secret::new("super-secret-value".to_string()),
        };
        let rendered = format!("{creds:?}");
        assert!(!rendered.contains("AKIAEXAMPLE"));
        assert!(!rendered.contains("super-secret-value"));
        assert!(rendered.contains("REDACTED"));
    }
}
