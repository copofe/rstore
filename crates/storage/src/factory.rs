//! The storage factory seam.
//!
//! The application core calls [`StorageFactory`] to turn a
//! [`ConnectionConfig`] plus its stored credentials into a connected [`Storage`].
//! The concrete factory is [`crate::s3::S3StorageFactory`]; the engine's
//! `Runtime` selects it, and tests may inject another.

use std::sync::Arc;

use rstore_domain::connection::ConnectionConfig;
use rstore_domain::{Result, Secret};

use crate::Storage;

/// Static credentials for authenticating against an endpoint. The fields are
/// [`Secret`] so the redaction invariant travels the seam by type: the
/// derived `Debug` prints redacted fields, and adapters call `expose()` only
/// where the SDK needs plaintext.
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
    ///
    /// # Errors
    /// Fails when the configuration cannot produce a usable adapter (e.g. an
    /// invalid endpoint or TTL).
    fn build(
        &self,
        config: &ConnectionConfig,
        creds: &StorageCredentials,
    ) -> Result<Arc<dyn Storage>>;
}
