//! Secret storage abstraction (DATA-009) and a redacting [`Secret`] wrapper
//! (SEC-001).
//!
//! Backends (DATA-013/014) — portable across every platform:
//! - [`EnvSecretStore`] — read-only, resolved from `RSTORE_SECRET_*`
//!   environment variables; the CI/script-friendly override.
//! - [`FileSecretStore`] — the `0600` JSON file under the config directory;
//!   the default writable store for credentials on every platform. Plaintext
//!   at rest by design — `doctor` surfaces a warning.
//! - [`ChainStore`] — resolves through several stores in precedence order
//!   (参数 > 环境变量 > 配置文件, DATA-014).
//!
//! Secrets never enter the SQLite catalog or logs (SEC-001); the catalog
//! stores only the `secret_ref` pointer.

mod chain;
mod credentials;
mod env;
mod file;
mod memory;

pub use chain::ChainStore;
pub use credentials::{
    access_key_ref, default_secret_ref, delete_credentials, get_credentials, put_credentials,
    require_credentials, secret_key_ref,
};
pub use env::EnvSecretStore;
pub use file::FileSecretStore;
pub use memory::{InMemorySecretStore, NullSecretStore};

use std::fmt;

use async_trait::async_trait;
use rstore_domain::Result;

/// A secret value whose [`fmt::Debug`] is always redacted, so it cannot leak via
/// `tracing`/`dbg!`/error contexts (SEC-001).
pub struct Secret<T>(T);

impl<T> Secret<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// Access the inner value. Call sites are auditable grep targets.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Consume and yield the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(**REDACTED**)")
    }
}

impl<T: Clone> Clone for Secret<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Pluggable secret storage (DATA-009).
///
/// Keys are opaque reference strings; the credential helpers in
/// [`credentials`] define the `<ref>#access_key` / `<ref>#secret_key`
/// convention on top of this flat KV contract.
#[async_trait]
pub trait SecretStore: Send + Sync {
    /// Fetch a secret by its reference key. `Ok(None)` means "not stored".
    async fn get(&self, key: &str) -> Result<Option<Secret<String>>>;

    /// Store (or overwrite) a secret. Read-only backends (env) return
    /// `CapabilityUnsupported`.
    async fn set(&self, key: &str, value: &str) -> Result<()>;

    /// Delete a secret if present. Deleting an absent key is not an error.
    async fn delete(&self, key: &str) -> Result<()>;

    /// List all stored secret reference keys (never values). Backends that
    /// cannot enumerate return an empty list.
    async fn list_refs(&self) -> Result<Vec<String>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_debug_is_redacted() {
        let s = Secret::new("super-secret-value");
        assert_eq!(format!("{s:?}"), "Secret(**REDACTED**)");
    }

    #[tokio::test]
    async fn null_store_is_empty() {
        let store = NullSecretStore;
        assert!(store.get("k").await.unwrap().is_none());
        assert!(store.list_refs().await.unwrap().is_empty());
    }
}
