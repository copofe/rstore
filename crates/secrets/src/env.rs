//! Read-only store backed by environment variables (DATA-013).
//!
//! Entry `conn_prod#access_key` resolves to
//! `RSTORE_SECRET_CONN_PROD__ACCESS_KEY`: the reference upper-cased with
//! non-alphanumerics folded to `_`, then a double underscore and the field.
//! Values are never written back to the environment (set/delete are
//! unsupported).

use async_trait::async_trait;
use rstore_domain::{Error, Result};

use crate::{Secret, SecretStore};

/// The env prefix every variable must carry.
pub const ENV_PREFIX: &str = "RSTORE_SECRET_";

/// Map a store entry key to its environment variable name.
pub fn env_name(key: &str) -> String {
    let (reference, field) = match key.split_once('#') {
        Some((r, f)) => (r, f),
        None => (key, ""),
    };
    let reference: String = reference
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    if field.is_empty() {
        format!("{ENV_PREFIX}{reference}")
    } else {
        format!("{ENV_PREFIX}{reference}__{}", field.to_ascii_uppercase())
    }
}

/// Read-only environment-variable secret store.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvSecretStore;

#[async_trait]
impl SecretStore for EnvSecretStore {
    async fn get(&self, key: &str) -> Result<Option<Secret<String>>> {
        let value = match std::env::var(env_name(key)) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        if value.is_empty() {
            return Ok(None);
        }
        Ok(Some(Secret::new(value)))
    }

    async fn set(&self, _key: &str, _value: &str) -> Result<()> {
        Err(Error::unsupported(
            "the environment-variable secret store is read-only",
        ))
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        // Deleting an absent key is not an error; the env store can never hold
        // writable state, so this is a no-op by construction.
        Ok(())
    }

    async fn list_refs(&self) -> Result<Vec<String>> {
        let mut refs: Vec<String> = std::env::vars()
            .filter_map(|(name, _)| name.strip_prefix(ENV_PREFIX).map(str::to_string))
            // Cut the `__FIELD` suffix to recover the bare reference.
            .map(|rest| match rest.split_once("__") {
                Some((r, _)) => r.to_string(),
                None => rest,
            })
            .map(|r| r.to_lowercase())
            .collect();
        refs.sort();
        refs.dedup();
        Ok(refs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env mutation is process-global; serialize all env tests behind one mutex.
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        static MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());
        MUTEX.lock().expect("env test mutex poisoned")
    }

    #[test]
    fn env_name_mapping() {
        assert_eq!(
            env_name("conn_prod#access_key"),
            "RSTORE_SECRET_CONN_PROD__ACCESS_KEY"
        );
        assert_eq!(
            env_name("conn_prod#secret_key"),
            "RSTORE_SECRET_CONN_PROD__SECRET_KEY"
        );
        assert_eq!(env_name("plain_ref"), "RSTORE_SECRET_PLAIN_REF");
    }

    #[test]
    fn reads_and_listing() {
        let _guard = lock();
        std::env::set_var("RSTORE_SECRET_CONN_TESTY__ACCESS_KEY", "AK");
        std::env::set_var("RSTORE_SECRET_CONN_TESTY__SECRET_KEY", "SK");

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let store = EnvSecretStore;
        rt.block_on(async {
            let got = store.get("conn-Testy#access_key").await.unwrap().unwrap();
            assert_eq!(got.expose(), "AK");
            assert!(store
                .list_refs()
                .await
                .unwrap()
                .contains(&"conn_testy".to_string()));

            let err = store.set("x#access_key", "v").await.unwrap_err();
            assert_eq!(err.code, rstore_domain::ErrorCode::CapabilityUnsupported);
        });

        std::env::remove_var("RSTORE_SECRET_CONN_TESTY__ACCESS_KEY");
        std::env::remove_var("RSTORE_SECRET_CONN_TESTY__SECRET_KEY");
    }
}
