//! The credential-entry convention on top of the flat [`SecretStore`] KV
//! contract (DATA-010..013).
//!
//! One *secret reference* (e.g. `conn_prod_r2`) maps to two entries:
//! `conn_prod_r2#access_key` and `conn_prod_r2#secret_key`. The reference is
//! stored in the catalog; the values live only in the secret store.

use rstore_domain::{Error, Result};

use crate::SecretStore;

/// Field suffix for the access key entry.
pub const ACCESS_FIELD: &str = "access_key";
/// Field suffix for the secret key entry.
pub const SECRET_FIELD: &str = "secret_key";

/// Entry key holding the access key for `secret_ref`.
pub fn access_key_ref(secret_ref: &str) -> String {
    format!("{secret_ref}#{ACCESS_FIELD}")
}

/// Entry key holding the secret key for `secret_ref`.
pub fn secret_key_ref(secret_ref: &str) -> String {
    format!("{secret_ref}#{SECRET_FIELD}")
}

/// Default secret reference derived from a connection name: `conn_` + the name
/// normalised to `[a-z0-9_]`.
pub fn default_secret_ref(name: &str) -> String {
    let normalised: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("conn_{normalised}")
}

/// A resolved access/secret pair (values wrapped for redaction).
pub struct CredentialPair {
    pub access_key: crate::Secret<String>,
    pub secret_key: crate::Secret<String>,
}

/// Store both credential entries under `secret_ref`.
pub async fn put_credentials(
    store: &dyn SecretStore,
    secret_ref: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<()> {
    store.set(&access_key_ref(secret_ref), access_key).await?;
    store.set(&secret_key_ref(secret_ref), secret_key).await
}

/// Read both credential entries. `Ok(None)` when either half is absent — a
/// half-written pair is treated as missing so callers never auth with a blank
/// secret.
pub async fn get_credentials(
    store: &dyn SecretStore,
    secret_ref: &str,
) -> Result<Option<CredentialPair>> {
    let access = store.get(&access_key_ref(secret_ref)).await?;
    let secret = store.get(&secret_key_ref(secret_ref)).await?;
    match (access, secret) {
        (Some(a), Some(s)) => Ok(Some(CredentialPair {
            access_key: a,
            secret_key: s,
        })),
        _ => Ok(None),
    }
}

/// Delete both credential entries for `secret_ref` (used by
/// `connection remove`).
pub async fn delete_credentials(store: &dyn SecretStore, secret_ref: &str) -> Result<()> {
    store.delete(&access_key_ref(secret_ref)).await?;
    store.delete(&secret_key_ref(secret_ref)).await
}

/// Read credentials or fail with a stable `Config` error naming the reference.
pub async fn require_credentials(
    store: &dyn SecretStore,
    secret_ref: &str,
) -> Result<CredentialPair> {
    get_credentials(store, secret_ref)
        .await?
        .ok_or_else(|| Error::config(format!("no credentials stored under `{secret_ref}`")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemorySecretStore;

    #[tokio::test]
    async fn pair_round_trip_and_delete() {
        let store = InMemorySecretStore::new();
        assert!(get_credentials(&store, "conn_x").await.unwrap().is_none());

        put_credentials(&store, "conn_x", "AKIA1", "sk-1")
            .await
            .unwrap();
        let pair = get_credentials(&store, "conn_x")
            .await
            .unwrap()
            .expect("present");
        assert_eq!(pair.access_key.expose(), "AKIA1");
        assert_eq!(pair.secret_key.expose(), "sk-1");

        // Half a pair reads as missing.
        store.delete(&access_key_ref("conn_x")).await.unwrap();
        assert!(get_credentials(&store, "conn_x").await.unwrap().is_none());

        delete_credentials(&store, "conn_x").await.unwrap();
        assert!(store.list_refs().await.unwrap().is_empty());
    }

    #[test]
    fn default_ref_normalises() {
        assert_eq!(default_secret_ref("Prod R2"), "conn_prod_r2");
        assert_eq!(default_secret_ref("nas.local"), "conn_nas_local");
    }
}
