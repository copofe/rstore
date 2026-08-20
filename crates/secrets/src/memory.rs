//! In-memory and null stores.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use rstore_domain::Result;

use crate::SecretStore;

/// In-memory store, primarily for tests (CORE-014).
#[derive(Default)]
pub struct InMemorySecretStore {
    entries: Mutex<HashMap<String, String>>,
}

impl InMemorySecretStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SecretStore for InMemorySecretStore {
    async fn get(&self, key: &str) -> Result<Option<crate::Secret<String>>> {
        Ok(self
            .entries
            .lock()
            .expect("secret store poisoned")
            .get(key)
            .cloned()
            .map(crate::Secret::new))
    }

    async fn set(&self, key: &str, value: &str) -> Result<()> {
        self.entries
            .lock()
            .expect("secret store poisoned")
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.entries
            .lock()
            .expect("secret store poisoned")
            .remove(key);
        Ok(())
    }

    async fn list_refs(&self) -> Result<Vec<String>> {
        Ok(self
            .entries
            .lock()
            .expect("secret store poisoned")
            .keys()
            .cloned()
            .collect())
    }
}

/// A store that holds nothing. Used when no backend is configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSecretStore;

#[async_trait]
impl SecretStore for NullSecretStore {
    async fn get(&self, _key: &str) -> Result<Option<crate::Secret<String>>> {
        Ok(None)
    }
    async fn set(&self, _key: &str, _value: &str) -> Result<()> {
        Ok(())
    }
    async fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }
    async fn list_refs(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_round_trip() {
        let store = InMemorySecretStore::new();
        store.set("conn_a", "hunter2").await.unwrap();
        let got = store.get("conn_a").await.unwrap().expect("present");
        assert_eq!(got.expose(), "hunter2");
        assert_eq!(store.list_refs().await.unwrap(), vec!["conn_a".to_string()]);
        store.delete("conn_a").await.unwrap();
        assert!(store.get("conn_a").await.unwrap().is_none());
    }
}
