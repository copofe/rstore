//! Resolution through several stores in precedence order (DATA-014).
//!
//! `get` walks the chain and returns the first hit. `set` writes to the first
//! backend that accepts writes (read-only backends such as the env store
//! report `CapabilityUnsupported`, which the chain skips). `delete` removes
//! the key from every backend so a stale copy cannot survive a precedence
//! change.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_domain::{Error, ErrorCode, Result};

use crate::{Secret, SecretStore};

/// Ordered secret-store chain. First backend wins for reads and receives
/// writes; later backends are fallbacks.
pub struct ChainStore {
    backends: Vec<Arc<dyn SecretStore>>,
}

impl ChainStore {
    /// Build from an ordered, non-empty list of backends.
    pub fn new(backends: Vec<Arc<dyn SecretStore>>) -> Self {
        Self { backends }
    }

    /// Prepend a highest-precedence backend (参数 > 环境变量 > 配置文件).
    pub fn with_first(mut self, backend: Arc<dyn SecretStore>) -> Self {
        self.backends.insert(0, backend);
        self
    }
}

#[async_trait]
impl SecretStore for ChainStore {
    async fn get(&self, key: &str) -> Result<Option<Secret<String>>> {
        for backend in &self.backends {
            if let Some(value) = backend.get(key).await? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    async fn set(&self, key: &str, value: &str) -> Result<()> {
        let mut last_error: Option<Error> = None;
        for backend in &self.backends {
            match backend.set(key, value).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Read-only backends (env) are skipped silently; a hard
                    // failure (e.g. no Secret Service on headless Linux) also
                    // falls through to the next backend.
                    if e.code != ErrorCode::CapabilityUnsupported {
                        tracing::warn!(error = %e, backend = "secret-store", "write failed, trying next backend");
                    }
                    last_error = Some(e);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| Error::config("no secret backend configured")))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut saw_failure: Option<Error> = None;
        let mut succeeded = false;
        for backend in &self.backends {
            match backend.delete(key).await {
                Ok(()) => succeeded = true,
                Err(e) if e.code == ErrorCode::CapabilityUnsupported => {}
                Err(e) => saw_failure = Some(e),
            }
        }
        if succeeded {
            Ok(())
        } else {
            Err(saw_failure.unwrap_or_else(|| Error::config("no secret backend configured")))
        }
    }

    async fn list_refs(&self) -> Result<Vec<String>> {
        let mut all: Vec<String> = Vec::new();
        for backend in &self.backends {
            all.extend(backend.list_refs().await?);
        }
        all.sort();
        all.dedup();
        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EnvSecretStore, InMemorySecretStore};

    #[tokio::test]
    async fn first_hit_wins_for_reads() {
        let a = Arc::new(InMemorySecretStore::new());
        let b = Arc::new(InMemorySecretStore::new());
        a.set("k", "from-a").await.unwrap();
        b.set("k", "from-b").await.unwrap();

        let chain = ChainStore::new(vec![a, b]);
        let got = chain.get("k").await.unwrap().unwrap();
        assert_eq!(got.expose(), "from-a");
    }

    #[tokio::test]
    async fn writes_skip_read_only_backends() {
        let env = Arc::new(EnvSecretStore);
        let mem = Arc::new(InMemorySecretStore::new());
        let chain = ChainStore::new(vec![env, mem.clone()]);

        chain.set("k", "v").await.unwrap();
        let got = mem.get("k").await.unwrap().unwrap();
        assert_eq!(got.expose(), "v");
    }

    #[tokio::test]
    async fn chain_of_only_read_only_backends_rejects_writes() {
        let chain = ChainStore::new(vec![Arc::new(EnvSecretStore)]);
        let err = chain.set("k", "v").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::CapabilityUnsupported);
    }

    struct FailingStore;

    #[async_trait]
    impl SecretStore for FailingStore {
        async fn get(&self, _key: &str) -> Result<Option<crate::Secret<String>>> {
            Ok(None)
        }
        async fn set(&self, _key: &str, _value: &str) -> Result<()> {
            Err(Error::config("backend unavailable"))
        }
        async fn delete(&self, _key: &str) -> Result<()> {
            Err(Error::config("backend unavailable"))
        }
        async fn list_refs(&self) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn hard_failure_falls_through_to_next_backend() {
        let mem = Arc::new(InMemorySecretStore::new());
        let chain = ChainStore::new(vec![Arc::new(FailingStore), mem.clone()]);
        chain.set("k", "v").await.unwrap();
        assert_eq!(mem.get("k").await.unwrap().unwrap().expose(), "v");
    }

    #[tokio::test]
    async fn delete_removes_from_every_backend() {
        let a = Arc::new(InMemorySecretStore::new());
        let b = Arc::new(InMemorySecretStore::new());
        a.set("k", "1").await.unwrap();
        b.set("k", "2").await.unwrap();

        let chain = ChainStore::new(vec![a, b]);
        chain.delete("k").await.unwrap();
        assert!(chain.get("k").await.unwrap().is_none());
    }
}
