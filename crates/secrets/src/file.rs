//! File-backed secret store (DATA-012): a JSON map at a fixed path, created
//! with `0600` permissions.
//!
//! This is the fallback for headless Linux where no Secret Service exists.
//! Values are plaintext at rest by design (no master-password prompt in a
//! non-interactive CLI); `doctor` surfaces a warning when this backend holds
//! credentials.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use rstore_domain::{Error, Result};

use crate::{Secret, SecretStore};

/// A JSON-file secret store. All operations serialise through one mutex and
/// re-read the file, so multiple handles see consistent state.
pub struct FileSecretStore {
    path: PathBuf,
    io_lock: Mutex<()>,
}

impl FileSecretStore {
    /// Use the file at `path`. The file is created lazily on first write.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            io_lock: Mutex::new(()),
        }
    }

    fn load(&self) -> Result<BTreeMap<String, String>> {
        let raw = match std::fs::read_to_string(&self.path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => {
                return Err(Error::database("read secrets file").with_source(e));
            }
        };
        serde_json::from_str(&raw).map_err(|e| Error::database("parse secrets file").with_source(e))
    }

    fn save(&self, entries: &BTreeMap<String, String>) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| Error::database("create secrets dir").with_source(e))?;
            }
        }
        let raw = serde_json::to_string_pretty(entries)
            .map_err(|e| Error::database("serialise secrets file").with_source(e))?;
        std::fs::write(&self.path, raw)
            .map_err(|e| Error::database("write secrets file").with_source(e))?;
        restrict_permissions(&self.path);
        Ok(())
    }
}

/// Best-effort `0600` on unix; other platforms rely on user-profile defaults.
fn restrict_permissions(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        if let Err(e) = std::fs::set_permissions(path, perms) {
            tracing::warn!(error = %e, ?path, "could not restrict secrets file permissions");
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

#[async_trait]
impl SecretStore for FileSecretStore {
    async fn get(&self, key: &str) -> Result<Option<Secret<String>>> {
        let _guard = self.io_lock.lock().expect("secrets file poisoned");
        Ok(self.load()?.get(key).cloned().map(Secret::new))
    }

    async fn set(&self, key: &str, value: &str) -> Result<()> {
        let _guard = self.io_lock.lock().expect("secrets file poisoned");
        let mut entries = self.load()?;
        entries.insert(key.to_string(), value.to_string());
        self.save(&entries)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let _guard = self.io_lock.lock().expect("secrets file poisoned");
        let mut entries = self.load()?;
        entries.remove(key);
        self.save(&entries)
    }

    async fn list_refs(&self) -> Result<Vec<String>> {
        let _guard = self.io_lock.lock().expect("secrets file poisoned");
        Ok(self.load()?.keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_and_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.json");
        let store = FileSecretStore::new(&path);
        store.set("conn_a#access_key", "AK").await.unwrap();
        store.set("conn_a#secret_key", "SK").await.unwrap();

        // A second handle sees the persisted file.
        let reopened = FileSecretStore::new(&path);
        let got = reopened.get("conn_a#access_key").await.unwrap().unwrap();
        assert_eq!(got.expose(), "AK");
        assert_eq!(
            reopened.list_refs().await.unwrap().len(),
            2,
            "raw entry keys are listed"
        );

        reopened.delete("conn_a#access_key").await.unwrap();
        assert!(reopened.get("conn_a#access_key").await.unwrap().is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_is_created_user_read_write_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.json");
        let store = FileSecretStore::new(&path);
        store.set("k", "v").await.unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
