//! Standard application data directories.
//!
//! Resolves `config / data / cache / log` per platform:
//! - macOS  — `~/Library/Application Support/rstore`, `~/Library/Caches/rstore`
//! - Linux  — XDG (`~/.config/rstore`, `~/.local/share/rstore`, `~/.cache/rstore`)
//! - Windows — `%APPDATA%\rstore`, `%LOCALAPPDATA%\rstore\cache`
//!
//! A single portable root may be forced via the `RSTORE_HOME` env var or by
//! passing a `root` to [`resolve_app_dirs`] — essential for tests and for
//! portable installations.

use std::path::{Path, PathBuf};

use serde::Serialize;

/// The four well-known application directories.
#[derive(Debug, Clone, Serialize)]
pub struct AppDirs {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub log_dir: PathBuf,
    /// Database file for the local catalog (under `data_dir`).
    pub db_path: PathBuf,
    /// File used by the cross-process write lock.
    pub lock_path: PathBuf,
}

impl AppDirs {
    /// Ensure every directory exists (idempotent).
    pub fn ensure_created(&self) -> std::io::Result<()> {
        for dir in [
            &self.config_dir,
            &self.data_dir,
            &self.cache_dir,
            &self.log_dir,
        ] {
            std::fs::create_dir_all(dir)?;
        }
        Ok(())
    }
}

/// Resolve the standard app directories, honouring `RSTORE_HOME` and an optional
/// explicit `root` override.
pub fn resolve_app_dirs(root: Option<&Path>) -> AppDirs {
    if let Some(root) = root {
        return from_root(root);
    }
    if let Ok(env_root) = std::env::var(crate::HOME_ENV) {
        if !env_root.is_empty() {
            return from_root(Path::new(&env_root));
        }
    }
    platform_dirs().unwrap_or_else(|| from_root(Path::new(".rstore")))
}

/// The database and lock files always live directly under the data dir; both
/// directory resolvers share this one layout so a rename cannot drift.
fn from_dirs(
    config_dir: PathBuf,
    data_dir: PathBuf,
    cache_dir: PathBuf,
    log_dir: PathBuf,
) -> AppDirs {
    AppDirs {
        config_dir,
        db_path: data_dir.join("catalog.db"),
        lock_path: data_dir.join("rstore.lock"),
        data_dir,
        cache_dir,
        log_dir,
    }
}

fn from_root(root: &Path) -> AppDirs {
    from_dirs(
        root.join("config"),
        root.join("data"),
        root.join("cache"),
        root.join("log"),
    )
}

fn platform_dirs() -> Option<AppDirs> {
    let proj = directories::ProjectDirs::from("dev", "rstore", crate::APP_NAME)?;
    let data_dir = proj.data_dir().to_path_buf();
    let log_dir = data_dir.join("log");
    Some(from_dirs(
        proj.config_dir().to_path_buf(),
        data_dir,
        proj.cache_dir().to_path_buf(),
        log_dir,
    ))
}
