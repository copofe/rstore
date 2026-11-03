//! Where rstore keeps its files: the database and the logs.
//!
//! Everything durable lives in one SQLite file, `rstore.db`; the only other
//! directory is for logs. Per platform:
//! - macOS   — `~/Library/Application Support/rstore`, logs in `~/Library/Logs/rstore`
//! - Linux   — `~/.local/share/rstore`, logs in `~/.local/state/rstore` (XDG)
//! - Windows — `%LOCALAPPDATA%\rstore`, logs in `%LOCALAPPDATA%\rstore\log`
//!
//! The database is machine-local state (job checkpoints name local paths), so
//! it never goes to a roaming profile. A single portable root may be forced
//! via the `RSTORE_HOME` env var or by passing a `root` to
//! [`resolve_app_dirs`]; it then holds `rstore.db` and `log/`.

use std::path::{Path, PathBuf};

use serde::Serialize;

/// Directory name under each platform base directory.
pub const APP_NAME: &str = "rstore";

/// Override env var for a single root directory (tests / portable installs).
pub const HOME_ENV: &str = "RSTORE_HOME";

/// The database file name inside the data directory.
const DB_FILE: &str = "rstore.db";

/// The resolved locations.
#[derive(Debug, Clone, Serialize)]
pub struct AppDirs {
    /// Holds the database.
    pub data_dir: PathBuf,
    pub log_dir: PathBuf,
    /// The SQLite database: every connection, bucket, workspace, tag, job,
    /// and the catalog cache.
    pub db_path: PathBuf,
}

impl AppDirs {
    fn new(data_dir: PathBuf, log_dir: PathBuf) -> Self {
        Self {
            db_path: data_dir.join(DB_FILE),
            data_dir,
            log_dir,
        }
    }

    /// Ensure both directories exist (idempotent).
    pub fn ensure_created(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.data_dir)?;
        std::fs::create_dir_all(&self.log_dir)
    }
}

/// Resolve the app directories: an explicit `root` wins, then `RSTORE_HOME`,
/// then the platform default.
pub fn resolve_app_dirs(root: Option<&Path>) -> AppDirs {
    if let Some(root) = root {
        return from_root(root);
    }
    if let Ok(env_root) = std::env::var(HOME_ENV) {
        if !env_root.is_empty() {
            return from_root(Path::new(&env_root));
        }
    }
    platform_dirs().unwrap_or_else(|| from_root(Path::new(".rstore")))
}

fn from_root(root: &Path) -> AppDirs {
    AppDirs::new(root.to_path_buf(), root.join("log"))
}

fn platform_dirs() -> Option<AppDirs> {
    let base = directories::BaseDirs::new()?;
    let data_dir = base.data_local_dir().join(APP_NAME);
    let log_dir = if cfg!(target_os = "macos") {
        base.home_dir().join("Library/Logs").join(APP_NAME)
    } else if let Some(state) = base.state_dir() {
        state.join(APP_NAME)
    } else {
        data_dir.join("log")
    };
    Some(AppDirs::new(data_dir, log_dir))
}
