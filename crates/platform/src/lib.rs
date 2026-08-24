//! Platform integration: standard application data directories and a
//! cross-process write lock.
//!
//! Dependency rule: this crate MUST NOT depend on `domain`. It exposes its own
//! small error type; the application layer maps it into the domain error model
//! where needed.

pub mod dirs;
pub mod lock;

pub use dirs::{resolve_app_dirs, AppDirs};
pub use lock::{AppLock, LockError};

/// Logical application name used for per-platform directory resolution.
pub const APP_NAME: &str = "rstore";

/// Override env var for a single root data directory (tests / portable installs).
pub const HOME_ENV: &str = "RSTORE_HOME";
