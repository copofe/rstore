//! Cross-process write lock (DATA-008).
//!
//! The catalog has a single logical writer. A second runtime attempting to
//! acquire the lock receives [`LockError::Busy`] so the CLI can surface a clear
//! message instead of fighting SQLite.

use std::fs::{File, OpenOptions};
use std::path::Path;

use fs4::fs_std::FileExt;
use thiserror::Error;

/// Outcome of failing to acquire the write lock.
#[derive(Debug, Error)]
pub enum LockError {
    /// Another process holds the lock.
    #[error("another rstore runtime holds the write lock")]
    Busy,
    /// An underlying IO error occurred.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// RAII handle to an exclusive write lock. Released on drop (and automatically on
/// process death, so no stale lock files survive a crash).
#[derive(Debug)]
pub struct AppLock {
    _file: File,
}

impl AppLock {
    /// Try to acquire an exclusive lock on `path` without blocking.
    pub fn try_acquire(path: &Path) -> Result<Self, LockError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        match file.try_lock_exclusive() {
            Ok(true) => Ok(Self { _file: file }),
            Ok(false) => Err(LockError::Busy),
            Err(e) => Err(LockError::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_is_busy() {
        let tmp = tempfile::tempdir().unwrap();
        let lockfile = tmp.path().join("test.lock");
        let first = AppLock::try_acquire(&lockfile).expect("first acquire");
        match AppLock::try_acquire(&lockfile) {
            Err(LockError::Busy) => {}
            other => panic!("expected Busy, got {other:?}"),
        }
        drop(first);
        // After release, re-acquire should succeed.
        let _again = AppLock::try_acquire(&lockfile).expect("re-acquire after drop");
    }
}
