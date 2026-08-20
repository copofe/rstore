//! A migrated SQLite catalog in a throwaway directory (QA-003).

use rstore_domain::Result;
use rstore_persistence::Database;
use tempfile::TempDir;

/// Owns a temp directory and the [`Database`] living inside it.
pub struct TempDb {
    _dir: TempDir,
    db: Database,
}

impl TempDb {
    /// Create a fresh, migrated database in a new temp directory.
    pub async fn open() -> Result<Self> {
        let dir = tempfile::tempdir()
            .map_err(|e| rstore_domain::Error::database("create temp dir").with_source(e))?;
        let db = Database::open(&dir.path().join("catalog.db")).await?;
        Ok(Self { _dir: dir, db })
    }

    pub fn db(&self) -> &Database {
        &self.db
    }
}
