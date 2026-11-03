//! Runtime: the process-startup sequence every entry point runs.
//!
//! This is the only production code that opens SQLite and selects the
//! concrete storage factory. Several processes may hold a runtime over the
//! same data directory at once — the CLI next to the desktop, two terminals —
//! because SQLite in WAL mode arbitrates writers and running jobs hold leases
//! instead of a process-wide lock.

use std::path::PathBuf;
use std::sync::Arc;

use rstore_domain::{Clock, Error, Result, SystemClock};
use rstore_storage::s3::S3StorageFactory;
use rstore_storage::StorageFactory;

use crate::db::Database;
use crate::dirs::AppDirs;
use crate::Application;

/// Inputs supplied by a binary-specific entry point.
pub struct RuntimeConfig {
    pub db_path: PathBuf,
    pub storage_factory: Arc<dyn StorageFactory>,
    pub clock: Arc<dyn Clock>,
}

impl RuntimeConfig {
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
            storage_factory: Arc::new(S3StorageFactory::new()),
            clock: Arc::new(SystemClock),
        }
    }

    pub fn with_storage_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = factory;
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
}

/// The opened engine owned by the process entry point.
pub struct Runtime {
    application: Application,
}

impl Runtime {
    /// Open the database at the configured path (migrating it), wire the
    /// engine, and run startup recovery.
    pub async fn open(config: RuntimeConfig) -> Result<Self> {
        // The configured clock becomes the database's clock: every repository
        // stamp and the services share one time source.
        let db = Database::open_with_clock(&config.db_path, config.clock.clone()).await?;
        let application = Application::new(&db, config.storage_factory)
            .initialize()
            .await?;
        Ok(Self { application })
    }

    /// The full process-startup sequence: create the app directories, then
    /// open the runtime over the resolved database path.
    pub async fn open_in(app_dirs: &AppDirs) -> Result<Self> {
        app_dirs
            .ensure_created()
            .map_err(|error| Error::database("create data directories").with_source(error))?;
        Self::open(RuntimeConfig::new(app_dirs.db_path.clone())).await
    }

    pub fn application(&self) -> &Application {
        &self.application
    }
}
