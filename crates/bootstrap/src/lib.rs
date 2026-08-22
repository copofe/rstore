//! Runtime composition root.
//!
//! This crate is the only production layer that opens SQLite and selects the
//! concrete persistence/storage implementations. The application crate sees
//! only `ApplicationDependencies`, so a future desktop binary can use the
//! same core without importing SQLx or an AWS SDK.
//!
//! Startup has exactly two entry shapes: [`Runtime::open`] (no lock; tests and
//! throwaway databases) and [`Runtime::open_locked`] (the full process-startup
//! sequence every binary runs). Both assemble dependencies through the single
//! assembly point `rstore_persistence::assemble`, which the test fixture shares.

use std::path::PathBuf;
use std::sync::Arc;

use rstore_application::{Application, OutboxDispatcher};
use rstore_domain::{Clock, Error, Result, SystemClock};
use rstore_persistence::Database;
use rstore_platform::{AppDirs, AppLock, LockError};
use rstore_storage_api::StorageFactory;
use rstore_storage_s3::S3StorageFactory;
use tokio::sync::broadcast;

/// Inputs supplied by a binary-specific entry point.
pub struct RuntimeConfig {
    pub db_path: PathBuf,
    pub storage_factory: Option<Arc<dyn StorageFactory>>,
    pub clock: Arc<dyn Clock>,
}

impl RuntimeConfig {
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
            storage_factory: Some(Arc::new(S3StorageFactory::new())),
            clock: Arc::new(SystemClock),
        }
    }

    pub fn with_storage_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(factory);
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
}

/// Concrete runtime owned by the process entry point. Holding the optional
/// [`AppLock`] keeps the cross-process write lock (DATA-008) alive for the
/// process lifetime; dropping the runtime releases it.
pub struct Runtime {
    _lock: Option<AppLock>,
    db: Database,
    application: Application,
    dispatcher: OutboxDispatcher,
    notifications: broadcast::Sender<rstore_application::JobNotification>,
}

impl Runtime {
    /// Open a runtime without taking the cross-process write lock. Intended
    /// for tests and throwaway databases; binaries use [`Runtime::open_locked`].
    pub async fn open(config: RuntimeConfig) -> Result<Self> {
        let db = Database::open(&config.db_path).await?;
        let outbox = Arc::new(db.outbox());
        let dispatcher = OutboxDispatcher::new(outbox.clone());
        dispatcher.recover_processing(config.clock.now()).await?;
        let (notifications, _) = broadcast::channel(256);
        let dependencies = rstore_persistence::assemble(&db, config.storage_factory, config.clock);
        let application = Application::from_dependencies(dependencies)
            .initialize()
            .await?;
        Ok(Self {
            _lock: None,
            db,
            application,
            dispatcher,
            notifications,
        })
    }

    /// The full process-startup sequence (CONTEXT.md: 启动编排): create the
    /// app directories, take the cross-process write lock (DATA-008), then
    /// open the runtime over the resolved database path. The lock error
    /// mapping lives here — and only here — so both entry points report the
    /// same domain error for a busy runtime.
    pub async fn open_locked(app_dirs: &AppDirs) -> Result<Self> {
        app_dirs
            .ensure_created()
            .map_err(|error| Error::database("create data directories").with_source(error))?;
        let lock = match AppLock::try_acquire(&app_dirs.lock_path) {
            Ok(lock) => lock,
            Err(LockError::Busy) => {
                return Err(Error::lock_busy(
                    "another rstore runtime holds the write lock",
                ));
            }
            Err(LockError::Io(error)) => {
                return Err(Error::database("acquire write lock").with_source(error));
            }
        };
        let mut runtime = Self::open(RuntimeConfig::new(app_dirs.db_path.clone())).await?;
        runtime._lock = Some(lock);
        Ok(runtime)
    }

    pub fn application(&self) -> &Application {
        &self.application
    }

    pub fn into_application(self) -> Application {
        self.application
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn outbox_dispatcher(&self) -> &OutboxDispatcher {
        &self.dispatcher
    }

    /// Subscribe to best-effort runtime job notifications. Desktop code can
    /// drop and recreate this receiver without touching the application core.
    pub fn subscribe_job_notifications(
        &self,
    ) -> broadcast::Receiver<rstore_application::JobNotification> {
        self.notifications.subscribe()
    }

    pub fn publish_job_notification(
        &self,
        notification: rstore_application::JobNotification,
    ) -> usize {
        self.notifications.send(notification).unwrap_or(0)
    }
}
