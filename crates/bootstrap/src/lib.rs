//! Runtime composition root.
//!
//! This crate is the only production layer that opens SQLite and selects the
//! concrete persistence/storage implementations. The application crate sees
//! only `ApplicationDependencies`, so a future desktop binary can use the
//! same core without importing SQLx or an AWS SDK.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use rstore_application::{
    Application, ApplicationDependencies, CatalogDependencies, OutboxDispatcher,
    RegistryDependencies, RuntimeDependencies, TransferDependencies,
};
use rstore_application_ports::{SecretSourceCounts, SecretSourceStats};
use rstore_domain::{Clock, Error, Result, SystemClock};
use rstore_persistence::Database;
use rstore_secrets::{ChainStore, EnvSecretStore, FileSecretStore, SecretStore};
use rstore_storage_api::StorageFactory;
use rstore_storage_s3::S3StorageFactory;
use tokio::sync::broadcast;

/// Inputs supplied by a binary-specific entry point.
pub struct RuntimeConfig {
    pub db_path: PathBuf,
    pub secrets: Arc<dyn SecretStore>,
    pub secret_source_stats: Option<Arc<dyn SecretSourceStats>>,
    pub storage_factory: Option<Arc<dyn StorageFactory>>,
    pub clock: Arc<dyn Clock>,
}

impl RuntimeConfig {
    pub fn new(db_path: impl Into<PathBuf>, secrets: Arc<dyn SecretStore>) -> Self {
        Self {
            db_path: db_path.into(),
            secrets,
            secret_source_stats: None,
            storage_factory: Some(Arc::new(S3StorageFactory::new())),
            clock: Arc::new(SystemClock),
        }
    }

    pub fn with_secret_source_stats(mut self, stats: Arc<dyn SecretSourceStats>) -> Self {
        self.secret_source_stats = Some(stats);
        self
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

/// Concrete runtime owned by the process entry point.
pub struct Runtime {
    db: Database,
    application: Application,
    dispatcher: OutboxDispatcher,
    notifications: broadcast::Sender<rstore_application::JobNotification>,
}

impl Runtime {
    pub async fn open(config: RuntimeConfig) -> Result<Self> {
        let secret_source_stats = config.secret_source_stats.ok_or_else(|| {
            Error::config("no secret source inventory configured")
                .with_context("layer", "bootstrap")
        })?;
        let db = Database::open(&config.db_path).await?;
        let outbox = Arc::new(db.outbox());
        let dispatcher = OutboxDispatcher::new(outbox.clone());
        dispatcher.recover_processing(config.clock.now()).await?;
        let (notifications, _) = broadcast::channel(256);
        let catalog = Arc::new(db.remote_objects());
        let tags = Arc::new(db.tags());
        let dependencies = ApplicationDependencies::new(
            RegistryDependencies {
                connections: Arc::new(db.connections()),
                buckets: Arc::new(db.buckets()),
                workspaces: Arc::new(db.workspaces()),
                ignore_rules: Arc::new(db.ignore_rules()),
                settings: Arc::new(db.settings()),
                links: Arc::new(db.links()),
                tags: tags.clone(),
            },
            CatalogDependencies {
                catalog: catalog.clone(),
                catalog_query: catalog,
                index_runs: Arc::new(db.index_runs()),
                catalog_tags: tags,
            },
            TransferDependencies {
                jobs: Arc::new(db.jobs()),
                multipart: Arc::new(db.multipart(config.clock.clone())),
            },
            RuntimeDependencies {
                stats: Arc::new(db.clone()),
                clock: config.clock,
                secrets: config.secrets,
                secret_source_stats,
                storage_factory: config.storage_factory,
                outbox: Some(outbox),
                unit_of_work: Some(Arc::new(db.clone())),
                repairs: Some(Arc::new(db.outbox())),
                mutation_receipts: Some(Arc::new(db.mutation_receipts())),
            },
        );
        let application = Application::from_dependencies(dependencies)
            .initialize()
            .await?;
        Ok(Self {
            db,
            application,
            dispatcher,
            notifications,
        })
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

/// Build the default CLI secret chain. The bootstrap owns the policy; callers
/// can still inject a different `SecretStore` through `RuntimeConfig`.
pub fn default_secret_store(config_dir: &Path) -> Arc<dyn SecretStore> {
    Arc::new(ChainStore::new(vec![
        Arc::new(EnvSecretStore),
        Arc::new(FileSecretStore::new(config_dir.join("secrets.json"))),
    ]))
}

/// The production credential-source inventory used by `doctor`. It is kept
/// in Bootstrap next to the policy that selects the environment and file
/// stores; the Application Core only sees the narrow stats port.
struct DefaultSecretSourceStats {
    env: EnvSecretStore,
    file: FileSecretStore,
}

#[async_trait]
impl SecretSourceStats for DefaultSecretSourceStats {
    async fn counts(&self) -> Result<SecretSourceCounts> {
        Ok(SecretSourceCounts {
            env_refs: self.env.list_refs().await.unwrap_or_default().len(),
            file_refs: self.file.list_refs().await.unwrap_or_default().len(),
        })
    }
}

pub fn default_secret_source_stats(config_dir: &Path) -> Arc<dyn SecretSourceStats> {
    Arc::new(DefaultSecretSourceStats {
        env: EnvSecretStore,
        file: FileSecretStore::new(config_dir.join("secrets.json")),
    })
}

/// Turn a missing runtime dependency into a stable configuration error at the
/// composition boundary.
pub fn require_storage_factory(
    factory: Option<Arc<dyn StorageFactory>>,
) -> Result<Arc<dyn StorageFactory>> {
    factory.ok_or_else(|| {
        Error::config("no storage adapter configured").with_context("layer", "bootstrap")
    })
}
