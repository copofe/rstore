//! SQLite persistence for the rstore catalog.
//!
//! Owns the write pool, the read-only pool, the embedded migration set and the
//! repository handles built over them; writes are serialised through a bounded
//! single-writer actor, reads go through the read-only pool.
pub mod actor;
pub mod codec;
pub mod error;
pub mod ports;
pub mod repo;
pub mod tx;

pub use codec::{unix_secs, uuid_to_text, RowReader};
pub use repo::bucket::BucketRepository;
pub use repo::connection::ConnectionRepository;
pub use repo::ignore::IgnoreRuleRepository;
pub use repo::index::IndexRunRepository;
pub use repo::job::SqliteJobRepository;
pub use repo::link::LinkRepository;
pub use repo::multipart::SqliteMultipartStore;
pub use repo::outbox::OutboxRepository;
pub use repo::remote_object::SqliteRemoteObjectRepository;
pub use repo::repair::RepairRepository;
pub use repo::settings::SettingsRepository;
pub use repo::tag::TagRepository;
pub use repo::workspace::WorkspaceRepository;

use std::path::Path;
use std::time::Duration;

use rstore_domain::{Clock, Error, Result, SystemClock};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use std::sync::Arc;

use actor::SqliteWriter;

pub use error::map_sqlx;

const BASELINE_MIGRATION_VERSION: i64 = 20260819000001;

#[derive(Clone)]
/// Handle to the open SQLite catalog database. It owns the injected clock, so
/// every repository it hands out stamps time from a single source.
pub struct Database {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: SqliteWriter,
    clock: Arc<dyn Clock>,
}

impl Database {
    /// Open (or create) the database at `db_path`, apply migrations and set the
    /// connection pragmas (WAL, foreign keys, busy timeout). Uses the system
    /// clock; [`Self::open_with_clock`] injects a deterministic one.
    pub async fn open(db_path: &Path) -> Result<Self> {
        Self::open_with_clock(db_path, Arc::new(SystemClock)).await
    }

    /// Open with an explicit clock: the composition root passes its configured
    /// clock, deterministic tests pass a fixed one.
    pub async fn open_with_clock(db_path: &Path, clock: Arc<dyn Clock>) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::database("create db directory").with_source(e))?;
        }

        let options = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5))
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .map_err(|e| Error::database("open sqlite pool").with_source(e))?;

        let read_pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(read_options(db_path))
            .await
            .map_err(|e| Error::database("open sqlite read pool").with_source(e))?;
        let db = Self {
            pool,
            read_pool,
            writer: SqliteWriter::new(64),
            clock,
        };
        db.migrate().await?;
        Ok(db)
    }

    /// Open an ephemeral in-memory database using the system clock. Each call owns a private database (no
    /// `cache=shared`) so parallel tests never collide; `max_connections(1)` keeps the single connection's
    /// memory alive for the pool's lifetime.
    pub async fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with_clock(Arc::new(SystemClock)).await
    }

    /// In-memory database over an injected clock for deterministic tests. Each call owns a private database.
    pub async fn open_in_memory_with_clock(clock: Arc<dyn Clock>) -> Result<Self> {
        use std::str::FromStr;
        let options = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|e| Error::database("parse in-memory conn str").with_source(e))?
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| Error::database("open in-memory pool").with_source(e))?;
        let db = Self {
            read_pool: pool.clone(),
            pool,
            writer: SqliteWriter::new(64),
            clock,
        };
        db.migrate().await?;
        Ok(db)
    }

    /// Run the embedded schema migrations inside a transaction, so a failure leaves the prior schema intact.
    async fn migrate(&self) -> Result<()> {
        // Real legacy-file upgrade path: pre-baseline databases carry a multipart table shape not covered by
        // CREATE TABLE IF NOT EXISTS in the baseline.
        self.ensure_legacy_multipart_columns().await?;

        let mut migrator = sqlx::migrate!("./migrations");
        // Databases created before the squashed baseline keep their migration history; SQLx must ignore it
        // instead of failing on missing entries.
        if self.has_pre_baseline_migrations().await? {
            migrator.set_ignore_missing(true);
        }
        migrator.run(&self.pool).await.map_err(migration_error)?;
        Ok(())
    }

    async fn ensure_legacy_multipart_columns(&self) -> Result<()> {
        let table_exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = 'multipart_uploads'
             )",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(error::map_sqlx)?;
        if table_exists == 0 {
            return Ok(());
        }

        let source_size_exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('multipart_uploads')
                 WHERE name = 'source_size'
             )",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(error::map_sqlx)?;
        let source_modified_ns_exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('multipart_uploads')
                 WHERE name = 'source_modified_ns'
             )",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(error::map_sqlx)?;
        if source_size_exists != 0 && source_modified_ns_exists != 0 {
            return Ok(());
        }

        let mut tx = self.pool.begin().await.map_err(error::map_sqlx)?;
        if source_size_exists == 0 {
            sqlx::query(
                "ALTER TABLE multipart_uploads
                 ADD COLUMN source_size INTEGER NOT NULL DEFAULT 0",
            )
            .execute(&mut *tx)
            .await
            .map_err(error::map_sqlx)?;
        }
        if source_modified_ns_exists == 0 {
            sqlx::query(
                "ALTER TABLE multipart_uploads
                 ADD COLUMN source_modified_ns INTEGER",
            )
            .execute(&mut *tx)
            .await
            .map_err(error::map_sqlx)?;
        }
        tx.commit().await.map_err(error::map_sqlx)?;
        Ok(())
    }

    async fn has_pre_baseline_migrations(&self) -> Result<bool> {
        let table_exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = '_sqlx_migrations'
             )",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(error::map_sqlx)?;
        if table_exists == 0 {
            return Ok(false);
        }

        let has_legacy_rows: i64 = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM _sqlx_migrations WHERE version < ?
             )",
        )
        .bind(BASELINE_MIGRATION_VERSION)
        .fetch_one(&self.pool)
        .await
        .map_err(error::map_sqlx)?;
        Ok(has_legacy_rows != 0)
    }

    /// The write pool, for tests and direct diagnostics.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    // Repositories reach the write pool only through this gate: permit and transaction are acquired
    // together, never separately.
    fn gate(&self) -> crate::tx::WriteGate {
        crate::tx::WriteGate::new(self.pool.clone(), self.writer.clone())
    }

    /// Read-side pool reserved for catalog and diagnostics queries. In-memory databases share the write pool:
    /// a separate SQLite memory pool would be a different database.
    pub fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    /// The injected clock — the single time source for every stamped timestamp.
    pub fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }

    /// A repository for configured storage connections.
    pub fn connections(&self) -> ConnectionRepository {
        ConnectionRepository::new(self.gate(), self.read_pool.clone())
    }

    /// A repository for registered buckets.
    pub fn buckets(&self) -> BucketRepository {
        BucketRepository::new(self.gate(), self.read_pool.clone())
    }

    /// A repository for app settings (capability cache, machine-local state).
    pub fn settings(&self) -> SettingsRepository {
        SettingsRepository::new(self.gate(), self.read_pool.clone(), self.clock.clone())
    }

    /// The remote-object catalog repository.
    pub fn remote_objects(&self) -> SqliteRemoteObjectRepository {
        SqliteRemoteObjectRepository::new(self.gate(), self.read_pool.clone())
    }

    /// The transfer-job repository.
    pub fn jobs(&self) -> SqliteJobRepository {
        SqliteJobRepository::new(self.gate(), self.read_pool.clone())
    }

    /// The multipart checkpoint store for resumable uploads.
    pub fn multipart(&self) -> SqliteMultipartStore {
        SqliteMultipartStore::new(self.gate(), self.read_pool.clone(), self.clock.clone())
    }

    /// The domain-event outbox and its delivery lifecycle.
    pub fn outbox(&self) -> OutboxRepository {
        OutboxRepository::new(self.gate())
    }

    /// The repair queue for failed follow-ups.
    pub fn repairs(&self) -> RepairRepository {
        RepairRepository::new(self.gate(), self.read_pool.clone(), self.clock.clone())
    }

    /// A repository for workspaces and their bucket memberships.
    pub fn workspaces(&self) -> WorkspaceRepository {
        WorkspaceRepository::new(self.gate(), self.read_pool.clone())
    }

    /// A repository for bucket ignore rules.
    pub fn ignore_rules(&self) -> IgnoreRuleRepository {
        IgnoreRuleRepository::new(self.gate(), self.read_pool.clone())
    }

    /// A repository for tag definitions and associations.
    pub fn tags(&self) -> TagRepository {
        TagRepository::new(self.gate(), self.read_pool.clone())
    }

    /// A repository for index runs, scan checkpoints and catalog revisions.
    pub fn index_runs(&self) -> IndexRunRepository {
        IndexRunRepository::new(self.gate(), self.read_pool.clone())
    }

    /// A repository for per-bucket link settings.
    pub fn links(&self) -> repo::link::LinkRepository {
        repo::link::LinkRepository::new(self.gate(), self.read_pool.clone())
    }

    /// Rows currently in `remote_objects`.
    pub async fn count_remote_objects(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM remote_objects",
            Vec::new(),
        )
        .await
    }

    /// Rows in `remote_objects` whose state is `stale`.
    pub async fn count_stale_objects(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM remote_objects WHERE state = 'stale'",
            Vec::new(),
        )
        .await
    }

    /// Rows in `metadata_tombstones`.
    pub async fn count_tombstones(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM metadata_tombstones",
            Vec::new(),
        )
        .await
    }

    /// Outbox events neither published nor dead-lettered.
    pub async fn count_pending_outbox(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM outbox_events WHERE status NOT IN ('published', 'dead_letter')",
            Vec::new(),
        )
        .await
    }

    /// Repair items not yet resolved.
    pub async fn count_pending_repairs(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM repair_items WHERE status != 'resolved'",
            Vec::new(),
        )
        .await
    }

    /// Configured storage connections.
    pub async fn count_connections(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM storage_connections",
            Vec::new(),
        )
        .await
    }

    /// Connections that have never been tested.
    pub async fn count_untested_connections(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM storage_connections WHERE last_test_status IS NULL",
            Vec::new(),
        )
        .await
    }

    /// Transfer jobs in the `interrupted` state.
    pub async fn count_interrupted_jobs(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM transfer_jobs WHERE state = 'interrupted'",
            Vec::new(),
        )
        .await
    }

    /// On-disk database size in bytes (page count × page size).
    pub async fn size_bytes(&self) -> Result<u64> {
        let row: sqlx::sqlite::SqliteRow = sqlx::query(
            "SELECT (SELECT page_count FROM pragma_page_count) * \
             (SELECT page_size FROM pragma_page_size) AS bytes",
        )
        .fetch_one(self.read_pool())
        .await
        .map_err(error::map_sqlx)?;
        Ok(crate::codec::RowReader::new(row).integer("bytes")? as u64)
    }
}

/// Runtime statistics implemented right beside their SQL — the port and its
/// adapter in one place; adding a statistic touches the trait and this impl
/// only.
#[async_trait::async_trait]
impl rstore_application_ports::RuntimeStats for Database {
    async fn count_remote_objects(&self) -> Result<u64> {
        self.count_remote_objects().await
    }

    async fn count_stale_objects(&self) -> Result<u64> {
        self.count_stale_objects().await
    }

    async fn count_tombstones(&self) -> Result<u64> {
        self.count_tombstones().await
    }

    async fn count_pending_outbox(&self) -> Result<u64> {
        self.count_pending_outbox().await
    }

    async fn count_pending_repairs(&self) -> Result<u64> {
        self.count_pending_repairs().await
    }

    async fn count_connections(&self) -> Result<u64> {
        self.count_connections().await
    }

    async fn count_untested_connections(&self) -> Result<u64> {
        self.count_untested_connections().await
    }

    async fn count_interrupted_jobs(&self) -> Result<u64> {
        self.count_interrupted_jobs().await
    }

    async fn size_bytes(&self) -> Result<u64> {
        self.size_bytes().await
    }
}

fn read_options(db_path: &Path) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(db_path)
        .read_only(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5))
}

fn migration_error(e: sqlx::migrate::MigrateError) -> Error {
    Error::database("run migrations").with_source(e)
}

/// The single assembly point: turn an opened database and an optional storage factory into the application
/// dependency graph. `bootstrap::Runtime::open` and the application test fixture both call this — neither
/// re-transcribes the dependency list.
pub fn assemble(
    db: &Database,
    storage_factory: Option<Arc<dyn rstore_storage_api::StorageFactory>>,
) -> rstore_application_ports::ApplicationDependencies {
    use rstore_application_ports::{
        CatalogDependencies, RegistryDependencies, RuntimeDependencies, TransferDependencies,
    };
    let catalog = Arc::new(db.remote_objects());
    let tags = Arc::new(db.tags());
    rstore_application_ports::ApplicationDependencies::new(
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
            multipart: Arc::new(db.multipart()),
        },
        RuntimeDependencies {
            stats: Arc::new(db.clone()),
            clock: db.clock(),
            storage_factory,
            unit_of_work: Arc::new(db.clone()),
            repairs: Arc::new(db.repairs()),
            outbox: Arc::new(db.outbox()),
        },
    )
}
