//! SQLite persistence layer (DATA-003..008, CORE-013).
//!
//! Owns the write pool, read-only pool, embedded migration set, and repositories.
//! Writes are serialised across *processes* by the application-layer write lock
//! (DATA-008) and within a process by the bounded SQLite Writer Actor; reads
//! use the read-only pool.

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
pub use repo::receipt::SqliteRemoteMutationRepository;
pub use repo::remote_object::SqliteRemoteObjectRepository;
pub use repo::settings::SettingsRepository;
pub use repo::tag::TagRepository;
pub use repo::workspace::WorkspaceRepository;

use std::path::Path;
use std::time::Duration;

use rstore_domain::{Clock, Error, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use std::sync::Arc;

use actor::SqliteWriter;

pub use error::map_sqlx;

const BASELINE_MIGRATION_VERSION: i64 = 20260819000001;

/// Handle to the open SQLite catalog database.
#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: SqliteWriter,
}

impl Database {
    /// Open (or create) the database at `db_path`, apply migrations and set
    /// connection-wide pragmas (WAL, foreign keys, busy timeout).
    pub async fn open(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
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
        };
        db.migrate().await?;
        Ok(db)
    }

    /// Open an ephemeral in-memory database (handy for unit tests). Each call
    /// owns a private database (no `cache=shared`) so parallel tests never
    /// collide; `max_connections(1)` keeps that single connection's memory alive
    /// for the pool's lifetime.
    pub async fn open_in_memory() -> Result<Self> {
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
        };
        db.migrate().await?;
        Ok(db)
    }

    /// Run the embedded schema migrations. Migrations run inside a transaction,
    /// so a failure leaves the prior schema intact (DATA-005).
    async fn migrate(&self) -> Result<()> {
        self.ensure_legacy_multipart_columns().await?;

        let mut migrator = sqlx::migrate!("./migrations");
        // The current source is a squashed baseline. Older databases retain
        // their pre-baseline history, which SQLx should keep rather than
        // treating as a missing migration.
        if self.has_pre_baseline_migrations().await? {
            migrator.set_ignore_missing(true);
        }
        migrator.run(&self.pool).await.map_err(migration_error)?;
        Ok(())
    }

    /// Add columns introduced after the historical migrations that preceded
    /// the current baseline. This is intentionally narrow: it upgrades the
    /// only legacy table whose shape is not covered by `CREATE TABLE IF NOT
    /// EXISTS` in the baseline.
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

    /// Borrow the underlying pool for repository queries.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// The write path every repository shares: the write pool paired with the
    /// bounded writer actor. Acquiring the permit and opening a transaction
    /// are one operation on the gate ([`tx::WriteGate::write_unit`]) — this is
    /// the only way repositories reach the write pool for multi-statement
    /// units.
    fn gate(&self) -> crate::tx::WriteGate {
        crate::tx::WriteGate::new(self.pool.clone(), self.writer.clone())
    }

    /// Read-side pool reserved for catalog and diagnostics queries. In-memory
    /// databases share the write pool because a separate SQLite memory pool
    /// would be a different database.
    pub fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    /// A repository for storage connections.
    pub fn connections(&self) -> ConnectionRepository {
        ConnectionRepository::new(self.gate(), self.read_pool.clone())
    }

    /// A repository for registered buckets.
    pub fn buckets(&self) -> BucketRepository {
        BucketRepository::new(self.gate(), self.read_pool.clone())
    }

    /// A repository for app settings (capability cache, machine-local state).
    pub fn settings(&self) -> SettingsRepository {
        SettingsRepository::new(self.gate(), self.read_pool.clone())
    }

    /// The remote-object catalog repository.
    pub fn remote_objects(&self) -> SqliteRemoteObjectRepository {
        SqliteRemoteObjectRepository::new(self.gate(), self.read_pool.clone())
    }

    /// The job/transfer-task repository.
    pub fn jobs(&self) -> SqliteJobRepository {
        SqliteJobRepository::new(self.gate(), self.read_pool.clone())
    }

    /// The multipart checkpoint store (resumable uploads).
    pub fn multipart(&self, clock: Arc<dyn Clock>) -> SqliteMultipartStore {
        SqliteMultipartStore::new(self.gate(), self.read_pool.clone(), clock)
    }

    /// Domain-event outbox and repair queue.
    pub fn outbox(&self) -> OutboxRepository {
        OutboxRepository::new(self.gate(), self.read_pool.clone())
    }

    pub fn mutation_receipts(&self) -> SqliteRemoteMutationRepository {
        SqliteRemoteMutationRepository::new(self.read_pool.clone())
    }

    /// Workspaces and their bucket memberships (WSP-).
    pub fn workspaces(&self) -> WorkspaceRepository {
        WorkspaceRepository::new(self.gate(), self.read_pool.clone())
    }

    /// Bucket ignore rules (IGN-).
    pub fn ignore_rules(&self) -> IgnoreRuleRepository {
        IgnoreRuleRepository::new(self.gate(), self.read_pool.clone())
    }

    /// Tag definitions and associations (TAG-).
    pub fn tags(&self) -> TagRepository {
        TagRepository::new(self.gate(), self.read_pool.clone())
    }

    /// Index runs, checkpoints and revisions (IDX-).
    pub fn index_runs(&self) -> IndexRunRepository {
        IndexRunRepository::new(self.gate(), self.read_pool.clone())
    }

    /// Per-bucket link settings (LINK-).
    pub fn links(&self) -> repo::link::LinkRepository {
        repo::link::LinkRepository::new(self.gate(), self.read_pool.clone())
    }

    /// Row counts for `index stats` (IDX-021). Plain COUNT(*)s over the
    /// catalog tables; callers render them.
    pub async fn count_remote_objects(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM remote_objects",
            Vec::new(),
        )
        .await
    }

    pub async fn count_stale_objects(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM remote_objects WHERE state = 'stale'",
            Vec::new(),
        )
        .await
    }

    pub async fn count_tombstones(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM metadata_tombstones",
            Vec::new(),
        )
        .await
    }

    pub async fn count_pending_outbox(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM outbox_events WHERE status NOT IN ('published', 'dead_letter')",
            Vec::new(),
        )
        .await
    }

    pub async fn count_pending_repairs(&self) -> Result<u64> {
        codec::count_scalar(
            self.read_pool(),
            "SELECT COUNT(*) AS n FROM repair_items WHERE status != 'resolved'",
            Vec::new(),
        )
        .await
    }

    /// On-disk database size in bytes (IDX-021): page count × page size.
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

/// The single assembly point (CONTEXT.md: 组装点): turn an opened database,
/// an optional storage factory and a clock into the application dependency
/// graph. `bootstrap::Runtime::open` and the application test fixture both
/// call this; neither re-transcribes the dependency list.
pub fn assemble(
    db: &Database,
    storage_factory: Option<Arc<dyn rstore_storage_api::StorageFactory>>,
    clock: Arc<dyn Clock>,
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
            multipart: Arc::new(db.multipart(clock.clone())),
        },
        RuntimeDependencies {
            stats: Arc::new(db.clone()),
            clock,
            storage_factory,
            unit_of_work: Arc::new(db.clone()),
            repairs: Arc::new(db.outbox()),
        },
    )
}
