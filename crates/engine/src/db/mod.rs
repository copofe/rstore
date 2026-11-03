//! The SQLite store: write gate, read-only pool, embedded migrations, and the
//! repositories built over them.
//!
//! SQLite is the application's file format, not a swappable adapter: use cases
//! call these repositories directly. The module stays private to the engine,
//! so entry points never reach SQL.
pub mod codec;
pub mod error;
pub mod records;
pub mod repo;
pub mod tx;

pub use records::*;
pub use repo::bucket::BucketRepository;
pub use repo::connection::ConnectionRepository;
pub use repo::ignore::IgnoreRuleRepository;
pub use repo::index::IndexRunRepository;
pub use repo::job::JobRepository;
pub use repo::link::LinkRepository;
pub use repo::multipart::MultipartStore;
pub use repo::remote_object::CatalogRepository;
pub use repo::settings::SettingsRepository;
pub use repo::tag::TagRepository;
pub use repo::workspace::WorkspaceRepository;

use std::path::Path;
use std::time::Duration;

use rstore_domain::{Clock, Error, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use std::sync::Arc;

#[derive(Clone)]
/// Handle to the open SQLite database. It owns the injected clock, so
/// every repository it hands out stamps time from a single source.
pub struct Database {
    pool: SqlitePool,
    read_pool: SqlitePool,
    clock: Arc<dyn Clock>,
    /// Where the database lives: the file path, or `:memory:`.
    location: String,
}

impl Database {
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

        // One write connection: in-process writes serialise here, WAL and the
        // busy timeout arbitrate with other processes.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
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
            clock,
            location: db_path.display().to_string(),
        };
        db.migrate().await?;
        Ok(db)
    }

    /// Run the embedded schema migrations inside a transaction, so a failure leaves the prior schema intact.
    async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(migration_error)
    }

    // Repositories reach the write pool only through this gate.
    fn gate(&self) -> crate::db::tx::WriteGate {
        crate::db::tx::WriteGate::new(self.pool.clone())
    }

    /// Read-side pool reserved for catalog and diagnostics queries. In-memory databases share the write pool:
    /// a separate SQLite memory pool would be a different database.
    fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    /// Where the catalog is stored (a file path, or `:memory:`), for diagnostics.
    pub fn location(&self) -> String {
        self.location.clone()
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

    /// A repository for app settings (machine-local state).
    pub fn settings(&self) -> SettingsRepository {
        SettingsRepository::new(self.gate(), self.read_pool.clone(), self.clock.clone())
    }

    /// The remote-object catalog repository.
    pub fn catalog(&self) -> CatalogRepository {
        CatalogRepository::new(self.gate(), self.read_pool.clone())
    }

    /// The transfer-job repository.
    pub fn jobs(&self) -> JobRepository {
        JobRepository::new(self.gate(), self.read_pool.clone())
    }

    /// The multipart checkpoint store for resumable uploads.
    pub fn multipart(&self) -> MultipartStore {
        MultipartStore::new(self.gate(), self.read_pool.clone(), self.clock.clone())
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
        Ok(crate::db::codec::RowReader::new(row).integer("bytes")? as u64)
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
