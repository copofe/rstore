//! Unit-of-work seam for multi-statement writes.
//!
//! `WriteGate` is the write side of one SQLite database: a single-connection
//! pool, so in-process writes serialise on that connection while WAL plus the
//! busy timeout arbitrate between processes. Multi-statement writes run inside
//! `BEGIN IMMEDIATE`, which takes the database write lock up front — a deferred
//! transaction that later upgrades can fail with `SQLITE_BUSY` instead of
//! waiting when another process committed in between.
use std::future::Future;
use std::pin::Pin;

use rstore_domain::{Error, Result};
use sqlx::sqlite::{Sqlite, SqlitePool};
use sqlx::Transaction;

#[derive(Clone)]
pub(crate) struct WriteGate {
    pool: SqlitePool,
}

impl WriteGate {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// The write pool, for reads that must observe the writer's view and for single atomic statements.
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Run `work` inside one immediate transaction named `label` in begin/commit failures. An error from
    /// `work` rolls back and stays the answer; `work` receives the transaction by exclusive borrow and must
    /// own its inputs (clone at the call site).
    pub(crate) async fn write_unit<T, F>(&self, label: &str, work: F) -> Result<T>
    where
        F: for<'c> FnOnce(
            &'c mut Transaction<'_, Sqlite>,
        ) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>,
    {
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| Error::database(format!("begin {label}")).with_source(e))?;
        match work(&mut tx).await {
            Ok(value) => {
                tx.commit()
                    .await
                    .map_err(|e| Error::database(format!("commit {label}")).with_source(e))?;
                Ok(value)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
}
