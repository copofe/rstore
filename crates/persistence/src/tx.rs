//! Unit-of-work seam for multi-statement writes.
//!
//! `WriteGate` pairs the write permit with the transaction so the statements
//! of one logical operation commit atomically or not at all; no repository can
//! open a transaction outside the bounded writer gate.
use std::future::Future;
use std::pin::Pin;

use rstore_domain::{Error, Result};
use sqlx::sqlite::{Sqlite, SqlitePool};
use sqlx::Transaction;

use crate::actor::{SqliteWriter, WritePermit};

#[derive(Clone)]
/// The write path of one SQLite database: the write pool plus the bounded writer actor, held together
/// because their pairing is the invariant.
pub(crate) struct WriteGate {
    pool: SqlitePool,
    writer: SqliteWriter,
}

impl WriteGate {
    pub(crate) fn new(pool: SqlitePool, writer: SqliteWriter) -> Self {
        Self { pool, writer }
    }

    /// The write pool, for reads that must observe the writer's view and for single atomic statements.
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Acquire the write permit for a single atomic statement; multi-statement writes must use
    /// [`Self::write_unit`].
    pub(crate) async fn permit(&self) -> Result<WritePermit> {
        self.writer.acquire().await
    }

    /// Acquire the write permit, then run `work` inside one transaction named `label` in begin/commit
    /// failures. An error from `work` rolls back and stays the answer; `work` receives the transaction
    /// by exclusive borrow and must own its inputs (clone at the call site).
    pub(crate) async fn write_unit<T, F>(&self, label: &str, work: F) -> Result<T>
    where
        F: for<'c> FnOnce(
            &'c mut Transaction<'_, Sqlite>,
        ) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>,
    {
        let _write = self.permit().await?;
        atomically(&self.pool, label, work).await
    }
}

/// Callers reach transactions only through [`WriteGate::write_unit`], which owns the permit/transaction
/// pairing.
async fn atomically<T, F>(pool: &SqlitePool, label: &str, work: F) -> Result<T>
where
    F: for<'c> FnOnce(
        &'c mut Transaction<'_, Sqlite>,
    ) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>,
{
    let mut tx = pool
        .begin()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::SqliteWriter;
    use crate::Database;
    use rstore_domain::WorkspaceId;

    #[tokio::test]
    async fn failed_work_rolls_back_completely() {
        let db = Database::open_in_memory().await.unwrap();
        let gate = WriteGate::new(db.pool().clone(), SqliteWriter::new(1));
        let id = WorkspaceId::new_random();

        let result: Result<()> = gate
            .write_unit("test unit", |tx| {
                Box::pin(async move {
                    sqlx::query(
                        "INSERT INTO workspaces (id, name, description, created_at, updated_at) \
                         VALUES (?, 'ghost', NULL, 0, 0)",
                    )
                    .bind(crate::codec::uuid_to_text(&id))
                    .execute(&mut **tx)
                    .await
                    .map_err(crate::error::map_sqlx)?;
                    Err(Error::conflict("boom"))
                })
            })
            .await;

        assert_eq!(result.unwrap_err().code, rstore_domain::ErrorCode::Conflict);
        let ghost: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workspaces WHERE name = 'ghost'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(ghost, 0, "the half-done write must not survive");
    }
}
