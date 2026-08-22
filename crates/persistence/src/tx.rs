//! Unit-of-work seam for multi-statement writes.
//!
//! One interface owns begin/commit/rollback translation: a crash or
//! interleaved write between the statements of one logical operation cannot
//! leave half-applied state — a tombstone deleted without its marker, a
//! merged tag whose source associations still dangle. Repositories declare
//! *what* belongs together; [`WriteGate::write_unit`] owns *that it is
//! atomic* — the write permit and the transaction are acquired together or
//! not at all, so no repository can open a transaction outside the bounded
//! writer gate, nor re-acquire the permit from inside the actor's own task.

use std::future::Future;
use std::pin::Pin;

use rstore_domain::{Error, Result};
use sqlx::sqlite::{Sqlite, SqlitePool};
use sqlx::Transaction;

use crate::actor::{SqliteWriter, WritePermit};

/// The write path of one SQLite database: the write pool plus the bounded
/// writer actor. Repositories hold a clone of the gate instead of the two
/// pieces separately — the pairing is the invariant.
#[derive(Clone)]
pub(crate) struct WriteGate {
    pool: SqlitePool,
    writer: SqliteWriter,
}

impl WriteGate {
    pub(crate) fn new(pool: SqlitePool, writer: SqliteWriter) -> Self {
        Self { pool, writer }
    }

    /// The write pool. Reads that must observe the writer's view (or single
    /// atomic statements) execute here; catalog reads use the read pool.
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Acquire the write permit for a single atomic statement. Multi-statement
    /// writes must use [`write_unit`](Self::write_unit) instead — a permit
    /// held across hand-managed transactions is the shape this gate removes.
    pub(crate) async fn permit(&self) -> Result<WritePermit> {
        self.writer.acquire().await
    }

    /// Acquire the write permit, then run `work` inside one transaction.
    /// `label` names the operation in begin/commit failures. An error from
    /// `work` rolls back (best-effort — dropping the transaction rolls back
    /// too) and stays the answer; the permit drops with the future.
    ///
    /// `work` receives the transaction by exclusive borrow and must own its
    /// inputs (clone at the call site) — the unit of work has no other
    /// lifetimes to lean on.
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

/// Run `work` inside one transaction. `label` names the operation in
/// begin/commit failures. An error from `work` rolls back (best-effort —
/// dropping the transaction rolls back too) and stays the answer.
///
/// Private to this module: callers reach transactions through
/// [`WriteGate::write_unit`], which owns the permit/transaction pairing.
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

    /// The seam's contract: a failed unit of work leaves no trace, and the
    /// original error stays the answer.
    #[tokio::test]
    async fn failed_work_rolls_back_completely() {
        let db = Database::open_in_memory().await.unwrap();
        let gate = WriteGate::new(db.pool().clone(), SqliteWriter::new(1));
        let id = WorkspaceId::new();

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
