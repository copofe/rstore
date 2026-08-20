//! Unit-of-work seam for multi-statement writes.
//!
//! One interface owns begin/commit/rollback translation: a crash or
//! interleaved write between the statements of one logical operation cannot
//! leave half-applied state — a tombstone deleted without its marker, a
//! merged tag whose source associations still dangle. Repositories declare
//! *what* belongs together; this module owns *that it is atomic*. The bounded
//! SQLite writer gate is acquired by each write repository before entering
//! this seam.

use std::future::Future;
use std::pin::Pin;

use rstore_domain::{Error, Result};
use sqlx::sqlite::{Sqlite, SqlitePool};
use sqlx::Transaction;

/// Run `work` inside one transaction. `label` names the operation in
/// begin/commit failures. An error from `work` rolls back (best-effort —
/// dropping the transaction rolls back too) and stays the answer.
///
/// `work` receives the transaction by exclusive borrow and must own its
/// inputs (clone at the call site) — the unit of work has no other
/// lifetimes to lean on.
pub async fn atomically<T, F>(pool: &SqlitePool, label: &str, work: F) -> Result<T>
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
    use crate::Database;
    use rstore_domain::WorkspaceId;

    /// The seam's contract: a failed unit of work leaves no trace, and the
    /// original error stays the answer.
    #[tokio::test]
    async fn failed_work_rolls_back_completely() {
        let db = Database::open_in_memory().await.unwrap();
        let id = WorkspaceId::new();

        let result: Result<()> = atomically(db.pool(), "test unit", |tx| {
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
