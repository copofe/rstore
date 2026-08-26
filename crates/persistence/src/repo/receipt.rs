//! Durable receipts for completed remote mutations.
use rstore_application_ports::RemoteMutationReceipt;
use rstore_domain::Result;

use crate::codec::unix_secs;
use crate::error::map_sqlx;

/// Insert one remote-mutation receipt through any SQLite executor. Keeping the column list here prevents
/// the standalone read path and the outbox transaction path from drifting apart.
pub(crate) async fn insert_receipt<'e, E>(
    executor: E,
    receipt: &RemoteMutationReceipt,
) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query(
        "INSERT INTO remote_mutation_receipts \
         (receipt_id, job_id, operation, locator_json, remote_version, remote_etag, \
          completed_at, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(receipt_id) DO NOTHING",
    )
    .bind(receipt.receipt_id.to_string())
    .bind(receipt.job_id.map(|id| id.to_string()))
    .bind(&receipt.operation)
    .bind(&receipt.locator_json)
    .bind(&receipt.remote_version)
    .bind(&receipt.remote_etag)
    .bind(unix_secs(receipt.completed_at))
    .bind(receipt.payload.as_ref().map(ToString::to_string))
    .execute(executor)
    .await
    .map_err(map_sqlx)?;
    Ok(())
}
