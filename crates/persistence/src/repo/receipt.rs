//! Durable receipts for completed remote mutations.
use async_trait::async_trait;
use rstore_application_ports::{RemoteMutationReceipt, RemoteMutationRepository};
use rstore_domain::id::ReceiptId;
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;

use crate::codec::{unix_secs, RowReader};
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

#[derive(Clone)]
/// Read access to mutation receipts. Receipts are written only through the outbox transaction path;
/// this repository exists for reads.
pub struct SqliteRemoteMutationRepository {
    read_pool: SqlitePool,
}

impl SqliteRemoteMutationRepository {
    pub(crate) fn new(read_pool: SqlitePool) -> Self {
        Self { read_pool }
    }
}

#[async_trait]
impl RemoteMutationRepository for SqliteRemoteMutationRepository {
    async fn get(&self, receipt_id: ReceiptId) -> Result<Option<RemoteMutationReceipt>> {
        let row = sqlx::query(
            "SELECT receipt_id, job_id, operation, locator_json, remote_version, remote_etag, \
                    completed_at, payload_json FROM remote_mutation_receipts WHERE receipt_id = ?",
        )
        .bind(receipt_id.to_string())
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            let reader = RowReader::new(row);
            let payload = reader
                .opt_text("payload_json")?
                .map(|raw| {
                    serde_json::from_str(&raw).map_err(|error| {
                        Error::database("decode mutation receipt payload").with_source(error)
                    })
                })
                .transpose()?;
            Ok(RemoteMutationReceipt {
                receipt_id: reader.text("receipt_id")?.parse()?,
                job_id: reader
                    .opt_text("job_id")?
                    .map(|token| token.parse())
                    .transpose()?,
                operation: reader.text("operation")?,
                locator_json: reader.text("locator_json")?,
                remote_version: reader.opt_text("remote_version")?,
                remote_etag: reader.opt_text("remote_etag")?,
                completed_at: reader.timestamp("completed_at")?,
                payload,
            })
        })
        .transpose()
    }
}
