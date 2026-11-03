//! SQLite-backed multipart checkpoint store.
//!
//! Implements the storage-layer multipart port over `multipart_uploads` /
//! `multipart_parts` so an interrupted upload resumes from its completed parts
//! on the next run.
use std::sync::Arc;

use crate::db::{FileFingerprint, PendingUpload};
use rstore_domain::id::ConnectionId;
use rstore_domain::{Clock, Error, Result};
use rstore_storage::UploadedPart;
use sqlx::sqlite::SqlitePool;

use crate::db::codec::{Bind, Insert, RowReader};
use crate::db::error::map_sqlx;
use crate::db::tx::WriteGate;

#[derive(Clone)]
pub struct MultipartStore {
    gate: WriteGate,
    read_pool: SqlitePool,
    clock: Arc<dyn Clock>,
}

impl MultipartStore {
    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool, clock: Arc<dyn Clock>) -> Self {
        Self {
            gate,
            read_pool,
            clock,
        }
    }

    async fn row_id<'e, E>(
        executor: E,
        connection: ConnectionId,
        bucket: &str,
        key: &str,
    ) -> Result<Option<i64>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let row = sqlx::query(
            "SELECT id FROM multipart_uploads
             WHERE connection_id = ? AND bucket = ? AND key = ?",
        )
        .bind(crate::db::codec::uuid_to_text(&connection))
        .bind(bucket)
        .bind(key)
        .fetch_optional(executor)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| RowReader::new(row).integer("id")).transpose()
    }
}

impl MultipartStore {
    pub async fn pending(
        &self,
        connection: ConnectionId,
        bucket: &str,
        key: &str,
    ) -> Result<Option<PendingUpload>> {
        let row = sqlx::query(
            "SELECT id, upload_id, source_size, source_modified_ns
             FROM multipart_uploads
             WHERE connection_id = ? AND bucket = ? AND key = ?",
        )
        .bind(crate::db::codec::uuid_to_text(&connection))
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let row = RowReader::new(row);
        let row_id = row.integer("id")?;
        let upload_id = row.text("upload_id")?;
        let source_size = u64::try_from(row.integer("source_size")?)
            .map_err(|e| Error::database("negative multipart source size").with_source(e))?;
        let fingerprint = FileFingerprint {
            size: source_size,
            modified_ns: row.opt_integer("source_modified_ns")?,
        };
        let rows = sqlx::query(
            "SELECT part_number, etag, size FROM multipart_parts
             WHERE multipart_upload_id = ? ORDER BY part_number",
        )
        .bind(row_id)
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        let parts = rows
            .into_iter()
            .map(|row| {
                let row = RowReader::new(row);
                Ok(UploadedPart {
                    part_number: u32::try_from(row.integer("part_number")?).map_err(|e| {
                        Error::database("invalid multipart part number").with_source(e)
                    })?,
                    etag: row.text("etag")?,
                    size: u64::try_from(row.integer("size")?).map_err(|e| {
                        Error::database("negative multipart part size").with_source(e)
                    })?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(PendingUpload {
            upload_id,
            parts,
            fingerprint,
        }))
    }

    pub async fn started(
        &self,
        connection: ConnectionId,
        bucket: &str,
        key: &str,
        upload_id: &str,
        fingerprint: &FileFingerprint,
    ) -> Result<()> {
        let source_size = i64::try_from(fingerprint.size)
            .map_err(|e| Error::invalid_input("multipart source is too large").with_source(e))?;
        Insert::into("multipart_uploads")
            .column("job_id", Bind::NullInteger)
            .column("connection_id", crate::db::codec::uuid_to_text(&connection))
            .column("bucket", bucket)
            .column("key", key)
            .column("upload_id", upload_id)
            .column("created_at", crate::db::codec::unix_secs(self.clock.now()))
            .column("source_size", source_size)
            .column("source_modified_ns", fingerprint.modified_ns)
            .execute(self.gate.pool())
            .await?;
        Ok(())
    }

    pub async fn part_done(
        &self,
        connection: ConnectionId,
        bucket: &str,
        key: &str,
        part: &UploadedPart,
    ) -> Result<()> {
        self.record_part(connection, bucket, key, part).await
    }

    pub async fn cleared(&self, connection: ConnectionId, bucket: &str, key: &str) -> Result<()> {
        sqlx::query(
            "DELETE FROM multipart_uploads
             WHERE connection_id = ? AND bucket = ? AND key = ?",
        )
        .bind(crate::db::codec::uuid_to_text(&connection))
        .bind(bucket)
        .bind(key)
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}

impl MultipartStore {
    async fn record_part(
        &self,
        connection: ConnectionId,
        bucket: &str,
        key: &str,
        part: &UploadedPart,
    ) -> Result<()> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let part = part.clone();
        let uploaded_at = self.clock.now();
        self.gate
            .write_unit("multipart part", move |tx| {
                Box::pin(async move {
                    let part_size = i64::try_from(part.size).map_err(|e| {
                        Error::invalid_input("multipart part is too large").with_source(e)
                    })?;
                    let row_id = Self::row_id(&mut **tx, connection, &bucket, &key)
                        .await?
                        .ok_or_else(|| Error::not_found("multipart checkpoint row"))?;
                    Insert::into("multipart_parts")
                        .column("multipart_upload_id", row_id)
                        .column("part_number", part.part_number as i64)
                        .column("etag", &part.etag)
                        .column("size", part_size)
                        .column("uploaded_at", crate::db::codec::unix_secs(uploaded_at))
                        .suffix(
                            "ON CONFLICT(multipart_upload_id, part_number) DO UPDATE SET \
                             etag = excluded.etag, size = excluded.size",
                        )
                        .execute(&mut **tx)
                        .await?;
                    Ok(())
                })
            })
            .await
    }
}
