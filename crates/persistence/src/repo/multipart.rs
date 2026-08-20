//! SQLite-backed multipart checkpoint store (UP-014/015, JOB-013).
//!
//! Implements [`MultipartStore`] over `multipart_uploads` / `multipart_parts`
//! so an interrupted upload resumes from its completed parts on the next run.

use async_trait::async_trait;
use std::sync::Arc;

use rstore_domain::{Clock, Error, Result};
use rstore_storage_api::model::UploadedPart;
use rstore_storage_api::{FileFingerprint, MultipartStore, PendingUpload};
use sqlx::sqlite::SqlitePool;

use crate::codec::{Bind, Insert, RowReader};
use crate::error::map_sqlx;

/// SQLite checkpoint store.
#[derive(Clone)]
pub struct SqliteMultipartStore {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: crate::actor::SqliteWriter,
    clock: Arc<dyn Clock>,
}

impl SqliteMultipartStore {
    pub fn new(pool: SqlitePool, clock: Arc<dyn Clock>) -> Self {
        Self::new_with_pools(
            pool.clone(),
            pool,
            crate::actor::SqliteWriter::new(64),
            clock,
        )
    }

    pub fn new_with_pools(
        pool: SqlitePool,
        read_pool: SqlitePool,
        writer: crate::actor::SqliteWriter,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            pool,
            read_pool,
            writer,
            clock,
        }
    }

    /// The `multipart_uploads.id` row for (connection, bucket, key).
    async fn row_id(&self, connection: &str, bucket: &str, key: &str) -> Result<Option<i64>> {
        let row = sqlx::query(
            "SELECT id FROM multipart_uploads
             WHERE connection_id = ? AND bucket = ? AND key = ?",
        )
        .bind(connection)
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| RowReader::new(row).integer("id")).transpose()
    }
}

#[async_trait]
impl MultipartStore for SqliteMultipartStore {
    async fn pending(
        &self,
        connection: &str,
        bucket: &str,
        key: &str,
    ) -> Result<Option<PendingUpload>> {
        let row = sqlx::query(
            "SELECT id, upload_id, source_size, source_modified_ns
             FROM multipart_uploads
             WHERE connection_id = ? AND bucket = ? AND key = ?",
        )
        .bind(connection)
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

    async fn started(
        &self,
        connection: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        fingerprint: &FileFingerprint,
    ) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let source_size = i64::try_from(fingerprint.size)
            .map_err(|e| Error::invalid_input("multipart source is too large").with_source(e))?;
        Insert::into("multipart_uploads")
            .column("job_id", Bind::NullInteger)
            .column("connection_id", connection)
            .column("bucket", bucket)
            .column("key", key)
            .column("upload_id", upload_id)
            .column("created_at", crate::codec::unix_secs(self.clock.now()))
            .column("source_size", source_size)
            .column("source_modified_ns", fingerprint.modified_ns)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn part_done(
        &self,
        connection: &str,
        bucket: &str,
        key: &str,
        part: &UploadedPart,
    ) -> Result<()> {
        self.record_part(connection, bucket, key, part).await
    }

    async fn cleared(&self, connection: &str, bucket: &str, key: &str) -> Result<()> {
        let _write = self.writer.acquire().await?;
        sqlx::query(
            "DELETE FROM multipart_uploads
             WHERE connection_id = ? AND bucket = ? AND key = ?",
        )
        .bind(connection)
        .bind(bucket)
        .bind(key)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}

impl SqliteMultipartStore {
    async fn record_part(
        &self,
        connection: &str,
        bucket: &str,
        key: &str,
        part: &UploadedPart,
    ) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let part_size = i64::try_from(part.size)
            .map_err(|e| Error::invalid_input("multipart part is too large").with_source(e))?;
        let row_id = self
            .row_id(connection, bucket, key)
            .await?
            .ok_or_else(|| Error::not_found("multipart checkpoint row"))?;
        Insert::into("multipart_parts")
            .column("multipart_upload_id", row_id)
            .column("part_number", part.part_number as i64)
            .column("etag", &part.etag)
            .column("size", part_size)
            .column("uploaded_at", crate::codec::unix_secs(self.clock.now()))
            .suffix(
                "ON CONFLICT(multipart_upload_id, part_number) DO UPDATE SET \
                 etag = excluded.etag, size = excluded.size",
            )
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_domain::Clock;
    use time::OffsetDateTime;

    #[derive(Debug)]
    struct FixedClock(OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }

    #[tokio::test]
    async fn checkpoint_round_trips_fingerprint_parts_and_injected_time() {
        let db = crate::Database::open_in_memory().await.unwrap();
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_123).unwrap();
        let store = SqliteMultipartStore::new(db.pool().clone(), Arc::new(FixedClock(now)));
        let fingerprint = FileFingerprint {
            size: 42,
            modified_ns: Some(1_700_000_123_456_789_000),
        };
        store
            .started("conn", "bucket", "key", "upload-1", &fingerprint)
            .await
            .unwrap();
        store
            .part_done(
                "conn",
                "bucket",
                "key",
                &UploadedPart {
                    part_number: 1,
                    etag: "etag-1".into(),
                    size: 42,
                },
            )
            .await
            .unwrap();

        let pending = store
            .pending("conn", "bucket", "key")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.upload_id, "upload-1");
        assert_eq!(pending.fingerprint, fingerprint);
        assert_eq!(pending.parts.len(), 1);
        let created_at: i64 =
            sqlx::query_scalar("SELECT created_at FROM multipart_uploads WHERE upload_id = ?")
                .bind("upload-1")
                .fetch_one(db.pool())
                .await
                .unwrap();
        let uploaded_at: i64 = sqlx::query_scalar("SELECT uploaded_at FROM multipart_parts")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(created_at, now.unix_timestamp());
        assert_eq!(uploaded_at, now.unix_timestamp());

        store.cleared("conn", "bucket", "key").await.unwrap();
        assert!(store
            .pending("conn", "bucket", "key")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn corrupt_checkpoint_is_an_error_not_a_missing_checkpoint() {
        let db = crate::Database::open_in_memory().await.unwrap();
        let store = SqliteMultipartStore::new(
            db.pool().clone(),
            Arc::new(FixedClock(OffsetDateTime::UNIX_EPOCH)),
        );
        store
            .started(
                "conn",
                "bucket",
                "key",
                "upload-1",
                &FileFingerprint {
                    size: 42,
                    modified_ns: None,
                },
            )
            .await
            .unwrap();
        sqlx::query("UPDATE multipart_uploads SET source_size = -1")
            .execute(db.pool())
            .await
            .unwrap();

        let error = store.pending("conn", "bucket", "key").await.unwrap_err();
        assert_eq!(error.code, rstore_domain::ErrorCode::Database);
    }
}
