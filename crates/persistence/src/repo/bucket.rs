//! Repository for locally registered [`Bucket`] rows (BKT-).

use rstore_domain::bucket::{Bucket, BucketConfig};
use rstore_domain::id::{BucketId, ConnectionId};
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;

use crate::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::error::map_sqlx;

const COLUMNS: &str = "id, connection_id, name, display_name, read_only, allow_delete, \
                       index_enabled, default_conflict, concurrency, created_at, updated_at";

/// CRUD access to `buckets`.
#[derive(Clone)]
pub struct BucketRepository {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: crate::actor::SqliteWriter,
}

impl BucketRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self::new_with_pools(pool.clone(), pool, crate::actor::SqliteWriter::new(64))
    }

    pub fn new_with_pools(
        pool: SqlitePool,
        read_pool: SqlitePool,
        writer: crate::actor::SqliteWriter,
    ) -> Self {
        Self {
            pool,
            read_pool,
            writer,
        }
    }

    /// Insert a bucket registration. A duplicate `(connection, name)` surfaces
    /// as `Conflict` (BKT-002).
    pub async fn insert(&self, bucket: &Bucket) -> Result<()> {
        let _write = self.writer.acquire().await?;
        Insert::into("buckets")
            .column("id", uuid_to_text(&bucket.id))
            .column("connection_id", uuid_to_text(&bucket.connection_id))
            .column("name", &bucket.name)
            .column("display_name", bucket.config.display_name.as_ref())
            .column("read_only", bucket.config.read_only)
            .column("allow_delete", bucket.config.allow_delete)
            .column("index_enabled", bucket.config.index_enabled)
            .column("default_conflict", bucket.config.default_conflict.as_str())
            .column("concurrency", bucket.config.concurrency.map(|c| c as i64))
            .column("created_at", unix_secs(bucket.created_at))
            .column("updated_at", unix_secs(bucket.updated_at))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// All registered buckets, ordered by connection then name.
    pub async fn list(&self) -> Result<Vec<Bucket>> {
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM buckets ORDER BY connection_id, name"
        ))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(row_to_bucket).collect()
    }

    pub async fn get(&self, id: BucketId) -> Result<Option<Bucket>> {
        let row = sqlx::query(&format!("SELECT {COLUMNS} FROM buckets WHERE id = ?"))
            .bind(uuid_to_text(&id))
            .fetch_optional(&self.read_pool)
            .await
            .map_err(map_sqlx)?;
        row.map(row_to_bucket).transpose()
    }

    pub async fn get_by_name(
        &self,
        connection: ConnectionId,
        name: &str,
    ) -> Result<Option<Bucket>> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM buckets WHERE connection_id = ? AND name = ?"
        ))
        .bind(uuid_to_text(&connection))
        .bind(name)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(row_to_bucket).transpose()
    }

    /// All buckets registered under a connection (CON-013 reference check).
    pub async fn list_by_connection(&self, connection: ConnectionId) -> Result<Vec<Bucket>> {
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM buckets WHERE connection_id = ? ORDER BY name"
        ))
        .bind(uuid_to_text(&connection))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(row_to_bucket).collect()
    }

    /// Update an existing bucket's mutable fields.
    pub async fn update(&self, bucket: &Bucket) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let affected = Update::table("buckets")
            .set("display_name", bucket.config.display_name.as_ref())
            .set("read_only", bucket.config.read_only)
            .set("allow_delete", bucket.config.allow_delete)
            .set("index_enabled", bucket.config.index_enabled)
            .set("default_conflict", bucket.config.default_conflict.as_str())
            .set("concurrency", bucket.config.concurrency.map(|c| c as i64))
            .set("updated_at", unix_secs(bucket.updated_at))
            .filter("id = ?", [Bind::Text(uuid_to_text(&bucket.id))])
            .execute(&self.pool)
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(Error::not_found("bucket not found"));
        }
        Ok(())
    }

    /// Delete a bucket registration (workspace links cascade via FK).
    pub async fn delete(&self, id: BucketId) -> Result<bool> {
        let _write = self.writer.acquire().await?;
        let affected = sqlx::query("DELETE FROM buckets WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }
}

fn row_to_bucket(row: sqlx::sqlite::SqliteRow) -> Result<Bucket> {
    let row = RowReader::new(row);
    Ok(Bucket {
        id: BucketId::from_uuid(row.uuid("id")?),
        connection_id: ConnectionId::from_uuid(row.uuid("connection_id")?),
        name: row.text("name")?,
        config: BucketConfig {
            display_name: row.opt_text("display_name")?,
            read_only: row.bool("read_only")?,
            allow_delete: row.bool("allow_delete")?,
            index_enabled: row.bool("index_enabled")?,
            default_conflict: row.token("default_conflict", ConflictPolicy::parse)?,
            concurrency: row.opt_integer("concurrency")?.map(|c| c as u32),
        },
        created_at: row.timestamp("created_at")?,
        updated_at: row.timestamp("updated_at")?,
    })
}
