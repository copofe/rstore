//! SQLite-backed [`RemoteObjectRepository`] plus the tombstone write that
//! records deletions for catalog staleness (DEL-009 / IDX-011).

use async_trait::async_trait;
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::id::{BucketId, ConnectionId, RemoteObjectId};
use rstore_domain::remote::{ObjectPath, RemoteObject};
use rstore_domain::ObjectState;
use rstore_domain::{Error, Result, TypedId};
use sqlx::sqlite::SqlitePool;
use time::OffsetDateTime;

use crate::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader};
use crate::error::map_sqlx;

const COLUMNS: &str = "id, connection_id, bucket_id, object_path, size, etag, content_type, \
                       last_modified, storage_class, version_id, confirmed_at, state, file_type";

/// Catalog persistence for indexed remote objects.
#[derive(Clone)]
pub struct SqliteRemoteObjectRepository {
    gate: crate::tx::WriteGate,
    read_pool: SqlitePool,
}

impl SqliteRemoteObjectRepository {
    pub(crate) fn new(gate: crate::tx::WriteGate, read_pool: SqlitePool) -> Self {
        Self { gate, read_pool }
    }

    /// A tombstone expires at its own `expires_at` when set, else
    /// `created_at + retention` (IDX-020 / TAG-026).
    fn expired_clause(retention_secs: u64) -> String {
        format!(
            "(expires_at IS NOT NULL AND expires_at <= ?1) \
             OR (expires_at IS NULL AND created_at <= ?1 - {retention_secs})"
        )
    }

    async fn find_id<'e, E>(
        executor: E,
        connection: ConnectionId,
        bucket: BucketId,
        path: &ObjectPath,
    ) -> Result<Option<(RemoteObjectId, String)>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let row = sqlx::query(
            "SELECT id, state FROM remote_objects
             WHERE connection_id = ? AND bucket_id = ? AND object_path = ?",
        )
        .bind(uuid_to_text(&connection))
        .bind(uuid_to_text(&bucket))
        .bind(path.as_str())
        .fetch_optional(executor)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            let reader = RowReader::new(row);
            Ok((
                RemoteObjectId::from_uuid(reader.uuid("id")?),
                reader.text("state")?,
            ))
        })
        .transpose()
    }

    pub(crate) async fn upsert_with_executor<'e, E>(
        executor: E,
        object: &RemoteObject,
    ) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        // The row is stored as given: confirmation semantics belong to the
        // `confirmed_object` constructor, not the repository.
        Insert::into("remote_objects")
            .column("id", uuid_to_text(&object.id))
            .column("connection_id", uuid_to_text(&object.connection_id))
            .column("bucket_id", uuid_to_text(&object.bucket_id))
            .column("object_path", object.path.as_str())
            .column("size", object.size as i64)
            .column("etag", &object.etag)
            .column("content_type", &object.content_type)
            .column("last_modified", unix_secs(object.last_modified))
            .column("storage_class", &object.storage_class)
            .column("version_id", &object.version_id)
            .column("confirmed_at", unix_secs(object.confirmed_at))
            .column("state", object.state.as_str())
            .column("file_type", object.type_hint().as_str())
            .suffix(
                "ON CONFLICT(connection_id, bucket_id, object_path) DO UPDATE SET \
                 size = excluded.size, \
                 etag = excluded.etag, \
                 content_type = excluded.content_type, \
                 last_modified = excluded.last_modified, \
                 storage_class = excluded.storage_class, \
                 version_id = excluded.version_id, \
                 confirmed_at = excluded.confirmed_at, \
                 state = excluded.state, \
                 file_type = excluded.file_type",
            )
            .execute(executor)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl RemoteObjectRepository for SqliteRemoteObjectRepository {
    /// Record a deletion (DEL-009): drop the catalog row (if any) and insert
    /// a tombstone carrying the locator for later index reconciliation —
    /// one unit of work, so a crash between the two cannot lose the marker.
    async fn tombstone(
        &self,
        connection: ConnectionId,
        bucket: BucketId,
        path: &ObjectPath,
        at: OffsetDateTime,
    ) -> Result<()> {
        let path = path.as_str().to_string();
        self.gate
            .write_unit("tombstone", |tx| {
                Box::pin(async move {
                    let existing =
                        Self::find_id(&mut **tx, connection, bucket, &ObjectPath::new(&path)?)
                            .await?;
                    if let Some((id, _)) = existing {
                        sqlx::query("DELETE FROM remote_objects WHERE id = ?")
                            .bind(uuid_to_text(&id))
                            .execute(&mut **tx)
                            .await
                            .map_err(map_sqlx)?;
                    }
                    let object_id = existing
                        .map(|(id, _)| id)
                        .unwrap_or_else(RemoteObjectId::new);
                    Insert::into("metadata_tombstones")
                        .column("workspace_id", Bind::NullText)
                        .column("remote_object_id", uuid_to_text(&object_id))
                        .column("tag_id", Bind::NullText)
                        .column("locator", format!("{}/{}", bucket.as_uuid(), path))
                        .column("created_at", unix_secs(at))
                        .column("expires_at", Bind::NullInteger)
                        .execute(&mut **tx)
                        .await?;
                    Ok(())
                })
            })
            .await
    }

    async fn upsert(&self, object: &RemoteObject) -> Result<()> {
        let _write = self.gate.permit().await?;
        Self::upsert_with_executor(self.gate.pool(), object).await
    }

    async fn upsert_many(&self, objects: &[RemoteObject]) -> Result<()> {
        let objects = objects.to_vec();
        self.gate
            .write_unit("catalog page", |tx| {
                Box::pin(async move {
                    for object in &objects {
                        Self::upsert_with_executor(&mut **tx, object).await?;
                    }
                    Ok(())
                })
            })
            .await
    }

    async fn get(&self, id: RemoteObjectId) -> Result<Option<RemoteObject>> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM remote_objects WHERE id = ?"
        ))
        .bind(uuid_to_text(&id))
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(row_to_object).transpose()
    }

    async fn delete(&self, id: RemoteObjectId) -> Result<()> {
        let _write = self.gate.permit().await?;
        sqlx::query("DELETE FROM remote_objects WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    async fn count_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        crate::codec::count_scalar(
            &self.read_pool,
            "SELECT COUNT(*) AS n FROM remote_objects WHERE bucket_id = ?",
            vec![Bind::Text(uuid_to_text(&bucket))],
        )
        .await
    }

    async fn find_by_locator(
        &self,
        connection: ConnectionId,
        bucket: BucketId,
        path: &ObjectPath,
    ) -> Result<Option<RemoteObject>> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM remote_objects \
             WHERE connection_id = ? AND bucket_id = ? AND object_path = ?"
        ))
        .bind(uuid_to_text(&connection))
        .bind(uuid_to_text(&bucket))
        .bind(path.as_str())
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(row_to_object).transpose()
    }

    async fn count_stale(&self, bucket: BucketId) -> Result<u64> {
        crate::codec::count_scalar(
            &self.read_pool,
            "SELECT COUNT(*) AS n FROM remote_objects WHERE bucket_id = ? AND state = 'stale'",
            vec![Bind::Text(uuid_to_text(&bucket))],
        )
        .await
    }

    async fn mark_stale_older_than(
        &self,
        bucket: BucketId,
        before: time::OffsetDateTime,
    ) -> Result<u64> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query(
            "UPDATE remote_objects SET state = 'stale'              WHERE bucket_id = ? AND confirmed_at < ? AND state = 'confirmed'",
        )
        .bind(uuid_to_text(&bucket))
        .bind(crate::codec::unix_secs(before))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }

    async fn mark_all_stale_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query(
            "UPDATE remote_objects SET state = 'stale' WHERE bucket_id = ? AND state != 'stale'",
        )
        .bind(uuid_to_text(&bucket))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }

    async fn delete_stale_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        let _write = self.gate.permit().await?;
        let affected =
            sqlx::query("DELETE FROM remote_objects WHERE bucket_id = ? AND state = 'stale'")
                .bind(uuid_to_text(&bucket))
                .execute(self.gate.pool())
                .await
                .map_err(map_sqlx)?
                .rows_affected();
        Ok(affected)
    }

    async fn delete_all_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query("DELETE FROM remote_objects WHERE bucket_id = ?")
            .bind(uuid_to_text(&bucket))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected)
    }

    async fn count_expired_tombstones(
        &self,
        now: time::OffsetDateTime,
        retention_secs: u64,
    ) -> Result<u64> {
        let row = sqlx::query(&format!(
            "SELECT COUNT(*) AS n FROM metadata_tombstones WHERE {}",
            Self::expired_clause(retention_secs)
        ))
        .bind(crate::codec::unix_secs(now))
        .fetch_one(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        let reader = RowReader::new(row);
        Ok(reader.integer("n")? as u64)
    }

    async fn purge_expired_tombstones(
        &self,
        now: time::OffsetDateTime,
        retention_secs: u64,
    ) -> Result<u64> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query(&format!(
            "DELETE FROM metadata_tombstones WHERE {}",
            Self::expired_clause(retention_secs)
        ))
        .bind(crate::codec::unix_secs(now))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }
}

pub(crate) fn row_to_object(row: sqlx::sqlite::SqliteRow) -> Result<RemoteObject> {
    let row = RowReader::new(row);
    object_from_reader(&row)
}

fn object_from_reader(row: &RowReader) -> Result<RemoteObject> {
    Ok(RemoteObject {
        id: RemoteObjectId::from_uuid(row.uuid("id")?),
        connection_id: ConnectionId::from_uuid(row.uuid("connection_id")?),
        bucket_id: BucketId::from_uuid(row.uuid("bucket_id")?),
        path: ObjectPath::new(row.text("object_path")?)
            .map_err(|e| Error::database("invalid object_path in catalog").with_source(e))?,
        size: row.integer("size")? as u64,
        etag: row.opt_text("etag")?,
        content_type: row.opt_text("content_type")?,
        last_modified: row
            .opt_timestamp("last_modified")?
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
        storage_class: row.opt_text("storage_class")?,
        version_id: row.opt_text("version_id")?,
        confirmed_at: row.timestamp("confirmed_at")?,
        state: row.token("state", ObjectState::parse)?,
    })
}

#[async_trait]
impl rstore_catalog::CatalogQuery for SqliteRemoteObjectRepository {
    /// Mixed-list query (MIX-001..024): one SQL statement with dynamic
    /// filters and keyset pagination over `(sort_field, path, id)` —
    /// stable under concurrent upserts (IDX-024). The SQL itself is
    /// assembled by [`crate::repo::catalog_sql`].
    async fn search(
        &self,
        scope: rstore_catalog::Scope,
        filter: &rstore_catalog::FileFilter,
        sort: rstore_catalog::FileSort,
        page: &rstore_catalog::PageRequest,
    ) -> Result<rstore_catalog::Page<RemoteObject>> {
        let query = super::catalog_sql::CatalogSql::select(COLUMNS, &scope.workspace, filter)?;
        let query = match &page.after {
            Some(cursor) => query.ordered_after(sort, cursor)?,
            None => query,
        };
        let mut rows = query
            .ordered(sort, page.limit)
            .fetch(&self.read_pool)
            .await?;
        let has_more = rows.len() > page.limit as usize;
        rows.truncate(page.limit as usize);
        let items: Vec<RemoteObject> =
            rows.into_iter().map(row_to_object).collect::<Result<_>>()?;
        let next_cursor = if has_more {
            items
                .last()
                .map(|o| rstore_catalog::order::cursor_of(o, sort.field))
        } else {
            None
        };
        Ok(rstore_catalog::Page { items, next_cursor })
    }

    async fn count(
        &self,
        scope: rstore_catalog::Scope,
        filter: &rstore_catalog::FileFilter,
    ) -> Result<u64> {
        let rows = super::catalog_sql::CatalogSql::count(&scope.workspace, filter)?
            .fetch(&self.read_pool)
            .await?;
        let reader = RowReader::new(rows.into_iter().next().expect("COUNT returns one row"));
        Ok(reader.integer("n")? as u64)
    }

    async fn page(
        &self,
        scope: rstore_catalog::Scope,
        filter: &rstore_catalog::FileFilter,
        sort: rstore_catalog::FileSort,
        page: &rstore_catalog::PageRequest,
    ) -> Result<rstore_catalog::CatalogPage> {
        let limit = page.limit.max(1);
        // Count and page rows from one read transaction. The count is taken
        // before applying the keyset cursor, so `total` remains the full
        // filtered relation on every page and on the empty page after the
        // final cursor.
        let mut read = self.read_pool.begin().await.map_err(map_sqlx)?;
        let total_rows = super::catalog_sql::CatalogSql::count(&scope.workspace, filter)?
            .fetch(&mut *read)
            .await?;
        let total = RowReader::new(
            total_rows
                .into_iter()
                .next()
                .expect("COUNT returns one row"),
        )
        .integer("n")? as u64;
        let query = super::catalog_sql::CatalogSql::page(COLUMNS, &scope.workspace, filter)?;
        let query = match &page.after {
            Some(cursor) => query.ordered_after(sort, cursor)?,
            None => query,
        };
        let mut rows = query.ordered(sort, limit).fetch(&mut *read).await?;
        read.commit().await.map_err(map_sqlx)?;
        let has_more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let items = rows
            .into_iter()
            .map(|row| {
                let row = RowReader::new(row);
                row_to_catalog_page_item(row)
            })
            .collect::<Result<Vec<_>>>()?;
        let next_cursor = if has_more {
            items
                .last()
                .map(|item| rstore_catalog::order::cursor_of(&item.object, sort.field))
        } else {
            None
        };
        Ok(rstore_catalog::CatalogPage {
            items,
            total,
            next_cursor,
        })
    }
}

fn row_to_catalog_page_item(row: RowReader) -> Result<rstore_catalog::CatalogPageItem> {
    let tags_json = row.text("tags_json")?;
    let tags = serde_json::from_str(&tags_json)
        .map_err(|error| Error::database("decode catalog page tags").with_source(error))?;
    Ok(rstore_catalog::CatalogPageItem {
        object: object_from_reader(&row)?,
        bucket_name: row.text("bucket_name")?,
        connection_name: row.text("connection_name")?,
        tags,
    })
}
