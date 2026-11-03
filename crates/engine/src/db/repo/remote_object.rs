//! Catalog persistence: the cached rows of indexed buckets and tagged
//! objects, their staleness lifecycle, and the search queries over them.
use rstore_domain::id::{BucketId, ConnectionId, RemoteObjectId};
use rstore_domain::remote::{ObjectPath, RemoteObject};
use rstore_domain::ObjectState;
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;
use time::OffsetDateTime;

use crate::db::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader};
use crate::db::error::map_sqlx;

/// Rows no workspace tag pins.
const UNTAGGED: &str = "NOT EXISTS (SELECT 1 FROM object_tags ot \
                        WHERE ot.bucket_id = remote_objects.bucket_id \
                          AND ot.object_path = remote_objects.object_path)";

const COLUMNS: &str = "id, connection_id, bucket_id, object_path, size, etag, content_type, \
                       last_modified, storage_class, version_id, confirmed_at, state, file_type";

#[derive(Clone)]
pub struct CatalogRepository {
    gate: crate::db::tx::WriteGate,
    read_pool: SqlitePool,
}

impl CatalogRepository {
    pub(crate) fn new(gate: crate::db::tx::WriteGate, read_pool: SqlitePool) -> Self {
        Self { gate, read_pool }
    }
}

impl CatalogRepository {
    /// Drop the row for an object rstore deleted. Idempotent.
    pub async fn forget(&self, bucket: BucketId, path: &ObjectPath) -> Result<()> {
        sqlx::query("DELETE FROM remote_objects WHERE bucket_id = ? AND object_path = ?")
            .bind(uuid_to_text(&bucket))
            .bind(path.as_str())
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    /// Drop the row once nothing keeps it: its bucket is not indexed and no
    /// workspace tags it.
    pub async fn forget_untracked(&self, bucket: BucketId, path: &ObjectPath) -> Result<()> {
        sqlx::query(
            "DELETE FROM remote_objects WHERE bucket_id = ?1 AND object_path = ?2 \
               AND NOT EXISTS (SELECT 1 FROM buckets b WHERE b.id = ?1 AND b.index_enabled = 1) \
               AND NOT EXISTS (SELECT 1 FROM object_tags ot \
                               WHERE ot.bucket_id = ?1 AND ot.object_path = ?2)",
        )
        .bind(uuid_to_text(&bucket))
        .bind(path.as_str())
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Drop every row nothing keeps any more: unindexed bucket, no tags. Runs
    /// after a tag deletion strips the last tag off objects.
    pub async fn forget_all_untracked(&self) -> Result<u64> {
        let affected = sqlx::query(&format!(
            "DELETE FROM remote_objects \
             WHERE bucket_id IN (SELECT id FROM buckets WHERE index_enabled = 0) AND {UNTAGGED}"
        ))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }

    /// Drop every untagged row of a bucket whose index was switched off;
    /// tagged objects stay as long as their tags do.
    pub async fn drop_untagged_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        let affected = sqlx::query(&format!(
            "DELETE FROM remote_objects WHERE bucket_id = ? AND {UNTAGGED}"
        ))
        .bind(uuid_to_text(&bucket))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }

    pub async fn upsert(&self, object: &RemoteObject) -> Result<()> {
        upsert_object_on(self.gate.pool(), object).await
    }

    pub async fn upsert_many(&self, objects: &[RemoteObject]) -> Result<()> {
        let objects = objects.to_vec();
        self.gate
            .write_unit("catalog page", |tx| {
                Box::pin(async move {
                    for object in &objects {
                        upsert_object_on(&mut **tx, object).await?;
                    }
                    Ok(())
                })
            })
            .await
    }

    pub async fn count_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        crate::db::codec::count_scalar(
            &self.read_pool,
            "SELECT COUNT(*) AS n FROM remote_objects WHERE bucket_id = ?",
            vec![Bind::Text(uuid_to_text(&bucket))],
        )
        .await
    }

    pub async fn find_by_locator(
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

    pub async fn count_stale(&self, bucket: BucketId) -> Result<u64> {
        crate::db::codec::count_scalar(
            &self.read_pool,
            "SELECT COUNT(*) AS n FROM remote_objects WHERE bucket_id = ? AND state = 'stale'",
            vec![Bind::Text(uuid_to_text(&bucket))],
        )
        .await
    }

    pub async fn mark_stale_older_than(
        &self,
        bucket: BucketId,
        before: time::OffsetDateTime,
    ) -> Result<u64> {
        let affected = sqlx::query(
            "UPDATE remote_objects SET state = 'stale' WHERE bucket_id = ? AND confirmed_at < ? AND state = 'confirmed'",
        )
        .bind(uuid_to_text(&bucket))
        .bind(crate::db::codec::unix_secs(before))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }

    /// Prune a bucket's stale rows after a complete rebuild. Tagged rows stay
    /// (still flagged stale): tags are user data and are never dropped
    /// implicitly.
    pub async fn delete_stale_in_bucket(&self, bucket: BucketId) -> Result<u64> {
        let affected = sqlx::query(&format!(
            "DELETE FROM remote_objects WHERE bucket_id = ? AND state = 'stale' AND {UNTAGGED}"
        ))
        .bind(uuid_to_text(&bucket))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }

    /// Untagged stale rows across every bucket — what [`Self::purge_stale`]
    /// would remove.
    pub async fn count_purgeable_stale(&self) -> Result<u64> {
        crate::db::codec::count_scalar(
            &self.read_pool,
            &format!(
                "SELECT COUNT(*) AS n FROM remote_objects WHERE state = 'stale' AND {UNTAGGED}"
            ),
            Vec::new(),
        )
        .await
    }

    /// Remove untagged stale rows across every bucket.
    pub async fn purge_stale(&self) -> Result<u64> {
        let affected = sqlx::query(&format!(
            "DELETE FROM remote_objects WHERE state = 'stale' AND {UNTAGGED}"
        ))
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

impl CatalogRepository {
    pub async fn page(
        &self,
        scope: crate::catalog::Scope,
        filter: &crate::catalog::FileFilter,
        sort: crate::catalog::FileSort,
        page: &crate::catalog::PageRequest,
    ) -> Result<crate::catalog::CatalogPage> {
        let mut read = self.read_pool.begin().await.map_err(map_sqlx)?;
        let total = count_matches(&mut *read, &scope.workspace, filter).await?;
        let query = super::catalog_sql::CatalogSql::page(COLUMNS, &scope.workspace, filter)?;
        let fetched = fetch_keyset_page(
            &mut *read,
            query,
            sort,
            page,
            |row| row_to_catalog_page_item(RowReader::new(row)),
            |item| crate::catalog::order::cursor_of(&item.object, sort.field),
        )
        .await?;
        read.commit().await.map_err(map_sqlx)?;
        Ok(crate::catalog::CatalogPage {
            items: fetched.items,
            total,
            next_cursor: fetched.next_cursor,
        })
    }
}

/// The one keyset-pagination choreography: apply the cursor predicate, fetch
/// one row past the limit, truncate, and derive the continuation cursor from
/// the last delivered item. Every read shape (search, mixed page) shares it.
async fn fetch_keyset_page<'e, E, T>(
    executor: E,
    query: super::catalog_sql::CatalogSql,
    sort: crate::catalog::FileSort,
    page: &crate::catalog::PageRequest,
    decode: impl FnMut(sqlx::sqlite::SqliteRow) -> Result<T>,
    cursor_of_item: impl Fn(&T) -> String,
) -> Result<crate::catalog::Page<T>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let limit = page.limit.max(1);
    let query = match &page.after {
        Some(cursor) => query.ordered_after(sort, cursor)?,
        None => query,
    };
    let mut rows = query.ordered(sort, limit).fetch(executor).await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let items = rows.into_iter().map(decode).collect::<Result<Vec<_>>>()?;
    let next_cursor = has_more
        .then(|| items.last().map(&cursor_of_item))
        .flatten();
    Ok(crate::catalog::Page { items, next_cursor })
}

/// The one scalar-count shape over the filter, shared by `count` and the
/// mixed page's total.
async fn count_matches<'e, E>(
    executor: E,
    workspace: &rstore_domain::WorkspaceId,
    filter: &crate::catalog::FileFilter,
) -> Result<u64>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let rows = super::catalog_sql::CatalogSql::count(workspace, filter)?
        .fetch(executor)
        .await?;
    let Some(row) = rows.into_iter().next() else {
        return Err(Error::database("count query returned no scalar row"));
    };
    Ok(RowReader::new(row).integer("n")? as u64)
}

fn row_to_catalog_page_item(row: RowReader) -> Result<crate::catalog::CatalogPageItem> {
    let tags_json = row.text("tags_json")?;
    let tags = serde_json::from_str(&tags_json)
        .map_err(|error| Error::database("decode catalog page tags").with_source(error))?;
    Ok(crate::catalog::CatalogPageItem {
        object: object_from_reader(&row)?,
        bucket_name: row.text("bucket_name")?,
        connection_name: row.text("connection_name")?,
        tags,
    })
}

/// The shared remote-object upsert statement. A free function on purpose:
/// the index run's page commit calls it inside its own transaction — the
/// shared thing is the SQL, not a repository identity.
pub(crate) async fn upsert_object_on<'e, E>(executor: E, object: &RemoteObject) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
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
