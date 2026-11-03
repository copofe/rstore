//! Repository for tag definitions and their object associations.
//!
//! Associations key on `(bucket, path)`, not on a catalog row, so any object
//! can carry tags whether or not its bucket is indexed.
use crate::db::TagSummary;
use rstore_domain::id::{BucketId, TagId, WorkspaceId};
use rstore_domain::remote::ObjectPath;
use rstore_domain::workspace::Tag;
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use time::OffsetDateTime;

use crate::db::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::db::error::map_sqlx;
use crate::db::tx::WriteGate;

#[derive(Clone)]
pub struct TagRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
}

impl TagRepository {
    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool) -> Self {
        Self { gate, read_pool }
    }

    async fn get_by_id_with_executor<'e, E>(executor: E, id: TagId) -> Result<Option<Tag>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let row = sqlx::query(
            "SELECT id, workspace_id, name, normalized_name, color, description, created_at \
             FROM tags WHERE id = ?",
        )
        .bind(uuid_to_text(&id))
        .fetch_optional(executor)
        .await
        .map_err(map_sqlx)?;
        row.map(|r| row_to_tag(&RowReader::new(r))).transpose()
    }
}

impl TagRepository {
    /// Attach `tag` to the object at `(bucket, path)` in `workspace`
    /// (idempotent). The bucket must be a member of the workspace; the object
    /// itself need not be indexed.
    pub async fn attach(
        &self,
        workspace: WorkspaceId,
        bucket: BucketId,
        path: &ObjectPath,
        tag: TagId,
        at: OffsetDateTime,
    ) -> Result<()> {
        let path = path.as_str().to_owned();
        self.gate
            .write_unit("tag attach", |tx| {
                Box::pin(async move {
                    let member = sqlx::query(
                        "SELECT COUNT(*) FROM workspace_buckets \
                         WHERE workspace_id = ? AND bucket_id = ?",
                    )
                    .bind(uuid_to_text(&workspace))
                    .bind(uuid_to_text(&bucket))
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(map_sqlx)?
                    .get::<i64, _>(0);
                    if member == 0 {
                        return Err(Error::invalid_input("object is not part of this workspace"));
                    }
                    Insert::into("object_tags")
                        .column("workspace_id", uuid_to_text(&workspace))
                        .column("bucket_id", uuid_to_text(&bucket))
                        .column("object_path", path.as_str())
                        .column("tag_id", uuid_to_text(&tag))
                        .column("applied_at", unix_secs(at))
                        .suffix("ON CONFLICT DO NOTHING")
                        .execute(&mut **tx)
                        .await?;
                    Ok(())
                })
            })
            .await
    }

    /// Mirror every association of `from` onto `to`, in each workspace that
    /// also contains the target bucket; returns how many associations were
    /// added. The tag preservation of copy and move.
    pub async fn copy_associations(
        &self,
        from: (BucketId, &ObjectPath),
        to: (BucketId, &ObjectPath),
        at: OffsetDateTime,
    ) -> Result<u64> {
        let affected = sqlx::query(
            "INSERT INTO object_tags (workspace_id, bucket_id, object_path, tag_id, applied_at) \
             SELECT ot.workspace_id, ?3, ?4, ot.tag_id, ?5 FROM object_tags ot \
             WHERE ot.bucket_id = ?1 AND ot.object_path = ?2 \
               AND EXISTS (SELECT 1 FROM workspace_buckets wb \
                           WHERE wb.workspace_id = ot.workspace_id AND wb.bucket_id = ?3) \
             ON CONFLICT DO NOTHING",
        )
        .bind(uuid_to_text(&from.0))
        .bind(from.1.as_str())
        .bind(uuid_to_text(&to.0))
        .bind(to.1.as_str())
        .bind(unix_secs(at))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }

    /// Whether any workspace tags the object at `(bucket, path)`.
    pub async fn is_tagged(&self, bucket: BucketId, path: &ObjectPath) -> Result<bool> {
        let n: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM object_tags WHERE bucket_id = ? AND object_path = ?)",
        )
        .bind(uuid_to_text(&bucket))
        .bind(path.as_str())
        .fetch_one(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        Ok(n != 0)
    }

    /// Drop the object's associations in every workspace — the object itself
    /// was deleted by rstore.
    pub async fn forget_object(&self, bucket: BucketId, path: &ObjectPath) -> Result<()> {
        sqlx::query("DELETE FROM object_tags WHERE bucket_id = ? AND object_path = ?")
            .bind(uuid_to_text(&bucket))
            .bind(path.as_str())
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }
}

impl TagRepository {
    pub async fn insert(&self, tag: &Tag) -> Result<()> {
        Insert::into("tags")
            .column("id", uuid_to_text(&tag.id))
            .column("workspace_id", uuid_to_text(&tag.workspace_id))
            .column("name", &tag.name)
            .column("normalized_name", &tag.normalized_name)
            .column("color", &tag.color)
            .column("description", &tag.description)
            .column("created_at", unix_secs(tag.created_at))
            .execute(self.gate.pool())
            .await
            .map_err(|e| {
                crate::db::error::with_conflict_message(e, || {
                    format!("tag `{}` already exists in this workspace", tag.name)
                })
            })?;
        Ok(())
    }

    pub async fn list(&self, workspace: WorkspaceId) -> Result<Vec<TagSummary>> {
        let rows = sqlx::query(
            "SELECT t.id, t.workspace_id, t.name, t.normalized_name, t.color, t.description, t.created_at, \
                    (SELECT COUNT(*) FROM object_tags ot \
                      WHERE ot.tag_id = t.id AND ot.workspace_id = t.workspace_id) AS object_count \
             FROM tags t WHERE t.workspace_id = ? ORDER BY t.normalized_name",
        )
        .bind(uuid_to_text(&workspace))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let reader = RowReader::new(row);
                let object_count = reader.integer("object_count")? as u64;
                let tag = row_to_tag(&reader)?;
                Ok(TagSummary { tag, object_count })
            })
            .collect()
    }

    pub async fn get(&self, workspace: WorkspaceId, name: &str) -> Result<Option<Tag>> {
        let row = sqlx::query(
            "SELECT id, workspace_id, name, normalized_name, color, description, created_at \
             FROM tags WHERE workspace_id = ? AND normalized_name = ?",
        )
        .bind(uuid_to_text(&workspace))
        .bind(rstore_domain::normalize_tag_name(name))
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|r| row_to_tag(&RowReader::new(r))).transpose()
    }

    pub async fn get_by_id(&self, id: TagId) -> Result<Option<Tag>> {
        Self::get_by_id_with_executor(&self.read_pool, id).await
    }

    pub async fn update(
        &self,
        id: TagId,
        name: Option<&str>,
        color: Option<&str>,
        description: Option<Option<&str>>,
    ) -> Result<()> {
        let name = name.map(str::to_owned);
        let color = color.map(str::to_owned);
        let description = description.map(|value| value.map(str::to_owned));
        self.gate
            .write_unit("tag update", move |tx| {
                Box::pin(async move {
                    let existing = Self::get_by_id_with_executor(&mut **tx, id)
                        .await?
                        .ok_or_else(|| Error::not_found("tag not found"))?;
                    let new_name = name.as_deref().unwrap_or(&existing.name).to_owned();
                    let normalized = rstore_domain::normalize_tag_name(&new_name);
                    let new_description = match description {
                        None => existing.description,
                        Some(None) => None,
                        Some(Some(value)) => Some(value),
                    };
                    let new_color = color.as_deref().unwrap_or(&existing.color).to_owned();
                    Update::table("tags")
                        .set("name", &new_name)
                        .set("normalized_name", &normalized)
                        .set("color", &new_color)
                        .set("description", &new_description)
                        .filter("id = ?", [Bind::Text(uuid_to_text(&id))])
                        .execute(&mut **tx)
                        .await
                        .map_err(|e| {
                            crate::db::error::with_conflict_message(e, || {
                                format!("tag `{new_name}` already exists in this workspace")
                            })
                        })?;
                    Ok(())
                })
            })
            .await
    }

    pub async fn delete(&self, id: TagId) -> Result<bool> {
        let affected = sqlx::query("DELETE FROM tags WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }

    pub async fn detach(
        &self,
        workspace: WorkspaceId,
        bucket: BucketId,
        path: &ObjectPath,
        tag: TagId,
    ) -> Result<bool> {
        let affected = sqlx::query(
            "DELETE FROM object_tags \
             WHERE workspace_id = ? AND bucket_id = ? AND object_path = ? AND tag_id = ?",
        )
        .bind(uuid_to_text(&workspace))
        .bind(uuid_to_text(&bucket))
        .bind(path.as_str())
        .bind(uuid_to_text(&tag))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected > 0)
    }

    pub async fn tags_for_object(
        &self,
        workspace: WorkspaceId,
        bucket: BucketId,
        path: &ObjectPath,
    ) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT t.name FROM object_tags ot JOIN tags t ON t.id = ot.tag_id \
             WHERE ot.workspace_id = ? AND ot.bucket_id = ? AND ot.object_path = ? \
             ORDER BY t.normalized_name",
        )
        .bind(uuid_to_text(&workspace))
        .bind(uuid_to_text(&bucket))
        .bind(path.as_str())
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| RowReader::new(row).text("name"))
            .collect()
    }

    pub async fn merge_associations(
        &self,
        workspace: WorkspaceId,
        source: TagId,
        target: TagId,
        at: OffsetDateTime,
    ) -> Result<u64> {
        self.gate
            .write_unit("tag merge", |tx| {
                Box::pin(async move {
                    let moved = sqlx::query(
                        "INSERT INTO object_tags \
                         (workspace_id, bucket_id, object_path, tag_id, applied_at) \
                         SELECT workspace_id, bucket_id, object_path, ?3, ?4 \
                         FROM object_tags WHERE workspace_id = ?1 AND tag_id = ?2 \
                         ON CONFLICT DO NOTHING",
                    )
                    .bind(uuid_to_text(&workspace))
                    .bind(uuid_to_text(&source))
                    .bind(uuid_to_text(&target))
                    .bind(unix_secs(at))
                    .execute(&mut **tx)
                    .await
                    .map_err(map_sqlx)?
                    .rows_affected();
                    sqlx::query("DELETE FROM object_tags WHERE workspace_id = ? AND tag_id = ?")
                        .bind(uuid_to_text(&workspace))
                        .bind(uuid_to_text(&source))
                        .execute(&mut **tx)
                        .await
                        .map_err(map_sqlx)?;
                    Ok(moved)
                })
            })
            .await
    }

    pub async fn clear_object(
        &self,
        workspace: WorkspaceId,
        bucket: BucketId,
        path: &ObjectPath,
    ) -> Result<()> {
        sqlx::query(
            "DELETE FROM object_tags WHERE workspace_id = ? AND bucket_id = ? AND object_path = ?",
        )
        .bind(uuid_to_text(&workspace))
        .bind(uuid_to_text(&bucket))
        .bind(path.as_str())
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}

fn row_to_tag(row: &RowReader) -> Result<Tag> {
    Ok(Tag {
        id: TagId::from_uuid(row.uuid("id")?),
        workspace_id: WorkspaceId::from_uuid(row.uuid("workspace_id")?),
        name: row.text("name")?,
        normalized_name: row.text("normalized_name")?,
        color: row.text("color")?,
        description: row.opt_text("description")?,
        created_at: row.timestamp("created_at")?,
    })
}
