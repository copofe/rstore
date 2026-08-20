//! Repository for tag definitions and their object associations (TAG-).

use rstore_domain::id::{TagId, WorkspaceId};
use rstore_domain::workspace::Tag;
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use time::OffsetDateTime;

use crate::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::error::map_sqlx;

/// A tag row plus its live association count (TAG-005). Internal to the
/// repository layer; the application defines its own view type.
#[derive(Debug, Clone)]
pub struct TagRowWithCount {
    pub tag: Tag,
    pub object_count: u64,
}

/// CRUD + associations over `tags` / `remote_object_tags`.
#[derive(Clone)]
pub struct TagRepository {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: crate::actor::SqliteWriter,
}

impl TagRepository {
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

    pub async fn insert(&self, tag: &Tag) -> Result<()> {
        let _write = self.writer.acquire().await?;
        // Uniqueness is the schema's guarantee — UNIQUE(workspace_id,
        // normalized_name); map_sqlx classifies the violation as Conflict
        // and this remap adds the tag's name to the message.
        Insert::into("tags")
            .column("id", uuid_to_text(&tag.id))
            .column("workspace_id", uuid_to_text(&tag.workspace_id))
            .column("name", &tag.name)
            .column("normalized_name", &tag.normalized_name)
            .column("color", &tag.color)
            .column("description", &tag.description)
            .column("created_at", unix_secs(tag.created_at))
            .execute(&self.pool)
            .await
            .map_err(|e| {
                if e.code == rstore_domain::ErrorCode::Conflict {
                    Error::conflict(format!(
                        "tag `{}` already exists in this workspace",
                        tag.name
                    ))
                } else {
                    e
                }
            })?;
        Ok(())
    }

    /// Tags in a workspace with live object counts (TAG-005).
    pub async fn list(&self, workspace: WorkspaceId) -> Result<Vec<TagRowWithCount>> {
        let rows = sqlx::query(
            "SELECT t.id, t.workspace_id, t.name, t.normalized_name, t.color, t.description, t.created_at, \
                    (SELECT COUNT(*) FROM remote_object_tags rot \
                      WHERE rot.tag_id = t.id AND rot.workspace_id = t.workspace_id) AS object_count \
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
                Ok(TagRowWithCount { tag, object_count })
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
        let row = sqlx::query(
            "SELECT id, workspace_id, name, normalized_name, color, description, created_at \
             FROM tags WHERE id = ?",
        )
        .bind(uuid_to_text(&id))
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|r| row_to_tag(&RowReader::new(r))).transpose()
    }

    /// Update mutable fields; name conflicts within the workspace are rejected
    /// (TAG-006).
    pub async fn update(
        &self,
        id: TagId,
        name: Option<&str>,
        color: Option<&str>,
        description: Option<&str>,
    ) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let existing = self
            .get_by_id(id)
            .await?
            .ok_or_else(|| Error::not_found("tag not found"))?;
        let new_name = name.unwrap_or(&existing.name).to_string();
        let normalized = rstore_domain::normalize_tag_name(&new_name);
        let new_description = match description {
            Some("") => None,
            None => existing.description,
            Some(d) => Some(d.to_string()),
        };
        let new_color = color.unwrap_or(&existing.color).to_string();
        // Uniqueness rides the schema's UNIQUE(workspace_id,
        // normalized_name); a rename to the same name does not violate it.
        Update::table("tags")
            .set("name", &new_name)
            .set("normalized_name", &normalized)
            .set("color", &new_color)
            .set("description", &new_description)
            .filter("id = ?", [Bind::Text(uuid_to_text(&id))])
            .execute(&self.pool)
            .await
            .map_err(|e| {
                if e.code == rstore_domain::ErrorCode::Conflict {
                    Error::conflict(format!("tag `{new_name}` already exists in this workspace"))
                } else {
                    e
                }
            })?;
        Ok(())
    }

    /// Delete a tag and its associations; `true` when a row went (TAG-007).
    pub async fn delete(&self, id: TagId) -> Result<bool> {
        let _write = self.writer.acquire().await?;
        let affected = sqlx::query("DELETE FROM tags WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }

    /// Attach a tag to an object (TAG-009). The object must live in a bucket
    /// belonging to the workspace. Application paths resolve objects through
    /// the catalog locator, which already enforces workspace membership
    /// (WSP-013); this id-level join is the adapter's own defense in depth
    /// and never interprets a locator. Duplicate attaches are no-ops.
    pub async fn attach(
        &self,
        workspace: WorkspaceId,
        object_id: rstore_domain::RemoteObjectId,
        tag: TagId,
        at: OffsetDateTime,
    ) -> Result<()> {
        let _write = self.writer.acquire().await?;
        crate::tx::atomically(&self.pool, "tag attach", |tx| {
            Box::pin(async move {
                // WSP-013 isolation: the object's bucket must belong to the
                // workspace, checked in the same unit of work as the insert.
                let member = sqlx::query(
                    "SELECT COUNT(*) FROM remote_objects ro \
                     JOIN workspace_buckets wb ON wb.bucket_id = ro.bucket_id \
                     WHERE ro.id = ? AND wb.workspace_id = ?",
                )
                .bind(uuid_to_text(&object_id))
                .bind(uuid_to_text(&workspace))
                .fetch_one(&mut **tx)
                .await
                .map_err(map_sqlx)?
                .get::<i64, _>(0);
                if member == 0 {
                    return Err(Error::invalid_input("object is not part of this workspace"));
                }
                Insert::into("remote_object_tags")
                    .column("workspace_id", uuid_to_text(&workspace))
                    .column("remote_object_id", uuid_to_text(&object_id))
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

    /// Detach (TAG-010); `true` when an association was removed.
    pub async fn detach(
        &self,
        workspace: WorkspaceId,
        object_id: rstore_domain::RemoteObjectId,
        tag: TagId,
    ) -> Result<bool> {
        let _write = self.writer.acquire().await?;
        let affected = sqlx::query(
            "DELETE FROM remote_object_tags \
             WHERE workspace_id = ? AND remote_object_id = ? AND tag_id = ?",
        )
        .bind(uuid_to_text(&workspace))
        .bind(uuid_to_text(&object_id))
        .bind(uuid_to_text(&tag))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected > 0)
    }

    /// Tag names attached to an object within a workspace (MIX-007).
    pub async fn tags_for_object(
        &self,
        workspace: WorkspaceId,
        object_id: rstore_domain::RemoteObjectId,
    ) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT t.name FROM remote_object_tags rot JOIN tags t ON t.id = rot.tag_id \
             WHERE rot.workspace_id = ? AND rot.remote_object_id = ? ORDER BY t.normalized_name",
        )
        .bind(uuid_to_text(&workspace))
        .bind(uuid_to_text(&object_id))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| RowReader::new(row).text("name"))
            .collect()
    }

    /// Every `(workspace, tag)` association of an object, across all
    /// workspaces — the source list for tag preservation on copy/move
    /// (MOVE-015).
    pub async fn associations_for_object(
        &self,
        object_id: rstore_domain::RemoteObjectId,
    ) -> Result<Vec<(rstore_domain::WorkspaceId, TagId)>> {
        let rows = sqlx::query(
            "SELECT workspace_id, tag_id FROM remote_object_tags WHERE remote_object_id = ?",
        )
        .bind(uuid_to_text(&object_id))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let row = RowReader::new(row);
                Ok((
                    rstore_domain::WorkspaceId::from_uuid(row.uuid("workspace_id")?),
                    TagId::from_uuid(row.uuid("tag_id")?),
                ))
            })
            .collect()
    }

    /// Move every association of `source` onto `target` within a workspace,
    /// deduplicating rows that already exist (TAG-008). Returns the number
    /// of associations moved.
    pub async fn merge_associations(
        &self,
        workspace: WorkspaceId,
        source: TagId,
        target: TagId,
        at: OffsetDateTime,
    ) -> Result<u64> {
        let _write = self.writer.acquire().await?;
        crate::tx::atomically(&self.pool, "tag merge", |tx| {
            Box::pin(async move {
                let moved = sqlx::query(
                    "INSERT INTO remote_object_tags \
                     (workspace_id, remote_object_id, tag_id, applied_at) \
                     SELECT workspace_id, remote_object_id, ?3, ?4 \
                     FROM remote_object_tags WHERE workspace_id = ?1 AND tag_id = ?2 \
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
                sqlx::query("DELETE FROM remote_object_tags WHERE workspace_id = ? AND tag_id = ?")
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

    /// Remove every tag association for one object in a workspace (TAG-013).
    pub async fn clear_object(
        &self,
        workspace: WorkspaceId,
        object_id: rstore_domain::RemoteObjectId,
    ) -> Result<()> {
        let _write = self.writer.acquire().await?;
        sqlx::query(
            "DELETE FROM remote_object_tags WHERE workspace_id = ? AND remote_object_id = ?",
        )
        .bind(uuid_to_text(&workspace))
        .bind(uuid_to_text(&object_id))
        .execute(&self.pool)
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

/// MOVE-015 tag preservation goes through the catalog port so the copy
/// service holds `Arc<dyn TagStore>` instead of this concrete type.
#[async_trait::async_trait]
impl rstore_catalog::TagStore for TagRepository {
    async fn attach(
        &self,
        workspace: WorkspaceId,
        object: rstore_domain::RemoteObjectId,
        tag: TagId,
        at: OffsetDateTime,
    ) -> Result<()> {
        TagRepository::attach(self, workspace, object, tag, at).await
    }

    async fn associations_for_object(
        &self,
        object: rstore_domain::RemoteObjectId,
    ) -> Result<Vec<(rstore_domain::WorkspaceId, TagId)>> {
        TagRepository::associations_for_object(self, object).await
    }
}
