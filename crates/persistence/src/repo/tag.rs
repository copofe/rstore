//! Repository for tag definitions and their object associations.
use async_trait::async_trait;
use rstore_application_ports::{TagStore, TagSummary};
use rstore_domain::id::{RemoteObjectId, TagId, WorkspaceId};
use rstore_domain::workspace::Tag;
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use time::OffsetDateTime;

use crate::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::error::map_sqlx;
use crate::tx::WriteGate;

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

#[async_trait::async_trait]
impl rstore_catalog::TagStore for TagRepository {
    async fn attach(
        &self,
        workspace: WorkspaceId,
        object_id: RemoteObjectId,
        tag: TagId,
        at: OffsetDateTime,
    ) -> Result<()> {
        self.gate
            .write_unit("tag attach", |tx| {
                Box::pin(async move {
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

    async fn associations_for_object(
        &self,
        object_id: RemoteObjectId,
    ) -> Result<Vec<(WorkspaceId, TagId)>> {
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
}

#[async_trait]
impl TagStore for TagRepository {
    async fn insert(&self, tag: &Tag) -> Result<()> {
        let _write = self.gate.permit().await?;
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
                crate::error::with_conflict_message(e, || {
                    format!("tag `{}` already exists in this workspace", tag.name)
                })
            })?;
        Ok(())
    }

    async fn list(&self, workspace: WorkspaceId) -> Result<Vec<TagSummary>> {
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
                Ok(TagSummary { tag, object_count })
            })
            .collect()
    }

    async fn get(&self, workspace: WorkspaceId, name: &str) -> Result<Option<Tag>> {
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

    async fn get_by_id(&self, id: TagId) -> Result<Option<Tag>> {
        Self::get_by_id_with_executor(&self.read_pool, id).await
    }

    async fn update(
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
                            crate::error::with_conflict_message(e, || {
                                format!("tag `{new_name}` already exists in this workspace")
                            })
                        })?;
                    Ok(())
                })
            })
            .await
    }

    async fn delete(&self, id: TagId) -> Result<bool> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query("DELETE FROM tags WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }

    async fn detach(
        &self,
        workspace: WorkspaceId,
        object_id: RemoteObjectId,
        tag: TagId,
    ) -> Result<bool> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query(
            "DELETE FROM remote_object_tags \
             WHERE workspace_id = ? AND remote_object_id = ? AND tag_id = ?",
        )
        .bind(uuid_to_text(&workspace))
        .bind(uuid_to_text(&object_id))
        .bind(uuid_to_text(&tag))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected > 0)
    }

    async fn tags_for_object(
        &self,
        workspace: WorkspaceId,
        object_id: RemoteObjectId,
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

    async fn merge_associations(
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
                    sqlx::query(
                        "DELETE FROM remote_object_tags WHERE workspace_id = ? AND tag_id = ?",
                    )
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

    async fn clear_object(&self, workspace: WorkspaceId, object_id: RemoteObjectId) -> Result<()> {
        let _write = self.gate.permit().await?;
        sqlx::query(
            "DELETE FROM remote_object_tags WHERE workspace_id = ? AND remote_object_id = ?",
        )
        .bind(uuid_to_text(&workspace))
        .bind(uuid_to_text(&object_id))
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
