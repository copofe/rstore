//! Repository for workspaces and their bucket memberships (WSP-).

use rstore_domain::id::{BucketId, WorkspaceId};
use rstore_domain::workspace::Workspace;
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;
use time::OffsetDateTime;

use crate::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::error::map_sqlx;

const COLUMNS: &str = "id, name, description, created_at, updated_at";

/// A workspace row plus its bucket-count summary (WSP-002). Internal to
/// the repository layer; the application defines its own view type.
#[derive(Debug, Clone)]
pub struct WorkspaceRowWithCount {
    pub workspace: Workspace,
    pub bucket_count: u64,
}

#[derive(Clone)]
pub struct WorkspaceRepository {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: crate::actor::SqliteWriter,
}

impl WorkspaceRepository {
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

    pub async fn insert(&self, workspace: &Workspace) -> Result<()> {
        let _write = self.writer.acquire().await?;
        // Uniqueness (case-insensitive) is the schema's guarantee — the
        // UNIQUE index on lower(name); map_sqlx classifies the violation as
        // Conflict and this remap adds the name to the message.
        Insert::into("workspaces")
            .column("id", uuid_to_text(&workspace.id))
            .column("name", &workspace.name)
            .column("description", &workspace.description)
            .column("created_at", unix_secs(workspace.created_at))
            .column("updated_at", unix_secs(workspace.updated_at))
            .execute(&self.pool)
            .await
            .map_err(|e| {
                if e.code == rstore_domain::ErrorCode::Conflict {
                    Error::conflict(format!("workspace `{}` already exists", workspace.name))
                } else {
                    e
                }
            })?;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<WorkspaceRowWithCount>> {
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS}, \
                    (SELECT COUNT(*) FROM workspace_buckets wb WHERE wb.workspace_id = workspaces.id) AS bucket_count \
             FROM workspaces ORDER BY name"
        ))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let reader = RowReader::new(row);
                Ok(WorkspaceRowWithCount {
                    workspace: row_to_workspace(&reader)?,
                    bucket_count: reader.integer("bucket_count")? as u64,
                })
            })
            .collect()
    }

    /// Catalog object count per member bucket for workspace stats (WSP-012).
    pub async fn object_counts_by_bucket(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Vec<(BucketId, u64)>> {
        let rows = sqlx::query(
            "SELECT ro.bucket_id, COUNT(*) AS n FROM remote_objects ro \
             JOIN workspace_buckets wb ON wb.bucket_id = ro.bucket_id \
             WHERE wb.workspace_id = ? AND ro.state != 'deleted' \
             GROUP BY ro.bucket_id",
        )
        .bind(uuid_to_text(&workspace))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let reader = RowReader::new(row);
                Ok((
                    BucketId::from_uuid(reader.uuid("bucket_id")?),
                    reader.integer("n")? as u64,
                ))
            })
            .collect()
    }

    pub async fn get(&self, id: WorkspaceId) -> Result<Option<Workspace>> {
        let row = sqlx::query(&format!("SELECT {COLUMNS} FROM workspaces WHERE id = ?"))
            .bind(uuid_to_text(&id))
            .fetch_optional(&self.read_pool)
            .await
            .map_err(map_sqlx)?;
        row.map(|r| row_to_workspace(&RowReader::new(r)))
            .transpose()
    }

    pub async fn get_by_name(&self, name: &str) -> Result<Option<Workspace>> {
        let row = sqlx::query(&format!("SELECT {COLUMNS} FROM workspaces WHERE name = ?"))
            .bind(name)
            .fetch_optional(&self.read_pool)
            .await
            .map_err(map_sqlx)?;
        row.map(|r| row_to_workspace(&RowReader::new(r)))
            .transpose()
    }

    pub async fn update(
        &self,
        id: WorkspaceId,
        name: Option<&str>,
        description: Option<&str>,
        at: OffsetDateTime,
    ) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let existing = self
            .get(id)
            .await?
            .ok_or_else(|| Error::not_found("workspace not found"))?;
        let new_name = name.unwrap_or(&existing.name).to_string();
        let new_description = match description {
            Some(d) => {
                if d.is_empty() {
                    None
                } else {
                    Some(d.to_string())
                }
            }
            None => existing.description.clone(),
        };
        // Uniqueness rides the schema's lower(name) index; a self-rename to
        // the same name does not violate it (the final table state keeps
        // exactly one row with that name).
        Update::table("workspaces")
            .set("name", &new_name)
            .set("description", &new_description)
            .set("updated_at", unix_secs(at))
            .filter("id = ?", [Bind::Text(uuid_to_text(&id))])
            .execute(&self.pool)
            .await
            .map_err(|e| {
                if e.code == rstore_domain::ErrorCode::Conflict {
                    Error::conflict(format!("workspace `{new_name}` already exists"))
                } else {
                    e
                }
            })?;
        Ok(())
    }

    pub async fn delete(&self, id: WorkspaceId) -> Result<bool> {
        let _write = self.writer.acquire().await?;
        let affected = sqlx::query("DELETE FROM workspaces WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }

    /// Buckets in a workspace (WSP-006), ordered by connection then name.
    pub async fn buckets(&self, id: WorkspaceId) -> Result<Vec<BucketId>> {
        let rows = sqlx::query(
            "SELECT wb.bucket_id FROM workspace_buckets wb \
             JOIN buckets b ON b.id = wb.bucket_id \
             WHERE wb.workspace_id = ? ORDER BY b.connection_id, b.name",
        )
        .bind(uuid_to_text(&id))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                RowReader::new(row)
                    .uuid("bucket_id")
                    .map(BucketId::from_uuid)
            })
            .collect()
    }

    /// Add a bucket membership; duplicate pairs are a no-op (WSP-007).
    pub async fn add_bucket(
        &self,
        workspace: WorkspaceId,
        bucket: BucketId,
        at: OffsetDateTime,
    ) -> Result<()> {
        let _write = self.writer.acquire().await?;
        Insert::into("workspace_buckets")
            .column("workspace_id", uuid_to_text(&workspace))
            .column("bucket_id", uuid_to_text(&bucket))
            .column("added_at", unix_secs(at))
            .suffix("ON CONFLICT DO NOTHING")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn remove_bucket(&self, workspace: WorkspaceId, bucket: BucketId) -> Result<bool> {
        let _write = self.writer.acquire().await?;
        let affected =
            sqlx::query("DELETE FROM workspace_buckets WHERE workspace_id = ? AND bucket_id = ?")
                .bind(uuid_to_text(&workspace))
                .bind(uuid_to_text(&bucket))
                .execute(&self.pool)
                .await
                .map_err(map_sqlx)?
                .rows_affected();
        Ok(affected > 0)
    }
}

fn row_to_workspace(row: &RowReader) -> Result<Workspace> {
    Ok(Workspace {
        id: WorkspaceId::from_uuid(row.uuid("id")?),
        name: row.text("name")?,
        description: row.opt_text("description")?,
        created_at: row.timestamp("created_at")?,
        updated_at: row.timestamp("updated_at")?,
    })
}
