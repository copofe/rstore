//! Repository for index runs, scan checkpoints and catalog revisions.
use async_trait::async_trait;

use rstore_application_ports::{IndexCheckpoint, IndexPageCommit, IndexRunCommit, IndexRunStore};
use rstore_domain::id::{BucketId, IndexRunId};
use rstore_domain::workspace::{IndexRun, IndexRunState};
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use time::OffsetDateTime;

use crate::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::error::map_sqlx;
use crate::tx::WriteGate;

#[derive(Clone)]
pub struct IndexRunRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
}

impl IndexRunRepository {
    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool) -> Self {
        Self { gate, read_pool }
    }
}

#[async_trait]
impl IndexRunStore for IndexRunRepository {
    async fn create(&self, run: &IndexRun) -> Result<()> {
        let _write = self.gate.permit().await?;
        Insert::into("index_runs")
            .column("id", uuid_to_text(&run.id))
            .column("bucket_id", uuid_to_text(&run.bucket_id))
            .column("state", run.state.as_str())
            .column("started_at", unix_secs(run.started_at))
            .column("finished_at", run.finished_at.map(unix_secs))
            .column("objects_seen", run.objects_seen as i64)
            .column("error_count", run.error_count as i64)
            .execute(self.gate.pool())
            .await?;
        Ok(())
    }

    async fn update(
        &self,
        id: IndexRunId,
        state: Option<IndexRunState>,
        finished_at: Option<OffsetDateTime>,
        objects_seen: u64,
        error_count: u64,
    ) -> Result<()> {
        let _write = self.gate.permit().await?;
        Update::table("index_runs")
            .set_sql("state = COALESCE(?, state)", state.map(|s| s.as_str()))
            .set_sql(
                "finished_at = COALESCE(?, finished_at)",
                finished_at.map(unix_secs),
            )
            .set("objects_seen", objects_seen as i64)
            .set("error_count", error_count as i64)
            .filter("id = ?", [Bind::Text(uuid_to_text(&id))])
            .execute(self.gate.pool())
            .await?;
        Ok(())
    }

    async fn get(&self, id: IndexRunId) -> Result<Option<IndexRun>> {
        let row = sqlx::query(
            "SELECT id, bucket_id, state, started_at, finished_at, objects_seen, error_count \
             FROM index_runs WHERE id = ?",
        )
        .bind(uuid_to_text(&id))
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|r| row_to_run(RowReader::new(r))).transpose()
    }

    async fn latest_per_bucket(&self) -> Result<Vec<IndexRun>> {
        let rows = sqlx::query(
            "SELECT id, bucket_id, state, started_at, finished_at, objects_seen, error_count \
             FROM index_runs ir \
             WHERE started_at = (SELECT MAX(started_at) FROM index_runs WHERE bucket_id = ir.bucket_id) \
             ORDER BY started_at DESC",
        )
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|r| row_to_run(RowReader::new(r)))
            .collect()
    }

    async fn checkpoint(&self, bucket: BucketId) -> Result<Option<IndexCheckpoint>> {
        let row = sqlx::query(
            "SELECT continuation_token, updated_at FROM index_checkpoints WHERE bucket_id = ?",
        )
        .bind(uuid_to_text(&bucket))
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|r| {
            let reader = RowReader::new(r);
            Ok(IndexCheckpoint {
                continuation_token: reader.opt_text("continuation_token")?,
                updated_at: reader.timestamp("updated_at")?,
            })
        })
        .transpose()
    }

    async fn save_checkpoint(
        &self,
        bucket: BucketId,
        token: Option<&str>,
        at: OffsetDateTime,
    ) -> Result<()> {
        let _write = self.gate.permit().await?;
        Insert::into("index_checkpoints")
            .column("bucket_id", uuid_to_text(&bucket))
            .column("continuation_token", token)
            .column("updated_at", unix_secs(at))
            .suffix(
                "ON CONFLICT(bucket_id) DO UPDATE SET \
                 continuation_token = excluded.continuation_token, \
                 updated_at = excluded.updated_at",
            )
            .execute(self.gate.pool())
            .await?;
        Ok(())
    }

    async fn publish_page(&self, commit: &IndexPageCommit) -> Result<()> {
        let objects = commit.objects.clone();
        let run = commit.run;
        let bucket = commit.bucket;
        let token = commit.continuation_token.clone();
        let at = commit.at;
        let objects_seen = commit.objects_seen;
        let error_count = commit.error_count;
        self.gate
            .write_unit("index page", |tx| {
                Box::pin(async move {
                    for object in &objects {
                        super::remote_object::SqliteRemoteObjectRepository::upsert_with_executor(
                            &mut **tx, object,
                        )
                        .await?;
                    }
                    if let Some(token) = token {
                        Insert::into("index_checkpoints")
                            .column("bucket_id", uuid_to_text(&bucket))
                            .column("continuation_token", token)
                            .column("updated_at", unix_secs(at))
                            .suffix(
                                "ON CONFLICT(bucket_id) DO UPDATE SET \
                                 continuation_token = excluded.continuation_token, \
                                 updated_at = excluded.updated_at",
                            )
                            .execute(&mut **tx)
                            .await?;
                    } else {
                        sqlx::query("DELETE FROM index_checkpoints WHERE bucket_id = ?")
                            .bind(uuid_to_text(&bucket))
                            .execute(&mut **tx)
                            .await
                            .map_err(map_sqlx)?;
                    }
                    let result = Update::table("index_runs")
                        .set("objects_seen", objects_seen as i64)
                        .set("error_count", error_count as i64)
                        .filter("id = ?", [Bind::Text(uuid_to_text(&run))])
                        .execute(&mut **tx)
                        .await?;
                    if result.rows_affected() == 0 {
                        return Err(Error::not_found(format!("index run {run} not found")));
                    }
                    Ok(())
                })
            })
            .await
    }

    async fn clear_checkpoint(&self, bucket: BucketId) -> Result<()> {
        let _write = self.gate.permit().await?;
        sqlx::query("DELETE FROM index_checkpoints WHERE bucket_id = ?")
            .bind(uuid_to_text(&bucket))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    async fn finish(&self, commit: &IndexRunCommit) -> Result<Option<i64>> {
        let run = commit.run;
        let bucket = commit.bucket;
        let state = commit.state;
        let finished_at = commit.finished_at;
        let objects_seen = commit.objects_seen;
        let error_count = commit.error_count;
        let at = commit.at;
        self.gate
            .write_unit("index run finish", |tx| {
                Box::pin(async move {
                    let result = Update::table("index_runs")
                        .set("state", state.as_str())
                        .set_sql(
                            "finished_at = COALESCE(?, finished_at)",
                            finished_at.map(unix_secs),
                        )
                        .set("objects_seen", objects_seen as i64)
                        .set("error_count", error_count as i64)
                        .filter("id = ?", [Bind::Text(uuid_to_text(&run))])
                        .execute(&mut **tx)
                        .await?;
                    if result.rows_affected() == 0 {
                        return Err(Error::not_found(format!("index run {run} not found")));
                    }
                    if state != IndexRunState::Succeeded {
                        return Ok(None);
                    }

                    Insert::into("catalog_revisions")
                        .column("bucket_id", uuid_to_text(&bucket))
                        .column("revision", 1_i64)
                        .column("last_refreshed_at", unix_secs(at))
                        .suffix(
                            "ON CONFLICT(bucket_id) DO UPDATE SET revision = revision + 1, \
                             last_refreshed_at = excluded.last_refreshed_at",
                        )
                        .execute(&mut **tx)
                        .await?;
                    let row =
                        sqlx::query("SELECT revision FROM catalog_revisions WHERE bucket_id = ?")
                            .bind(uuid_to_text(&bucket))
                            .fetch_one(&mut **tx)
                            .await
                            .map_err(map_sqlx)?;
                    Ok(Some(row.get::<i64, _>(0)))
                })
            })
            .await
    }

    async fn revision(&self, bucket: BucketId) -> Result<Option<(i64, OffsetDateTime)>> {
        let row = sqlx::query(
            "SELECT revision, last_refreshed_at FROM catalog_revisions WHERE bucket_id = ?",
        )
        .bind(uuid_to_text(&bucket))
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|r| {
            let reader = RowReader::new(r);
            Ok((
                reader.integer("revision")?,
                reader.timestamp("last_refreshed_at")?,
            ))
        })
        .transpose()
    }
}

pub(crate) fn row_to_run(row: RowReader) -> Result<IndexRun> {
    Ok(IndexRun {
        id: IndexRunId::from_uuid(row.uuid("id")?),
        bucket_id: BucketId::from_uuid(row.uuid("bucket_id")?),
        state: row.token("state", IndexRunState::parse)?,
        started_at: row.timestamp("started_at")?,
        finished_at: row.opt_timestamp("finished_at")?,
        objects_seen: row.integer("objects_seen")? as u64,
        error_count: row.integer("error_count")? as u64,
    })
}
