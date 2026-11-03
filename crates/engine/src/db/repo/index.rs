//! Repository for index runs and scan checkpoints.

use crate::db::{IndexCheckpoint, IndexPageCommit, IndexRunCommit};
use rstore_domain::id::{BucketId, IndexRunId};
use rstore_domain::workspace::{IndexRun, IndexRunState};
use rstore_domain::{Error, Result};
use sqlx::sqlite::SqlitePool;
use time::OffsetDateTime;

use crate::db::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::db::error::map_sqlx;
use crate::db::tx::WriteGate;

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

impl IndexRunRepository {
    pub async fn create(&self, run: &IndexRun) -> Result<()> {
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

    pub async fn update(
        &self,
        id: IndexRunId,
        state: Option<IndexRunState>,
        finished_at: Option<OffsetDateTime>,
        objects_seen: u64,
        error_count: u64,
    ) -> Result<()> {
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

    pub async fn latest_per_bucket(&self) -> Result<Vec<IndexRun>> {
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

    pub async fn checkpoint(&self, bucket: BucketId) -> Result<Option<IndexCheckpoint>> {
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

    pub async fn save_checkpoint(
        &self,
        bucket: BucketId,
        token: Option<&str>,
        at: OffsetDateTime,
    ) -> Result<()> {
        upsert_checkpoint_on(self.gate.pool(), bucket, token, at).await
    }

    pub async fn publish_page(&self, commit: &IndexPageCommit) -> Result<()> {
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
                        super::remote_object::upsert_object_on(&mut **tx, object).await?;
                    }
                    if let Some(token) = token {
                        upsert_checkpoint_on(&mut **tx, bucket, Some(token.as_str()), at).await?;
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

    pub async fn clear_checkpoint(&self, bucket: BucketId) -> Result<()> {
        sqlx::query("DELETE FROM index_checkpoints WHERE bucket_id = ?")
            .bind(uuid_to_text(&bucket))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    pub async fn finish(&self, commit: &IndexRunCommit) -> Result<()> {
        let result = Update::table("index_runs")
            .set("state", commit.state.as_str())
            .set_sql(
                "finished_at = COALESCE(?, finished_at)",
                commit.finished_at.map(unix_secs),
            )
            .set("objects_seen", commit.objects_seen as i64)
            .set("error_count", commit.error_count as i64)
            .filter("id = ?", [Bind::Text(uuid_to_text(&commit.run))])
            .execute(self.gate.pool())
            .await?;
        if result.rows_affected() == 0 {
            return Err(Error::not_found(format!(
                "index run {} not found",
                commit.run
            )));
        }
        Ok(())
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

/// The index-checkpoint upsert: one statement shared by `save_checkpoint`
/// and the index run's page commit so the ON CONFLICT shape cannot drift.
async fn upsert_checkpoint_on(
    executor: impl sqlx::Executor<'_, Database = sqlx::Sqlite>,
    bucket: BucketId,
    token: Option<&str>,
    at: OffsetDateTime,
) -> Result<()> {
    Insert::into("index_checkpoints")
        .column("bucket_id", uuid_to_text(&bucket))
        .column("continuation_token", token)
        .column("updated_at", unix_secs(at))
        .suffix(
            "ON CONFLICT(bucket_id) DO UPDATE SET \
             continuation_token = excluded.continuation_token, \
             updated_at = excluded.updated_at",
        )
        .execute(executor)
        .await?;
    Ok(())
}
