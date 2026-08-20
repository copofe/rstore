//! SQLite-backed [`JobRepository`] (JOB-001/002/015).

use async_trait::async_trait;
use rstore_domain::job::JobRepository;
use rstore_domain::job::{ItemState, Job, JobItem, JobKind, JobState};
use rstore_domain::{Error, JobId, Result};
use sqlx::sqlite::SqlitePool;
use time::OffsetDateTime;

use crate::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::error::map_sqlx;

/// Persists jobs and their items.
#[derive(Clone)]
pub struct SqliteJobRepository {
    pool: SqlitePool,
    read_pool: SqlitePool,
    writer: crate::actor::SqliteWriter,
}

impl SqliteJobRepository {
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

    async fn load_items<'e, E>(executor: E, id: JobId) -> Result<Vec<JobItem>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let rows = sqlx::query(
            "SELECT remote_uri, state, bytes_total, bytes_done, error_code
             FROM transfer_job_items WHERE job_id = ?",
        )
        .bind(uuid_to_text(&id))
        .fetch_all(executor)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let row = RowReader::new(row);
                Ok(JobItem {
                    remote_uri: row.text("remote_uri")?,
                    state: row.token("state", ItemState::parse)?,
                    bytes_total: row.integer("bytes_total")? as u64,
                    bytes_done: row.integer("bytes_done")? as u64,
                    error_code: row.opt_text("error_code")?,
                })
            })
            .collect()
    }

    async fn load_one<'e, E>(row: sqlx::sqlite::SqliteRow, executor: E) -> Result<Job>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let row = RowReader::new(row);
        let id = JobId::from_uuid(row.uuid("id")?);
        Ok(Job {
            id,
            kind: row.token("kind", JobKind::parse)?,
            state: row.token("state", JobState::parse)?,
            created_at: row.timestamp("created_at")?,
            updated_at: row.timestamp("updated_at")?,
            items: Self::load_items(executor, id).await?,
            error: row.opt_text("error")?,
        })
    }

    async fn insert_items(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: JobId,
        items: &[JobItem],
    ) -> Result<()> {
        for item in items {
            Insert::into("transfer_job_items")
                .column("job_id", uuid_to_text(&id))
                .column("remote_uri", &item.remote_uri)
                .column("state", item.state.as_str())
                .column("bytes_total", item.bytes_total as i64)
                .column("bytes_done", item.bytes_done as i64)
                .column("error_code", &item.error_code)
                .execute(&mut **tx)
                .await?;
        }
        Ok(())
    }
}

const JOB_COLUMNS: &str = "id, kind, state, created_at, updated_at, error";

#[async_trait]
impl JobRepository for SqliteJobRepository {
    async fn create(&self, job: &Job) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let job = job.clone();
        crate::tx::atomically(&self.pool, "job insert", |tx| {
            Box::pin(async move {
                Insert::into("transfer_jobs")
                    .column("id", uuid_to_text(&job.id))
                    .column("kind", job.kind.as_str())
                    .column("state", job.state.as_str())
                    .column("created_at", unix_secs(job.created_at))
                    .column("updated_at", unix_secs(job.updated_at))
                    .column("error", &job.error)
                    .execute(&mut **tx)
                    .await?;
                Self::insert_items(tx, job.id, &job.items).await?;
                Ok(())
            })
        })
        .await
    }

    async fn get(&self, id: JobId) -> Result<Option<Job>> {
        let row = sqlx::query(&format!(
            "SELECT {JOB_COLUMNS} FROM transfer_jobs WHERE id = ?"
        ))
        .bind(uuid_to_text(&id))
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        match row {
            Some(row) => Ok(Some(Self::load_one(row, &self.read_pool).await?)),
            None => Ok(None),
        }
    }

    async fn transition(&self, id: JobId, target: JobState, at: OffsetDateTime) -> Result<Job> {
        let _write = self.writer.acquire().await?;
        crate::tx::atomically(&self.pool, "job transition", |tx| {
            Box::pin(async move {
                let row = sqlx::query(&format!(
                    "SELECT {JOB_COLUMNS} FROM transfer_jobs WHERE id = ?"
                ))
                .bind(uuid_to_text(&id))
                .fetch_optional(&mut **tx)
                .await
                .map_err(map_sqlx)?;
                let row = row.ok_or_else(|| Error::not_found(format!("job {id} not found")))?;
                // Validate against the domain state machine before writing.
                RowReader::new(row)
                    .token::<JobState>("state", JobState::parse)?
                    .transition(target)?;
                let affected = Update::table("transfer_jobs")
                    .set("state", target.as_str())
                    .set("updated_at", unix_secs(at))
                    .filter("id = ?", [Bind::Text(uuid_to_text(&id))])
                    .execute(&mut **tx)
                    .await?
                    .rows_affected();
                if affected == 0 {
                    return Err(Error::not_found(format!("job {id} not found")));
                }
                let row = sqlx::query(&format!(
                    "SELECT {JOB_COLUMNS} FROM transfer_jobs WHERE id = ?"
                ))
                .bind(uuid_to_text(&id))
                .fetch_one(&mut **tx)
                .await
                .map_err(map_sqlx)?;
                Self::load_one(row, &mut **tx).await
            })
        })
        .await
    }

    async fn commit_terminal(
        &self,
        id: JobId,
        items: &[JobItem],
        target: JobState,
        at: OffsetDateTime,
    ) -> Result<Job> {
        if !target.is_terminal() && target != JobState::Interrupted {
            return Err(Error::invalid_input(
                "settled commit needs a terminal or interrupted state",
            ));
        }
        let _write = self.writer.acquire().await?;
        let items = items.to_vec();
        crate::tx::atomically(&self.pool, "job terminal commit", |tx| {
            Box::pin(async move {
                let row = sqlx::query(&format!(
                    "SELECT {JOB_COLUMNS} FROM transfer_jobs WHERE id = ?"
                ))
                .bind(uuid_to_text(&id))
                .fetch_optional(&mut **tx)
                .await
                .map_err(map_sqlx)?;
                let row = row.ok_or_else(|| Error::not_found(format!("job {id} not found")))?;
                RowReader::new(row)
                    .token::<JobState>("state", JobState::parse)?
                    .transition(target)?;

                sqlx::query("DELETE FROM transfer_job_items WHERE job_id = ?")
                    .bind(uuid_to_text(&id))
                    .execute(&mut **tx)
                    .await
                    .map_err(map_sqlx)?;
                Self::insert_items(tx, id, &items).await?;

                let affected = Update::table("transfer_jobs")
                    .set("state", target.as_str())
                    .set("updated_at", unix_secs(at))
                    .filter("id = ?", [Bind::Text(uuid_to_text(&id))])
                    .execute(&mut **tx)
                    .await?
                    .rows_affected();
                if affected == 0 {
                    return Err(Error::not_found(format!("job {id} not found")));
                }
                let row = sqlx::query(&format!(
                    "SELECT {JOB_COLUMNS} FROM transfer_jobs WHERE id = ?"
                ))
                .bind(uuid_to_text(&id))
                .fetch_one(&mut **tx)
                .await
                .map_err(map_sqlx)?;
                Self::load_one(row, &mut **tx).await
            })
        })
        .await
    }

    async fn list(&self) -> Result<Vec<Job>> {
        let rows = sqlx::query(&format!(
            "SELECT {JOB_COLUMNS} FROM transfer_jobs ORDER BY created_at DESC"
        ))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            jobs.push(Self::load_one(row, &self.read_pool).await?);
        }
        Ok(jobs)
    }

    async fn delete(&self, id: JobId) -> Result<bool> {
        let _write = self.writer.acquire().await?;
        let affected = sqlx::query("DELETE FROM transfer_jobs WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }

    /// Replace a job's item rows and persisted progress checkpoints.
    async fn replace_items(&self, id: JobId, items: &[JobItem]) -> Result<()> {
        let _write = self.writer.acquire().await?;
        let items = items.to_vec();
        crate::tx::atomically(&self.pool, "item update", |tx| {
            Box::pin(async move {
                sqlx::query("DELETE FROM transfer_job_items WHERE job_id = ?")
                    .bind(uuid_to_text(&id))
                    .execute(&mut **tx)
                    .await
                    .map_err(map_sqlx)?;
                Self::insert_items(tx, id, &items).await?;
                Ok(())
            })
        })
        .await
    }

    /// JOB-015: jobs orphaned by a dead process are marked `interrupted` so
    /// they can be resumed. There is no daemon — no job survives the process
    /// that owned it.
    async fn mark_stale_jobs_interrupted(&self, at: OffsetDateTime) -> Result<u64> {
        let stale: Vec<JobId> = self
            .list()
            .await?
            .into_iter()
            .filter(|job| {
                matches!(
                    job.state,
                    JobState::Running | JobState::Retrying | JobState::Pending
                )
            })
            .map(|job| job.id)
            .collect();
        for id in &stale {
            self.transition(*id, JobState::Interrupted, at).await?;
        }
        Ok(stale.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[tokio::test]
    async fn job_and_items_round_trip_with_transitions() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SqliteJobRepository::new(db.pool().clone());
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

        let mut job = Job::new(JobKind::Upload, JobId::new(), now);
        job.items = vec![
            JobItem {
                remote_uri: "nas/media/a.png".into(),
                state: ItemState::Succeeded,
                bytes_total: 100,
                bytes_done: 100,
                error_code: None,
            },
            JobItem {
                remote_uri: "nas/media/b.png".into(),
                state: ItemState::Failed,
                bytes_total: 50,
                bytes_done: 10,
                error_code: Some("network_unreachable".into()),
            },
        ];
        repo.create(&job).await.unwrap();

        let fetched = repo.get(job.id).await.unwrap().unwrap();
        assert_eq!(fetched.kind, JobKind::Upload);
        assert_eq!(fetched.items.len(), 2);
        assert_eq!(fetched.items[1].state, ItemState::Failed);

        // Legal transition persists; an illegal one is a Conflict.
        let moved = repo
            .transition(job.id, JobState::Running, now)
            .await
            .unwrap();
        assert_eq!(moved.state, JobState::Running);
        let paused = repo
            .transition(job.id, JobState::Paused, now)
            .await
            .unwrap();
        assert_eq!(paused.state, JobState::Paused);
        // Paused cannot leap straight to a terminal state (JOB-003).
        let err = repo
            .transition(job.id, JobState::Succeeded, now)
            .await
            .unwrap_err();
        assert_eq!(err.code, rstore_domain::ErrorCode::Conflict);

        let resumed = repo
            .transition(job.id, JobState::Running, now)
            .await
            .unwrap();
        assert_eq!(resumed.state, JobState::Running);
        let terminal = repo
            .commit_terminal(
                job.id,
                &[JobItem {
                    remote_uri: "nas/media/done.png".into(),
                    state: ItemState::Succeeded,
                    bytes_total: 4,
                    bytes_done: 4,
                    error_code: None,
                }],
                JobState::Succeeded,
                now,
            )
            .await
            .unwrap();
        assert_eq!(terminal.state, JobState::Succeeded);
        assert_eq!(terminal.items[0].remote_uri, "nas/media/done.png");

        // Items can be checkpointed.
        repo.replace_items(job.id, &[]).await.unwrap();
        assert!(repo.get(job.id).await.unwrap().unwrap().items.is_empty());
        assert_eq!(repo.list().await.unwrap().len(), 1);
        assert!(repo.delete(job.id).await.unwrap());
    }

    #[tokio::test]
    async fn stale_non_terminal_jobs_are_interrupted_via_domain_transitions() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = SqliteJobRepository::new(db.pool().clone());
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

        let running = Job::new(JobKind::Download, JobId::new(), now);
        repo.create(&running).await.unwrap();
        repo.transition(running.id, JobState::Running, now)
            .await
            .unwrap();

        let pending = Job::new(JobKind::Upload, JobId::new(), now);
        repo.create(&pending).await.unwrap();

        let retrying = Job::new(JobKind::Copy, JobId::new(), now);
        repo.create(&retrying).await.unwrap();
        repo.transition(retrying.id, JobState::Running, now)
            .await
            .unwrap();
        repo.transition(retrying.id, JobState::Retrying, now)
            .await
            .unwrap();

        let stale = repo.mark_stale_jobs_interrupted(now).await.unwrap();
        assert_eq!(stale, 3);
        for id in [running.id, pending.id, retrying.id] {
            assert_eq!(
                repo.get(id).await.unwrap().unwrap().state,
                JobState::Interrupted
            );
        }
    }
}
