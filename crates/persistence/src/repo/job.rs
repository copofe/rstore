//! SQLite-backed transfer-job repository.
use async_trait::async_trait;
use rstore_application_ports::JobRepository;
use rstore_domain::job::{ItemState, Job, JobItem, JobKind, JobState};
use rstore_domain::{Error, JobId, Result, TypedId as _};
use sqlx::sqlite::SqlitePool;
use time::OffsetDateTime;

use crate::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::error::map_sqlx;
use crate::tx::WriteGate;

#[derive(Clone)]
pub struct SqliteJobRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
}

impl SqliteJobRepository {
    async fn validate_transition(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: JobId,
        target: JobState,
    ) -> Result<()> {
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
        Ok(())
    }

    async fn apply_state(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: JobId,
        target: JobState,
        at: OffsetDateTime,
    ) -> Result<()> {
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
        Ok(())
    }

    async fn reload(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, id: JobId) -> Result<Job> {
        let row = sqlx::query(&format!(
            "SELECT {JOB_COLUMNS} FROM transfer_jobs WHERE id = ?"
        ))
        .bind(uuid_to_text(&id))
        .fetch_one(&mut **tx)
        .await
        .map_err(map_sqlx)?;
        Self::load_one(row, &mut **tx).await
    }

    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool) -> Self {
        Self { gate, read_pool }
    }

    async fn load_items<'e, E>(executor: E, id: JobId) -> Result<Vec<JobItem>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let rows = sqlx::query(
            "SELECT label, state, bytes_total, bytes_done, error
             FROM transfer_job_items WHERE job_id = ?",
        )
        .bind(uuid_to_text(&id))
        .fetch_all(executor)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| Self::item_from_row(RowReader::new(row)))
            .collect()
    }

    fn item_from_row(row: RowReader) -> Result<JobItem> {
        Ok(JobItem {
            label: row.text("label")?,
            state: row.token("state", ItemState::parse)?,
            bytes_total: row.integer("bytes_total")? as u64,
            bytes_done: row.integer("bytes_done")? as u64,
            error: row.opt_text("error")?,
        })
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
                .column("label", &item.label)
                .column("state", item.state.as_str())
                .column("bytes_total", item.bytes_total as i64)
                .column("bytes_done", item.bytes_done as i64)
                .column("error", &item.error)
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
        let job = job.clone();
        self.gate
            .write_unit("job insert", |tx| {
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
        self.gate
            .write_unit("job transition", |tx| {
                Box::pin(async move {
                    Self::validate_transition(tx, id, target).await?;
                    Self::apply_state(tx, id, target, at).await?;
                    Self::reload(tx, id).await
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
        let items = items.to_vec();
        self.gate
            .write_unit("job terminal commit", |tx| {
                Box::pin(async move {
                    Self::validate_transition(tx, id, target).await?;
                    sqlx::query("DELETE FROM transfer_job_items WHERE job_id = ?")
                        .bind(uuid_to_text(&id))
                        .execute(&mut **tx)
                        .await
                        .map_err(map_sqlx)?;
                    Self::insert_items(tx, id, &items).await?;
                    Self::apply_state(tx, id, target, at).await?;
                    Self::reload(tx, id).await
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
            let row = RowReader::new(row);
            jobs.push(Job {
                id: JobId::from_uuid(row.uuid("id")?),
                kind: row.token("kind", JobKind::parse)?,
                state: row.token("state", JobState::parse)?,
                created_at: row.timestamp("created_at")?,
                updated_at: row.timestamp("updated_at")?,
                items: Vec::new(),
                error: row.opt_text("error")?,
            });
        }

        const ITEM_COLUMNS: &str = "job_id, label, state, bytes_total, bytes_done, error";
        let item_rows = sqlx::query(&format!(
            "SELECT {ITEM_COLUMNS} FROM transfer_job_items ORDER BY id"
        ))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        let mut by_job: std::collections::HashMap<String, Vec<JobItem>> =
            std::collections::HashMap::with_capacity(jobs.len());
        for row in item_rows {
            let row = RowReader::new(row);
            let job_id = row.text("job_id")?;
            by_job
                .entry(job_id)
                .or_default()
                .push(Self::item_from_row(row)?);
        }
        for job in &mut jobs {
            if let Some(items) = by_job.remove(&job.id.as_uuid().to_string()) {
                job.items = items;
            }
        }
        Ok(jobs)
    }

    async fn delete(&self, id: JobId) -> Result<bool> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query("DELETE FROM transfer_jobs WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }

    async fn replace_items(&self, id: JobId, items: &[JobItem]) -> Result<()> {
        let items = items.to_vec();
        self.gate
            .write_unit("item update", |tx| {
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

    async fn mark_stale_jobs_interrupted(&self, at: OffsetDateTime) -> Result<u64> {
        let _write = self.gate.permit().await?;
        let affected = sqlx::query(
            "UPDATE transfer_jobs SET state = 'interrupted', updated_at = ? \
             WHERE state IN ('pending', 'running', 'retrying')",
        )
        .bind(unix_secs(at))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[tokio::test]
    async fn job_and_items_round_trip_with_transitions() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = db.jobs();
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

        let mut job = Job::new(JobKind::Upload, JobId::new_random(), now);
        job.items = vec![
            JobItem {
                label: "nas/media/a.png".into(),
                state: ItemState::Succeeded,
                bytes_total: 100,
                bytes_done: 100,
                error: None,
            },
            JobItem {
                label: "nas/media/b.png".into(),
                state: ItemState::Failed,
                bytes_total: 50,
                bytes_done: 10,
                error: Some("network_unreachable".into()),
            },
        ];
        repo.create(&job).await.unwrap();

        let fetched = repo.get(job.id).await.unwrap().unwrap();
        assert_eq!(fetched.kind, JobKind::Upload);
        assert_eq!(fetched.items.len(), 2);
        assert_eq!(fetched.items[1].state, ItemState::Failed);

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
                    label: "nas/media/done.png".into(),
                    state: ItemState::Succeeded,
                    bytes_total: 4,
                    bytes_done: 4,
                    error: None,
                }],
                JobState::Succeeded,
                now,
            )
            .await
            .unwrap();
        assert_eq!(terminal.state, JobState::Succeeded);
        assert_eq!(terminal.items[0].label, "nas/media/done.png");

        repo.replace_items(job.id, &[]).await.unwrap();
        assert!(repo.get(job.id).await.unwrap().unwrap().items.is_empty());
        assert_eq!(repo.list().await.unwrap().len(), 1);
        assert!(repo.delete(job.id).await.unwrap());
    }

    #[tokio::test]
    async fn stale_non_terminal_jobs_are_interrupted_via_domain_transitions() {
        let db = Database::open_in_memory().await.unwrap();
        let repo = db.jobs();
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

        let running = Job::new(JobKind::Download, JobId::new_random(), now);
        repo.create(&running).await.unwrap();
        repo.transition(running.id, JobState::Running, now)
            .await
            .unwrap();

        let pending = Job::new(JobKind::Upload, JobId::new_random(), now);
        repo.create(&pending).await.unwrap();

        let retrying = Job::new(JobKind::Copy, JobId::new_random(), now);
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
