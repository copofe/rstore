//! SQLite-backed transfer-job repository.
use rstore_domain::job::{ItemState, Job, JobItem, JobKind, JobState};
use rstore_domain::{Error, JobId, Result, TypedId as _};
use sqlx::sqlite::SqlitePool;
use time::OffsetDateTime;

use crate::db::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::db::error::map_sqlx;
use crate::db::tx::WriteGate;

#[derive(Clone)]
pub struct JobRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
}

impl JobRepository {
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
             FROM transfer_job_items WHERE job_id = ? ORDER BY id",
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

impl JobRepository {
    /// Insert a job leased to `owner` until `lease_until`.
    pub async fn create(&self, job: &Job, owner: &str, lease_until: OffsetDateTime) -> Result<()> {
        let job = job.clone();
        let owner = owner.to_owned();
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
                        .column("lease_owner", owner.as_str())
                        .column("lease_expires_at", unix_secs(lease_until))
                        .execute(&mut **tx)
                        .await?;
                    Self::insert_items(tx, job.id, &job.items).await?;
                    Ok(())
                })
            })
            .await
    }

    pub async fn get(&self, id: JobId) -> Result<Option<Job>> {
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

    pub async fn transition(&self, id: JobId, target: JobState, at: OffsetDateTime) -> Result<Job> {
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

    pub async fn commit_terminal(
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

    pub async fn list(&self) -> Result<Vec<Job>> {
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

    /// Extend the owner's lease and report whether another process asked the
    /// job to cancel. A lease another owner holds is left alone.
    pub async fn renew_lease(&self, id: JobId, owner: &str, until: OffsetDateTime) -> Result<bool> {
        sqlx::query(
            "UPDATE transfer_jobs SET lease_expires_at = ? WHERE id = ? AND lease_owner = ?",
        )
        .bind(unix_secs(until))
        .bind(uuid_to_text(&id))
        .bind(owner)
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?;
        let requested: Option<i64> =
            sqlx::query_scalar("SELECT cancel_requested FROM transfer_jobs WHERE id = ?")
                .bind(uuid_to_text(&id))
                .fetch_optional(self.gate.pool())
                .await
                .map_err(map_sqlx)?;
        Ok(requested.unwrap_or(0) != 0)
    }

    /// When the job's lease runs out, if it has one.
    pub async fn lease_expires_at(&self, id: JobId) -> Result<Option<OffsetDateTime>> {
        let at: Option<Option<i64>> =
            sqlx::query_scalar("SELECT lease_expires_at FROM transfer_jobs WHERE id = ?")
                .bind(uuid_to_text(&id))
                .fetch_optional(&self.read_pool)
                .await
                .map_err(map_sqlx)?;
        at.flatten()
            .map(|secs| {
                OffsetDateTime::from_unix_timestamp(secs)
                    .map_err(|e| Error::database("invalid lease timestamp").with_source(e))
            })
            .transpose()
    }

    /// Ask the owning process to cancel the job at its next heartbeat.
    pub async fn request_cancel(&self, id: JobId) -> Result<()> {
        sqlx::query("UPDATE transfer_jobs SET cancel_requested = 1 WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    /// Persist one item's state as it settles, so other processes see the run
    /// advance. `ordinal` is the item's position in the job.
    pub async fn update_item(&self, id: JobId, ordinal: usize, item: &JobItem) -> Result<()> {
        sqlx::query(
            "UPDATE transfer_job_items SET state = ?, bytes_total = ?, bytes_done = ?, error = ? \
             WHERE id = (SELECT id FROM transfer_job_items WHERE job_id = ? \
                         ORDER BY id LIMIT 1 OFFSET ?)",
        )
        .bind(item.state.as_str())
        .bind(item.bytes_total as i64)
        .bind(item.bytes_done as i64)
        .bind(&item.error)
        .bind(uuid_to_text(&id))
        .bind(ordinal as i64)
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Jobs whose owner stopped renewing the lease (or never had one) while
    /// pending or running become `interrupted`; returns how many. Live jobs
    /// of other processes keep running.
    pub async fn mark_stale_jobs_interrupted(&self, at: OffsetDateTime) -> Result<u64> {
        let affected = sqlx::query(
            "UPDATE transfer_jobs SET state = 'interrupted', updated_at = ?1 \
             WHERE state IN ('pending', 'running') \
               AND (lease_expires_at IS NULL OR lease_expires_at < ?1)",
        )
        .bind(unix_secs(at))
        .execute(self.gate.pool())
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(affected)
    }
}
