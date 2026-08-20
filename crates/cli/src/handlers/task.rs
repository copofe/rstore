//! `task …` handlers (JOB-).

use super::HandlerResult;
use crate::output::HandlerOutput;

// --- task ------------------------------------------------------------------

pub(super) async fn task(
    action: &crate::args::TaskCommand,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    use rstore_application::JobRef;

    let svc = app.jobs();
    match action {
        crate::args::TaskCommand::List => {
            let jobs = svc.list().await?;
            let repairs = svc.pending_repairs().await?;
            let data = serde_json::json!({"jobs": jobs, "repairs": repairs});
            let mut table = super::format::Table::new(
                &["#", "STATE", "KIND", "ITEMS", "ID"],
                &[
                    super::format::Align::Left,
                    super::format::Align::Left,
                    super::format::Align::Left,
                    super::format::Align::Left,
                    super::format::Align::Left,
                ],
            );
            for (i, job) in jobs.iter().enumerate() {
                let id_short: String = job.id.to_string().chars().take(17).collect();
                table.row(vec![
                    format!("#{}", i + 1),
                    job.state.as_str().to_string(),
                    job.kind.as_str().to_string(),
                    format!("{}/{}", job.items_done(), job.items.len()),
                    format!("{id_short}…"),
                ]);
            }
            let mut human = super::format::listing(
                "Jobs (newest first):",
                "No jobs recorded yet. Uploads/downloads create them.",
                table,
            );
            if !repairs.is_empty() {
                human.push_str(&format!(
                    "Pending repairs: {} (inspect `doctor` for details)\n",
                    repairs.len()
                ));
            }
            Ok(HandlerOutput::human(human).with_data(data))
        }
        crate::args::TaskCommand::Show { reference, errors } => {
            let job = svc.resolve(&JobRef::parse(reference)?).await?;
            let data = super::json_value(&job)?;
            let mut fields = super::format::Fields::new();
            fields
                .header(format!(
                    "job {} — {} {}",
                    job.id,
                    job.kind.as_str(),
                    job.state.as_str()
                ))
                .push("created", rstore_domain::clock::iso8601(job.created_at))
                .push("updated", rstore_domain::clock::iso8601(job.updated_at))
                .push("items", job.items.len());
            if let Some(error) = &job.error {
                fields.push("error", error);
            }
            let mut human = fields.render();
            for item in &job.items {
                human.push_str(&format!(
                    "  [{:<9}] {} ({} / {} bytes)\n",
                    item.state.as_str(),
                    item.remote_uri,
                    item.bytes_done,
                    item.bytes_total,
                ));
                if *errors {
                    if let Some(code) = &item.error_code {
                        human.push_str(&format!("      error: {code}\n"));
                    }
                }
            }
            Ok(HandlerOutput::human(human).with_data(data))
        }
        crate::args::TaskCommand::Pause { reference } => {
            let job = svc.pause(&JobRef::parse(reference)?).await?;
            control_output(job, "Paused")
        }
        crate::args::TaskCommand::Resume { reference } => {
            let job = svc.resume(&JobRef::parse(reference)?).await?;
            control_output(job, "Resumed (running)")
        }
        crate::args::TaskCommand::Retry { reference } => {
            let job = svc.retry(&JobRef::parse(reference)?).await?;
            control_output(job, "Retrying (running)")
        }
        crate::args::TaskCommand::Cancel { reference } => {
            let job = svc.cancel(&JobRef::parse(reference)?).await?;
            control_output(job, "Cancelled")
        }
    }
}

fn control_output(job: rstore_domain::job::Job, verb: &str) -> HandlerResult {
    let data = super::json_value(&job)?;
    Ok(
        HandlerOutput::human(format!("{verb} job {} ({})\n", job.id, job.kind.as_str()))
            .with_data(data),
    )
}
