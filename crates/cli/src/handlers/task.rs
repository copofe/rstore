//! `task …` handlers (JOB-).

use super::{Ctx, HandlerResult};
use crate::output::HandlerOutput;

// --- task ------------------------------------------------------------------

pub(super) async fn task(ctx: &Ctx<'_>, action: &crate::args::TaskCommand) -> HandlerResult {
    use rstore_engine::JobRef;

    let svc = ctx.app.jobs();
    match action {
        crate::args::TaskCommand::List => {
            let jobs = svc.list().await?;
            let data = serde_json::json!({"jobs": jobs});
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
            let human = super::format::listing(
                "Jobs (newest first):",
                "No jobs recorded yet. Uploads/downloads create them.",
                table,
            );
            Ok(HandlerOutput::human(human).with_data(data))
        }
        crate::args::TaskCommand::Show { job, errors } => {
            // Without a reference, the newest job (ordinal 1).
            let job = svc
                .resolve(&JobRef::parse(job.as_deref().unwrap_or("1"))?)
                .await?;
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
                    item.label,
                    item.bytes_done,
                    item.bytes_total,
                ));
                if *errors {
                    if let Some(code) = &item.error {
                        human.push_str(&format!("      error: {code}\n"));
                    }
                }
            }
            Ok(HandlerOutput::human(human).with_data(data))
        }
        crate::args::TaskCommand::Cancel { job } => {
            let report = svc.cancel(&JobRef::parse(job)?).await?;
            let data = super::json_value(&report)?;
            let verb = if report.requested {
                "Cancel requested for"
            } else {
                "Cancelled"
            };
            Ok(HandlerOutput::human(format!(
                "{verb} job {} ({})\n",
                report.job.id,
                report.job.kind.as_str()
            ))
            .with_data(data))
        }
    }
}
