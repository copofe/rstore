//! Handlers for the `tag` subcommands: batched add/remove over remote
//! objects plus list/edit/delete/merge of tag definitions, all scoped to the
//! workspace from `-w` or the current one. The batch arms render through the
//! shared batch-report grammar.

use rstore_application::{TagRef, UpdateTagInput, WorkspaceRef};

use super::{Ctx, HandlerResult};
use crate::args::TagCommand;
use crate::output::HandlerOutput;

pub(super) async fn tag(ctx: &Ctx<'_>, action: &TagCommand) -> HandlerResult {
    let svc = ctx.app.catalog().tags();
    let workspace = ctx.workspace().await?;
    let reference = WorkspaceRef::Id(workspace.id);
    let app = ctx.app;
    match action {
        TagCommand::Add {
            references,
            tags,
            replace,
        } => {
            let (inputs, uris) = expand_uris(app, references).await?;
            let report = svc.attach_many(&reference, &uris, tags, *replace).await?;
            let created: Vec<&str> = report.created.iter().map(|t| t.name.as_str()).collect();
            let data = serde_json::json!({
                "workspace": workspace.name,
                "outcomes": super::json_value(report.outcome.items())?,
                "tags": tags,
                "created": created,
                "replace": replace,
            });
            // Surface created tags as warnings so a mistyped name is noticed.
            let mut out = render_tag_batch(&inputs, &report.outcome, "tagged", data)?;
            for name in created {
                out = out.with_warning(format!("created tag `{name}` in `{}`", workspace.name));
            }
            Ok(out)
        }
        TagCommand::Remove { references, tags } => {
            let (inputs, uris) = expand_uris(app, references).await?;
            let outcomes = svc.detach_many(&reference, &uris, tags).await?;
            let data = serde_json::json!({
                "workspace": workspace.name,
                "outcomes": super::json_value(outcomes.items())?,
                "tags": tags,
            });
            render_tag_batch(&inputs, &outcomes, "untagged", data)
        }
        TagCommand::List => {
            let list = svc.list(&reference).await?;
            let mut table = super::format::Table::new(
                &["NAME", "COLOR", "FILES"],
                &[
                    super::format::Align::Left,
                    super::format::Align::Left,
                    super::format::Align::Left,
                ],
            );
            for t in &list {
                table.row(vec![
                    t.tag.name.clone(),
                    t.tag.color.clone(),
                    format!("{} files", t.object_count),
                ]);
            }
            let human = super::format::listing(
                &format!("Tags in `{}`:", workspace.name),
                &format!("No tags in `{}`.", workspace.name),
                table,
            );
            Ok(HandlerOutput::human(human).with_data(super::json_value(&list)?))
        }
        TagCommand::Edit {
            tag,
            name,
            color,
            description,
        } => {
            let updated = svc
                .update(
                    &reference,
                    &TagRef::parse(tag),
                    UpdateTagInput {
                        name: name.clone(),
                        color: color.clone(),
                        // Presentation convention: an empty --description
                        // means "clear"; the API expresses it as Some(None).
                        description: description.clone().map(|d| (!d.is_empty()).then_some(d)),
                    },
                )
                .await?;
            Ok(
                HandlerOutput::human(format!("tag `{}` updated\n", updated.name))
                    .with_data(super::json_value(&updated)?),
            )
        }
        TagCommand::Delete { tag } => {
            // Resolve first so the confirmation names the exact target.
            let tag_ref = TagRef::parse(tag);
            let existing = svc.resolve(&reference, &tag_ref).await?;
            super::tty::remove_flow(
                format!(
                    "delete tag `{}` from `{}` (its associations go with it)",
                    existing.name, workspace.name
                ),
                ctx.yes,
                &ctx.session,
                svc.remove(&reference, &tag_ref),
                format!(
                    "tag `{}` deleted from `{}`\n",
                    existing.name, workspace.name
                ),
            )
            .await
            .map(|out| {
                out.with_data(serde_json::json!({
                    "workspace": workspace.name,
                    "deleted": existing.id.to_string(),
                }))
            })
        }
        TagCommand::Merge { source, target } => {
            // Merging deletes the source tag, so the destructive
            // -y/prompt discipline applies.
            super::tty::confirm_destructive(
                &format!("merge `{source}` into `{target}` (the source tag is deleted)"),
                ctx.yes,
                &ctx.session,
            )?;
            let report = svc
                .merge(&reference, &TagRef::parse(source), &TagRef::parse(target))
                .await?;
            Ok(HandlerOutput::human(format!(
                "merged `{}` into `{}` ({} association(s) moved)\n",
                report.source.name, report.target.name, report.moved
            ))
            .with_data(super::json_value(&report)?))
        }
    }
}

/// Renders an attach/detach batch through the shared grammar; failures carry
/// their error as the detail line and drive the exit-13 policy.
fn render_tag_batch(
    inputs: &[String],
    outcomes: &rstore_application::BatchOutcome<()>,
    success_marker: &str,
    data: serde_json::Value,
) -> HandlerResult {
    // Outcomes come back in submission order; lines name the reference as
    // the user wrote it.
    let entries = inputs.iter().zip(outcomes.items()).map(|(input, item)| {
        if item.disposition.is_success() {
            super::report::BatchEntry {
                marker: success_marker.to_string(),
                label: input.clone(),
                detail: None,
            }
        } else {
            super::report::BatchEntry {
                marker: "failed".to_string(),
                label: input.clone(),
                detail: failure_message(item),
            }
        }
    });
    super::report::render_entries(
        data,
        None,
        entries,
        "re-run to retry failures",
        &super::report::BatchFacts {
            summary: Some(format!(
                "{} succeeded, {} failed",
                outcomes.succeeded(),
                outcomes.failed()
            )),
            interrupted: false,
            severity: outcomes.severity(),
        },
    )
}

/// The projected failure message of one batch item, if it failed.
fn failure_message(item: &rstore_application::ItemOutcome<()>) -> Option<String> {
    match &item.disposition {
        rstore_application::Disposition::Failed(f) => Some(f.message.clone()),
        _ => None,
    }
}

/// `-` expands to one reference per stdin line; returns the references as
/// written next to their expansions, in the same order.
async fn expand_uris(
    app: &rstore_application::ApplicationApi,
    uris: &[String],
) -> rstore_domain::Result<(Vec<String>, Vec<rstore_application::RemoteUri>)> {
    let inputs = super::tty::resolve_uris(
        uris,
        "pass at least one remote URI (or `-` to read them from stdin)",
    )?;
    let expanded = super::expand_remote_batch(app, &inputs).await?;
    Ok((inputs, expanded))
}
