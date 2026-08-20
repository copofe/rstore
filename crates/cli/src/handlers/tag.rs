//! `tag …` handlers (TAG-).

use rstore_application::{CreateTagInput, TagRef, UpdateTagInput, WorkspaceRef};

use super::HandlerResult;
use crate::args::TagCommand;
use crate::output::HandlerOutput;

pub(super) async fn tag(
    action: &TagCommand,
    app: &rstore_application::ApplicationApi,
    session: &super::tty::Session,
) -> HandlerResult {
    let svc = app.catalog().tags();
    match action {
        TagCommand::Create {
            workspace,
            name,
            color,
            description,
        } => {
            let tag = svc
                .create(CreateTagInput {
                    workspace: WorkspaceRef::parse(workspace),
                    name: name.clone(),
                    color: color.clone(),
                    description: description.clone(),
                })
                .await?;
            Ok(HandlerOutput::human(format!(
                "tag `{}` created in `{workspace}` ({})\n",
                tag.name, tag.id
            ))
            .with_data(super::json_value(&tag)?))
        }
        TagCommand::List { workspace } => {
            let list = svc.list(&WorkspaceRef::parse(workspace)).await?;
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
                &format!("Tags in `{workspace}`:"),
                &format!("No tags in `{workspace}`."),
                table,
            );
            Ok(HandlerOutput::human(human).with_data(super::json_value(&list)?))
        }
        TagCommand::Edit {
            workspace,
            tag,
            name,
            color,
            description,
        } => {
            let updated = svc
                .edit(
                    &WorkspaceRef::parse(workspace),
                    &TagRef::parse(tag),
                    UpdateTagInput {
                        name: name.clone(),
                        color: color.clone(),
                        description: description.clone(),
                    },
                )
                .await?;
            Ok(
                HandlerOutput::human(format!("tag `{}` updated\n", updated.name))
                    .with_data(super::json_value(&updated)?),
            )
        }
        TagCommand::Remove {
            workspace,
            tag,
            yes,
        } => {
            let reference = WorkspaceRef::parse(workspace);
            let tag_ref = TagRef::parse(tag);
            let existing = svc.resolve(&reference, &tag_ref).await?;
            super::tty::remove_flow(
                format!("remove tag `{}` from `{}`", existing.name, workspace),
                *yes,
                session,
                svc.remove(&reference, &tag_ref),
                format!("tag `{}` removed from `{}`\n", existing.name, workspace),
            )
            .await
            .map(|out| {
                out.with_data(
                    serde_json::json!({"workspace": workspace, "removed": existing.id.to_string()}),
                )
            })
        }
        TagCommand::Merge {
            workspace,
            source,
            target,
        } => {
            let source_ref = TagRef::parse(source);
            let target_ref = TagRef::parse(target);
            let report = svc
                .merge(&WorkspaceRef::parse(workspace), &source_ref, &target_ref)
                .await?;
            Ok(HandlerOutput::human(format!(
                "merged `{}` into `{}` ({} association(s) moved)\n",
                report.source.name, report.target.name, report.moved
            ))
            .with_data(super::json_value(&report)?))
        }
        TagCommand::Add {
            workspace,
            uris,
            tags,
            replace,
        } => {
            let uris = expand_uris(app, uris).await?;
            let outcomes = svc
                .attach_many(&WorkspaceRef::parse(workspace), &uris, tags, *replace)
                .await?;
            let data = serde_json::json!({
                "outcomes": super::json_value(&outcomes)?,
                "tags": tags,
                "replace": replace,
            });
            render_tag_batch(&outcomes, "tagged", data)
        }
        TagCommand::Detach {
            workspace,
            uris,
            tags,
        } => {
            let uris = expand_uris(app, uris).await?;
            let outcomes = svc
                .detach_many(&WorkspaceRef::parse(workspace), &uris, tags)
                .await?;
            let data = serde_json::json!({
                "outcomes": super::json_value(&outcomes)?,
                "tags": tags,
            });
            render_tag_batch(&outcomes, "detached", data)
        }
    }
}

fn render_tag_batch(
    outcomes: &[rstore_application::TagBatchOutcome],
    success_marker: &str,
    data: serde_json::Value,
) -> HandlerResult {
    let succeeded = outcomes.iter().filter(|o| o.succeeded()).count();
    let failed = outcomes.len() - succeeded;
    let entries = outcomes.iter().map(|outcome| {
        if outcome.succeeded() {
            super::report::BatchEntry {
                marker: success_marker.to_string(),
                label: outcome.uri.clone(),
                detail: None,
            }
        } else {
            super::report::BatchEntry {
                marker: "failed".to_string(),
                label: outcome.uri.clone(),
                detail: outcome.error.clone(),
            }
        }
    });
    super::report::render_entries(
        data,
        None,
        entries,
        "re-run to retry failures",
        &super::report::BatchFacts {
            summary: Some(format!("{succeeded} succeeded, {failed} failed")),
            interrupted: false,
            failed,
            succeeded,
        },
    )
}

/// `-` expands to one URI per stdin line (TAG-011/012); anything else passes
/// through untouched.
async fn expand_uris(
    app: &rstore_application::ApplicationApi,
    uris: &[String],
) -> rstore_domain::Result<Vec<String>> {
    let uris = super::tty::resolve_uris(
        uris,
        "pass at least one remote URI (or `-` to read them from stdin)",
    )?;
    super::expand_remote_batch(app, &uris).await
}
