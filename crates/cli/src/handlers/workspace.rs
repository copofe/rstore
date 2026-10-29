//! `workspace …` handlers (WSP-). Subcommands without a workspace argument
//! act on the workspace from `-w`, else the current one.

use rstore_application::{BucketRef, CreateWorkspaceInput, UpdateWorkspaceInput, WorkspaceRef};
use rstore_domain::{Bucket, Workspace};

use super::{Ctx, HandlerResult};
use crate::args::WorkspaceCommand;
use crate::output::HandlerOutput;

pub(super) async fn workspace(ctx: &Ctx<'_>, action: &WorkspaceCommand) -> HandlerResult {
    let svc = ctx.app.catalog().workspaces();
    match action {
        WorkspaceCommand::List => {
            let list = svc.list().await?;
            let data: Vec<serde_json::Value> = list
                .iter()
                .map(|w| {
                    serde_json::json!({
                        "id": w.workspace.id.to_string(),
                        "name": w.workspace.name,
                        "description": w.workspace.description,
                        "bucket_count": w.bucket_count,
                        "current": w.current,
                    })
                })
                .collect();
            let mut table = super::format::Table::new(
                &["", "NAME", "BUCKETS", "ID"],
                &[
                    super::format::Align::Left,
                    super::format::Align::Left,
                    super::format::Align::Left,
                    super::format::Align::Left,
                ],
            );
            for w in &list {
                table.row(vec![
                    if w.current { "*" } else { "" }.to_string(),
                    w.workspace.name.clone(),
                    format!("{} buckets", w.bucket_count),
                    w.workspace.id.to_string(),
                ]);
            }
            let human = super::format::listing(
                "Workspaces:",
                "No workspaces. Registering a bucket creates `default`, or run \
                 `rstore workspace create <name>`.",
                table,
            );
            Ok(HandlerOutput::human(human).with_data(serde_json::json!(data)))
        }
        WorkspaceCommand::Use { workspace } => {
            let ws = svc.select(&WorkspaceRef::parse(workspace)).await?;
            Ok(
                HandlerOutput::human(format!("now using workspace `{}`\n", ws.name))
                    .with_data(super::json_value(&ws)?),
            )
        }
        WorkspaceCommand::Create { name, description } => {
            let ws = svc
                .create(CreateWorkspaceInput {
                    name: name.clone(),
                    description: description.clone(),
                })
                .await?;
            let current = svc.current().await?.is_some_and(|c| c.id == ws.id);
            let note = if current { " — now current" } else { "" };
            Ok(HandlerOutput::human(format!(
                "workspace `{}` created ({}){note}\n",
                ws.name, ws.id
            ))
            .with_data(super::merge_field(
                super::json_value(&ws)?,
                "current",
                Some(serde_json::Value::Bool(current)),
            )?))
        }
        WorkspaceCommand::Show => {
            let target = ctx.workspace().await?;
            let detail = svc.show(&WorkspaceRef::Id(target.id)).await?;
            let labels = super::bucket_labels(ctx.app).await?;
            let names: Vec<String> = detail
                .buckets
                .iter()
                .map(|id| labels.get(id).cloned().unwrap_or_else(|| id.to_string()))
                .collect();
            let mut fields = super::format::Fields::new();
            fields
                .header(format!(
                    "workspace `{}` ({})",
                    detail.workspace.name, detail.workspace.id
                ))
                .push("buckets", detail.buckets.len())
                .push("objects", detail.object_count);
            if let Some(d) = &detail.workspace.description {
                fields.push("description", d);
            }
            let mut human = fields.render();
            for name in &names {
                human.push_str(&format!("  - {name}\n"));
            }
            let data = serde_json::json!({
                "id": detail.workspace.id.to_string(),
                "name": detail.workspace.name,
                "description": detail.workspace.description,
                "buckets": detail.buckets.iter().map(|b| b.to_string()).collect::<Vec<_>>(),
                "bucket_names": names,
                "object_count": detail.object_count,
            });
            Ok(HandlerOutput::human(human).with_data(data))
        }
        WorkspaceCommand::Edit { name, description } => {
            let target = ctx.workspace().await?;
            let ws = svc
                .update(
                    &WorkspaceRef::Id(target.id),
                    UpdateWorkspaceInput {
                        name: name.clone(),
                        description: description.clone().map(|d| {
                            if d.is_empty() {
                                None
                            } else {
                                Some(d)
                            }
                        }),
                    },
                )
                .await?;
            Ok(
                HandlerOutput::human(format!("workspace `{}` updated\n", ws.name))
                    .with_data(super::json_value(&ws)?),
            )
        }
        WorkspaceCommand::Delete { workspace } => {
            let reference = WorkspaceRef::parse(workspace);
            let ws = svc.resolve(&reference).await?;
            super::tty::remove_flow(
                format!(
                    "delete workspace `{}` (buckets and remote data are kept)",
                    ws.name
                ),
                ctx.yes,
                &ctx.session,
                svc.remove(&reference),
                format!("workspace `{}` deleted; buckets untouched\n", ws.name),
            )
            .await
            .map(|out| out.with_data(serde_json::json!({"deleted": ws.id.to_string()})))
        }
        WorkspaceCommand::Add { buckets } => {
            let target = ctx.workspace().await?;
            let resolved = resolve_all(ctx, buckets).await?;
            for bucket in &resolved {
                svc.add_bucket(&WorkspaceRef::Id(target.id), &BucketRef::Id(bucket.id))
                    .await?;
            }
            membership_output(&target, buckets, "added to")
        }
        WorkspaceCommand::Remove { buckets } => {
            let target = ctx.workspace().await?;
            let resolved = resolve_all(ctx, buckets).await?;
            for bucket in &resolved {
                svc.remove_bucket(&WorkspaceRef::Id(target.id), &BucketRef::Id(bucket.id))
                    .await?;
            }
            membership_output(&target, buckets, "removed from")
        }
    }
}

/// Resolve every bucket reference before any membership changes, so a typo
/// leaves the workspace untouched.
async fn resolve_all(ctx: &Ctx<'_>, references: &[String]) -> rstore_domain::Result<Vec<Bucket>> {
    let buckets = ctx.app.registry().buckets();
    let mut resolved = Vec::with_capacity(references.len());
    for reference in references {
        resolved.push(buckets.resolve(&BucketRef::parse(reference)).await?);
    }
    Ok(resolved)
}

fn membership_output(workspace: &Workspace, buckets: &[String], verb: &str) -> HandlerResult {
    let human: String = buckets
        .iter()
        .map(|bucket| format!("bucket `{bucket}` {verb} `{}`\n", workspace.name))
        .collect();
    Ok(HandlerOutput::human(human)
        .with_data(serde_json::json!({"workspace": workspace.name, "buckets": buckets})))
}
