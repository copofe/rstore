//! `workspace …` handlers (WSP-/IGN-).

use rstore_application::{BucketRef, CreateWorkspaceInput, UpdateWorkspaceInput, WorkspaceRef};

use super::HandlerResult;
use crate::args::{IgnoreCommand, WorkspaceBucketCommand, WorkspaceCommand};
use crate::output::HandlerOutput;

pub(super) async fn workspace(
    action: &WorkspaceCommand,
    app: &rstore_application::ApplicationApi,
    session: &super::tty::Session,
) -> HandlerResult {
    let svc = app.catalog().workspaces();
    match action {
        WorkspaceCommand::Create { name, description } => {
            let ws = svc
                .create(CreateWorkspaceInput {
                    name: name.clone(),
                    description: description.clone(),
                })
                .await?;
            Ok(
                HandlerOutput::human(format!("workspace `{}` created ({})\n", ws.name, ws.id))
                    .with_data(super::json_value(&ws)?),
            )
        }
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
                    })
                })
                .collect();
            let mut table = super::format::Table::new(
                &["NAME", "BUCKETS", "ID"],
                &[
                    super::format::Align::Left,
                    super::format::Align::Left,
                    super::format::Align::Left,
                ],
            );
            for w in &list {
                table.row(vec![
                    w.workspace.name.clone(),
                    format!("{} buckets", w.bucket_count),
                    w.workspace.id.to_string(),
                ]);
            }
            let human = super::format::listing(
                "Workspaces:",
                "No workspaces. Create one with `rstore workspace create <name>`.",
                table,
            );
            Ok(HandlerOutput::human(human).with_data(serde_json::json!(data)))
        }
        WorkspaceCommand::Show { reference } => {
            let detail = svc.show(&WorkspaceRef::parse(reference)).await?;
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
            for b in &detail.buckets {
                human.push_str(&format!("  - {b}\n"));
            }
            let data = serde_json::json!({
                "id": detail.workspace.id.to_string(),
                "name": detail.workspace.name,
                "description": detail.workspace.description,
                "buckets": detail.buckets.iter().map(|b| b.to_string()).collect::<Vec<_>>(),
                "object_count": detail.object_count,
            });
            Ok(HandlerOutput::human(human).with_data(data))
        }
        WorkspaceCommand::Edit {
            reference,
            name,
            description,
        } => {
            let ws = svc
                .edit(
                    &WorkspaceRef::parse(reference),
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
        WorkspaceCommand::Remove { reference, yes } => {
            let reference = WorkspaceRef::parse(reference);
            let ws = svc.resolve(&reference).await?;
            super::tty::remove_flow(
                format!(
                    "remove workspace `{}` (buckets and remote data are kept)",
                    ws.name
                ),
                *yes,
                session,
                svc.remove(&reference),
                format!("workspace `{}` removed; buckets untouched\n", ws.name),
            )
            .await
            .map(|out| out.with_data(serde_json::json!({"removed": ws.id.to_string()})))
        }
        WorkspaceCommand::Bucket { action } => bucket_action(svc.as_ref(), action, app).await,
    }
}

async fn bucket_action(
    svc: &dyn rstore_application::WorkspaceService,
    action: &WorkspaceBucketCommand,
    _app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    match action {
        WorkspaceBucketCommand::Add { workspace, bucket } => {
            let id = svc
                .add_bucket(&WorkspaceRef::parse(workspace), &BucketRef::parse(bucket))
                .await?;
            Ok(
                HandlerOutput::human(format!("bucket {id} added to `{workspace}`\n")).with_data(
                    serde_json::json!({"workspace": workspace, "bucket": id.to_string()}),
                ),
            )
        }
        WorkspaceBucketCommand::Remove { workspace, bucket } => {
            svc.remove_bucket(&WorkspaceRef::parse(workspace), &BucketRef::parse(bucket))
                .await?;
            Ok(
                HandlerOutput::human(format!("bucket `{bucket}` removed from `{workspace}`\n"))
                    .with_data(serde_json::json!({"workspace": workspace, "bucket": bucket})),
            )
        }
        WorkspaceBucketCommand::List { workspace } => {
            let detail = svc.show(&WorkspaceRef::parse(workspace)).await?;
            let data: Vec<String> = detail.buckets.iter().map(|b| b.to_string()).collect();
            let mut table =
                super::format::Table::new(&["BUCKET ID"], &[super::format::Align::Left]);
            for b in &detail.buckets {
                table.row(vec![b.to_string()]);
            }
            let human = super::format::listing(
                &format!("Buckets in `{workspace}`:"),
                &format!("workspace `{workspace}` has no buckets"),
                table,
            );
            Ok(HandlerOutput::human(human).with_data(serde_json::json!(data)))
        }
    }
}

pub(super) async fn ignore(
    action: &IgnoreCommand,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    let svc = app.catalog().workspaces();
    match action {
        IgnoreCommand::Add { bucket, directory } => {
            let rule = svc.ignore_add(&BucketRef::parse(bucket), directory).await?;
            Ok(
                HandlerOutput::human(format!("ignoring `{}` in {bucket}\n", rule.directory))
                    .with_data(super::json_value(&rule)?),
            )
        }
        IgnoreCommand::List { bucket } => {
            let rules = svc.ignore_list(&BucketRef::parse(bucket)).await?;
            let mut table =
                super::format::Table::new(&["DIRECTORY"], &[super::format::Align::Left]);
            for r in &rules {
                table.row(vec![r.directory.clone()]);
            }
            let human = super::format::listing(
                &format!("Ignore rules for {bucket}:"),
                &format!("no ignore rules for {bucket}"),
                table,
            );
            Ok(HandlerOutput::human(human).with_data(super::json_value(&rules)?))
        }
        IgnoreCommand::Remove { bucket, directory } => {
            svc.ignore_remove(&BucketRef::parse(bucket), directory)
                .await?;
            Ok(HandlerOutput::human(format!(
                "ignore rule removed for `{directory}` in {bucket}\n"
            ))
            .with_data(serde_json::json!({"bucket": bucket, "directory": directory})))
        }
    }
}
