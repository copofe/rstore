//! `remote …` and `rm` handlers (REM- / DEL-).

use super::format::human_size;
use super::HandlerResult;
use crate::output::HandlerOutput;
// --- remote ----------------------------------------------------------------

pub(super) async fn remote(
    action: &crate::args::RemoteCommand,
    app: &rstore_application::ApplicationApi,
    _session: &super::tty::Session,
) -> HandlerResult {
    let svc = app.transfer().remote();
    match action {
        crate::args::RemoteCommand::Ls {
            uri,
            cursor,
            limit,
            update_catalog,
        } => {
            let shown = uri.clone();
            let uri = super::expand_remote(app, uri).await?;
            let cursor = cursor
                .as_deref()
                .map(super::format::decode_opaque_token)
                .transpose()?;
            let page = svc.ls(&uri, cursor, *limit, *update_catalog).await?;
            let next_cursor = page
                .next_cursor
                .as_deref()
                .map(super::format::encode_opaque_token);
            let mut data = super::json_value(&page)?;
            if let Some(cursor) = &next_cursor {
                data["next_cursor"] = serde_json::Value::String(cursor.clone());
            }
            let mut table = super::format::Table::new(
                &["TYPE", "SIZE", "NAME"],
                &[
                    super::format::Align::Left,
                    super::format::Align::Right,
                    super::format::Align::Left,
                ],
            );
            for entry in &page.entries {
                table.row(vec![
                    if entry.is_dir { "DIR" } else { "" }.into(),
                    if entry.is_dir {
                        "-".to_string()
                    } else {
                        human_size(entry.size)
                    },
                    entry.name.clone(),
                ]);
            }
            let human = super::format::dir_listing(
                shown.trim_end_matches('/'),
                "  (empty)",
                next_cursor.as_deref().map(|cursor| ("--cursor", cursor)),
                table,
            );
            let mut out = HandlerOutput::human(human).with_data(data);
            if page.catalog_updated {
                out = out.with_warning("catalog updated from this listing");
            }
            Ok(out)
        }
        crate::args::RemoteCommand::Stat { uri } => {
            let shown = uri.clone();
            let uri = super::expand_remote(app, uri).await?;
            let meta = svc.stat(&uri).await?;
            let data = super::json_value(&meta)?;
            let mut fields = super::format::Fields::new();
            fields
                .header(&shown)
                .push(
                    "size",
                    format!("{} ({} bytes)", human_size(meta.size), meta.size),
                )
                .push(
                    "last modified",
                    rstore_domain::clock::iso8601(meta.last_modified),
                )
                .push_opt("etag", meta.etag.as_deref())
                .push_opt("content type", meta.content_type.as_deref())
                .push_opt("storage class", meta.storage_class.as_deref())
                .push_opt("version", meta.version_id.as_deref());
            Ok(HandlerOutput::human(fields.render()).with_data(data))
        }
    }
}

// --- rm --------------------------------------------------------------------

pub(super) async fn rm(
    app: &rstore_application::ApplicationApi,
    uris: &[String],
    recursive: bool,
    dry_run: bool,
    yes: bool,
    cancel: &rstore_transfer::CancellationToken,
    session: &super::tty::Session,
) -> HandlerResult {
    let uris = super::tty::resolve_uris(uris, "rm needs at least one remote URI")?;
    let uris = super::expand_remote_batch(app, &uris).await?;
    // One expansion serves the confirmation count and the run (DEL-003): the
    // plan is the exact object set, so "Delete N object(s)" and the executed
    // deletions cannot disagree, and a recursive prefix is listed once.
    let plan = app
        .transfer()
        .remote()
        .plan_removals(&uris, recursive)
        .await?;
    if !dry_run {
        // SEC-006: destructive operations need explicit confirmation.
        super::tty::confirm_destructive(
            &format!("Delete {} object(s) ({})", plan.count(), uris.join(", ")),
            yes,
            session,
        )?;
    }

    let report = app
        .transfer()
        .remote()
        .remove_planned(plan, dry_run, cancel)
        .await?;
    let data = super::json_value(&report)?;
    let verb = if dry_run { "Would delete" } else { "Deleted" };
    // Only failure-shaped outcomes carry their detail into the listing.
    let entries = report.items.iter().map(|item| super::report::BatchEntry {
        marker: super::report::remove_marker(item.outcome).to_string(),
        label: item.uri.clone(),
        detail: match item.outcome {
            rstore_application::RemoveOutcome::Failed
            | rstore_application::RemoveOutcome::GuardBlocked => item.detail.clone(),
            _ => None,
        },
    });
    let out = super::report::render_entries(
        data,
        dry_run.then_some("(dry run — nothing deleted)"),
        entries,
        "completed deletions stand; re-run to remove the rest",
        &super::report::BatchFacts {
            summary: (!dry_run).then(|| {
                format!(
                    "{} object(s), {} blocked, {} failed",
                    verb, report.blocked, report.failed
                )
            }),
            interrupted: report.is_interrupted(),
            failed: report.failed,
            succeeded: report.deleted,
        },
    )?;
    if report.is_interrupted() {
        return Ok(out);
    }
    if report.failed > 0 || report.blocked > 0 {
        return Ok(out.with_warning(format!(
            "{} blocked, {} failed — see per-item details",
            report.blocked, report.failed
        )));
    }
    Ok(out)
}
