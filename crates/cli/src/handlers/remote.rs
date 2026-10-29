//! Handlers for the live `ls` / `stat` inspection commands and the counted
//! `rm` batch removal: user-supplied references are expanded through the
//! registry grammar, then delegated to the transfer service's remote API.

use super::format::human_size;
use super::{Ctx, HandlerResult};
use crate::args::{LsArgs, RmArgs};
use crate::output::HandlerOutput;

/// `ls [REF]`: one remote directory page, or the registered buckets as browse
/// roots when no reference is given.
pub(super) async fn ls(ctx: &Ctx<'_>, args: &LsArgs) -> HandlerResult {
    let Some(reference) = &args.reference else {
        return list_roots(ctx).await;
    };
    let shown = reference.clone();
    let uri = super::expand_remote(ctx.app, reference).await?;
    let cursor = args
        .cursor
        .as_deref()
        .map(super::format::decode_opaque_token)
        .transpose()?;
    let page = ctx
        .app
        .transfer()
        .remote()
        .ls(&uri, cursor, args.limit, args.catalog)
        .await?;
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
    // The human heading shows what the user typed, not the expanded URI.
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

/// `ls` without a reference: the registered buckets, each a reference that
/// `ls` accepts next.
async fn list_roots(ctx: &Ctx<'_>) -> HandlerResult {
    let mut roots: Vec<(String, String)> = super::bucket_labels(ctx.app)
        .await?
        .into_iter()
        .map(|(id, label)| (label, id.to_string()))
        .collect();
    roots.sort();
    let mut table = super::format::Table::new(&["BUCKET"], &[super::format::Align::Left]);
    for (label, _) in &roots {
        table.row(vec![format!("{label}/")]);
    }
    let data: Vec<serde_json::Value> = roots
        .iter()
        .map(|(label, id)| serde_json::json!({ "bucket": label, "id": id }))
        .collect();
    let human = super::format::listing(
        "Registered buckets:",
        "No buckets registered. Add one with `rstore bucket add <bucket>`.",
        table,
    );
    Ok(HandlerOutput::human(human).with_data(serde_json::json!(data)))
}

/// `stat REF`: live object metadata, or the catalog entry with `--offline`.
pub(super) async fn stat(ctx: &Ctx<'_>, reference: &str, offline: bool) -> HandlerResult {
    if offline {
        return super::files::show(ctx, reference).await;
    }
    let uri = super::expand_remote(ctx.app, reference).await?;
    let meta = ctx.app.transfer().remote().stat(&uri).await?;
    let data = super::json_value(&meta)?;
    let mut fields = super::format::Fields::new();
    fields
        .header(reference)
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

/// Plan the removal, confirm the exact object count, then delete.
///
/// A single expansion feeds both the confirmation prompt and the run: the
/// plan is the exact object set, so "Delete N object(s)" and the executed
/// deletions cannot disagree, and a recursive prefix is listed once.
pub(super) async fn rm(ctx: &Ctx<'_>, args: &RmArgs) -> HandlerResult {
    let dry_run = args.dry_run;
    let inputs = super::tty::resolve_uris(&args.references, "rm needs at least one remote URI")?;
    let uris = super::expand_remote_batch(ctx.app, &inputs).await?;
    let remote = ctx.app.transfer().remote();
    let plan = remote.plan_removals(&uris, args.recursive).await?;
    if !dry_run {
        // Destructive: nothing is deleted without explicit consent. The
        // prompt names the references as the user wrote them.
        super::tty::confirm_destructive(
            &format!("Delete {} object(s) ({})", plan.count(), inputs.join(", ")),
            ctx.yes,
            &ctx.session,
        )?;
    }

    let report = remote.remove_planned(plan, dry_run, ctx.cancel).await?;
    let data = super::json_value(&report)?;
    let verb = if dry_run { "Would delete" } else { "Deleted" };
    let entries = report.outcome.items().iter().map(|item| {
        let show_detail = matches!(
            item.disposition,
            rstore_application::Disposition::Failed(_)
                | rstore_application::Disposition::Blocked(_)
        );
        super::report::BatchEntry {
            marker: super::report::remove_marker(&item.disposition).to_string(),
            label: item.target.clone(),
            detail: show_detail.then(|| item.detail.clone()).flatten(),
        }
    });
    let severity = report.outcome.severity();
    let out = super::report::render_entries(
        data,
        dry_run.then_some("(dry run — nothing deleted)"),
        entries,
        "completed deletions stand; re-run to remove the rest",
        &super::report::BatchFacts {
            summary: (!dry_run).then(|| {
                format!(
                    "{} object(s), {} not run, {} failed",
                    verb,
                    report.outcome.not_run(),
                    report.outcome.failed()
                )
            }),
            interrupted: report.is_interrupted(),
            severity,
        },
    )?;
    if report.is_interrupted() {
        return Ok(out);
    }
    if severity != rstore_application::Severity::AllOk {
        return Ok(out.with_warning(format!(
            "{} not run, {} failed — see per-item details",
            report.outcome.not_run(),
            report.outcome.failed()
        )));
    }
    Ok(out)
}
