//! Handlers for the workspace catalog: `find`, the offline `stat` view, and
//! `index …` runs and maintenance.
//!
//! Each handler renders one [`HandlerOutput`] carrying both the structured
//! data and the pre-rendered human text; destructive actions go through the
//! shared confirmation discipline before touching state.

use rstore_application::{
    BatchOutcome, BucketRef, Disposition, FileFilter, FileSort, IndexMode, IndexReport, IndexScope,
    PageRequest, SortField, SortOrder, TagRef, WorkspaceRef,
};
use rstore_domain::workspace::IndexRunState;
use rstore_domain::{Error, FileType, Workspace};

use super::{Ctx, HandlerResult};
use crate::args::{FindArgs, IndexCommand};
use crate::output::HandlerOutput;

/// `find [PATTERN]`: one filtered, sorted, paginated catalog query over the
/// workspace's buckets.
pub(super) async fn find(ctx: &Ctx<'_>, args: &FindArgs) -> HandlerResult {
    let workspace = ctx.workspace().await?;
    let reference = WorkspaceRef::Id(workspace.id);
    let tag_service = ctx.app.catalog().tags();
    let mut tags = Vec::with_capacity(args.tags.len());
    for name in &args.tags {
        tags.push(
            tag_service
                .resolve(&reference, &TagRef::parse(name))
                .await?
                .id,
        );
    }
    let filter = FileFilter {
        name_contains: args.pattern.clone(),
        path_contains: args.path.clone(),
        buckets: super::resolve_bucket_ids(ctx.app, &args.buckets).await?,
        tags,
        tag_mode_or: args.any_tag,
        untagged: args.untagged,
        file_type: args.file_type.as_deref().map(FileType::parse).transpose()?,
        extension: args
            .extension
            .as_deref()
            .map(|ext| ext.trim_start_matches('.').to_string()),
        ..Default::default()
    };
    let page = ctx
        .app
        .catalog()
        .files()
        .list(
            &reference,
            &filter,
            parse_sort(&args.sort)?,
            &PageRequest {
                limit: args.limit,
                after: args.after.clone(),
            },
        )
        .await?;
    let mut table = super::format::Table::new(
        &["NAME", "SIZE", "LOCATION", "STATE"],
        &[
            super::format::Align::Left,
            super::format::Align::Right,
            super::format::Align::Left,
            super::format::Align::Left,
        ],
    );
    for f in &page.items {
        table.row(vec![
            f.name.clone(),
            super::format::human_size(f.size),
            format!("{}/{}", f.connection, f.bucket),
            f.state.as_str().to_string(),
        ]);
    }
    let mut human = super::format::listing(
        &format!(
            "Files in `{}` ({} of {}):",
            workspace.name,
            page.items.len(),
            page.total
        ),
        &format!("No files match in `{}`.", workspace.name),
        table,
    );
    if let Some(cursor) = &page.next_cursor {
        human.push_str(&super::format::more("--after", cursor));
    }
    let data = serde_json::json!({
        "workspace": workspace.name,
        "total": page.total,
        "next_cursor": page.next_cursor,
        "items": super::json_value(&page.items)?,
    });
    Ok(HandlerOutput::human(human).with_data(data))
}

/// `stat --offline`: one catalog entry with tags and staleness, no network.
pub(super) async fn show(ctx: &Ctx<'_>, reference: &str) -> HandlerResult {
    let workspace = ctx.workspace().await?;
    let uri = super::expand_remote(ctx.app, reference).await?;
    let entry = ctx
        .app
        .catalog()
        .files()
        .show(&WorkspaceRef::Id(workspace.id), &uri)
        .await?;
    let mut fields = super::format::Fields::new();
    fields
        .header(&entry.uri)
        .push("bucket", &entry.bucket)
        .push("connection", &entry.connection)
        .push("size", super::format::human_size(entry.size))
        .push(
            "modified",
            rstore_domain::clock::iso8601(entry.last_modified),
        )
        .push(
            "confirmed",
            format!(
                "{} ({})",
                rstore_domain::clock::iso8601(entry.confirmed_at),
                entry.state.as_str()
            ),
        )
        .push("type", entry.content_type.as_deref().unwrap_or("-"))
        .push(
            "tags",
            if entry.tags.is_empty() {
                "-".to_string()
            } else {
                entry.tags.join(", ")
            },
        );
    Ok(HandlerOutput::human(fields.render()).with_data(super::json_value(&entry)?))
}

/// Parse a `field:dir` spec into a [`FileSort`], defaulting to descending
/// order when the direction is omitted.
///
/// # Errors
/// Returns [`Error::invalid_input`] for an unrecognized field or order.
fn parse_sort(spec: &str) -> Result<FileSort, Error> {
    let (field, order) = match spec.split_once(':') {
        Some((f, o)) => (f, o),
        None => (spec, "desc"),
    };
    let field = match field {
        "name" => SortField::Name,
        "size" => SortField::Size,
        "modified" => SortField::Modified,
        "type" => SortField::Type,
        "bucket" => SortField::Bucket,
        "connection" => SortField::Connection,
        other => {
            return Err(Error::invalid_input(format!(
                "unknown sort field `{other}` (name|size|modified|type|bucket|connection)"
            )))
        }
    };
    let order = match order {
        "asc" => SortOrder::Asc,
        "desc" => SortOrder::Desc,
        other => {
            return Err(Error::invalid_input(format!(
                "unknown sort order `{other}` (asc|desc)"
            )))
        }
    };
    Ok(FileSort { field, order })
}

/// Run an index subcommand: run, status, cancel, or gc.
///
/// `--rebuild` drops catalog rows and `gc` purges tombstones, so both ask for
/// confirmation unless `-y` was given.
pub(super) async fn index(ctx: &Ctx<'_>, action: &IndexCommand) -> HandlerResult {
    let svc = ctx.app.catalog().indexer();
    match action {
        IndexCommand::Run {
            buckets,
            all,
            refresh,
            rebuild,
        } => {
            let mode = if *rebuild {
                IndexMode::Rebuild
            } else if *refresh {
                IndexMode::Refresh
            } else {
                IndexMode::Resume
            };
            let (scope, described) = if !buckets.is_empty() {
                (
                    IndexScope::Buckets(buckets.iter().map(|b| BucketRef::parse(b)).collect()),
                    buckets.join(", "),
                )
            } else if *all {
                (IndexScope::All, "every index-enabled bucket".to_string())
            } else {
                let workspace: Workspace = ctx.workspace().await?;
                (
                    IndexScope::Workspace(workspace.id),
                    format!("the buckets of workspace `{}`", workspace.name),
                )
            };
            if mode == IndexMode::Rebuild {
                super::tty::confirm_destructive(
                    &format!(
                        "rebuild the index of {described} (catalog rows are dropped and \
                         rescanned; tags survive)"
                    ),
                    ctx.yes,
                    &ctx.session,
                )?;
            }
            let outcome = svc.run(&scope, mode, ctx.cancel).await?;
            render_runs(outcome, &described)
        }
        IndexCommand::Status => {
            let stats = svc.stats().await?;
            let status = svc.status().await?;
            let labels = super::bucket_labels(ctx.app).await?;
            let mut table = super::format::Table::new(
                &[
                    "BUCKET", "ENABLED", "OBJECTS", "STALE", "IGNORED", "LAST RUN",
                ],
                &[
                    super::format::Align::Left,
                    super::format::Align::Left,
                    super::format::Align::Right,
                    super::format::Align::Right,
                    super::format::Align::Right,
                    super::format::Align::Left,
                ],
            );
            for st in &status {
                let run = match &st.last_run {
                    Some(r) => {
                        format!("{} ({} objects seen)", r.state.as_str(), r.objects_seen)
                    }
                    None => "never indexed".to_string(),
                };
                table.row(vec![
                    labels
                        .get(&st.bucket)
                        .cloned()
                        .unwrap_or_else(|| st.bucket.to_string()),
                    st.index_enabled.to_string(),
                    st.object_count.to_string(),
                    st.stale_count.to_string(),
                    st.ignore_rules.to_string(),
                    run,
                ]);
            }
            let mut human = format!(
                "Catalog: {} objects, {} stale, {} tombstones, {} on disk\n",
                stats.object_rows,
                stats.stale_rows,
                stats.tombstone_rows,
                super::format::human_size(stats.db_bytes),
            );
            human.push_str(&super::format::listing(
                "Index status per bucket:",
                "No buckets registered.",
                table,
            ));
            let data = serde_json::json!({
                "stats": super::json_value(&stats)?,
                "buckets": super::json_value(&status)?,
            });
            Ok(HandlerOutput::human(human).with_data(data))
        }
        IndexCommand::Cancel { bucket } => {
            let run = svc.cancel(&BucketRef::parse(bucket)).await?;
            Ok(HandlerOutput::human(format!(
                "discarded the paused scan of {bucket}; indexed rows stay\n"
            ))
            .with_data(serde_json::json!({"bucket": bucket, "run": super::json_value(&run)?})))
        }
        IndexCommand::Gc { dry_run, retention } => {
            if !*dry_run {
                super::tty::confirm_destructive("purge expired tombstones", ctx.yes, &ctx.session)?;
            }
            let retention_override = match retention {
                Some(raw) => Some(parse_retention(raw)?),
                None => None,
            };
            let report = svc.gc(retention_override, *dry_run).await?;
            let data = super::json_value(&report)?;
            let human = format!(
                "{} {} tombstone(s) past retention ({}s); {} retained\n",
                if *dry_run { "would purge" } else { "purged" },
                report.expired,
                report.retention_seconds,
                report.retained,
            );
            Ok(HandlerOutput::human(human).with_data(data))
        }
    }
}

/// Render one line per scanned bucket through the shared batch grammar: a
/// paused bucket marks the run interrupted (exit 130), a failed one exits 13.
fn render_runs(outcome: BatchOutcome<IndexReport>, described: &str) -> HandlerResult {
    if outcome.is_empty() {
        return Ok(
            HandlerOutput::human(format!("No index-enabled bucket in {described}.\n"))
                .with_data(serde_json::json!([])),
        );
    }
    let interrupted = outcome
        .items()
        .iter()
        .filter_map(|item| item.value.as_ref())
        .any(|report| report.state == IndexRunState::Paused);
    let entries = outcome.items().iter().map(|item| match &item.value {
        Some(report) => super::report::BatchEntry {
            marker: if report.state == IndexRunState::Paused {
                "paused"
            } else {
                "ok"
            }
            .to_string(),
            label: item.target.clone(),
            detail: Some(format!(
                "seen {}  upserted {}  pruned {}  stale {}  revision {}",
                report.objects_seen,
                report.objects_upserted,
                report.objects_pruned,
                report.objects_stale,
                report.catalog_revision,
            )),
        },
        None => super::report::BatchEntry {
            marker: "failed".to_string(),
            label: item.target.clone(),
            detail: match &item.disposition {
                Disposition::Failed(failure) => Some(failure.message.clone()),
                _ => None,
            },
        },
    });
    super::report::render_entries(
        super::json_value(&outcome)?,
        None,
        entries,
        "re-run `rstore index run` to resume from the checkpoint",
        &super::report::BatchFacts {
            summary: Some(format!(
                "{} bucket(s) indexed, {} failed",
                outcome.succeeded(),
                outcome.failed()
            )),
            interrupted,
            severity: outcome.severity(),
        },
    )
}

/// Parse a retention span: plain seconds or `30m`/`1h`/`2d`/`1w` units,
/// bounded to a year. Unlike presign TTLs this bound may far exceed seven
/// days, because tombstone retention legitimately outlives short-lived URLs.
///
/// # Errors
/// Returns a domain error for malformed or out-of-range spans.
fn parse_retention(raw: &str) -> rstore_domain::Result<u64> {
    rstore_domain::duration::parse_span(
        raw,
        "smhdw",
        365 * 86_400,
        "retention (seconds, or 30m/1h/2d/1w)",
        "365d",
    )
}
