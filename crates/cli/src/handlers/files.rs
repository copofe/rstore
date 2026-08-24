//! Handlers for `index …`, `files …`, and catalog maintenance commands.
//!
//! Each handler renders one [`HandlerOutput`] carrying both the structured
//! data and the pre-rendered human text; destructive actions go through the
//! shared confirmation discipline before touching state.

use rstore_application::{BucketRef, WorkspaceRef};
use rstore_application::{FileFilter, FileSort, PageRequest, SortField, SortOrder};
use rstore_domain::Error;
use rstore_transfer::CancellationToken;

use super::HandlerResult;
use crate::args::{FilesCommand, IndexCommand};
use crate::output::HandlerOutput;

/// Run an index subcommand: start, stats, status, pause, resume, cancel,
/// refresh, or rebuild.
///
/// Rebuild drops catalog rows for the bucket and rescans, so it always asks
/// for confirmation unless `--yes` was given.
pub(super) async fn index(
    action: &IndexCommand,
    app: &rstore_application::ApplicationApi,
    cancel: &CancellationToken,
    session: &super::tty::Session,
) -> HandlerResult {
    let svc = app.catalog().indexer();
    match action {
        IndexCommand::Start { bucket } => {
            let report = svc.start(&BucketRef::parse(bucket), cancel).await?;
            render_report(report)
        }
        IndexCommand::Stats => {
            let stats = svc.stats().await?;
            let data = super::json_value(&stats)?;
            let human = format!(
                "Catalog stats:\n  objects    {}\n  stale      {}\n  tombstones {}\n  db size    {}\n",
                stats.object_rows,
                stats.stale_rows,
                stats.tombstone_rows,
                super::format::human_size(stats.db_bytes),
            );
            Ok(HandlerOutput::human(human).with_data(data))
        }
        IndexCommand::Status => {
            let status = svc.status().await?;
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
                    st.bucket.to_string(),
                    st.index_enabled.to_string(),
                    st.object_count.to_string(),
                    st.stale_count.to_string(),
                    st.ignore_rules.to_string(),
                    run,
                ]);
            }
            let human =
                super::format::listing("Index status per bucket:", "No buckets registered.", table);
            Ok(HandlerOutput::human(human).with_data(super::json_value(&status)?))
        }
        IndexCommand::Pause { bucket } => {
            let run = svc.pause(&BucketRef::parse(bucket)).await?;
            Ok(HandlerOutput::human(format!("index for {bucket} paused\n"))
                .with_data(serde_json::json!({"bucket": bucket, "run": super::json_value(&run)?})))
        }
        IndexCommand::Resume { bucket } => {
            // Resume reuses the start path through the persisted checkpoint,
            // keeping a single scan entry point with no service-level alias.
            let report = svc.start(&BucketRef::parse(bucket), cancel).await?;
            render_report(report)
        }
        IndexCommand::Cancel { bucket } => {
            let run = svc.cancel(&BucketRef::parse(bucket)).await?;
            Ok(HandlerOutput::human(format!(
                "index for {bucket} cancelled; completed data kept\n"
            ))
            .with_data(serde_json::json!({"bucket": bucket, "run": super::json_value(&run)?})))
        }
        IndexCommand::Refresh { bucket } => {
            let report = svc.refresh(&BucketRef::parse(bucket), cancel).await?;
            render_report(report)
        }
        IndexCommand::Rebuild { bucket, yes } => {
            super::tty::confirm_destructive(
                &format!("rebuild the index of {bucket} (catalog rows are dropped and rescanned; tags survive)"),
                *yes,
                session,
            )?;
            let report = svc.rebuild(&BucketRef::parse(bucket), cancel).await?;
            render_report(report)
        }
    }
}

/// Render an index-run summary line set from the application's report.
fn render_report(report: rstore_application::IndexReport) -> HandlerResult {
    let human = format!(
        "index run {} — {}\n  seen {}  upserted {}  pruned {}  stale {}\n  catalog revision {}\n",
        report.run_id,
        report.state.as_str(),
        report.objects_seen,
        report.objects_upserted,
        report.objects_pruned,
        report.objects_stale,
        report.catalog_revision,
    );
    Ok(HandlerOutput::human(human).with_data(super::json_value(&report)?))
}

/// Run a files subcommand: list, search, or show a catalog entry.
pub(super) async fn files(
    action: &FilesCommand,
    app: &rstore_application::ApplicationApi,
    _session: &super::tty::Session,
) -> HandlerResult {
    let svc = app.catalog().files();
    match action {
        FilesCommand::List {
            workspace,
            path,
            buckets,
            sort,
            limit,
            after,
            untagged,
        } => {
            let filter = FileFilter {
                path_contains: path.clone(),
                untagged: *untagged,
                buckets: super::resolve_bucket_ids(app, buckets).await?,
                ..Default::default()
            };
            let sort = parse_sort(sort)?;
            list_files(
                svc.as_ref(),
                workspace,
                &filter,
                sort,
                *limit,
                after.clone(),
            )
            .await
        }
        FilesCommand::Search {
            workspace,
            pattern,
            sort,
            limit,
        } => {
            let filter = FileFilter {
                name_contains: Some(pattern.clone()),
                ..Default::default()
            };
            let sort = parse_sort(sort)?;
            list_files(svc.as_ref(), workspace, &filter, sort, *limit, None).await
        }
        FilesCommand::Show { uri, workspace } => {
            let uri = super::expand_remote(app, uri).await?;
            let entry = svc.show(&WorkspaceRef::parse(workspace), &uri).await?;
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
    }
}

#[allow(clippy::too_many_arguments)]
async fn list_files(
    svc: &dyn rstore_application::FilesService,
    workspace: &str,
    filter: &FileFilter,
    sort: FileSort,
    limit: u32,
    after: Option<String>,
) -> HandlerResult {
    let page = svc
        .list(
            &WorkspaceRef::parse(workspace),
            filter,
            sort,
            &PageRequest {
                limit,
                after: after.clone(),
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
            "Files in `{workspace}` ({} of {}):",
            page.items.len(),
            page.total
        ),
        &format!("No files match in `{workspace}`."),
        table,
    );
    if let Some(cursor) = &page.next_cursor {
        human.push_str(&super::format::more("--after", cursor));
    }
    let data = serde_json::json!({
        "total": page.total,
        "next_cursor": page.next_cursor,
        "items": super::json_value(&page.items)?,
    });
    Ok(HandlerOutput::human(human).with_data(data))
}

/// Parse a `field:dir` spec into a [`FileSort`], defaulting to descending
/// order when the direction is omitted. Fields whose sort needs extra context
/// beyond the CLI surface are rejected like unknown names.
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

/// Run catalog maintenance; currently only tombstone garbage collection.
///
/// Purging tombstones is destructive, so a real (non-dry-run) run goes
/// through the same `--yes`/prompt confirmation as every other destructive
/// command.
pub(super) async fn catalog(
    action: &crate::args::CatalogCommand,
    app: &rstore_application::ApplicationApi,
    session: &super::tty::Session,
) -> HandlerResult {
    match action {
        crate::args::CatalogCommand::Gc {
            dry_run,
            retention,
            yes,
        } => {
            if !*dry_run {
                super::tty::confirm_destructive("purge expired tombstones", *yes, session)?;
            }
            let retention_override = match retention {
                Some(raw) => Some(parse_retention(raw)?),
                None => None,
            };
            let report = app
                .catalog()
                .indexer()
                .gc(retention_override, *dry_run)
                .await?;
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
