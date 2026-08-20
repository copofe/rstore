//! `config show` and `doctor` handlers (DATA-001 / OBS-004).

use super::HandlerResult;
use crate::output::HandlerOutput;
use rstore_platform::AppDirs;

// --- config show ----------------------------------------------------------

pub(super) fn config_show(dirs: &AppDirs) -> HandlerResult {
    let data = super::json_value(dirs)?;
    let mut fields = super::format::Fields::new();
    fields
        .header("Application directories:")
        .push("config", dirs.config_dir.display())
        .push("data", dirs.data_dir.display())
        .push("cache", dirs.cache_dir.display())
        .push("log", dirs.log_dir.display())
        .push("db", dirs.db_path.display());
    Ok(HandlerOutput::human(fields.render()).with_data(data))
}

// --- doctor ---------------------------------------------------------------

pub(super) async fn doctor(
    app: &rstore_application::ApplicationApi,
    dirs: &AppDirs,
) -> HandlerResult {
    let report = app
        .diagnostics()
        .health()
        .health(&dirs.db_path, app.diagnostics().storage_adapter())
        .await;

    let data = super::json_value(&report)?;

    let mut fields = super::format::Fields::new();
    fields
        .header("rstore doctor")
        .push("outbox pending", report.pending_outbox)
        .push("repairs pending", report.pending_repairs)
        .push(
            "connections",
            format!(
                "{} ({} untested)",
                report.connection_count, report.untested_connections
            ),
        )
        .push("database", &report.db_path)
        .push("adapter", report.storage_adapter)
        .push(
            "credentials",
            format!(
                "{} env ref(s), {} file ref(s)",
                report.credentials.env_refs, report.credentials.file_refs
            ),
        )
        .push("tls default", "on")
        .push("telemetry", "disabled");
    let mut out = HandlerOutput::human(fields.render()).with_data(data);

    if report.pending_repairs > 0 {
        out = out.with_warning(format!(
            "{} repair item(s) require attention — inspect with `job list` or `doctor`",
            report.pending_repairs
        ));
    }
    if report.interrupted_jobs > 0 {
        out = out.with_warning(format!(
            "{} interrupted job(s) — re-run the transfer or `task resume`",
            report.interrupted_jobs
        ));
    }
    if report.credentials.file_refs > 0 {
        out = out.with_warning(
            "credentials in the config file are plaintext at rest (0600); consider env vars for stricter secrecy",
        );
    }
    Ok(out)
}

// --- url (LINK-) -----------------------------------------------------------

pub(super) async fn url(
    args: &crate::args::UrlArgs,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    // The shared multi-URI entry: `-` reads one URI per stdin line like
    // every other batch command. (The human output below stays the
    // documented exception to the batch grammar: one pipeable link per
    // line — the exit-13 policy still rides `with_partial_failure`.)
    let uris = super::tty::resolve_uris(&args.uris, "url needs at least one remote URI")?;
    let uris = super::expand_remote_batch(app, &uris).await?;
    let format = rstore_application::LinkFormat::parse(&args.format)?;
    let ttl = match &args.ttl {
        Some(raw) => Some(rstore_application::parse_ttl(raw)?),
        None => None,
    };
    let request = rstore_application::LinkRequest {
        format,
        signed: args.signed,
        ttl_seconds: ttl,
    };
    let outcomes = app.access().links().generate(&uris, &request).await;

    let mut human = String::new();
    let mut failed = 0usize;
    let mut links = Vec::with_capacity(outcomes.len());
    for outcome in &outcomes {
        match (&outcome.link, &outcome.error) {
            (Some(link), _) => {
                human.push_str(&link.requested);
                human.push('\n');
                links.push(super::json_value(link)?);
            }
            (None, Some(err)) => {
                failed += 1;
                human.push_str(&format!("error: {}: {}\n", outcome.uri, err));
            }
            (None, None) => unreachable!("an outcome carries a link or an error"),
        }
    }
    let data = serde_json::json!({ "links": links, "failed": failed });
    Ok(super::report::with_partial_failure(
        HandlerOutput::human(human).with_data(data),
        failed,
        outcomes.len() - failed,
    ))
}

// --- catalog gc (IDX-020) ---------------------------------------------------

pub(super) async fn catalog(
    action: &crate::args::CatalogCommand,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    match action {
        crate::args::CatalogCommand::Gc { dry_run, retention } => {
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
/// bounded to a year (the retention setting, TAG-026 — unlike presign TTLs
/// this can far exceed seven days).
fn parse_retention(raw: &str) -> rstore_domain::Result<u64> {
    rstore_domain::duration::parse_span(
        raw,
        "smhdw",
        365 * 86_400,
        "retention (seconds, or 30m/1h/2d/1w)",
        "365d",
    )
}

// --- config export / import (DATA-015/016) ----------------------------------

pub(super) async fn config_export(
    out: Option<&str>,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    let bundle = app.config().transfer().export().await?;
    let json = serde_json::to_string_pretty(&bundle)
        .map_err(|e| rstore_domain::Error::database("serialise config bundle").with_source(e))?;
    match out {
        Some(path) => {
            std::fs::write(path, format!("{json}\n")).map_err(|e| {
                rstore_domain::Error::invalid_input("write export file").with_source(e)
            })?;
            Ok(HandlerOutput::human(format!("exported to {path}\n"))
                .with_data(serde_json::json!({"path": path, "connections": bundle.connections.len(), "buckets": bundle.buckets.len()})))
        }
        None => Ok(HandlerOutput::human(json).with_data(super::json_value(&bundle)?)),
    }
}

pub(super) async fn config_import(
    path: &str,
    dry_run: bool,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| rstore_domain::Error::invalid_input("read bundle").with_source(e))?;
    let bundle: rstore_application::ConfigBundle = serde_json::from_str(&raw)
        .map_err(|e| rstore_domain::Error::invalid_input("parse bundle").with_source(e))?;
    let report = app.config().transfer().import(&bundle, dry_run).await?;
    let data = super::json_value(&report)?;
    let human = format!(
        "{}{} connection(s) ({} skipped), {} bucket(s) ({} skipped){}\n",
        if dry_run {
            "would import "
        } else {
            "imported "
        },
        report.connections_added,
        report.connections_skipped,
        report.buckets_added,
        report.buckets_skipped,
        if dry_run { " — dry run" } else { "" },
    );
    Ok(HandlerOutput::human(human).with_data(data))
}
