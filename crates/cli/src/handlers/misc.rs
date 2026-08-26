//! Handlers for configuration inspection and transfer (`config show`,
//! `config export`, `config import`) plus environment diagnostics (`doctor`).

use super::HandlerResult;
use crate::output::HandlerOutput;
use rstore_platform::AppDirs;

/// Render the resolved application directories as a labeled detail view.
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

/// Report environment health: pending outbox and repair items, connection
/// counts, database location, and storage adapter.
///
/// Pending repairs and interrupted jobs are surfaced as warnings on top of
/// the detail view so they cannot scroll by unnoticed.
pub(super) async fn doctor(
    app: &rstore_application::ApplicationApi,
    dirs: &AppDirs,
) -> HandlerResult {
    let report = app.diagnostics().health(&dirs.db_path).await;

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
        .push("tls default", "on")
        .push("telemetry", "disabled");
    let mut out = HandlerOutput::human(fields.render()).with_data(data);

    if report.pending_repairs > 0 {
        out = out.with_warning(format!(
            "{} repair item(s) require attention — inspect with `task list` or `doctor`",
            report.pending_repairs
        ));
    }
    if report.interrupted_jobs > 0 {
        out = out.with_warning(format!(
            "{} interrupted job(s) — re-run the transfer or `task resume`",
            report.interrupted_jobs
        ));
    }
    Ok(out)
}

/// Export connections and buckets as a JSON config bundle, either to a file
/// or to stdout when `out` is `None`.
///
/// # Errors
/// Fails on serialization, file write, or any transfer-service error.
pub(super) async fn config_export(
    out: Option<&str>,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    let bundle = app.config().export().await?;
    let json = serde_json::to_string_pretty(&bundle)
        .map_err(|e| rstore_domain::Error::unknown("serialise config bundle").with_source(e))?;
    match out {
        Some(path) => {
            std::fs::write(path, format!("{json}\n"))
                .map_err(|e| rstore_domain::Error::io("write export file").with_source(e))?;
            Ok(HandlerOutput::human(format!("exported to {path}\n"))
                .with_data(serde_json::json!({"path": path, "connections": bundle.connections.len(), "buckets": bundle.buckets.len()})))
        }
        None => Ok(HandlerOutput::human(json).with_data(super::json_value(&bundle)?)),
    }
}

/// Import a JSON config bundle from `path`; `dry_run` reports what would be
/// added or skipped without persisting anything.
///
/// # Errors
/// Fails on file read, bundle deserialization, or any transfer-service error.
pub(super) async fn config_import(
    path: &str,
    dry_run: bool,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| rstore_domain::Error::io("read bundle").with_source(e))?;
    let bundle: rstore_application::ConfigBundle = serde_json::from_str(&raw)
        .map_err(|e| rstore_domain::Error::invalid_input("parse bundle").with_source(e))?;
    let report = app.config().import(&bundle, dry_run).await?;
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
