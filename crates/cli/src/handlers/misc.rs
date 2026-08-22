//! `config show` / `config export` / `config import` and `doctor` handlers
//! (DATA-001 / DATA-015/016 / OBS-004).

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
    Ok(out)
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
