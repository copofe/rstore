//! Command dispatch and the per-domain handler modules.
//!
//! Each domain owns one module (`connection`, `bucket`, `remote`, `task`,
//! `transfer`, `url`, plus `misc` for config/doctor and `files` for index /
//! files / catalog maintenance); interactive TTY policy lives in the `tty`
//! module and
//! shared formatting in [`format`]. Handlers produce a
//! [`crate::output::HandlerOutput`] carrying both structured `data` and the
//! pre-rendered human string — results go to stdout, diagnostics to stderr.

mod bucket;
mod connection;
mod files;
mod format;
mod misc;
mod remote;
mod report;
mod tag;
mod task;
mod transfer;
mod tty;
mod url;
mod workspace;

use rstore_domain::Result;
use rstore_platform::AppDirs;
use rstore_transfer::CancellationToken;
use serde::Serialize;

use crate::args::{Cli, Command, ConfigCommand};
use crate::output::{version_output, HandlerOutput};

/// A handler's outcome.
pub type HandlerResult = Result<HandlerOutput>;

/// Serialize handler data without silently replacing failures with JSON null.
/// A serialization failure is an internal output-contract error and keeps its
/// original cause in the domain error chain.
///
/// # Errors
/// Returns a domain error when serialization fails.
pub(super) fn json_value(value: impl Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(value)
        .map_err(|error| rstore_domain::Error::unknown("serialize CLI output").with_source(error))
}

/// Merge one optional section into a JSON envelope object. A `None` section
/// leaves the payload untouched; a non-object payload surfaces as an error
/// instead of being silently dropped — the same output-contract policy
/// [`json_value`] enforces, extended to attaching an optional section.
///
/// # Errors
/// Fails when a section is present but the payload is not a JSON object.
pub(super) fn merge_field(
    data: serde_json::Value,
    name: &str,
    section: Option<serde_json::Value>,
) -> Result<serde_json::Value> {
    let Some(section) = section else {
        return Ok(data);
    };
    let mut map = match data {
        serde_json::Value::Object(map) => map,
        _ => {
            return Err(rstore_domain::Error::unknown(format!(
                "cannot attach `{name}` to a non-object payload"
            )));
        }
    };
    map.insert(name.to_string(), section);
    Ok(serde_json::Value::Object(map))
}

/// Dispatch a parsed [`Cli`] invocation against the application API, sharing
/// one TTY session across every handler in the run.
pub async fn run_api(
    cli: &Cli,
    app: &rstore_application::ApplicationApi,
    dirs: &AppDirs,
    cancel: &CancellationToken,
) -> HandlerResult {
    // One prompt policy per run: every confirmation, credential prompt, and
    // conflict question reads it (`--json` closes the pipe).
    let session = tty::Session::detect(cli.json);
    match &cli.command {
        Command::Version => Ok(version_output()),
        Command::Doctor => misc::doctor(app, dirs).await,
        Command::Config { action } => match action {
            ConfigCommand::Show => misc::config_show(dirs),
            ConfigCommand::Export { out } => misc::config_export(out.as_deref(), app).await,
            ConfigCommand::Import { path, dry_run } => {
                misc::config_import(path, *dry_run, app).await
            }
        },
        Command::Connection { action } => connection::connection(action, app, &session).await,
        Command::Bucket { action } => bucket::bucket(action, app, &session).await,
        Command::Remote { action } => remote::remote(action, app, &session).await,
        Command::Rm {
            uris,
            recursive,
            dry_run,
            yes,
        } => remote::rm(app, uris, *recursive, *dry_run, *yes, cancel, &session).await,
        Command::Task { action } => task::task(action, app).await,
        Command::Upload(args) => {
            transfer::transfer(
                app,
                args,
                transfer::Direction::Upload,
                cancel,
                cli.quiet,
                &session,
            )
            .await
        }
        Command::Download(args) => {
            transfer::transfer(
                app,
                args,
                transfer::Direction::Download,
                cancel,
                cli.quiet,
                &session,
            )
            .await
        }
        Command::Workspace { action } => workspace::workspace(action, app, &session).await,
        Command::Index { action } => files::index(action, app, cancel, &session).await,
        Command::Files { action } => files::files(action, app, &session).await,
        Command::Tag { action } => tag::tag(action, app, &session).await,
        Command::Url(args) => url::url(args, app).await,
        Command::Catalog { action } => files::catalog(action, app, &session).await,
        Command::Cp(args) => {
            transfer::copy_move(false, args, app, cancel, cli.quiet, &session).await
        }
        Command::Mv(args) => {
            transfer::copy_move(true, args, app, cancel, cli.quiet, &session).await
        }
    }
}

/// Expand a user remote reference (short or canonical form) through the
/// application's one grammar — the rule lives in
/// `rstore_application::remote`, so other frontends expand identically.
pub(super) async fn expand_remote(
    app: &rstore_application::ApplicationApi,
    reference: &str,
) -> rstore_domain::Result<String> {
    rstore_application::expand_reference(
        app.registry().connections().as_ref(),
        app.registry().buckets().as_ref(),
        reference,
    )
    .await
}

/// Expand a batch of references through the same rule, preserving order.
pub(super) async fn expand_remote_batch(
    app: &rstore_application::ApplicationApi,
    references: &[String],
) -> Result<Vec<String>> {
    rstore_application::expand_references(
        app.registry().connections().as_ref(),
        app.registry().buckets().as_ref(),
        references,
    )
    .await
}

/// Resolve `--bucket` references (id / `<connection>/<name>` / bare name) to
/// bucket ids for file filters.
pub(super) async fn resolve_bucket_ids(
    app: &rstore_application::ApplicationApi,
    refs: &[String],
) -> Result<Vec<rstore_domain::BucketId>> {
    let mut ids = Vec::with_capacity(refs.len());
    for r in refs {
        ids.push(
            rstore_application::BucketService::resolve(
                app.registry().buckets().as_ref(),
                &rstore_application::BucketRef::parse(r),
            )
            .await?
            .id,
        );
    }
    Ok(ids)
}

/// Parse the top-level `--config` override into the resolved application
/// directories.
pub fn resolve_dirs(cli: &Cli) -> AppDirs {
    rstore_platform::resolve_app_dirs(cli.config.as_deref())
}
