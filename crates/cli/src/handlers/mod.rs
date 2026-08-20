//! Command dispatch and per-domain handlers.
//!
//! Each domain owns one module (`connection`, `bucket`, `remote`, `task`,
//! `transfer`, plus `misc` for config/doctor); interactive TTY policy lives in
//! [`tty`] and shared formatting in [`format`]. Handlers produce a
//! [`crate::output::HandlerOutput`] carrying both structured `data` and the
//! pre-rendered human string — results go to stdout, diagnostics to stderr
//! (CLI-006).

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
pub(super) fn json_value(value: impl Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(value)
        .map_err(|error| rstore_domain::Error::unknown("serialize CLI output").with_source(error))
}

/// Merge one optional section into a JSON envelope object. A `None` section
/// leaves the payload untouched; a serialisation failure or a non-object
/// payload surfaces as an error instead of being silently dropped — the
/// policy [`json_value`] established, extended to the "optional section
/// rides the payload" assembly.
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

/// Dispatch against the stable context-oriented Application API.
pub async fn run_api(
    cli: &Cli,
    app: &rstore_application::ApplicationApi,
    dirs: &AppDirs,
    cancel: &CancellationToken,
) -> HandlerResult {
    // One prompt policy per run: every confirmation, credential prompt and
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
        Command::Url(args) => misc::url(args, app).await,
        Command::Catalog { action } => misc::catalog(action, app).await,
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
/// `rstore_application::remote`, so the future desktop expands identically.
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

/// Expand a batch through the same rule, preserving order.
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
/// bucket ids for file filters (MIX-013).
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

/// Helper used by the binary: parse the top-level `--config` override into the
/// resolved application directories.
pub fn resolve_dirs(cli: &Cli) -> AppDirs {
    rstore_platform::resolve_app_dirs(cli.config.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_application::{AddBucketInput, AddConnectionInput};
    use rstore_domain::{AddressingStyle, Region};
    use rstore_persistence::Database;

    async fn app_with_media() -> rstore_application::Application {
        let app = rstore_application_test_support::application_from_db(
            Database::open_in_memory().await.unwrap(),
        );
        app.api()
            .registry()
            .connections()
            .add(AddConnectionInput {
                name: "nas".into(),
                endpoint: "http://localhost:9000".into(),
                region: Region::Auto,
                addressing_style: AddressingStyle::Path,
                tls_verify: true,
                secret_ref: None,
                credentials: None,
            })
            .await
            .unwrap();
        app.api()
            .registry()
            .buckets()
            .add(
                AddBucketInput {
                    connection: rstore_application::ConnectionRef::Name("nas".into()),
                    name: "media".into(),
                    config: Default::default(),
                },
                false,
            )
            .await
            .unwrap();
        app
    }

    #[tokio::test]
    async fn short_reference_preserves_the_directory_marker() {
        let app = app_with_media().await;
        let api = app.api();
        let directory = expand_remote(&api, "media/docs/").await.unwrap();
        let exact = expand_remote(&api, "media/docs").await.unwrap();

        assert!(directory.ends_with("/media/docs/"), "{directory}");
        assert!(exact.ends_with("/media/docs"), "{exact}");
    }
}
