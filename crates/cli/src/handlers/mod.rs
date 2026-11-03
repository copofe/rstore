//! Command dispatch and the per-domain handler modules.
//!
//! Each domain owns one module (`connection`, `bucket`, `workspace`, `tag`,
//! `task`, `url`; `remote` for `ls`/`stat`/`rm`, `transfer` for `cp`/`mv`,
//! `files` for `find` and `index`, `misc` for config/doctor); interactive TTY
//! policy lives in the `tty` module and shared formatting in [`format`].
//! Every handler reads the per-run [`Ctx`] and produces a
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

use std::collections::HashMap;

use rstore_domain::{BucketId, Result, Workspace};
use rstore_engine::AppDirs;
use rstore_engine::{ApplicationApi, CancellationToken, RemoteUri, WorkspaceRef};
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

/// Per-run inputs every handler reads: the API handle, the run's
/// cancellation token and prompt policy, and the global flags.
struct Ctx<'a> {
    app: &'a ApplicationApi,
    cancel: &'a CancellationToken,
    /// One prompt policy per run: every confirmation, credential prompt, and
    /// conflict question reads it (`--json` closes the pipe).
    session: tty::Session,
    /// `-y`: destructive actions are confirmed up front.
    yes: bool,
    quiet: bool,
    /// `-w`, when given.
    explicit_workspace: Option<WorkspaceRef>,
}

impl Ctx<'_> {
    /// The workspace a catalog command targets: `-w`, else the current one,
    /// else the only one.
    async fn workspace(&self) -> Result<Workspace> {
        self.app
            .catalog()
            .workspaces()
            .target(self.explicit_workspace.as_ref())
            .await
    }
}

/// Dispatch a parsed [`Cli`] invocation against the application API, sharing
/// one [`Ctx`] across every handler in the run.
pub async fn run_api(
    cli: &Cli,
    app: &ApplicationApi,
    dirs: &AppDirs,
    cancel: &CancellationToken,
) -> HandlerResult {
    let ctx = Ctx {
        app,
        cancel,
        session: tty::Session::detect(cli.json),
        yes: cli.yes,
        quiet: cli.quiet,
        explicit_workspace: cli.workspace.as_deref().map(WorkspaceRef::parse),
    };
    match &cli.command {
        Command::Version => Ok(version_output()),
        Command::Ls(args) => remote::ls(&ctx, args).await,
        Command::Stat { reference, offline } => remote::stat(&ctx, reference, *offline).await,
        Command::Cp(args) => transfer::cp(&ctx, args).await,
        Command::Mv(args) => transfer::mv(&ctx, args).await,
        Command::Rm(args) => remote::rm(&ctx, args).await,
        Command::Url(args) => url::url(&ctx, args).await,
        Command::Find(args) => files::find(&ctx, args).await,
        Command::Tag { action } => tag::tag(&ctx, action).await,
        Command::Connection { action } => connection::connection(&ctx, action).await,
        Command::Bucket { action } => bucket::bucket(&ctx, action).await,
        Command::Workspace { action } => workspace::workspace(&ctx, action).await,
        Command::Index { action } => files::index(&ctx, action).await,
        Command::Task { action } => task::task(&ctx, action).await,
        Command::Config { action } => match action {
            ConfigCommand::Show => misc::config_show(dirs),
            ConfigCommand::Export { out } => misc::config_export(out.as_deref(), app).await,
            ConfigCommand::Import { path, dry_run } => {
                misc::config_import(path, *dry_run, app).await
            }
        },
        Command::Doctor => misc::doctor(&ctx).await,
    }
}

/// Expand a user remote reference (short or canonical form) through the
/// application's one grammar — the rule lives in
/// `rstore_engine::References`, so other frontends expand identically.
async fn expand_remote(app: &ApplicationApi, reference: &str) -> Result<RemoteUri> {
    app.registry().references().expand(reference).await
}

/// Expand a batch of references through the same rule, preserving order.
async fn expand_remote_batch(
    app: &ApplicationApi,
    references: &[String],
) -> Result<Vec<RemoteUri>> {
    app.registry().references().expand_many(references).await
}

/// Resolve bucket references (id / `<connection>/<name>` / bare name) to
/// bucket ids for file filters.
async fn resolve_bucket_ids(app: &ApplicationApi, refs: &[String]) -> Result<Vec<BucketId>> {
    let buckets = app.registry().buckets();
    let mut ids = Vec::with_capacity(refs.len());
    for r in refs {
        ids.push(
            buckets
                .resolve(&rstore_engine::BucketRef::parse(r))
                .await?
                .id,
        );
    }
    Ok(ids)
}

/// `<connection>/<bucket>` labels for every registered bucket, for listings
/// whose rows carry only bucket ids.
async fn bucket_labels(app: &ApplicationApi) -> Result<HashMap<BucketId, String>> {
    let connections: HashMap<_, _> = app
        .registry()
        .connections()
        .list()
        .await?
        .into_iter()
        .map(|c| (c.id, c.name))
        .collect();
    Ok(app
        .registry()
        .buckets()
        .list()
        .await?
        .into_iter()
        .map(|b| {
            let connection = connections
                .get(&b.connection_id)
                .cloned()
                .unwrap_or_else(|| b.connection_id.to_string());
            (b.id, format!("{connection}/{}", b.name))
        })
        .collect())
}

/// Resolve the application directories, honouring the top-level `--home`.
pub fn resolve_dirs(cli: &Cli) -> AppDirs {
    rstore_engine::resolve_app_dirs(cli.home.as_deref())
}
