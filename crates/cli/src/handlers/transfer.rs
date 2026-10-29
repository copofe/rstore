//! `cp` / `mv` handlers plus the stderr progress sink: the application's
//! reference grammar routes each `cp` to an upload, download, or remote copy;
//! the conflict policy is settled here, the transfer runs through the
//! application seam, and results render via the shared batch-report grammar.

use std::io::Write;
use std::path::PathBuf;

use rstore_application::{
    BatchDownloadCommand, BatchDownloadReport, NullProgressSink, Progress, ProgressSink, RemoteUri,
    RunControl, RunOptions, TransferConfig, TransferReport, TransferRoute,
};

use super::format::{human_size, speed};
use super::tty::resolve_conflict;
use super::{Ctx, HandlerResult};
use crate::args::CopyArgs;
use rstore_domain::{ConflictPolicy, Error};

/// stderr progress line: `label 42% (12.3MB/29.4MB) 3.1MB/s`. Never writes
/// stdout; coalesces to one update per 250ms.
struct StderrProgress {
    label: String,
    start: std::time::Instant,
    last: std::sync::Mutex<std::time::Instant>,
}

impl StderrProgress {
    fn new(label: &str) -> Self {
        Self {
            label: label.to_string(),
            start: std::time::Instant::now(),
            last: std::sync::Mutex::new(
                std::time::Instant::now() - std::time::Duration::from_secs(1),
            ),
        }
    }
}

impl ProgressSink for StderrProgress {
    fn report(&self, progress: Progress) {
        let mut last = self.last.lock().expect("progress lock poisoned");
        let now = std::time::Instant::now();
        if now.duration_since(*last) < std::time::Duration::from_millis(250) {
            return;
        }
        *last = now;
        drop(last);
        let pct = (progress.fraction() * 100.0) as u64;
        // Byte progress carries the transfer speed; item progress has none.
        match progress {
            Progress::Bytes { done, total } => eprint!(
                "\r  {} {}% ({}/{}) {}   ",
                self.label,
                pct,
                human_size(done),
                human_size(total),
                speed(done, now.duration_since(self.start)),
            ),
            Progress::Items { done, total } => {
                eprint!("\r  {} {}% ({}/{}) items   ", self.label, pct, done, total)
            }
        }
        let _ = std::io::stderr().flush();
    }
}

fn progress_sink(ctx: &Ctx<'_>, args: &CopyArgs, label: &str) -> Box<dyn ProgressSink> {
    if args.no_progress || ctx.quiet {
        Box::new(NullProgressSink)
    } else {
        Box::new(StderrProgress::new(label))
    }
}

/// `cp SRC... DST`: the reference grammar decides the direction once, then
/// the matching application command runs.
pub(super) async fn cp(ctx: &Ctx<'_>, args: &CopyArgs) -> HandlerResult {
    let sources = super::tty::resolve_uris(
        &args.sources,
        "cp needs at least one source (or `-` to read them from stdin)",
    )?;
    match ctx
        .app
        .registry()
        .references()
        .route(&sources, &args.target)
        .await?
    {
        TransferRoute::Upload { sources, target } => {
            reject_keep_tags(args)?;
            upload(ctx, args, sources, target).await
        }
        TransferRoute::Download { sources, target } => {
            reject_keep_tags(args)?;
            download(ctx, args, &sources, target).await
        }
        TransferRoute::Copy { sources, target } => {
            remote_copy(ctx, args, sources, target, false).await
        }
    }
}

/// `mv SRC... DST`: remote-to-remote only — copy, verify, then delete.
pub(super) async fn mv(ctx: &Ctx<'_>, args: &CopyArgs) -> HandlerResult {
    let sources = super::tty::resolve_uris(
        &args.sources,
        "mv needs at least one remote source (or `-` to read them from stdin)",
    )?;
    match ctx
        .app
        .registry()
        .references()
        .route(&sources, &args.target)
        .await?
    {
        TransferRoute::Copy { sources, target } => {
            remote_copy(ctx, args, sources, target, true).await
        }
        TransferRoute::Upload { .. } | TransferRoute::Download { .. } => Err(Error::invalid_input(
            "mv moves objects between remote locations only; use cp to upload or download",
        )),
    }
}

fn reject_keep_tags(args: &CopyArgs) -> Result<(), Error> {
    if args.keep_tags {
        return Err(Error::invalid_input(
            "--keep-tags applies to remote-to-remote copies only",
        ));
    }
    Ok(())
}

/// Settle the conflict policy: `-f` or `--conflict` first, then the target
/// bucket's default (precedence owned by the application), then the prompt.
async fn conflict_policy(
    ctx: &Ctx<'_>,
    args: &CopyArgs,
    target: Option<&RemoteUri>,
) -> Result<ConflictPolicy, Error> {
    let explicit = if args.force {
        Some(ConflictPolicy::Overwrite)
    } else {
        args.conflict
            .as_deref()
            .map(ConflictPolicy::parse)
            .transpose()?
    };
    resolve_conflict(
        ctx.app.transfer().conflict_policy(explicit, target).await?,
        &ctx.session,
    )
}

fn run_options(args: &CopyArgs) -> RunOptions {
    RunOptions {
        dry_run: args.dry_run,
        ..Default::default()
    }
}

async fn upload(
    ctx: &Ctx<'_>,
    args: &CopyArgs,
    sources: Vec<PathBuf>,
    target: RemoteUri,
) -> HandlerResult {
    let conflict = conflict_policy(ctx, args, Some(&target)).await?;
    let plan = rstore_application::UploadPlan {
        connection: target.connection,
        bucket: target.bucket,
        prefix: target.key,
        sources,
        conflict,
        recursive: args.recursive,
    };
    let progress = progress_sink(ctx, args, "upload");
    let report = ctx
        .app
        .transfer()
        .run_upload(
            &rstore_application::UploadObjectsCommand {
                plan,
                options: run_options(args),
            },
            RunControl {
                progress: progress.as_ref(),
                cancel: ctx.cancel,
            },
        )
        .await?;
    render_transfer_report(report, args.dry_run)
}

async fn download(
    ctx: &Ctx<'_>,
    args: &CopyArgs,
    sources: &[RemoteUri],
    target: PathBuf,
) -> HandlerResult {
    let mode = rstore_application::classify_download(sources.len(), &target)?;
    let conflict = conflict_policy(ctx, args, None).await?;
    let mut plans = rstore_application::download_plans(sources, target, conflict, args.recursive);
    let options = run_options(args);
    let progress = progress_sink(ctx, args, "download");
    let control = RunControl {
        progress: progress.as_ref(),
        cancel: ctx.cancel,
    };
    match mode {
        rstore_application::DownloadMode::Batch => {
            // One child job per source; results aggregate at the
            // application seam.
            let report = ctx
                .app
                .transfer()
                .run_batch_download(&BatchDownloadCommand { plans, options }, control)
                .await?;
            render_batch_download_report(report, args.dry_run)
        }
        rstore_application::DownloadMode::Single => {
            let plan = plans
                .pop()
                .ok_or_else(|| Error::invalid_input("download needs a remote source reference"))?;
            let report = ctx
                .app
                .transfer()
                .run_download(
                    &rstore_application::DownloadObjectsCommand { plan, options },
                    control,
                )
                .await?;
            render_transfer_report(report, args.dry_run)
        }
    }
}

async fn remote_copy(
    ctx: &Ctx<'_>,
    args: &CopyArgs,
    sources: Vec<RemoteUri>,
    target: RemoteUri,
    is_move: bool,
) -> HandlerResult {
    let conflict = conflict_policy(ctx, args, Some(&target)).await?;
    let options = rstore_application::CopyOptions {
        dry_run: args.dry_run,
        conflict,
        recursive: args.recursive,
        keep_tags: args.keep_tags,
        config: TransferConfig::default(),
    };
    let pairs: Vec<rstore_application::CopyPair> = sources
        .into_iter()
        .map(|source| rstore_application::CopyPair {
            source,
            target: target.clone(),
        })
        .collect();
    let command = rstore_application::CopyObjectsCommand { pairs, options };
    let label = if is_move { "moving" } else { "copying" };
    let progress = progress_sink(ctx, args, label);
    let control = RunControl {
        progress: progress.as_ref(),
        cancel: ctx.cancel,
    };
    let report = if is_move {
        ctx.app.transfer().run_move(&command, control).await?
    } else {
        ctx.app.transfer().run_copy(&command, control).await?
    };
    render_transfer_report(report, args.dry_run)
}

fn render_transfer_report(report: TransferReport, dry_run: bool) -> HandlerResult {
    super::report::render_batch(
        super::json_value(&report)?,
        dry_run.then_some("(dry run — nothing transferred)"),
        &report.items,
        format!(
            "job {} — {} succeeded, {} skipped, {} failed, {} blocked ({})",
            report.job_id,
            report.succeeded(),
            report.skipped(),
            report.failed(),
            report.blocked(),
            report.state,
        ),
        report.is_interrupted(),
        report.severity(),
    )
}

fn render_batch_download_report(report: BatchDownloadReport, dry_run: bool) -> HandlerResult {
    super::report::render_batch(
        super::json_value(&report)?,
        dry_run.then_some("(dry run — nothing transferred)"),
        report.reports.iter().flat_map(|child| child.items.iter()),
        format!(
            "{} job(s) — {} succeeded, {} skipped, {} failed, {} blocked",
            report.reports.len(),
            report.succeeded(),
            report.skipped(),
            report.failed(),
            report.blocked(),
        ),
        report.is_interrupted(),
        report.severity(),
    )
}
