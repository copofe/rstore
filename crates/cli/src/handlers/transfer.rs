//! `upload` / `download` handlers and the stderr progress sink (UP- / DOWN- /
//! JOB-005/016/018).

use std::io::Write;
use std::path::PathBuf;

use rstore_application::{BatchDownloadReport, TransferReport};
use rstore_transfer::{CancellationToken, NullProgressSink, Progress, ProgressSink};

use super::format::{human_size, speed};
use super::tty::resolve_conflict;
use super::HandlerResult;
use rstore_domain::{ConflictPolicy, Error};

// --- upload / download -----------------------------------------------------

pub(super) enum Direction {
    Upload,
    Download,
}

/// stderr progress line: `label 42% (12.3MB/29.4MB) 3.1MB/s` (JOB-005/016/018).
/// Never writes stdout (CLI-006); coalesces to one update per 250ms.
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

fn sink_for(args: &crate::args::TransferArgs, label: &str, quiet: bool) -> Box<dyn ProgressSink> {
    sink_for_flags(progress_suppressed(args.no_progress, quiet), label)
}

fn progress_suppressed(no_progress: bool, quiet: bool) -> bool {
    no_progress || quiet
}

fn sink_for_flags(no_progress: bool, label: &str) -> Box<dyn ProgressSink> {
    if no_progress {
        Box::new(NullProgressSink)
    } else {
        Box::new(StderrProgress::new(label))
    }
}

pub(super) async fn transfer(
    app: &rstore_application::ApplicationApi,
    args: &crate::args::TransferArgs,
    direction: Direction,
    cancel: &CancellationToken,
    quiet: bool,
    session: &super::tty::Session,
) -> HandlerResult {
    use rstore_application::{RunOptions, UploadObjectsCommand, UploadPlan};

    let sources = super::tty::resolve_uris(
        &args.sources,
        match direction {
            Direction::Upload => "upload needs at least one local source path",
            Direction::Download => "download needs at least one remote source reference",
        },
    )?;
    let options = RunOptions {
        dry_run: args.dry_run,
        ..Default::default()
    };

    let report = match direction {
        Direction::Upload => {
            let target = super::expand_remote(app, &args.target).await?;
            let (connection, bucket, prefix) = rstore_application::RemoteUri::parse_base(&target)?;
            let explicit = args
                .conflict
                .as_deref()
                .map(ConflictPolicy::parse)
                .transpose()?;
            let conflict = resolve_conflict(
                app.transfer()
                    .conflict_policy(explicit, Some((&connection, &bucket)))
                    .await?,
                args.yes,
                session,
            )?;
            let plan = UploadPlan {
                connection,
                bucket,
                prefix,
                sources: sources.iter().map(PathBuf::from).collect(),
                conflict,
                recursive: args.recursive,
            };
            app.transfer()
                .run_upload(
                    &UploadObjectsCommand { plan, options },
                    sink_for(args, "upload", quiet).as_ref(),
                    cancel,
                )
                .await?
        }
        Direction::Download => {
            let target = std::path::PathBuf::from(&args.target);
            let mode = rstore_application::classify_download(sources.len(), &target)?;
            let explicit = args
                .conflict
                .as_deref()
                .map(ConflictPolicy::parse)
                .transpose()?;
            let conflict = resolve_conflict(
                app.transfer().conflict_policy(explicit, None).await?,
                args.yes,
                session,
            )?;
            let expanded = match mode {
                rstore_application::DownloadMode::Batch => {
                    super::expand_remote_batch(app, &sources).await?
                }
                rstore_application::DownloadMode::Single => vec![
                    super::expand_remote(
                        app,
                        sources.first().ok_or_else(|| {
                            Error::invalid_input("download needs a remote source reference")
                        })?,
                    )
                    .await?,
                ],
            };
            let plans =
                rstore_application::download_plans(&expanded, target, conflict, args.recursive)?;
            match mode {
                rstore_application::DownloadMode::Batch => {
                    // MIX-029: one child job per URI, results aggregate at the
                    // application seam.
                    let report = app
                        .transfer()
                        .run_batch_download(
                            &plans,
                            &options,
                            sink_for(args, "download", quiet).as_ref(),
                            cancel,
                        )
                        .await?;
                    return render_batch_download_report(report, args);
                }
                rstore_application::DownloadMode::Single => {
                    let plan = plans.into_iter().next().ok_or_else(|| {
                        Error::invalid_input("download needs a remote source reference")
                    })?;
                    app.transfer()
                        .run_download(
                            &rstore_application::DownloadObjectsCommand { plan, options },
                            sink_for(args, "download", quiet).as_ref(),
                            cancel,
                        )
                        .await?
                }
            }
        }
    };

    render_transfer_report(report, args)
}

fn render_transfer_report(
    report: TransferReport,
    args: &crate::args::TransferArgs,
) -> HandlerResult {
    super::report::render_batch(
        super::json_value(&report)?,
        args.dry_run.then_some("(dry run — nothing transferred)"),
        &report.items,
        format!(
            "job {} — {} succeeded, {} skipped, {} failed ({})",
            report.job_id, report.succeeded, report.skipped, report.failed, report.state,
        ),
        report.is_interrupted(),
        report.failed,
        report.succeeded,
    )
}

fn render_batch_download_report(
    report: BatchDownloadReport,
    args: &crate::args::TransferArgs,
) -> HandlerResult {
    super::report::render_batch(
        super::json_value(&report)?,
        args.dry_run.then_some("(dry run — nothing transferred)"),
        report.reports.iter().flat_map(|child| child.items.iter()),
        format!(
            "{} job(s) — {} succeeded, {} skipped, {} failed, {} blocked",
            report.reports.len(),
            report.succeeded,
            report.skipped,
            report.failed,
            report.blocked,
        ),
        report.interrupted,
        report.failed,
        report.succeeded,
    )
}

/// `cp` / `mv` (MOVE-): resolve sources against the target, run the copy
/// service, render the shared report shape.
pub(super) async fn copy_move(
    is_move: bool,
    args: &crate::args::CopyArgs,
    app: &rstore_application::ApplicationApi,
    cancel: &rstore_transfer::CancellationToken,
    quiet: bool,
    session: &super::tty::Session,
) -> HandlerResult {
    // MOVE-007/008: `-` reads one URI per line from stdin.
    let sources =
        super::tty::resolve_uris(&args.sources, "cp/mv needs at least one remote source URI")?;
    let sources = super::expand_remote_batch(app, &sources).await?;
    let target = super::expand_remote(app, &args.target).await?;
    let (dst_connection, dst_bucket, _) = rstore_application::RemoteUri::parse_base(&target)?;
    let explicit = args
        .conflict
        .as_deref()
        .map(ConflictPolicy::parse)
        .transpose()?;
    let conflict = resolve_conflict(
        app.transfer()
            .conflict_policy(explicit, Some((&dst_connection, &dst_bucket)))
            .await?,
        args.yes,
        session,
    )?;
    let options = rstore_application::CopyOptions {
        dry_run: args.dry_run,
        conflict,
        recursive: args.recursive,
        keep_tags: args.keep_tags,
        config: rstore_transfer::TransferConfig::default(),
    };
    let pairs: Vec<rstore_application::CopyPair> = sources
        .iter()
        .map(|src| rstore_application::CopyPair {
            source: src.clone(),
            target: target.clone(),
        })
        .collect();
    let command = rstore_application::CopyObjectsCommand { pairs, options };
    let label = if is_move { "moving" } else { "copying" };
    let progress = sink_for_flags(quiet, label);
    let report = if is_move {
        app.transfer()
            .run_move(&command, cancel, progress.as_ref())
            .await?
    } else {
        app.transfer()
            .run_copy(&command, cancel, progress.as_ref())
            .await?
    };
    render_copy_report(report, args)
}

fn render_copy_report(report: TransferReport, args: &crate::args::CopyArgs) -> HandlerResult {
    super::report::render_batch(
        super::json_value(&report)?,
        args.dry_run.then_some("(dry run — nothing written)"),
        &report.items,
        format!(
            "job {} — {} succeeded, {} skipped, {} failed, {} blocked ({})",
            report.job_id,
            report.succeeded,
            report.skipped,
            report.failed,
            report.blocked,
            report.state,
        ),
        false,
        report.failed,
        report.succeeded,
    )
}

#[cfg(test)]
mod tests {
    use super::progress_suppressed;

    #[test]
    fn quiet_disables_progress_alongside_the_explicit_flag() {
        assert!(!progress_suppressed(false, false));
        assert!(progress_suppressed(true, false));
        assert!(progress_suppressed(false, true));
    }
}
