//! `rstore` CLI entry point.
//!
//! Thin binary: parse → init tracing → resolve dirs → (for DB commands) run
//! the full startup sequence ([`rstore_bootstrap::Runtime::open_locked`]:
//! create dirs, write lock, migrate, recover) → dispatch via [`rstore_cli`] →
//! render human/JSON and map to a stable exit code.
//!
//! Command identity and the version renderer come from the library (single
//! source); the envelope timestamp flows from the injected clock.

use clap::Parser;
use rstore_bootstrap::Runtime;
use rstore_cli::args::{Cli, Command};
use rstore_cli::output::{envelope, error_envelope, exit_code_for};
use rstore_cli::{resolve_dirs, version_output, HandlerResult};
use rstore_domain::{Clock, SystemClock};
use time::OffsetDateTime;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let dirs = resolve_dirs(&cli);
    init_tracing(&cli.log_level, &dirs);

    // `version` needs no database; handle it before acquiring the lock.
    if let Command::Version = cli.command {
        emit(Ok(version_output()), &cli, "version", SystemClock.now());
        return;
    }

    let command = cli.command.path();
    let now = SystemClock.now();

    // Full startup sequence: create data directories, acquire the
    // cross-process write lock, open + migrate + recover. Startup failures
    // flow through the same envelope and the single-source exit-code mapping
    // as every other domain error.
    let runtime = match Runtime::open_locked(&dirs).await {
        Ok(runtime) => runtime,
        Err(e) => {
            emit(Err(e), &cli, command, now);
            return;
        }
    };

    // Ctrl+C: cancel the cooperative token; the running transfer persists its
    // checkpoints and reports state `interrupted`, which emit() turns into
    // exit 130.
    let cancel = rstore_transfer::CancellationToken::new();
    {
        let cancel = cancel.clone();
        let quiet = cli.quiet;
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                if !quiet {
                    eprintln!("\ninterrupt received — finishing current item, saving checkpoints…");
                }
                cancel.cancel();
            }
        });
    }

    // Honour the injected clock for the result envelope.
    let app = runtime.application();
    let api = app.api();
    let result = rstore_cli::run_api(&cli, &api, &dirs, &cancel).await;
    emit(result, &cli, command, app.now());
}

/// Initialise structured logging: stderr for the console and a daily-rolling
/// file under `log_dir` to bound disk usage. Progress and logs never go to
/// stdout.
fn init_tracing(level: &str, dirs: &rstore_platform::AppDirs) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("warn"));

    // Best-effort rolling file; a failure falls back to stderr-only logging.
    let file_layer = std::fs::create_dir_all(&dirs.log_dir).ok().map(|_| {
        let appender = tracing_appender::rolling::daily(&dirs.log_dir, "rstore.log");
        fmt::layer()
            .with_writer(appender)
            .with_ansi(false)
            .with_target(false)
    });

    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_filter(filter);

    match file_layer {
        Some(file_layer) => {
            let _ = tracing_subscriber::registry()
                .with(stderr_layer)
                .with(file_layer.with_filter(tracing_subscriber::filter::LevelFilter::INFO))
                .try_init();
        }
        None => {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("warn")),
                )
                .with_writer(std::io::stderr)
                .with_target(false)
                .try_init();
        }
    }
}

/// Render a handler result to stdout (result) / stderr (human diagnostics) and
/// exit with the mapped code. The timestamp argument comes from the injected
/// clock, so envelopes carry the same time source as domain stamps.
fn emit(result: HandlerResult, cli: &Cli, command: &str, now: OffsetDateTime) {
    match result {
        Ok(out) => {
            if cli.json {
                let env = envelope(command, &out, now);
                println!("{}", serde_json::to_string_pretty(&env).unwrap());
            } else if human_output_enabled(cli) {
                for warning in &out.warnings {
                    eprintln!("warning: {warning}");
                }
                print!("{}", out.human);
            }
            // Handlers can override the exit code for non-error outcomes (an
            // interrupted transfer still emits its envelope but exits 130).
            std::process::exit(out.exit_code.unwrap_or(0));
        }
        Err(e) => {
            if cli.json {
                // The envelope is the result — even on failure it goes to stdout.
                let env = error_envelope(command, &e, now);
                println!("{}", serde_json::to_string_pretty(&env).unwrap());
            } else {
                eprintln!("error: {e}");
            }
            std::process::exit(exit_code_for(&e));
        }
    }
}

fn human_output_enabled(cli: &Cli) -> bool {
    !cli.quiet && !cli.json
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_suppresses_human_success_output_without_affecting_json_mode() {
        let quiet = Cli::parse_from(["rstore", "--quiet", "version"]);
        assert!(!human_output_enabled(&quiet));

        let normal = Cli::parse_from(["rstore", "version"]);
        assert!(human_output_enabled(&normal));

        let json = Cli::parse_from(["rstore", "--json", "version"]);
        assert!(!human_output_enabled(&json));
    }
}
