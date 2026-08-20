//! Minimal desktop composition entry point.
//!
//! This deliberately contains no GPUI page. It proves that the future
//! desktop process can open the same Bootstrap runtime, obtain the stable
//! Application API and subscribe to runtime job notifications without
//! importing SQLite or a provider SDK.

use rstore_bootstrap::{default_secret_source_stats, default_secret_store, Runtime, RuntimeConfig};
use rstore_platform::{AppLock, LockError};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("rstore-desktop bootstrap failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> rstore_domain::Result<()> {
    let dirs = rstore_platform::resolve_app_dirs(None);
    dirs.ensure_created().map_err(|error| {
        rstore_domain::Error::database("create desktop data directories").with_source(error)
    })?;
    let _lock = AppLock::try_acquire(&dirs.lock_path).map_err(|error| match error {
        LockError::Busy => {
            rstore_domain::Error::lock_busy("another rstore runtime holds the write lock")
        }
        LockError::Io(error) => {
            rstore_domain::Error::database("acquire desktop write lock").with_source(error)
        }
    })?;
    let runtime = Runtime::open(
        RuntimeConfig::new(dirs.db_path, default_secret_store(&dirs.config_dir))
            .with_secret_source_stats(default_secret_source_stats(&dirs.config_dir)),
    )
    .await?;
    let _api = runtime.application().api();
    let _notifications = runtime.subscribe_job_notifications();
    println!("rstore desktop runtime ready (GPUI presentation pending)");
    Ok(())
}
