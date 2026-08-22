//! Minimal desktop composition entry point.
//!
//! This deliberately contains no GPUI page. It proves that the future
//! desktop process can open the same Bootstrap runtime, obtain the stable
//! Application API and subscribe to runtime job notifications without
//! importing SQLite or a provider SDK.

use rstore_bootstrap::Runtime;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("rstore-desktop bootstrap failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> rstore_domain::Result<()> {
    let dirs = rstore_platform::resolve_app_dirs(None);
    // Full startup sequence (CONTEXT.md: 启动编排) — identical to the CLI's:
    // create dirs, cross-process write lock (DATA-008), open + migrate +
    // recover. The lock guard lives inside the returned Runtime.
    let runtime = Runtime::open_locked(&dirs).await?;
    let _api = runtime.application().api();
    let _notifications = runtime.subscribe_job_notifications();
    println!("rstore desktop runtime ready (GPUI presentation pending)");
    Ok(())
}
