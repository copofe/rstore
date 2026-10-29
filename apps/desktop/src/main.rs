//! Minimal desktop composition entry point.
//!
//! This deliberately contains no GPUI page. It proves that the future
//! desktop process can open the same Bootstrap runtime and obtain the
//! stable Application API without importing SQLite or a provider SDK.

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
    // Full startup sequence, identical to the CLI's: create dirs,
    // cross-process write lock, open + migrate + recover. The lock guard
    // lives inside the returned Runtime.
    let runtime = Runtime::open_locked(&dirs).await?;
    // A long-lived host can afford a longer startup drain; a resident
    // consumer loop replaces it once the desktop runs continuously.
    runtime
        .drain_outbox(std::time::Duration::from_secs(10))
        .await?;
    let _api = runtime.application().api();
    println!("rstore desktop runtime ready (GPUI presentation pending)");
    Ok(())
}
