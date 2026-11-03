//! Minimal desktop composition entry point.
//!
//! This deliberately contains no GPUI page. It proves that the desktop
//! process opens the same engine runtime as the CLI — side by side with it,
//! over the same data directory — and drives its views from the engine's
//! event stream without importing SQLite or a provider SDK.

use rstore_engine::Runtime;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("rstore-desktop bootstrap failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> rstore_domain::Result<()> {
    let dirs = rstore_engine::resolve_app_dirs(None);
    // Full startup sequence, identical to the CLI's: create dirs, open +
    // migrate + recover orphaned jobs. No process lock: a CLI may run beside it.
    let runtime = Runtime::open_in(&dirs).await?;
    let api = runtime.application().api();
    // Views subscribe once and re-query the API when an event says something
    // changed; GPUI pages attach here.
    let _events = api.events().subscribe();
    println!("rstore desktop runtime ready (GPUI presentation pending)");
    Ok(())
}
