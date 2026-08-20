//! The `rstore` CLI library (CLI-001..030).
//!
//! The binary (`apps/cli`) is a thin wrapper: it parses arguments, initialises
//! tracing, opens the [`rstore_application::Application`] and calls [`run_api`]. All
//! command logic, the JSON envelope and exit-code mapping live here so they are
//! unit/snapshot-testable without spawning a process.

pub mod args;
pub mod handlers;
pub mod output;

pub use args::{command_domains, Cli, Command, ConfigCommand, ConnectionCommand};
pub use handlers::{resolve_dirs, run_api, HandlerResult};
pub use output::{envelope, error_envelope, exit_code_for, version_output, HandlerOutput};

// Re-export clap parser so the binary can `Cli::parse()`.
pub use clap::Parser;
