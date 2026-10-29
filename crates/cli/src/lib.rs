//! The `rstore` CLI library: argument tree, command dispatch, output envelope
//! and exit-code mapping. All command logic lives here so it stays
//! unit/snapshot-testable without spawning a process; the binary is a thin
//! wrapper that parses arguments, initialises tracing and calls [`run_api`].

pub mod args;
pub mod handlers;
pub mod output;

pub use args::{
    command_domains, parse_command_line, Cli, Command, ConfigCommand, ConnectionCommand,
};
pub use handlers::{resolve_dirs, run_api, HandlerResult};
pub use output::{envelope, error_envelope, exit_code_for, version_output, HandlerOutput};
/// The token [`run_api`] watches; the binary cancels it on Ctrl+C.
pub use rstore_application::CancellationToken;

pub use clap::Parser;
