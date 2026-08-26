//! Pins the command grammar: every dotted leaf of the clap tree. The
//! envelope's `command` identity is derived from this same tree
//! ([`rstore_cli::parse_command_line`]), so a grammar change (new command,
//! rename, moved subcommand) must consciously touch this snapshot.

use clap::CommandFactory;
use rstore_cli::args::Cli;

/// Collects the dotted identity of every dispatchable leaf, skipping clap's
/// built-in `help` machinery.
fn dotted_leaves() -> Vec<String> {
    fn walk(command: &clap::Command, prefix: &str, out: &mut Vec<String>) {
        if command.get_name() == "help" {
            return;
        }
        let path = if prefix.is_empty() {
            command.get_name().to_string()
        } else {
            format!("{prefix}.{}", command.get_name())
        };
        let subcommands: Vec<&clap::Command> = command.get_subcommands().collect();
        if subcommands.is_empty() {
            out.push(path);
        } else {
            for sub in subcommands {
                walk(sub, &path, out);
            }
        }
    }

    let mut leaves = Vec::new();
    for top in Cli::command().get_subcommands() {
        walk(top, "", &mut leaves);
    }
    leaves
}

#[test]
fn command_identity_leaves_are_pinned() {
    insta::assert_snapshot!(dotted_leaves().join("\n"));
}
