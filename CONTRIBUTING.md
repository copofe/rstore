# Contributing to rstore

Thank you for considering a contribution. This document covers the
development setup, the quality gates every change must pass, and the
conventions that keep the codebase coherent.

## Development setup

1. Install a stable Rust toolchain (the pinned version lives in
   `rust-toolchain.toml`; `rustup` will honor it automatically).
2. Clone the repository and verify your setup:

   ```sh
   cargo check --workspace
   cargo test --workspace
   ```

No other tooling is required. The optional MinIO integration test additionally
needs Docker (see the [README](README.md#development)).

## Quality gates

Every change must pass all four gates locally before it is submitted; CI runs
exactly the same commands:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps   # with RUSTDOCFLAGS=-D warnings
```

There is no warning budget: new warnings fail the build.

## How to contribute

1. Open an issue describing the bug or the proposed change before large
   refactors or new features, so scope can be agreed first.
2. Fork, create a topic branch, and keep commits atomic with clear messages.
3. Add or extend tests covering the observable behavior you changed:
   domain unit tests, repository/contract tests, CLI `insta` snapshots.
4. Run the full gate list above.
5. Submit a pull request referencing the issue, including what changed and
   why, plus any trade-offs worth recording.

## Architecture rules

The full contract lives in the
[architecture guide](docs/architecture-guide.md). The rules reviewers enforce
most often:

- **One engine, two entry points.** Business logic lives in `crates/engine`.
  The CLI and the future desktop must not duplicate orchestration, read SQLite
  directly, or call an AWS SDK.
- **Dependency direction is fixed.** `domain` depends on no I/O technology;
  the AWS SDK appears only in `storage`; the engine's store, transfer, and
  catalog modules are private; `engine::Runtime` is the only code that opens
  the database and selects the storage implementation.
- **Typed IDs everywhere.** Business identifiers use the newtypes from
  `rstore-domain::id`, never bare `String`/`Uuid`/integers.
- **Errors are domain errors.** Map external failures into
  `rstore_domain::Error` with a stable `ErrorCode`; never panic on external
  input; never put secrets in error context, logs, or exports.
- **Results to stdout, diagnostics to stderr.** JSON output uses the shared
  envelope; destructive commands require `--yes` non-interactively.

## Code style

- Formatting is `rustfmt` defaults (`rustfmt.toml`); do not hand-format.
- Comments follow a three-tier standard: one short module header, `///`
  documentation on public items (summary line plus only non-obvious
  contracts), and rare inline comments that explain *why*, not *what*.
  English only.
- Prefer deleting code over adding abstraction; a second implementation of
  the same idea needs a stronger justification than taste.

## Reporting bugs

Include the command line, the JSON envelope (`--json`) if available, the exit
code, and the relevant excerpt of `rstore doctor`. Never paste credentials —
connection secrets are redacted in output, but double-check before sharing.
