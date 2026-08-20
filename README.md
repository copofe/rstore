# rstore — Remote Storage Workbench

A local-first workbench for S3-compatible remote storage. It connects to your own
S3-compatible endpoints (AWS S3, Cloudflare R2, MinIO, Supabase S3, NAS), indexes whole buckets
into workspaces, and provides a complete CLI today; a GPUI desktop client follows
later and reuses the exact same Rust core. The CLI is the first implementation and
validation surface; this is an implementation order, not a product or capability
priority.

> Status: **Current architecture is in place.** The CLI and the future desktop
> client share an infrastructure-neutral Application Core; Bootstrap owns
> SQLite, storage adapters, secrets, recovery and an Outbox dispatcher seam. The
> dispatcher is currently callable but is not connected to a resident consumer.
> The desktop binary exercises the same Bootstrap/API/notification contract;
> GPUI pages remain deferred.

Product scope is described in [`docs/prd.md`](docs/prd.md); current architecture and
implementation details are maintained in [`docs/architecture-guide.md`](docs/architecture-guide.md).

For the current component diagram, directory responsibilities, naming rules and data-flow
constraints, see [`docs/architecture-guide.md`](docs/architecture-guide.md).

## What the current core delivers

- Credentials are stored together with the Connection configuration in the local
  SQLite table. `RSTORE_SECRET_*` environment variables provide a read-only
  override for scripts and CI. Secret values never enter exported config, API JSON
  or logs (SEC-001).
- `connection add/list/show/edit/test/remove` — including a per-check
  connection test with an optional write probe (CON-007/008) and a cached
  capability report (CON-009/STORE-022).
- `bucket discover/add/list/show/edit/remove` — manual add works without
  ListBuckets permission; `--read-only` / `--allow-delete` guards are enforced
  at the Core layer before any adapter call (SEC-004/005).
- `remote ls|stat` — live paginated listings (`--cursor/--limit`,
  `--update-catalog`), object stat (REM-).
- `upload` / `download` — small-file put, streaming multipart ≥8 MiB with
  SQLite part checkpoints, interrupt-safe resume (Ctrl+C → exit 130 → re-run),
  ranged-chunk downloads into `.part` files with atomic completion, conflict
  policies `ask|skip|overwrite|rename`, `--recursive`, `--dry-run` (UP-/DOWN-).
- `rm` — single/batch delete with `--dry-run`, `--yes`, guard checks and
  catalog tombstones (DEL-).
- `task list|show|pause|resume|retry|cancel` — jobs persisted from creation,
  stale running jobs marked `interrupted` on startup (JOB-).
- Doctor reports adapter, credential-source counts and interrupted jobs;
  logs roll daily under `log_dir` (OBS-002). It also reports pending outbox
  events and repair items.
- Remote mutations persist a receipt and versioned outbox fact; catalog
  follow-up failures become queryable repairs instead of warning-only drift.

## Build & test

```sh
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

Install for daily use (strip is safe on the final binary; only the build-time
proc-macro dylibs must not be stripped — see the note in `Cargo.toml`):

```sh
cargo build --release -p rstore && strip target/release/rstore \
  && cp target/release/rstore ~/.local/bin/
```

Run the CLI (uses a throwaway data dir):

```sh
RSTORE_HOME=/tmp/rstore-demo cargo run -p rstore -- connection add prod \
  --endpoint http://localhost:9000 --access-key AK --secret-key SK
RSTORE_HOME=/tmp/rstore-demo cargo run -p rstore -- connection test prod --write-test
RSTORE_HOME=/tmp/rstore-demo cargo run -p rstore -- bucket add prod media
RSTORE_HOME=/tmp/rstore-demo cargo run -p rstore -- upload ./file.bin prod/media/
```

> **Scripts/CI:** prefer env credentials (`RSTORE_SECRET_<REF>__ACCESS_KEY` /
> `__SECRET_KEY`) — they override the config file and involve no prompts on
> any platform.

## MinIO integration test

```sh
docker run -d --rm -p 19000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

RSTORE_MINIO_ENDPOINT=http://localhost:19000 \
RSTORE_MINIO_ACCESS_KEY=minioadmin RSTORE_MINIO_SECRET_KEY=minioadmin \
  cargo test -p rstore-storage-s3 --test minio -- --ignored --nocapture
```

Interrupt-resume was verified end-to-end against MinIO with a 500 MiB
multipart upload interrupted by SIGINT and resumed to a byte-exact copy.

## Workspace layout

```
apps/cli          rstore binary (thin entry point; Bootstrap owns the S3 factory)
apps/desktop      minimal Bootstrap/API/notification entry point (no GPUI pages yet)
crates/domain     four-context entities, IDs, events, error model, policies
crates/application-ports  infrastructure-neutral repository/UoW/outbox ports
crates/application  context APIs and orchestration; no normal SQLite dependency
crates/bootstrap    CLI/desktop composition root and runtime recovery
crates/application-test-support  test-only SQLite composition helper
crates/storage-api  Storage trait + multipart protocol + capability model
crates/storage-s3   AWS-SDK S3 adapter (list/stat/read/put/delete/multipart)
crates/catalog    RemoteObject query model + repository contracts
crates/persistence SQLite + migrations + repositories + multipart checkpoints
crates/transfer   transfer engine: multipart, resume, retry, cancellation
crates/secrets    SecretStore: environment overrides and Connection credentials
crates/platform   app dirs + cross-process write lock
crates/test-support  FakeStorage (incl. multipart), TempDb, TestClock
crates/cli        command tree, JSON envelope, exit codes
ui/*              GPUI crates
xtask             build/package helpers
```

## Architecture rules (enforced by dependency direction)

- `domain` uses only domain/basic serialization/time/ID/error dependencies — never
  GPUI, AWS SDK or SQLite.
- `application` depends on `domain` + interface crates + application ports,
  never SQLite/SQLx/AWS SDK/GPUI in its normal dependency graph.
- `bootstrap` selects SQLite, the S3 adapter and SecretStore, then injects
  them through `ApplicationDependencies`; the core never imports an SDK.
- Secret values are managed by `SecretStore` and persisted with the Connection
  configuration in SQLite; they never enter exported config, API JSON or logs
  (`secrets::Secret` redacts Debug).
- Implement and validate each command through the CLI first so the underlying
  capabilities are easy to test; once stable, the future GPUI desktop consumes
  the same context-oriented Application API. This is an implementation order,
  not a product or capability priority.

## Deliberately deferred

- Applying `--tls-verify=false` to a custom S3 TLS verifier (the setting is currently
  stored, but the S3 adapter still uses the system-default verifier).
- `task pause` across processes for a *live* transfer — use Ctrl+C; the
  write lock serialises runtimes (DATA-008).
- Full GPUI pages and the complete notification consumer UI remain deferred;
  the desktop composition contract is present.
- CORE-015 benchmark framework and cross-process live task pause remain deferred.

See `AGENTS.md` for the full working agreement.
