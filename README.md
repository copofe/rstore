# rstore — Remote Storage Workbench

rstore is a local-first workbench for S3-compatible object storage. It connects
to your own endpoints — AWS S3, Cloudflare R2, MinIO, Supabase S3, a NAS —
indexes whole buckets into local workspaces, and drives everyday object work
through a complete CLI. A GPUI desktop client sharing the same Rust core is
planned; the CLI ships today and is the primary validation surface.

Your objects never leave your storage. rstore keeps its catalog in a local
SQLite database, talks to providers only through their APIs, and writes no
index folders into your buckets.

## Features

- **Connections** — register any number of S3-compatible endpoints with stored
  credentials, per-check connection testing including an optional write probe,
  and live capability probes.
- **Buckets** — discover remote buckets or register them manually (no
  ListBuckets permission required); per-bucket read-only and allow-delete
  guards enforced in the core before any adapter call.
- **Workspaces** — group registered buckets, scope the catalog and tags to a
  team-visible subset, and manage bucket-wide ignore rules for indexing.
- **Catalog & search** — opt-in per-bucket indexing with resumable scans,
  full-text path search, keyset pagination, and stale-object reconciliation.
  The catalog is a cache: the provider stays the source of truth.
- **Tags** — workspace-scoped tags on any object, indexed or not, with batch
  attach/detach, merge, and copy/move tag preservation; tag queries cover the
  whole workspace.
- **Transfers** — upload/download with streaming multipart at ≥ 8 MiB,
  SQLite part checkpoints, interrupt-safe resume (Ctrl+C → re-run), ranged
  chunked downloads with atomic `.part` promotion, conflict policies
  `ask | skip | overwrite | rename`, dry runs, recursion.
- **Copy/move** — server-side copy within a connection (single-shot or part
  copy by size) and bounded-chunk relay across connections, preserving tags.
- **Removal** — single and batch deletes with precomputed confirmation counts
  and guard checks.
- **Links** — public or presigned URLs derived from bucket link profiles and
  connection CDN/public base URLs.
- **Shared data directory** — the CLI and the desktop (or two terminals) run
  side by side; running jobs hold leases, show progress across processes, and
  `task cancel` reaches them wherever they run.
- **Doctor** — one command reporting interrupted jobs, stale catalog rows,
  untested connections, and schema health.

## Installation

Build from source with a stable Rust toolchain (see `rust-toolchain.toml`):

```sh
cargo build --release -p rstore
strip target/release/rstore        # optional: shrink the final binary
cp target/release/rstore ~/.local/bin/
```

## Quickstart

```sh
# One step: register the connection, test it, register its buckets (they
# join the `default` workspace, created on first use).
export RSTORE_HOME=/tmp/rstore-demo   # optional: portable data root
rstore conn add prod --endpoint http://localhost:9000 --provider minio \
  --access-key AK --secret-key SK --buckets all

# Index a bucket to make it searchable (indexing is opt-in per bucket).
rstore index run media

# Everyday work. `cp` uploads, downloads, or copies remotely depending on
# which side is local (`./…`) and which names a registered bucket.
rstore ls media/docs/
rstore cp ./report.pdf media/reports/
rstore cp media/reports/report.pdf backup/archive/
rstore rm media/tmp/ -r -y
rstore url media/reports/report.pdf --ttl 1h
rstore tag add media/reports/report.pdf -t q3,final
rstore find report -t final
```

Scripts and CI must pass `--access-key`/`--secret-key` explicitly: with
`--json` the CLI never prompts.

## Documentation

| Document | Contents |
| --- | --- |
| [CLI guide](docs/cli-guide.md) | Every command, flag, exit code, and JSON envelope |
| [Architecture guide](docs/architecture-guide.md) | Crate layout, layering rules, data flow, naming conventions |
| [Domain glossary](CONTEXT.md) | The one definition of each domain term |
| [Product requirements](docs/prd.md) | Product scope and boundaries |
| [Contributing](CONTRIBUTING.md) | Development setup, gates, and conventions |

## Development

```sh
cargo fmt --all -- --check          # formatting gate
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace              # unit + contract + CLI snapshot tests
cargo doc --workspace --no-deps     # API docs
```

An optional end-to-end multipart resume test runs against a live MinIO:

```sh
docker run -d --rm -p 19000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

RSTORE_MINIO_ENDPOINT=http://localhost:19000 \
RSTORE_MINIO_ACCESS_KEY=minioadmin RSTORE_MINIO_SECRET_KEY=minioadmin \
  cargo test -p rstore-storage --test minio -- --ignored --nocapture
```

Interrupt-resume has been verified against MinIO with a 500 MiB multipart
upload interrupted by SIGINT and resumed byte-exact.

## Architecture in one paragraph

One engine, two entry points. `crates/domain` holds entities, value objects,
typed IDs, and the error model with zero I/O dependencies; `crates/storage`
is the provider-neutral `Storage` trait with the S3 adapter behind it;
`crates/engine` holds the use cases, the SQLite store, the transfer engine,
job leases, and the event stream, and its `Runtime` is the one place that
opens the database. The catalog is a cache of indexed buckets and tagged
objects; the provider stays the source of truth. Both the CLI and the future
desktop consume the same context-oriented API. See the
[architecture guide](docs/architecture-guide.md) for the crate map, dependency
rules, and data-flow constraints.

## Deliberately deferred

- Applying the stored `--tls-verify=false` setting to a custom S3 TLS
  verifier (the flag is persisted; the adapter still uses the system default).
- GPUI desktop pages; the desktop composition contract is present and
  exercised.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
