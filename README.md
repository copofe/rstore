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
  and cached capability reports.
- **Buckets** — discover remote buckets or register them manually (no
  ListBuckets permission required); per-bucket read-only and allow-delete
  guards enforced in the core before any adapter call.
- **Workspaces** — group registered buckets, scope the catalog and tags to a
  team-visible subset, and manage bucket-wide ignore rules for indexing.
- **Catalog & search** — index buckets with resumable scans, full-text path
  search, keyset pagination, stale-object reconciliation, and tombstone GC.
- **Tags** — workspace-scoped tag definitions with object associations,
  batch attach/detach, merge, and copy/move tag preservation.
- **Transfers** — upload/download with streaming multipart at ≥ 8 MiB,
  SQLite part checkpoints, interrupt-safe resume (Ctrl+C → re-run), ranged
  chunked downloads with atomic `.part` promotion, conflict policies
  `ask | skip | overwrite | rename`, dry runs, recursion.
- **Copy/move** — server-side copy within a connection (single-shot or part
  copy by size) and bounded-chunk relay across connections, preserving tags.
- **Removal** — single and batch deletes with precomputed confirmation counts,
  guard checks, and durable tombstones.
- **Links** — public or presigned URLs derived from bucket link profiles and
  connection CDN/public base URLs.
- **Reliable bookkeeping** — every remote mutation persists a receipt plus a
  versioned outbox fact in one transaction; failed follow-ups become queryable
  repair items instead of silent drift.
- **Doctor** — one command reporting interrupted jobs, pending outbox events,
  repairs, untested connections, and schema health.

## Installation

Build from source with a stable Rust toolchain (see `rust-toolchain.toml`):

```sh
cargo build --release -p rstore
strip target/release/rstore        # optional: shrink the final binary
cp target/release/rstore ~/.local/bin/
```

## Quickstart

```sh
# Register a connection (credentials are stored in the local SQLite catalog).
export RSTORE_HOME=/tmp/rstore-demo   # optional: portable data root
rstore connection add prod \
  --endpoint http://localhost:9000 --access-key AK --secret-key SK

# Verify connectivity, register a bucket, index it.
rstore connection test prod --write-test
rstore bucket add prod media
rstore index start media

# Everyday work.
rstore ls prod/media/docs/
rstore upload ./report.pdf prod/media/reports/
rstore cp prod/media/reports/report.pdf backup/archive/ --recursive
rstore rm prod/media/tmp/ --recursive --yes
rstore url prod/media/reports/report.pdf --signed --ttl 1h
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
  cargo test -p rstore-storage-s3 --test minio -- --ignored --nocapture
```

Interrupt-resume has been verified against MinIO with a 500 MiB multipart
upload interrupted by SIGINT and resumed byte-exact.

## Architecture in one paragraph

One Application Core, two entry points. `crates/domain` holds entities, value
objects, typed IDs, and the error model with zero I/O dependencies;
`crates/application` orchestrates use cases over ports defined in
`crates/application-ports` and `crates/storage-api`; `crates/persistence`
(SQLite) and `crates/storage-s3` (AWS SDK) are swappable adapters behind those
ports; `crates/bootstrap` is the sole composition root that opens the database
and wires concrete implementations. Both the CLI and the future desktop
consume the same context-oriented API. See the
[architecture guide](docs/architecture-guide.md) for the crate map, dependency
rules, and data-flow constraints.

## Deliberately deferred

- Applying the stored `--tls-verify=false` setting to a custom S3 TLS
  verifier (the flag is persisted; the adapter still uses the system default).
- A *resident* outbox consumer loop (startup drain via `dispatch_once` and the
  remote-verify projection exist; only a long-lived host process is missing).
- GPUI desktop pages; the desktop composition contract is present and
  exercised.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
