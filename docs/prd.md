# Product Requirements — Remote Storage Workbench

This document records product scope and boundaries. For implementation
status, the source code, [`README.md`](../README.md), and the
[architecture guide](architecture-guide.md) are authoritative.

## 1. Product definition

rstore is a local-first workbench for remote storage. Users register any
number of S3-compatible connections, group buckets into workspaces, build a
local catalog, and run browsing, search, tagging, upload, download, copy,
move, delete, and link generation through the CLI.

Files always remain in the user's own storage. rstore depends on no vendor
central service and writes no private index folders into buckets.

## 2. Entry points

The CLI is the complete automation surface today. It validates the
underlying capabilities first; the desktop follows once they stabilize. That
order is a development order, not a statement of product priority. The
future GPUI desktop shares the same application core — no duplicated logic,
and no direct SQLite or provider SDK access.

The current desktop entry point opens the engine runtime, obtains the
application API, and subscribes to its event stream; full pages are deferred.
The CLI and the desktop may run at the same time over one data directory.

## 3. Domain scope

### 3.1 Registry

- A **Connection** stores its name, S3 API endpoint, public/CDN delivery
  base defaults, region, addressing style, TLS policy, and credential fields.
- A **Bucket** stores connection ownership, name, read/write policy, index
  policy, concurrency override, and conflict default.
- A **LinkProfile** stores bucket-level public/CDN base overrides, presign
  permissions, and the default TTL.
- Effective link base precedence: bucket CDN override → connection CDN
  default → bucket public override → connection public default. Without a
  configured base, links are presigned; the S3 endpoint is never presented as
  a public URL. rstore never guesses URL shapes from the provider name.
- Credentials live directly in the connection's SQLite row, protected as
  `Secret<T>` in memory, and never enter exports, generic API JSON, or logs;
  connection details render masked previews only.

### 3.2 Catalog

- A **Workspace** is a set of buckets, a tag namespace, and a query scope.
- An **IgnoreRule** ignores an exact directory subtree relative to the bucket
  root during indexing.
- A **RemoteObject** is uniquely located by `(connection, bucket, path)`.
- An **IndexRun** records scan state, pagination checkpoints, object counts,
  and errors.
- The catalog supports state filtering, workspace membership scoping, tag
  associations, sorting, keyset pagination, and full-text path prescreening.
- Refresh/rebuild affect only the local catalog; no remote object is deleted.

### 3.3 Transfer

- **Job** and **JobItem** persist state, progress, and errors per item.
- Upload covers small files and multipart with resumable checkpoints.
- Download supports ranges, resume, a source identity sidecar, and atomic
  completion.
- Copy supports server-side copy within one connection, cross-connection
  relay, and recursive expansion.
- Move verifies the target before deleting the source.
- Delete supports batches, recursion, dry runs, exact confirmation counts,
  and bucket policy checks.
- Ctrl+C persists an observable interrupted state; re-running the original
  command resumes from checkpoints. `task cancel` reaches a running job in
  any process sharing the data directory.

### 3.4 Access diagnostics

- Connection testing checks stored credentials, connectivity, and optionally
  performs a write probe. Probes that write only touch registered buckets
  that allow writes and deletes, through the same guard as real mutations.
- There is no capability probe: an operation the provider cannot perform
  fails with `capability_unsupported` when it is attempted.
- `doctor` summarizes connection health, adapter identity, interrupted jobs,
  and stale catalog rows.
- Unsupported capabilities return a distinct `CapabilityUnsupported` error.

## 4. CLI commands

```text
rstore ls|stat|cp|mv|rm|url
rstore find
rstore tag add|remove|list|edit|delete|merge
rstore connection add|list|show|test|edit|remove        (alias: conn)
rstore bucket list|add|show|edit|remove
rstore workspace list|use|create|show|edit|delete|add|remove   (alias: ws)
rstore index run|status|cancel|gc
rstore task list|show|cancel
rstore config show|export|import
rstore doctor
rstore version
```

Global flags: `-w/--workspace` (env `RSTORE_WORKSPACE`) picks the workspace
for catalog commands, otherwise the current workspace — or the only one —
applies; `-y/--yes` confirms destructive actions. `add`/`remove` change a
registration or association and never touch remote data; `create`/`delete`
manage local entities; only `rm` deletes remote objects.

Short references expand to canonical strings first and resolve into
`RemoteUri` afterwards. Short-reference rules:

- `bucket/` or `connection/bucket/` is a directory reference.
- `bucket/key` is an exact object.
- Recursive operations require a directory reference or explicit
  `--recursive`.
- `-` reads one reference per line from stdin.
- `cp` routes by its arguments: `./`, `../`, `~/` and absolute paths are
  local; otherwise an argument is remote when its first segment names a
  registered connection or bucket. A first segment that is both registered
  and an existing local path is rejected as ambiguous.

## 5. Application API

The application exposes these context APIs:

```text
RegistryApi
CatalogApi
TransferApi
AccessApi
DiagnosticsApi
JobApi
ConfigApi
```

APIs accept typed commands, queries, and service input types, and return
domain types or application-facing pages, views, reports, and outcomes — for
example `FilesPage`, `TransferReport`, and `CancelReport`. Neither entry point
depends on repositories, SQLite, SQLx, or the AWS SDK. `ApplicationApi::events()`
streams job lifecycle and progress plus catalog and tag changes as
best-effort runtime messages.

## 6. Consistency

The provider is the source of truth; the local catalog is a cache of every
object of an indexed bucket plus any tagged object. Remote providers and SQLite
share no transaction, and none is simulated. A remote change flows through:

```text
job row (leased)
→ remote operation
→ catalog write-through (best-effort)
→ item state persisted
```

A succeeded remote change is never reported as failed because the cache could
not follow; the item carries a note, and listings or index refreshes correct
the row. Tags are user data: index toggles, rebuilds, and GC never drop them.

SQLite runs in WAL mode with foreign keys and a busy timeout. Writes go
through a single-connection pool (multi-statement writes in `BEGIN IMMEDIATE`
transactions); reads use a read-only pool. Several processes may share one
database; running jobs hold leases renewed by a heartbeat, so startup
recovery interrupts only jobs whose process died, and a cancel request from
another process reaches the owner at its next heartbeat.

## 7. Security

- Secrets are managed as `Secret<T>`, stored alongside the connection row in
  SQLite, and never appear in exports, generic API JSON, or logs. The CLI's
  `connection show` masks all but the first and last characters.
- TLS verification defaults to on.
- `--tls-verify=false` currently persists the setting only; the adapter does
  not yet install a custom verifier.
- Logs, JSON output, error contexts, and diagnostics must not leak secrets or
  signed URLs.
- Deletes, overwrites, and recursive operations pass their bucket policy
  checks (read-only, allow-delete) first.
- Destructive commands in non-interactive environments require explicit
  `--yes`.

## 8. Output and errors

Human output goes to stdout; progress and logs go to stderr. JSON uses one
envelope:

```json
{
  "command": "files.list",
  "ok": true,
  "data": {},
  "warnings": [],
  "errors": [],
  "meta": { "generated_at": "2026-08-13T18:20:31Z" }
}
```

Core errors carry a stable error code, a retryable flag, a source chain, and
structured context internally; the CLI JSON envelope exposes code, message,
and retryable. Exit codes map centrally, with a dedicated code for partially
failed batches and 130 for interruptions.

## 9. Observability and recovery

- Jobs persist at creation time, before any work starts, and persist each
  item as it settles.
- Startup marks jobs whose lease lapsed interrupted; live jobs of other
  processes keep running.
- Multipart checkpoints record source size, mtime, and completed parts.
- A remote success whose cache write failed carries a note in the item
  detail; `doctor` reports stale catalog rows.

## 10. Non-goals

- Full GPUI desktop pages and interactions.
- Event sourcing.
- Microservice decomposition.
- Multi-device synchronization.
- Additional protocols such as WebDAV or SFTP.
- Background agents or daemons.
- A cross-provider trash can.
- Writing rstore-private metadata into remote buckets.
