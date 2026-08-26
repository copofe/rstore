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

The current desktop entry point wires bootstrap and obtains the application
API only; job notifications and full pages are deferred.

## 3. Domain scope

### 3.1 Registry

- A **Connection** stores its name, S3 API endpoint, public/CDN delivery
  base defaults, region, addressing style, TLS policy, and credential fields.
- A **Bucket** stores connection ownership, name, read/write policy, index
  policy, concurrency override, and conflict default.
- A **LinkProfile** stores bucket-level public/CDN base overrides, presign
  permissions, and the default TTL.
- Effective link base precedence: bucket CDN override → connection CDN
  default → bucket public override → connection public default → native S3
  endpoint. rstore never guesses URL shapes from the provider name.
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
  bucket policy checks, and tombstones.
- Ctrl+C persists an observable interrupted state; re-running the original
  command resumes from checkpoints. The `task` commands never steer a live
  transfer inside another process.

### 3.4 Access diagnostics

- Connection testing checks stored credentials, connectivity, and optionally
  performs a write probe.
- Capability probes cover listing, multipart upload, server-side copy, and
  presigning.
- `doctor` summarizes connection health, adapter identity, job, outbox, and
  repair status.
- Unsupported capabilities return a distinct `CapabilityUnsupported` error.

## 4. CLI commands

```text
rstore connection add|list|show|edit|test|remove
rstore bucket discover|add|list|show|edit|remove
rstore bucket ignore add|list|remove
rstore bucket link show|set
rstore workspace create|list|show|edit|remove
rstore workspace bucket add|remove|list
rstore remote ls|stat
rstore index start|status|stats|pause|resume|cancel|refresh|rebuild
rstore files list|search|show
rstore tag create|list|edit|remove|merge|add|detach
rstore upload
rstore download
rstore cp
rstore mv
rstore rm
rstore task list|show|pause|resume|retry|cancel
rstore url
rstore catalog gc
rstore config show|export|import
rstore doctor
rstore version
```

Short references expand to canonical strings first and resolve into
`RemoteUri` afterwards; only flows that need stable catalog/outbox identity
construct a `RemoteLocator`. Short-reference rules:

- `bucket/` or `connection/bucket/` is a directory reference.
- `bucket/key` is an exact object.
- Recursive operations require a directory reference or explicit
  `--recursive`.
- `-` reads one reference per line from stdin.

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
example `FilesPage`, `TransferReport`, and `RepairItem`. Neither entry point
depends on repositories, SQLite, SQLx, the AWS SDK, or concrete service
implementations.

## 6. Consistency

SQLite writes serialize through a single bounded writer actor; reads use a
read-only pool. WAL, foreign keys, busy timeouts, and transaction helpers
define local boundaries. `UnitOfWork` currently carries exactly one
operation: the remote mutation whose receipt and outbox fact commit together.

Remote providers and SQLite share no cross-system transaction; a remote
change flows through:

```text
job intent
→ remote operation
→ RemoteMutationReceipt + outbox fact
→ catalog follow-up
→ repair item on failure
```

Outbox events carry event-id idempotency, aggregate ordering, backoff retry,
failure records, and repair state. The first projection consumer re-verifies
each settled mutation against the provider at startup (`RemoteVerifyHandler`
through `Runtime::drain_outbox`): present objects acknowledge the fact, missing
ones become repair rows, transient failures stay queued. Progress events are
runtime-only and never become domain events.

## 7. Security

- Secrets are managed as `Secret<T>`, stored alongside the connection row in
  SQLite, and never appear in exports, generic API JSON, or logs. The CLI's
  `connection show` masks all but the first and last characters.
- TLS verification defaults to on.
- `--tls-verify=false` currently persists the setting only; the adapter does
  not yet install a custom verifier.
- Logs, JSON output, error contexts, and diagnostics must not leak secrets or
  signed URLs.
- Deletes, overwrites, and recursive operations pass their bucket policy and
  capability checks first.
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

- Jobs persist at creation time, before any work starts.
- Startup marks unresumable running jobs interrupted.
- Multipart checkpoints record source size, mtime, and completed parts.
- A remote success followed by a failed catalog follow-up keeps both the
  receipt and a repair item.
- `doctor`, `task list`, and `task show` surface pending repairs.

## 10. Non-goals

- Full GPUI desktop pages and interactions.
- Event sourcing.
- Microservice decomposition.
- Multi-device synchronization.
- Additional protocols such as WebDAV or SFTP.
- Background agents or daemons.
- A cross-provider trash can.
- Writing rstore-private metadata into remote buckets.
