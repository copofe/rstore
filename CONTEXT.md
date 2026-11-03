# CONTEXT.md — Domain Glossary

This file defines the terms used in design discussions and code review, in
one place. Each term has exactly one meaning; the architecture guide
([`docs/architecture-guide.md`](docs/architecture-guide.md)) explains how the
terms map to crates. Entries are grouped by concern.

## Composition and startup

- **Runtime (`engine::Runtime`)** — the process-startup sequence and the only
  production code that opens SQLite and selects the storage factory:
  `Runtime::open_in(dirs)` creates the data directories, opens and migrates the
  database (carrying the configured clock), wires the engine, and runs startup
  recovery. There is no process lock; several runtimes may share one data
  directory.
- **Wiring (`Application::new`)** — the one place every service is assembled
  over the opened store. Construction is deterministic;
  `Application::initialize` applies startup recovery separately so a caller can
  construct without recovering.
- **Startup recovery** — pending or running jobs whose lease lapsed become
  `interrupted`. Jobs another live process owns are left alone.
- **Write gate (`db::tx::WriteGate`)** — the write side of the store: a
  single-connection pool, so in-process writes serialise on it while WAL and
  the busy timeout arbitrate between processes. Multi-statement writes run in
  `BEGIN IMMEDIATE` transactions through `write_unit`.
- **Store (`engine::db`)** — the SQLite repositories, private to the engine.
  SQLite is the application's file format, so use cases call repositories
  directly; no single-implementation trait sits in between.

## Core domains

- **Registry / Catalog / Transfer / AccessDiagnostics** — the four bounded
  contexts of the domain model.
- **Outcome vocabulary (`domain::outcome`)** — the one classification
  vocabulary for batch results, owned by the lowest shared layer:
  `Disposition` (succeeded / skipped / blocked / failed, with typed
  `SkipReason` and `BlockReason`), the `Failure` projection, `Severity`,
  and the single aggregation rule `severity_of`. The transfer engine's
  `ItemOutcome`, the run loop's verdicts, non-transfer batches (links,
  tags, removal), and renderers all speak it — no layer re-translates
  booleans into reasons, and no module keeps a private copy of the
  severity rule.
- **Connection** — one S3-compatible endpoint's configuration plus
  credentials. Credentials are stored in the connection row and held in
  memory as `Secret<T>`.
- **Bucket** — a remote bucket registered under a connection, carrying the
  `read_only` and `allow_delete` guard bits. The gateway enforces them at
  the seam by issuing guarded adapters for mutations — never raw ones.
- **Remote gateway (`RemoteGateway`)** — the one path from a reference to a
  registered bucket and a connected adapter: connection reference → config →
  registered bucket → retry-decorated adapter, plus the guarded `writer` /
  `deleter` issuance. Every service resolves remote targets through it, so a
  broken connection or an unregistered bucket fails the same way everywhere.
- **Guarded adapter** — the storage decorator `RemoteGateway::writer` /
  `deleter` issue over the retry-decorated adapter. The write methods
  (`put`, `copy`, `delete`, `create_multipart`, `adopt_multipart`) check
  the guard bits at call time and fail `Forbidden` (`delete` additionally
  requires `allow_delete`); reads (`stat`, `read`, `list`, `list_buckets`,
  `presign`) always pass. The guard wraps outside retry, so a refusal is
  never retried; multipart sessions need no extra guarding because the
  door — `create/adopt_multipart` — already passed it. Proving the guard
  and holding the execution adapter are the same object; no business
  mutation path holds a raw adapter. Connection diagnostics probe
  credentials, not bucket policy, and use the unguarded adapter by
  definition.
- **Workspace** — a local grouping of buckets that scopes catalog rows,
  tags, and ignore rules.
- **Current workspace** — the workspace selected in the `workspace.current`
  setting. `WorkspaceService::target` resolves an omitted workspace
  reference: explicit reference → current → the only workspace → error.
  The first workspace created becomes current; enrolling a bucket while no
  workspace exists creates and selects `default`.
- **Onboarding** — `RegistryApi::onboarding()`: registering buckets as one
  use case — add each bucket, then enroll it into the target workspace —
  plus `unregistered`, the visible buckets not yet registered. `bucket add`
  and `connection add` both go through it.
- **Reference grammar (`References`)** — `RegistryApi::references()`: the
  one expansion of `[<connection>/]<bucket>/[key]` into a `RemoteUri` bound
  to the connection id, and `route`, which classifies transfer
  arguments as local or remote (`./`, `../`, `~/`, absolute → local;
  registered first segment → remote; registered *and* present locally →
  ambiguous error) and yields upload, download, or copy.
- **Catalog** — the local cache of remote state: every object of an indexed
  bucket plus any tagged object. The provider is the source of truth; the
  catalog may be stale, never invented. Listings and index refreshes correct
  it.
- **RemoteObject** — one cached row of the catalog, `confirmed` or `stale`
  (not seen by the last complete refresh).
- **Indexed bucket** — a bucket whose objects the catalog holds in full, making
  it searchable by name and attributes. Opt-in, off by default; naming a
  bucket in `index run` switches it on, and switching it off drops the
  bucket's untagged rows.
- **Tag association** — a workspace tag on the object at `(bucket, path)`
  (`object_tags`). Independent of indexing; attaching first confirms the object
  exists. Tags are user data: GC, rebuilds, and index toggles never drop them;
  rstore's own `rm` clears them and `mv` carries them.
- **Write-through (`WriteThrough`)** — how the catalog follows what the
  provider just confirmed: a landed object is upserted when its bucket is
  indexed or the object is tagged or already cached; a deleted object loses its
  row and tags; an `ls --catalog` page of an indexed bucket refreshes its rows.
  Best-effort: a failed cache write becomes a note on the item (or a listing
  that did not update the catalog), never a failed remote operation. The same
  module owns `confirmed_object`, the one mapping from provider metadata to a
  cache row.
- **Job** — the persisted intent and state machine of one batch run
  (pending → running → …). Its items are persisted as they settle, so other
  processes see the run advance.
- **Job lease** — `lease_owner` plus `lease_expires_at` on a running job,
  renewed by the owning process's heartbeat every 10 seconds (30-second
  lease). A lapsed lease marks the job orphaned.
- **Cancel request** — `cancel_requested` on a job another process owns; its
  heartbeat turns the request into a cancel and the run ends `cancelled`.
- **Live jobs (`LiveJobs`)** — the registry of jobs this process runs, by id;
  a cancel for one of them trips its token immediately.
- **EngineEvent** — a best-effort runtime message on the broadcast stream from
  `ApplicationApi::events()`: job started / progress / finished, catalog
  changed for a bucket, tags changed for a workspace. Not a durable fact,
  never replayed; subscribers re-query the API.
- **Storage / StorageFactory** — the provider-neutral storage trait and its
  factory (`crates/storage`, with the S3 adapter in `storage::s3`).
  `Storage::copy` takes locators only: the adapter stats the source itself
  and owns its copy strategy; the source bucket must be reachable through the
  adapter (adapters are built per connection, so same-connection copies
  address any bucket the connection sees; cross-connection copies relay
  through the host).
- **Remote copy entry (`transfer::copy`)** — the one entry that executes a
  remote-to-remote copy: stat the source, apply the conflict decision (the
  probe and rename re-entry live here and nowhere else), route
  same-connection copies to provider-side `Storage::copy` and
  cross-connection copies to the internal relay, and report through
  `ItemOutcome`, including dry-run previews. Guarded adapters from the
  gateway remain the write-guard proof; settlement lives with the copy
  service's `CopySettle`.
- **Run context (`transfer::run`)** — the vocabulary of one transfer run,
  owned by no single entry: `TransferConfig` (tuning), `RunContext`
  (cancel + config + progress, constructed once per run and carried through
  engine, copy entry, relay, and pump — no layer re-assembles the trio),
  and `ItemOutcome` (the per-item conclusion). The module is the home for
  run-level types so the copy entry does not depend on the upload/download
  engine module for its vocabulary.

## Batch execution

- **Run loop (`job_run::run_items`)** — the one batch execution harness for
  upload, download, copy, move, and removal jobs. It owns the verdict
  vocabulary (`ItemVerdict` → `Step` → report item), the settle-on-success
  seam (`SettleHook<T>`: every successful remote mutation is settled
  exactly here, before it is counted; a settlement may revise the verdict,
  as a failed move verification does), the one cancellation interpretation
  (`Cancelled` interrupts the run; a cancel request ends it `cancelled`),
  the job lease and its heartbeat, per-item persistence, the job events, and
  the single terminal-state commit. Services allocate the job with
  `JobEnv::begin` and supply per-item execution plus one `SettleHook`
  implementation each — upload/download, copy/move, and removal.
- **Copy/move settlement (`CopySettle` in `copy`)** — the settle hook that
  owns everything between a succeeded copy item and its final step: rederives the landed
  target key from the adapter metadata, mirrors tags, lets the catalog follow
  through the write-through, and — for moves, marked by the item's
  `ItemKind` — verifies the target then deletes the source through the
  item's guard-issued adapters, revising the verdict on failure (the source
  is kept). Cache notes ride in the item detail.

## Multipart sessions

- **Multipart session (`MultipartSession`)** — the provider-side session trait
  in `storage`: `Storage::create_multipart` / `adopt_multipart` return a
  handle owning part upload (`upload_part`), server-side part copy
  (`copy_part`, default `CapabilityUnsupported`; S3 overrides with
  UploadPartCopy; its production consumer is the S3 adapter's
  oversized-copy walk, `copy_parts_loop`, which drives it through the
  session trait object once a copy exceeds the single-shot limit),
  `complete`, and `abort`. Three failure dispositions ride
  the type and exist exactly once: `complete` never aborts (a checkpointed
  caller resumes), `abort` is strict (failure surfaces),
  `abandon(self)` is warn-only teardown for callers without a resume path.
  The retry decorator wraps sessions transparently, preserving per-part
  retry semantics. Upload checkpoints persist in the store's
  `MultipartStore`; sessions never touch them.
