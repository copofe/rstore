# Changelog

All notable changes to rstore are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed — workbench architecture

- The workspace collapses to four library crates: `domain`, `storage`
  (provider-neutral `Storage` trait plus the S3 adapter), `engine` (use cases,
  SQLite store, transfer engine, jobs, events, `Runtime`), and `cli`.
  `application`, `application-ports`, `catalog`, `persistence`, `transfer`,
  `bootstrap`, `platform`, `storage-api`, and `storage-s3` are gone. Services
  are concrete types; single-implementation repository and service traits are
  removed.
- The catalog is a cache: every object of an indexed bucket plus any tagged
  object. Remote changes update it best-effort through `WriteThrough`; a
  failed cache write becomes an item note instead of a failure.
- Indexing is opt-in per bucket and off by default for new buckets; naming a
  bucket in `index run` switches its index on, switching it off drops the
  bucket's untagged rows. `find` lists the buckets it could not search
  (`unindexed`). `bucket add --no-index` is gone.
- Empty bucket listings and `bucket add` point at `connection add` while no
  connection is registered.
- `url` no longer presents the S3 endpoint as a public URL (R2, Supabase,
  and private buckets refuse unsigned reads there). A public URL needs a
  configured public or CDN base; without one, URL formats get a presigned
  link with the bucket's default TTL and a warning naming
  `bucket edit --public-base`.
- `conn test --write` writes only into registered buckets that allow writes
  and deletes, through the write guard; it used to pick the first visible
  bucket, read-only or not. It now checks each such bucket.
- `connection show --capabilities` is gone, with the capability report and
  the storage `Capability` model: nothing consumed it, listing duplicated
  `conn test`, presign only signed locally, and the multipart/copy probes
  wrote into user buckets. Unsupported operations still fail with
  `capability_unsupported`.
- A bucket's public/CDN base is that bucket's root; a connection's serves
  every bucket under `/<bucket>`. Bucket overrides no longer get the bucket
  name appended a second time.
- Guided bucket registration (`bucket add` without names, the bucket choice
  in `conn add`) asks each bucket's public URL, offering the provider's
  shape as an example (filled in for Supabase and AWS).
- Guided setup on a terminal: `connection add` asks for whatever is omitted
  (name, provider, region where the provider needs one, endpoint with the
  provider's default or shape, keys); `bucket add` and `workspace add`
  without arguments offer a choice; `workspace create` without a name asks
  for the name, description, and member buckets. Non-terminal and `--json`
  runs still require the arguments. Ctrl-D at any prompt cancels.
- Tags key on `(bucket, path)` (`object_tags`) and no longer require an
  indexed object; GC, rebuilds, and index toggles never drop them.
- `index gc` removes untagged stale rows; `--retention` is gone.
- No process lock: several processes share one data directory. Running jobs
  hold leases renewed by a heartbeat; startup recovery interrupts only jobs
  whose lease lapsed; `task cancel` reaches jobs in other processes (they end
  `cancelled`). Items persist as they settle. SQLite busy past the timeout
  maps to retryable `DatabaseBusy`; `LockBusy` is gone.
- Writes go through a single-connection pool with `BEGIN IMMEDIATE`
  transactions; the bounded writer actor is gone.
- Retry backoff uses equal jitter.
- Remote resolution has one home: `RemoteGateway` resolves references to
  registered buckets and issues retry-decorated (and guarded) adapters;
  `CatalogLocator` is gone. A bucket that is not registered under the named
  connection fails with the same message and hint everywhere.
- `ls --catalog` refreshes the cache best-effort: a cache write that fails no
  longer fails the listing, and a successful refresh publishes
  `CatalogChanged`.
- Files: the database is `rstore.db` (it holds everything, not just the
  catalog), under a plain `rstore` directory — macOS `~/Library/Application
  Support/rstore` with logs in `~/Library/Logs/rstore`, Linux
  `~/.local/share/rstore` with logs in `~/.local/state/rstore`, Windows
  `%LOCALAPPDATA%\rstore`. A `--home` / `RSTORE_HOME` root holds `rstore.db`
  and `log/` directly. The unused config and cache directories are gone;
  `config show` prints the database and log paths.
- The global `--config <PATH>` flag is now `--home <PATH>`.
- `doctor` drops the constant `tls_default` / `telemetry` fields; index
  reports and status drop the unused `catalog_revision` (and its table).
- The API groups are views over one shared service bundle;
  `DiagnosticsApi::storage_adapter` is gone (the adapter name is in the
  `doctor` report). `RuntimeConfig::storage_factory` is required.

### Added

- `ApplicationApi::events()`: an `EngineEvent` broadcast stream (job started,
  progress, finished; catalog changed; tags changed).

### Removed

- The receipt / outbox / repair chain (`UnitOfWork`, `OutboxStore`,
  `RepairStore`, `RemoteVerifyHandler`, `Runtime::drain_outbox`,
  `Command::reaches_remote`), deletion tombstones, and their tables; `doctor`
  reports stale catalog rows instead of outbox/repair counts. The `deleted`
  object state is gone.
- Schema migrations: the schema is one baseline again, with no upgrade path
  from earlier development databases (including the legacy multipart-column
  and pre-baseline history shims). Delete an existing data directory's
  `catalog.db` before running this build.

### Added (earlier)

- Apache-2.0 `LICENSE` file, CI workflow (format, clippy, tests, docs), and
  contributor documentation.
- Typed identifiers for receipts (`ReceiptId`) and repair items (`RepairId`),
  with closed `RepairKind`/`RepairStatus` enums replacing free-form strings.
- Ignore-rule rows carry a typed `IgnoreRuleId`.
- Health reporting counts connections, untested connections, and interrupted
  jobs through dedicated statistics queries instead of loading full lists.
- Engine-level guarantee: an unresolvable conflict-probe outage blocks the
  affected item instead of reading as "target absent".

### Changed

- **CLI command tree redesigned for fewer keystrokes** (breaking; see
  `docs/cli-guide.md`):
  - `upload`/`download`/`cp` merge into `cp`, which routes by its arguments;
    `remote ls`/`remote stat` become top-level `ls`/`stat` (`files show` is
    `stat --offline`); `files list`/`files search` become `find [PATTERN]`
    with tag, type, and extension filters.
  - Catalog commands no longer take a workspace argument: `-w/--workspace`
    (or `RSTORE_WORKSPACE`) overrides the current workspace selected with
    `workspace use`; the first workspace becomes current, and registering a
    bucket while none exists creates `default`.
  - `connection add` tests the connection and registers buckets
    (`--buckets all|a,b`, or an interactive choice); `--provider` presets the
    addressing style. `bucket add` takes `[<connection>/]<bucket>…` and joins
    the workspace; `bucket discover` is `bucket list --remote`; link-profile
    and ignore-rule subcommands fold into `bucket show`/`bucket edit`.
  - `tag add` creates missing tags; `tag detach` → `tag remove`, the old
    `tag remove` → `tag delete`; `tag create` is gone.
  - `workspace create|delete|add|remove|use`, `index run [--all|--refresh|
    --rebuild]`, `index gc` (was `catalog gc`), `index status` (absorbs
    `index stats`).
  - `-y/--yes` is global and only confirms destructive actions; overwriting
    on conflict is `-f` (`--conflict overwrite`). `-n` is `--dry-run`.
- Application API: reference expansion and transfer routing live on
  `RegistryApi::references()`; `WorkspaceService` gains
  `current`/`select`/`target`/`enroll`; `IndexService::run` takes an
  `IndexScope` and `IndexMode`; `TagService::attach_many` returns an
  `AttachReport`.
- Application API takes `RemoteUri` instead of reference strings (`ls`,
  `stat`, `plan_removals`, link generation, tag attach/detach, `files show`,
  `CopyPair`, `download_plans`, `conflict_policy`); `References::expand` and
  `route` return `RemoteUri`.
- Transfer runs share one signature, `(command, RunControl { progress,
  cancel })`; batch downloads take a `BatchDownloadCommand`. The run types
  (`CancellationToken`, `ProgressSink`, `TransferConfig`, …) are re-exported by
  `rstore-application`, so the CLI crates no longer depend on the transfer
  engine.
- `RegistryApi::onboarding()` registers buckets and enrolls them into a
  workspace as one use case (`bucket add` and `connection add` share it).
- Every `rstore-application` module is private; the crate-root re-exports
  are the whole public surface (gateway, dispatcher, and diagnostics service
  types are no longer exported).
- `DiagnosticsApi::health()` takes no path: the catalog location comes from
  the `RuntimeStats` port.
- The outbox drain is no longer part of `Runtime::open_locked`: entry points
  call `Runtime::drain_outbox(budget)`. The CLI drains for at most two
  seconds and only before commands that reach the remote anyway; a handler
  still running at the deadline counts as a failed attempt, and claimed
  events never started are released without spending one
  (`OutboxStore::release`).
- Outbox event ids are a typed `EventId`; receipts and repairs carry a typed
  `MutationOperation` and `RemoteLocator` instead of strings. Dead-letter
  repairs no longer put the event type into `operation`.
- `tag add` creates missing tags only once an object resolves; batch lines
  for `tag` and `url` name references as typed; `index run` lines read
  `<connection>/<bucket>`.

- **Unified batch outcome vocabulary** — every batch operation (`cp`/`mv`,
  `rm`, tag attach/detach, `url`) now reports per-item results through one
  `Disposition` type (succeeded / skipped with reason / blocked with reason /
  failed) and aggregates them into a `Severity` (`all_ok` / `partial` /
  `failed`). The partial-failure exit policy (exit 13) reads the severity from
  the application core instead of re-deriving it per command.
- Dry-run previews are now reported as `skipped` (reason: dry run) instead of
  `succeeded`; batch summaries count them accordingly.
- `rm` JSON output changed shape: items carry `target` + `disposition` instead
  of `uri` + `outcome`.
- **Guard enforcement moved to the gateway seam** — the gateway issues
  bucket-bound `Writer`/`Deleter` handles only when the bucket's `read_only`
  and `allow_delete` flags allow the intent. `cp`/`mv`/upload into a guarded
  bucket now fails fast with a `forbidden` error before any item runs
  (previously: N per-item `blocked` lines and exit 0). `rm` keeps per-item
  blocked lines because one batch may span buckets.
- All code comments and project documentation rewritten against a single
  English style standard; internal requirement IDs removed from comments.
- Update commands share one clearing convention (`Option<Option<T>>`:
  outer `None` keeps the value, inner `None` clears it) across connections,
  link profiles, tags, and workspaces.
- Batch commands exit non-zero when any item fails, including all-failed
  batches (previously only mixed batches exited non-zero).
- `catalog gc` and `tag merge` now enforce the destructive-command
  `--yes`/confirmation discipline like every other data-destroying command.
- Copy/move interruptions report exit 130 consistently with upload/download.
- S3 request timeouts map to the distinct `timeout` error code instead of
  collapsing into network-unreachable.
- Multipart checkpoints key on bare UUID connection identity, matching every
  other table's storage convention.
- Startup orphan-job recovery is a single bulk update; job listing loads all
  items in one extra query instead of one query per job.
- The cancellation token wakes waiters through an event-driven notification
  instead of a 10 ms poll.
- `connection test` exits 13 when any probe reports `Failed`, so scripts
  chaining on the command see the failure instead of a warning-only success.
- `Storage::copy` no longer ships a default implementation: the old
  read-whole-object-then-put fallback silently buffered arbitrary sizes, so
  every adapter now states its copy strategy explicitly.
- Download staging performs its identity sidecar I/O asynchronously.
- File sizes render through one human-size helper across list and show views;
  `-r` is accepted as the short form of `--recursive` on `rm`, `cp`, and `mv`
  as it already was on transfers.

### Fixed

- `connection test` reported `endpoint/auth` as passed for rejected
  credentials and failed for unreachable networks — inverted. It now passes
  only when the endpoint answered and accepted the credentials.
- Requests that never got a response (DNS failure, refused or dropped
  connection) were classified `unknown` and not retried; they are now
  `network_unreachable` (exit 6) and retryable.
- S3 requests could be retried by both the SDK and `RetryingStorage`,
  multiplying attempts; SDK retries are disabled.
- The startup drain summed per-cycle repair snapshots into
  `repair_required`; it now reports the latest snapshot.

### Added

- Outbox consumption is live: `RemoteVerifyHandler` re-stats every settled
  remote mutation when a runtime opens. Presence acknowledges the stored fact,
  absence records a repair item, transient provider faults stay queued for
  backoff retry.

### Removed

- `task pause|resume|retry` and `index pause|resume`: with one runtime per
  data directory they could only flip stored state. Ctrl+C stops a run;
  re-running the original command resumes from checkpoints.
- Job states `paused` and `retrying` and the transitions back into `running`:
  every run is a fresh job.
- Dead schema tables (`remote_object_revisions`, `index_errors`,
  `job_events`, `operation_audit`) and write-only tombstone columns.
- Unused port surface (`ConnectionStore::count`, `NoopUnitOfWork`,
  outbox dispatcher placeholder helpers, bootstrap accessors without
  callers), unused bounded-context facade modules, and the mis-homed job
  persistence port from the domain crate.
- The never-wired job notification seam (`JobNotification`,
  `Runtime::subscribe_job_notifications`/`publish_job_notification` and the
  desktop subscription) — nothing produced events and no consumer read them.
