# Changelog

All notable changes to rstore are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Removed

- Dead schema tables (`remote_object_revisions`, `index_errors`,
  `job_events`, `operation_audit`) and write-only tombstone columns.
- Unused port surface (`ConnectionStore::count`, `NoopUnitOfWork`,
  outbox dispatcher placeholder helpers, bootstrap accessors without
  callers), unused bounded-context facade modules, and the mis-homed job
  persistence port from the domain crate.
- The never-wired job notification seam (`JobNotification`,
  `Runtime::subscribe_job_notifications`/`publish_job_notification` and the
  desktop subscription) — nothing produced events and no consumer read them.
