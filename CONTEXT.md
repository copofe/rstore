# CONTEXT.md — Domain Glossary

This file defines the terms used in design discussions and code review, in
one place. Each term has exactly one meaning; the architecture guide
([`docs/architecture-guide.md`](docs/architecture-guide.md)) explains how the
terms map to crates. Entries are grouped by concern.

## Composition and startup

- **Composition root** — `crates/bootstrap`. The only layer allowed to choose
  concrete SQLite, storage adapter, outbox, and notification implementations;
  the only production code that opens the `Database`.
- **Assembly point (`assemble`)** — `rstore_persistence::assemble(db,
  storage_factory) -> ApplicationDependencies`. The single function that
  turns an opened `Database` (which carries the injected clock) plus an
  optional storage factory into the application dependency graph; the group
  structs and port aliases live in `rstore_application_ports::deps`.
  Bootstrap and the application test fixture both go through it — neither
  re-transcribes the dependency list.
- **Write gate** — `rstore_persistence::tx::WriteGate`. The write pool and
  the bounded writer actor as an inseparable pair, handed out only by
  `Database::gate()`. Multi-statement repository writes go through
  `gate.write_unit(label, work)` (the permit and the transaction are acquired
  together, so re-entrant deadlock is impossible by construction); single
  statements take `gate.permit()`. Repository constructors receive only
  `(gate, read_pool)` — no construction path can bypass the shared gate.
- **Startup sequence (`Runtime::open_locked`)** — the complete process-startup
  order: create data directories → take the cross-process write lock → open
  the database (with the configured clock) → run migrations →
  `Application::initialize` (startup recovery: orphaned non-terminal jobs are
  marked interrupted, outbox events stuck mid-processing return to the due
  queue) → drain the due outbox through the wired remote-verify consumer. The
  returned `Runtime` holds the lock guard for the process lifetime.
- **Construction vs recovery** — `Application::from_dependencies` only wires
  services and stays deterministic; `Application::initialize` applies startup
  recovery rules. Deliberately separate so tests can construct without
  recovering.
- **Process write lock (`AppLock`)** — the cross-process mutex from
  `crates/platform`. The platform crate does not depend on domain; mapping a
  `LockError` to a `domain::Error` happens only in bootstrap.

## Core domains

- **Registry / Catalog / Transfer / AccessDiagnostics** — the four bounded
  contexts of the domain model.
- **Connection** — one S3-compatible endpoint's configuration plus
  credentials. Credentials are stored in the connection row and held in
  memory as `Secret<T>`.
- **Bucket** — a remote bucket registered under a connection, carrying the
  `read_only` and `allow_delete` guard bits. The gateway enforces them at
  the seam by issuing guarded adapters for mutations — never raw ones.
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
- **RemoteObject** — the local projection of one remote object in the
  catalog, including its tombstone lifecycle.
- **Job** — the persisted intent and state machine of one batch run
  (pending → running → …); no job survives its process.
- **Outbox / Repair** — durable recording of remote-change facts, and the
  queryable record of follow-up failures. Repair identity is deterministic:
  event-driven repairs (delivery dead-letters, verification divergences)
  derive their `RepairId` from the outbox event, receipt-driven repairs
  (settlement/projection failures) from the mutation receipt, so
  re-recording the same cause upserts one row. One write path
  (`RepairRepository::insert`) serves every producer; the delivery policy —
  backoff schedule, attempt ceiling, dead-letter decision — lives entirely
  in `OutboxStore::retry`.
- **RemoteMutationReceipt** — the durable fact that a remote operation
  succeeded, committed in the same transaction as its outbox fact.
- **Storage / StorageFactory** — the provider-neutral storage port and its
  factory port (`crates/storage-api`).
- **Remote copy entry (`transfer::copy`)** — the one entry that executes a
  remote-to-remote copy: stat the source, apply the conflict decision (the
  probe and rename re-entry live here and nowhere else), route
  same-connection copies to provider-side `Storage::copy` and
  cross-connection copies to the internal relay, and report through
  `ItemOutcome`, including dry-run previews. Guarded adapters from the
  gateway remain the write-guard proof; settlement stays with the mutation
  coordinator.

## Batch execution

- **Run loop (`job_run::run_items`)** — the one batch execution harness for
  upload, download, copy, move, and removal jobs. It owns the verdict
  vocabulary (`ItemVerdict` → `Step` → report item), the settle-on-success
  hook (every successful remote mutation is settled exactly here, before it
  is counted; a settlement may revise the verdict, as a failed move
  verification does), the one cancellation interpretation (`Cancelled`
  interrupts the run), and the single terminal-state commit. Services supply
  per-item execution and settlement; they no longer call settlements
  inline.

## Multipart sessions

- **Multipart session (`MultipartSession`)** — the provider-side session port
  on `storage-api`: `Storage::create_multipart` / `adopt_multipart` return a
  handle owning part upload (`upload_part`), server-side part copy
  (`copy_part`, default `CapabilityUnsupported`; S3 overrides with
  UploadPartCopy), `complete`, and `abort`. Three failure dispositions ride
  the type and exist exactly once: `complete` never aborts (a checkpointed
  caller resumes), `abort` is strict (failure surfaces),
  `abandon(self)` is warn-only teardown for callers without a resume path.
  The retry decorator wraps sessions transparently, preserving per-part
  retry semantics.
