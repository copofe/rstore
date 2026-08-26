# CONTEXT.md — Domain Glossary

This file defines the terms used in design discussions and code review, in
one place. Each term has exactly one meaning; the architecture guide
([`docs/architecture-guide.md`](docs/architecture-guide.md)) explains how the
terms map to crates. Entries are grouped by concern.

## Composition and startup

- **Composition root** — `crates/bootstrap`. The only layer allowed to choose
  concrete SQLite, storage adapter, and outbox implementations; the only
  production code that opens the `Database`.
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
- **Outbox / Repair** — two separate durable lifecycles: the outbox records
  remote-change facts for delivery, while the repair store records queryable
  follow-up failures. `OutboxStore` owns append, ordering, claiming,
  acknowledgement, retry, and dead-lettering; `RepairStore` owns repair
  insert, list, and resolve, backed by the SQLite `RepairRepository` adapter.
  Repair identity is deterministic: event-driven repairs (delivery
  dead-letters, verification divergences) derive their `RepairId` from the
  outbox event, receipt-driven repairs (settlement/projection failures) from
  the mutation receipt, so re-recording the same cause upserts one row. One
  write path (`RepairStore::insert`) serves every producer; the delivery
  policy — backoff schedule, attempt ceiling, dead-letter decision — lives
  entirely in `OutboxStore::retry`, as the pure `delivery_decision` function
  plus its transactional write. Dead-letter status and repair insertion stay
  atomic through the shared `upsert_repair` statement.
- **Shared SQL statements** — cross-repository writes share free functions,
  not repository methods: `upsert_object_on` (remote-object upsert, used by
  the catalog repo and the index run's page commit) and
  `upsert_checkpoint_on` (index-checkpoint upsert, used by
  `save_checkpoint` and the page commit). The shared thing at the seam is
  the SQL statement, not a repository identity — an aggregate's
  transaction borrows statements, never another repository's interface.
- **RemoteMutationReceipt** — the durable fact that a remote operation
  succeeded, committed in the same transaction as its outbox fact.
- **Storage / StorageFactory** — the provider-neutral storage port and its
  factory port (`crates/storage-api`). `Storage::copy` takes locators only:
  the adapter stats the source itself and owns its copy strategy; the
  source bucket must be reachable through the adapter (adapters are built
  per connection, so same-connection copies address any bucket the
  connection sees; cross-connection copies relay through the host).
- **Remote copy entry (`transfer::copy`)** — the one entry that executes a
  remote-to-remote copy: stat the source, apply the conflict decision (the
  probe and rename re-entry live here and nowhere else), route
  same-connection copies to provider-side `Storage::copy` and
  cross-connection copies to the internal relay, and report through
  `ItemOutcome`, including dry-run previews. Guarded adapters from the
  gateway remain the write-guard proof; settlement lives with the
  copy/move settlement module.
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
  (`Cancelled` interrupts the run), and the single terminal-state commit.
  Services supply per-item execution plus one `SettleHook` implementation
  each — upload/download, copy/move, and removal are three adapters over
  the same seam; they no longer call settlements inline.
- **Copy/move settlement (`settlement`)** — the module that owns everything
  between a succeeded copy item and its final step: rederives the landed
  target key from the adapter metadata, settles through the mutation
  coordinator's shared primitive (receipt/outbox unit plus catalog
  projection), mirrors tags, and — for moves, marked by the item's
  `ItemKind` — verifies the target then deletes the source through the
  item's guard-issued adapters, revising the verdict on failure. The run
  loop's settle hook is a one-line handoff; the mutation coordinator is the
  shared settlement machine: one settlement primitive
  (`settle_confirmed`) behind every confirmed remote mutation — upload,
  delete, and copy alike — plus the single follow-up failure entry
  (`note_failure`) that copy-specific steps use to record repairs.

## Multipart sessions

- **Multipart session (`MultipartSession`)** — the provider-side session port
  on `storage-api`: `Storage::create_multipart` / `adopt_multipart` return a
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
  retry semantics.
