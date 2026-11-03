# TODO

Every audit finding that required code changes has been fixed or confirmed
fixed; none remain open. This file keeps only what was deliberately **not**
changed, with reasons — so nobody re-litigates them from the stale audit
reports.

## Closed by recorded decision

- **`as_token` vs `as_str`** — deliberate two-role convention: `as_str` is a
  closed set returning `&'static str`; `as_token` covers persisted tokens with
  dynamic values (e.g. `Region::specific(..)` returns `String`).
- **Config-import serde failure mapped `invalid_input("parse bundle")`** —
  correct as-is: a user-supplied file failing to parse is bad input (exit 2),
  distinct from internal serialization faults (`unknown`).

## Deliberately deferred (documented, not debt)

- A `submit` / job-handle API that runs jobs as engine-owned tasks. Today a
  caller awaits the API call (the desktop spawns it); jobs are still
  observable through `EngineEvent` and cancellable by id from any process.
- Live-scan fallback for `find` over unindexed buckets. A bounded listing
  cannot merge into keyset pagination and sorting, so `find` reports the
  buckets it could not search (`unindexed`) instead.
- Index runs as jobs. Index runs keep their own persisted run/checkpoint
  aggregate; they publish `CatalogChanged` but are not leased jobs.
