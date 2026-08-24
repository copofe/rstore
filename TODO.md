# TODO

Every audit finding that required code changes has been fixed or confirmed
fixed; none remain open. This file keeps only what was deliberately **not**
changed, with reasons — so nobody re-litigates them from the stale audit
reports.

## Closed by recorded decision

- **TagStore import path** — `mutation.rs` imports via `rstore_catalog` on
  purpose: it holds the narrow membership handle (`CatalogTagPort`), not the
  wide workspace store. A comment pins this at the import site.
- **Test assembly location** — moving the SQLite fixture into
  `application-test-support` was attempted and reverted: consuming that crate
  from application's own unit tests duplicates the `Application` type across
  the dev-cycle. The feature-gated fixture plus self-dev-dependency is the
  correct shape; both Cargo manifests document why.
- **`as_token` vs `as_str`** — deliberate two-role convention: `as_str` is a
  closed set returning `&'static str`; `as_token` covers persisted tokens with
  dynamic values (e.g. `Region::specific(..)` returns `String`).
- **Config-import serde failure mapped `invalid_input("parse bundle")`** —
  correct as-is: a user-supplied file failing to parse is bad input (exit 2),
  distinct from internal serialization faults (`unknown`).
- **Retry backoff jitter** — dropped. rstore enforces one runtime per data
  directory via a cross-process lock, so concurrent clients hammering one
  bucket cannot occur.

## Deliberately deferred (documented, not debt)

- Outbox *resident* consumer loop — the first projection consumer
  (`RemoteVerifyHandler`) and a startup drain via `Runtime::drain_outbox` are
  wired; only the always-running task is left, and it needs a long-lived host
  process (desktop) to justify existing.
- `MultipartStore` living in `storage-api` — intentional placement so
  persistence never depends on transfer (documented in module headers).
