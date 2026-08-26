# ADR 0001: MinIO integration tests stay local; CI does not provision MinIO

## Status

Accepted (2026-08-26)

## Context

`FakeStorage` simulates S3 multipart-legality rules (minimum 5 MiB non-final
parts, issued etags, ascending unique part numbers, the 1..=10000 part-number
range, `NoSuchUpload` semantics) so engine bugs fail offline. Until this ADR,
those rules were verified only by the fake's own unit tests — a fake testing
itself — while the real-adapter run of the shared `storage_contract` was
`#[ignore]`d behind `RSTORE_MINIO_ENDPOINT` and never executed by CI
(`.github/workflows/ci.yml` runs plain `cargo test --workspace`). Two adapters
sharing one seam with only one side's conformance ever checked is a drifting
fake-in-waiting.

The legality negatives now live in the shared contract
(`rstore_test_support::multipart_legality_negatives`), asserted with pinned
domain error codes (`InvalidInput` / `NotFound`); the S3 adapter's error
classification maps `InvalidPart` / `InvalidPartOrder` / `EntityTooSmall` /
`InvalidArgument` → `InvalidInput` and `NoSuchUpload` → `NotFound`, so the
fake's vocabulary is the contract's vocabulary. The MinIO test leg calls the
same function.

## Decision

CI stays MinIO-free. The real-adapter leg remains a local, opt-in run:

```sh
docker run -d --rm -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
RSTORE_MINIO_ENDPOINT=http://localhost:9000 \
  cargo test -p rstore-storage-s3 --test minio -- --ignored --nocapture
```

## Consequences

- Every `cargo test --workspace` run (local and CI) exercises the legality
  negatives against `FakeStorage`; the transcription is no longer self-tested.
- Real-adapter drift (a provider behavior change or adapter regression) is
  **not** caught automatically; it surfaces when a developer runs the local
  MinIO leg — run it before shipping storage-adapter changes.
- CI keeps zero external service dependencies and its current runtime profile.
- Revisit this ADR when either (a) a provider regression escapes to
  production, or (b) CI gains a service-container budget; adding a `minio`
  job is a one-step change (`services:` + `RSTORE_MINIO_ENDPOINT` + `--ignored`).
