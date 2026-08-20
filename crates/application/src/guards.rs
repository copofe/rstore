//! Per-item Core guards (SEC-004/005) and the one placement rule.
//!
//! Placement: a run whose every item writes one shared target bucket —
//! `upload`, whose `UploadPlan` names exactly one — guards once at batch
//! level (`DefaultTransferService::run_upload`) and fails the run before a
//! job exists. Runs whose items span buckets (`cp`/`mv`, `rm`) guard per
//! item, where a blocked guard is a per-item outcome carrying the guard's
//! detail, never a batch failure (MOVE-008 / DEL-002). The guard pair itself
//! — write targets must be writable, a bucket an operation deletes from must
//! be deletable — lives only here.

use crate::bucket::BucketService;
use rstore_domain::BucketId;

/// Run the per-item guard pair for one operation. `write` is the bucket
/// receiving data (`None` for `rm`, which only deletes); `delete` is the
/// bucket an operation removes from (moves, deletes). Returns the guard's
/// block detail when the item must not proceed.
pub async fn item_guards(
    buckets: &dyn BucketService,
    write: Option<BucketId>,
    delete: Option<BucketId>,
) -> Option<String> {
    if let Some(bucket) = write {
        if let Err(e) = buckets.assert_writable(bucket).await {
            return Some(e.to_string());
        }
    }
    if let Some(bucket) = delete {
        if let Err(e) = buckets.assert_deletable(bucket).await {
            return Some(e.to_string());
        }
    }
    None
}
