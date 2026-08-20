//! Probe adapters and the one target reaction for
//! [`ConflictProbe`](rstore_domain::conflict::ConflictProbe): the one place a
//! storage handle (or the local filesystem) becomes the existence probe
//! `domain::conflict::resolve_conflict` consumes — and the one interpretation
//! of its decision at a transfer target. Engine upload and copy share
//! `stat_probe`; download targets use `path_probe`; all three funnel through
//! [`decide_target`].

use std::sync::Arc;

use rstore_domain::conflict::{random_suffix8, resolve_conflict, ConflictProbe, Resolved};
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;
use rstore_storage_api::Storage;

use crate::engine::ItemOutcome;

/// A storage-backed existence probe: a name collides when the target bucket
/// already holds an object with that key. Probe errors read as "no conflict"
/// at the decision layer — the write itself surfaces real failures.
pub fn stat_probe(storage: &Arc<dyn Storage>, bucket: &str) -> Box<ConflictProbe> {
    let storage = storage.clone();
    let bucket = bucket.to_string();
    Box::new(move |name: String| {
        let storage = storage.clone();
        let bucket = bucket.clone();
        Box::pin(async move {
            match ObjectPath::new(&name) {
                // An unparseable candidate cannot exist as an object.
                Err(_) => false,
                Ok(key) => storage.stat(&bucket, &key).await.is_ok(),
            }
        })
    })
}

/// A local-filesystem existence probe (download targets on disk).
pub fn path_probe() -> Box<ConflictProbe> {
    Box::new(|name: String| Box::pin(async move { std::path::Path::new(&name).exists() }))
}

/// The one reaction to a conflict decision at a transfer target: write —
/// under the policy's final name — or finish the item untouched. The rename
/// Rename re-entry is a loop, so a renamed item does not re-run the caller's
/// preamble.
pub enum TargetDecision<T> {
    /// Write the item (the target differs from the given name when the policy
    /// renamed it).
    Write(T),
    /// The item is done without a transfer (skipped or blocked).
    Finish(ItemOutcome),
}

/// Interpret `policy` at `name`, mapping the final name through `rename`.
/// A `Write` name that differs from `name` is re-resolved (at most once more:
/// the rename path only returns probe-verified free names).
pub async fn decide_target<T>(
    name: &str,
    policy: ConflictPolicy,
    probe: &ConflictProbe,
    rename: impl Fn(&str) -> Result<T>,
) -> Result<TargetDecision<T>> {
    let mut current = name.to_string();
    loop {
        match resolve_conflict(&current, policy, probe, random_suffix8)
            .await
            .interpret()
        {
            Resolved::Skipped(detail) => {
                return Ok(TargetDecision::Finish(ItemOutcome::skipped(detail)))
            }
            Resolved::Blocked(detail) => {
                return Ok(TargetDecision::Finish(ItemOutcome::blocked(detail)))
            }
            Resolved::Write(final_name) => {
                let target = rename(&final_name)?;
                if final_name == current {
                    return Ok(TargetDecision::Write(target));
                }
                current = final_name;
            }
        }
    }
}
