//! Existence probes and the single target reaction for the domain conflict
//! decision table: a storage handle (or the local filesystem) becomes the
//! probe `domain::conflict::resolve_conflict` consumes, and its decision is
//! interpreted once at a transfer target via [`decide_target`].

use std::sync::Arc;

use rstore_domain::conflict::{
    random_suffix8, resolve_conflict, ConflictProbe, Existence, Resolved,
};
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{ErrorCode, Result};
use rstore_storage::Storage;

use crate::transfer::run::ItemOutcome;

/// A storage-backed existence probe: a name is taken when the target bucket
/// holds an object with that key. A missing object reads `Free`; anything
/// else — timeouts, 5xx, auth failures — reads `Unknown`, so the decision
/// layer can retry once and then block the item instead of overwriting a
/// target it never verified.
pub fn stat_probe(storage: &Arc<dyn Storage>, bucket: &str) -> Box<ConflictProbe> {
    let storage = storage.clone();
    let bucket = bucket.to_string();
    Box::new(move |name: String| {
        let storage = storage.clone();
        let bucket = bucket.clone();
        Box::pin(async move {
            let Ok(key) = ObjectPath::new(&name) else {
                // An unparseable candidate cannot exist as an object.
                return Existence::Free;
            };
            match storage.stat(&bucket, &key).await {
                Ok(_) => Existence::Taken,
                Err(error) if error.code == ErrorCode::NotFound => Existence::Free,
                Err(_) => Existence::Unknown,
            }
        })
    })
}

/// A local-filesystem existence probe (download targets on disk).
pub fn path_probe() -> Box<ConflictProbe> {
    Box::new(|name: String| {
        Box::pin(async move {
            if std::path::Path::new(&name).exists() {
                Existence::Taken
            } else {
                Existence::Free
            }
        })
    })
}

/// The one reaction to a conflict decision at a transfer target: write —
/// under the policy's final name — or finish the item untouched. The rename
/// re-entry is a loop, so a renamed item does not re-run the caller's
/// preamble.
pub enum TargetDecision<T> {
    /// Write the item (the target differs from the given name when the policy
    /// renamed it).
    Write(T),
    /// The item is done without a transfer (skipped or blocked).
    Finish(Box<ItemOutcome>),
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
                return Ok(TargetDecision::Finish(Box::new(ItemOutcome::skipped(
                    detail,
                ))))
            }
            Resolved::Blocked(detail) => {
                return Ok(TargetDecision::Finish(Box::new(ItemOutcome::blocked(
                    detail,
                ))))
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
