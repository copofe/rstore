//! Remote-to-remote copy: the one entry that stats the source, applies the
//! conflict decision, and executes the copy — provider-side when both ends
//! share a connection, relayed through this host otherwise.
//!
//! The conflict reaction (probe, rename re-entry, skip/block finish) lives
//! here and in the engine — never at call sites. Guards, settlement, and
//! move finishing stay in the application layer; callers hand over resolved
//! endpoints and receive an [`ItemOutcome`].

use std::sync::Arc;

use rstore_domain::id::ConnectionId;
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;

use rstore_storage_api::model::{CopyRequest as StorageCopyRequest, ObjectMetadata};
use rstore_storage_api::Storage;

use crate::conflict::{decide_target, stat_probe, TargetDecision};
use crate::progress::Progress;
use crate::relay::{relay, RelayRequest};
use crate::run::{ItemOutcome, RunContext};

/// One end of a remote copy: the connected adapter, the connection it
/// belongs to, and the bucket name the keys resolve against.
pub struct CopyEndpoint<'a> {
    storage: Arc<dyn Storage>,
    connection: ConnectionId,
    bucket: &'a str,
}

impl<'a> CopyEndpoint<'a> {
    /// Binds an endpoint to its adapter, connection, and bucket name.
    /// Owns the adapter: the conflict probe clones it, and the relay borrow
    /// dies with the call.
    pub fn new(storage: Arc<dyn Storage>, connection: ConnectionId, bucket: &'a str) -> Self {
        Self {
            storage,
            connection,
            bucket,
        }
    }
}

/// The locators and conflict policy for one remote-to-remote copy.
pub struct CopyRequest<'a> {
    source_key: &'a ObjectPath,
    target_key: &'a ObjectPath,
    conflict: ConflictPolicy,
}

impl<'a> CopyRequest<'a> {
    /// Copies `source_key` onto `target_key` under `conflict`.
    pub fn new(
        source_key: &'a ObjectPath,
        target_key: &'a ObjectPath,
        conflict: ConflictPolicy,
    ) -> Self {
        Self {
            source_key,
            target_key,
            conflict,
        }
    }
}

/// Copies one object between two resolved endpoints.
///
/// The source is stated first, so a missing source fails the item even in a
/// dry run. The conflict decision runs against the target (probe and rename
/// re-entry included); a policy that finishes the item returns its outcome
/// untouched, a dry run returns a preview outcome, and only then does the
/// copy execute: provider-side through [`Storage::copy`] when both ends
/// share a connection, streamed through this host otherwise.
///
/// # Errors
/// Propagates adapter failures from the source stat and the copy itself;
/// the outcome's `bytes`/`metadata` describe the object the target reported.
pub async fn copy(
    source: CopyEndpoint<'_>,
    target: CopyEndpoint<'_>,
    request: &CopyRequest<'_>,
    dry_run: bool,
    ctx: &RunContext<'_>,
) -> Result<ItemOutcome> {
    let (cancel, progress) = (ctx.cancel(), ctx.progress());
    cancel.check()?;
    let source_meta: ObjectMetadata = source
        .storage
        .stat(source.bucket, request.source_key)
        .await?;

    let key = match decide_target(
        request.target_key.as_str(),
        request.conflict,
        stat_probe(&target.storage, target.bucket).as_ref(),
        |name| ObjectPath::new(name.to_owned()),
    )
    .await?
    {
        TargetDecision::Write(key) => key,
        TargetDecision::Finish(outcome) => return Ok(*outcome),
    };

    if dry_run {
        return Ok(ItemOutcome::preview(format!(
            "would copy to {}/{}",
            target.bucket,
            key.as_str()
        )));
    }

    let copied = if source.connection == target.connection {
        let copied = target
            .storage
            .copy(&StorageCopyRequest {
                source_bucket: source.bucket.to_string(),
                source_key: request.source_key.clone(),
                target_bucket: target.bucket.to_string(),
                target_key: key.clone(),
            })
            .await?;
        // The provider-side route never crosses the wire to this host, so
        // batch progress reports the moved size once, here.
        progress.report(Progress::Bytes {
            done: source_meta.size,
            total: source_meta.size,
        });
        copied
    } else {
        let relay_request = RelayRequest::new(
            source.bucket,
            request.source_key,
            target.bucket,
            &key,
            &source_meta,
        );
        relay(
            source.storage.as_ref(),
            target.storage.as_ref(),
            &relay_request,
            ctx,
        )
        .await?
    };

    Ok(ItemOutcome::transferred(
        source_meta.size.max(copied.size),
        Some(copied),
    ))
}
