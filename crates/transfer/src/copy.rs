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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstore_domain::id::ConnectionId;
    use rstore_domain::policy::ConflictPolicy;
    use rstore_domain::remote::ObjectPath;
    use rstore_test_support::FakeStorage;

    use super::*;
    use crate::cancel::CancellationToken;
    use crate::progress::{NullProgressSink, ProgressSink};
    use crate::run::TransferConfig;

    fn conn() -> ConnectionId {
        ConnectionId::new_random()
    }

    fn key(path: &str) -> ObjectPath {
        ObjectPath::new(path).unwrap()
    }

    fn endpoints<'a>(
        source: &Arc<dyn Storage>,
        target: &Arc<dyn Storage>,
    ) -> (CopyEndpoint<'a>, CopyEndpoint<'a>) {
        (
            CopyEndpoint::new(source.clone(), conn(), "src"),
            CopyEndpoint::new(target.clone(), conn(), "dst"),
        )
    }

    async fn run<S: Storage + 'static>(
        source: &Arc<S>,
        target: &Arc<S>,
        src_key: &ObjectPath,
        dst_key: &ObjectPath,
        conflict: ConflictPolicy,
        dry_run: bool,
    ) -> rstore_domain::Result<ItemOutcome> {
        let source: Arc<dyn Storage> = source.clone();
        let target: Arc<dyn Storage> = target.clone();
        let (src, dst) = endpoints(&source, &target);
        copy(
            src,
            dst,
            &CopyRequest::new(src_key, dst_key, conflict),
            dry_run,
            &RunContext::new(
                &CancellationToken::new(),
                TransferConfig::default(),
                &NullProgressSink,
            ),
        )
        .await
    }

    #[tokio::test]
    async fn same_connection_routes_provider_side() {
        let storage = Arc::new(FakeStorage::new());
        storage.seed("src", "a.txt", b"alpha", None);
        let endpoint: Arc<dyn Storage> = storage.clone();
        let conn = conn();
        let src = CopyEndpoint::new(endpoint.clone(), conn, "src");
        let dst = CopyEndpoint::new(endpoint.clone(), conn, "dst");

        let outcome = copy(
            src,
            dst,
            &CopyRequest::new(&key("a.txt"), &key("b.txt"), ConflictPolicy::Overwrite),
            false,
            &RunContext::new(
                &CancellationToken::new(),
                TransferConfig::default(),
                &NullProgressSink,
            ),
        )
        .await
        .unwrap();

        assert!(outcome.disposition.is_success());
        assert_eq!(outcome.bytes, 5);
        assert!(outcome.metadata.is_some());
        storage.stat("dst", &key("b.txt")).await.unwrap();
    }

    #[tokio::test]
    async fn cross_connection_relays_through_the_host() {
        let source = Arc::new(FakeStorage::new());
        let target = Arc::new(FakeStorage::new());
        source.seed("src", "a.txt", b"beta", None);

        let outcome = run(
            &source,
            &target,
            &key("a.txt"),
            &key("b.txt"),
            ConflictPolicy::Overwrite,
            false,
        )
        .await
        .unwrap();

        assert_eq!(outcome.bytes, 4);
        let landed = target.stat("dst", &key("b.txt")).await.unwrap();
        assert_eq!(landed.size, 4);
    }

    #[tokio::test]
    async fn missing_source_fails_even_in_a_dry_run() {
        let source = Arc::new(FakeStorage::new());
        let target = Arc::new(FakeStorage::new());

        let error = run(
            &source,
            &target,
            &key("absent.txt"),
            &key("b.txt"),
            ConflictPolicy::Overwrite,
            true,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, rstore_domain::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn dry_run_previews_the_final_key_without_writing() {
        let source = Arc::new(FakeStorage::new());
        let target = Arc::new(FakeStorage::new());
        source.seed("src", "a.txt", b"x", None);

        let outcome = run(
            &source,
            &target,
            &key("a.txt"),
            &key("b.txt"),
            ConflictPolicy::Overwrite,
            true,
        )
        .await
        .unwrap();

        assert_eq!(
            outcome.disposition,
            rstore_domain::outcome::Disposition::Skipped(
                rstore_domain::outcome::SkipReason::DryRun
            )
        );
        assert_eq!(
            outcome.detail.as_deref(),
            Some("would copy to dst/b.txt"),
            "the preview names the planned target"
        );
        assert!(target.stat("dst", &key("b.txt")).await.is_err());
    }

    #[tokio::test]
    async fn skip_policy_finishes_without_a_transfer() {
        let source = Arc::new(FakeStorage::new());
        let target = Arc::new(FakeStorage::new());
        source.seed("src", "a.txt", b"new", None);
        target.seed("dst", "b.txt", b"original", None);

        let outcome = run(
            &source,
            &target,
            &key("a.txt"),
            &key("b.txt"),
            ConflictPolicy::Skip,
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            outcome.disposition,
            rstore_domain::outcome::Disposition::Skipped(
                rstore_domain::outcome::SkipReason::Conflict
            )
        );
        let kept = target.stat("dst", &key("b.txt")).await.unwrap();
        assert_eq!(kept.size, 8);
    }

    #[tokio::test]
    async fn rename_policy_derives_a_free_suffixed_key() {
        let source = Arc::new(FakeStorage::new());
        let target = Arc::new(FakeStorage::new());
        source.seed("src", "a.txt", b"new", None);
        target.seed("dst", "doc.txt", b"original", None);

        let outcome = run(
            &source,
            &target,
            &key("a.txt"),
            &key("doc.txt"),
            ConflictPolicy::Rename,
            false,
        )
        .await
        .unwrap();

        assert!(outcome.disposition.is_success());
        let page = target
            .list(
                "dst",
                &rstore_storage_api::model::ListOptions {
                    prefix: Some("doc-".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let keys: Vec<&str> = page
            .objects
            .iter()
            .map(|o| o.metadata.key.as_str())
            .collect();
        assert_eq!(keys.len(), 1, "one renamed target, got {keys:?}");
        let renamed = keys[0];
        assert!(renamed.starts_with("doc-") && renamed.ends_with(".txt"));
        let hex = &renamed["doc-".len()..renamed.len() - ".txt".len()];
        assert_eq!(hex.len(), 8);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn unresolved_ask_on_existing_target_blocks() {
        let source = Arc::new(FakeStorage::new());
        let target = Arc::new(FakeStorage::new());
        source.seed("src", "a.txt", b"new", None);
        target.seed("dst", "b.txt", b"original", None);

        let outcome = run(
            &source,
            &target,
            &key("a.txt"),
            &key("b.txt"),
            ConflictPolicy::Ask,
            false,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome.disposition,
            rstore_domain::outcome::Disposition::Blocked(_)
        ));
        let kept = target.stat("dst", &key("b.txt")).await.unwrap();
        assert_eq!(kept.size, 8);
    }

    /// One sink recording byte reports, so the provider-side route's single
    /// completion report is observable.
    #[derive(Default)]
    struct RecordingSink(parking_lot::Mutex<Vec<(u64, u64)>>);

    impl ProgressSink for RecordingSink {
        fn report(&self, progress: Progress) {
            if let Progress::Bytes { done, total } = progress {
                self.0.lock().push((done, total));
            }
        }
    }

    #[tokio::test]
    async fn provider_side_copy_reports_moved_bytes_once() {
        let storage = Arc::new(FakeStorage::new());
        storage.seed("src", "a.txt", b"12345", None);
        let sink = RecordingSink::default();

        let endpoint: Arc<dyn Storage> = storage.clone();
        let conn = conn();
        let src = CopyEndpoint::new(endpoint.clone(), conn, "src");
        let dst = CopyEndpoint::new(endpoint.clone(), conn, "dst");
        copy(
            src,
            dst,
            &CopyRequest::new(&key("a.txt"), &key("b.txt"), ConflictPolicy::Overwrite),
            false,
            &RunContext::new(&CancellationToken::new(), TransferConfig::default(), &sink),
        )
        .await
        .unwrap();

        assert_eq!(sink.0.lock().as_slice(), [(5, 5)]);
    }
}
