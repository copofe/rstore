//! Copy/move orchestration (MOVE-003..008, 012..014, 016).
//!
//! [`DefaultCopyService`] resolves `conn/bucket/key` source/target pairs,
//! applies
//! Core-level guards and the conflict policy, copies (server-side within one
//! connection, relayed across connections), and — for moves — deletes the
//! source only after verifying the target landed (MOVE-014). The catalog
//! follows every completion: the target is upserted, a moved source is
//! tombstoned (MOVE-016). Runs are persisted as `copy`/`move` jobs so
//! `task list` reports them like any other transfer.

use std::sync::Arc;

use rstore_domain::job::JobKind;
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::remote::ObjectPath;
use rstore_domain::JobRepository;
use rstore_domain::{Clock, Error, Result};
use rstore_storage_api::model::CopyRequest;
use rstore_transfer::{CancellationToken, ProgressSink, TargetDecision};

use crate::bucket::BucketService;
use crate::remote::RemoteUri;
use crate::transfer::{Outcome, TransferReport};

/// One `source → target` pair, both canonical `conn/bucket/key` references
/// (short `bucket/key` references are expanded by the CLI first). A target
/// key ending in `/` (or empty) keeps the source's file name:
/// `cp a.txt c/b/dir/` lands at `dir/a.txt`.
#[derive(Debug, Clone)]
pub struct CopyPair {
    pub source: String,
    pub target: String,
}

/// One fully resolved copy item: endpoints resolved once (through the shared
/// [`crate::batch_plan`] cache), keys typed.
struct Item {
    src: Arc<crate::batch_plan::Endpoint>,
    src_path: ObjectPath,
    dst: Arc<crate::batch_plan::Endpoint>,
    dst_path: ObjectPath,
    source_label: String,
    target_label: String,
}

/// Options for a copy/move run.
#[derive(Debug, Clone, Copy, Default)]
pub struct CopyOptions {
    pub dry_run: bool,
    pub conflict: ConflictPolicy,
    /// Expand directory-ish sources by listing their prefix (MOVE-009..011).
    pub recursive: bool,
    /// Carry tag associations from source to target (MOVE-015).
    pub keep_tags: bool,
}

/// Validate the target shape for a multi-source or recursive copy/move. This
/// is an application rule because every presentation must apply the same
/// pairing semantics before a remote adapter is touched.
pub fn validate_copy_target(pairs: &[CopyPair], recursive: bool) -> Result<()> {
    if pairs.is_empty() {
        return Err(Error::invalid_input("nothing to copy"));
    }
    let target = RemoteUri::parse(&pairs[0].target)?;
    let expands_directory = recursive
        && pairs.iter().try_fold(false, |expanded, pair| {
            Ok::<_, rstore_domain::Error>(
                expanded || RemoteUri::parse(&pair.source)?.is_directory_ref(),
            )
        })?;
    if (pairs.len() > 1 || expands_directory)
        && !target.key.is_empty()
        && !target.is_directory_ref()
    {
        return Err(Error::invalid_input(
            "multiple sources need a target bucket or prefix (end it with `/`)",
        ));
    }
    Ok(())
}

/// The copy/move orchestrator. Dependencies are trait objects wired by the
/// composition root; tests substitute fakes.
pub struct DefaultCopyService {
    locator: crate::locator::CatalogLocator,
    buckets: Arc<dyn BucketService>,
    jobs: Arc<dyn JobRepository>,
    clock: Arc<dyn Clock>,
    mutations: Arc<crate::mutation::MutationCoordinator>,
    /// Chunk policy for cross-connection relays — the same knobs the
    /// transfer engine spends (`multipart_threshold`, `part_size`).
    pub config: rstore_transfer::TransferConfig,
}

impl DefaultCopyService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        locator: crate::locator::CatalogLocator,
        buckets: Arc<dyn BucketService>,
        jobs: Arc<dyn JobRepository>,
        clock: Arc<dyn Clock>,
        mutations: Arc<crate::mutation::MutationCoordinator>,
    ) -> Self {
        Self {
            locator,
            buckets,
            jobs,
            clock,
            mutations,
            config: rstore_transfer::TransferConfig::default(),
        }
    }

    /// Resolve every pair into typed items (MOVE-009..011). Endpoints resolve
    /// exactly once through the shared [`crate::batch_plan`] cache; sources
    /// expand through the one prelude, so `one` never re-parses or
    /// re-resolves. A directory-ish target keeps the source's prefix-relative
    /// name.
    async fn expand(
        &self,
        pairs: &[CopyPair],
        recursive: bool,
        endpoints: &mut crate::batch_plan::Endpoints,
    ) -> Result<Vec<Item>> {
        let mut items = Vec::new();
        for pair in pairs {
            let src_uri = RemoteUri::parse(&pair.source)?;
            let dst_uri = RemoteUri::parse(&pair.target)?;
            crate::expand::require_recursive(&pair.source, &src_uri.key, recursive)?;
            let dst = endpoints.resolve(&self.locator, &dst_uri).await?;
            for entry in
                crate::batch_plan::remote_items(&self.locator, endpoints, &pair.source, recursive)
                    .await?
            {
                let dst_key = if dst_uri.is_directory_ref() {
                    format!("{}{}", dst_uri.key, entry.relative)
                } else {
                    dst_uri.key.clone()
                };
                let dst_path = ObjectPath::new(dst_key)?;
                items.push(Item {
                    source_label: entry.label.clone(),
                    target_label: dst.label(dst_path.as_str()),
                    src: entry.endpoint.clone(),
                    dst: dst.clone(),
                    src_path: entry.path,
                    dst_path,
                });
            }
        }
        if items.is_empty() {
            return Err(Error::not_found("recursive copy matched no objects"));
        }
        Ok(items)
    }

    /// Copy objects (MOVE-003/004/007). `cancel` is the run's Ctrl+C channel
    /// and `progress` receives per-item run progress like any batch.
    pub async fn run_copy(
        &self,
        pairs: &[CopyPair],
        options: &CopyOptions,
        cancel: &CancellationToken,
        progress: &dyn ProgressSink,
    ) -> Result<TransferReport> {
        self.run(JobKind::Copy, pairs, options, false, cancel, progress)
            .await
    }

    /// Move objects (MOVE-005/008): copy, verify, then delete the source.
    /// A source is only deleted after its target verified present — a failed
    /// copy never removes anything (MOVE-014).
    pub async fn run_move(
        &self,
        pairs: &[CopyPair],
        options: &CopyOptions,
        cancel: &CancellationToken,
        progress: &dyn ProgressSink,
    ) -> Result<TransferReport> {
        self.run(JobKind::Move, pairs, options, true, cancel, progress)
            .await
    }

    async fn run(
        &self,
        kind: JobKind,
        pairs: &[CopyPair],
        options: &CopyOptions,
        delete_source: bool,
        cancel: &CancellationToken,
        progress: &dyn ProgressSink,
    ) -> Result<TransferReport> {
        if pairs.is_empty() {
            return Err(Error::invalid_input("nothing to copy"));
        }
        validate_copy_target(pairs, options.recursive)?;
        let mut endpoints = crate::batch_plan::Endpoints::default();
        let items = self
            .expand(pairs, options.recursive, &mut endpoints)
            .await?
            .into_iter()
            .map(|item| {
                let source = item.source_label.clone();
                let target = item.target_label.clone();
                crate::job_run::RunItem::new(format!("{source} -> {target}"), source, target, item)
            })
            .collect();
        crate::job_run::run_items(
            self.jobs.as_ref(),
            self.clock.as_ref(),
            kind,
            items,
            options.dry_run,
            progress,
            cancel,
            |item| {
                let item_cancel = cancel.clone();
                let item_progress: &dyn ProgressSink = progress;
                async move {
                    match self
                        .one(&item, options, delete_source, &item_cancel, item_progress)
                        .await
                    {
                        Ok(step) => step,
                        Err(e) => crate::job_run::Step::failed(&e),
                    }
                }
            },
        )
        .await
    }

    /// Execute one resolved item. Guards and conflicts are per-item outcomes
    /// (`Blocked`/`Skipped`), never batch failures (MOVE-008). Endpoints are
    /// already resolved — no parsing or adapter construction happens here.
    async fn one(
        &self,
        item: &Item,
        options: &CopyOptions,
        delete_source: bool,
        cancel: &rstore_transfer::CancellationToken,
        progress: &dyn rstore_transfer::ProgressSink,
    ) -> Result<crate::job_run::Step> {
        let Item {
            src,
            dst,
            src_path,
            dst_path,
            ..
        } = item;

        // Core-level guards (SEC-004/005): blocked guards are per-item.
        if let Some(detail) = crate::guards::item_guards(
            self.buckets.as_ref(),
            Some(dst.bucket.id),
            delete_source.then(|| src.bucket.id),
        )
        .await
        {
            return Ok(crate::job_run::Step::done(Outcome::Blocked, Some(detail)));
        }

        // The source must exist; a missing source is a per-item failure.
        let meta = match src.storage.stat(&src.bucket.name, src_path).await {
            Ok(meta) => meta,
            Err(e) => return Ok(crate::job_run::Step::failed(&e)),
        };

        // Conflict policy (MOVE-013): the one decision, interpretation,
        // reaction and outcome mapping — shared with the engine.
        let dst_key = match rstore_transfer::decide_target(
            dst_path.as_str(),
            options.conflict,
            rstore_transfer::stat_probe(&dst.storage, &dst.bucket.name).as_ref(),
            |name| Ok(name.to_string()),
        )
        .await?
        {
            TargetDecision::Write(key) => key,
            TargetDecision::Finish(outcome) => {
                let (outcome, detail) = Outcome::from_engine(&outcome);
                return Ok(crate::job_run::Step::done(outcome, detail));
            }
        };
        let dst_path = ObjectPath::new(dst_key)?; // validate a renamed key

        if options.dry_run {
            // MOVE-012: no write requests leave the process; the preview
            // narrates what a real run would do.
            return Ok(crate::job_run::Step::preview(format!(
                "would copy to {}/{}{}",
                dst.bucket.name,
                dst_path.as_str(),
                if delete_source {
                    " (then delete source)"
                } else {
                    ""
                }
            )));
        }

        // The copy itself: server-side within one connection (MOVE-003),
        // relayed read+put across connections (MOVE-004/005).
        let copied = if src.bucket.connection_id == dst.bucket.connection_id {
            src.storage
                .copy(
                    &CopyRequest {
                        source_bucket: src.bucket.name.clone(),
                        source_key: src_path.clone(),
                        target_bucket: dst.bucket.name.clone(),
                        target_key: dst_path.clone(),
                    },
                    &meta,
                )
                .await
        } else {
            // MOVE-006: cross-connection relay streams in bounded chunks via
            // the target's multipart API once the object is large.
            let request = rstore_transfer::RelayRequest::new(
                &src.bucket.name,
                src_path,
                &dst.bucket.name,
                &dst_path,
                &meta,
            );
            rstore_transfer::relay(
                src.storage.as_ref(),
                dst.storage.as_ref(),
                &request,
                cancel,
                &self.config,
                progress,
            )
            .await
        };
        let copied = match copied {
            Ok(meta) => meta,
            Err(e) => return Ok(crate::job_run::Step::failed(&e)),
        };

        // MOVE-015/016: one post-copy module follows the completed remote
        // fact with the target row and optional tag associations. Its local
        // failures become durable repairs and cannot reclassify the remote
        // copy.
        let now = self.clock.now();
        let source = crate::mutation::CatalogObjectRef::new(
            src.bucket.connection_id,
            src.bucket.id,
            src_path,
        );
        let target = crate::mutation::CatalogObjectRef::new(
            dst.bucket.connection_id,
            dst.bucket.id,
            &dst_path,
        );
        let follow_up =
            crate::mutation::CopyFollowUp::new(source, target, &copied, options.keep_tags, now);
        self.mutations.settle_copy(&follow_up).await;

        if delete_source {
            // MOVE-014: verify the target landed before touching the source.
            match dst.storage.stat(&dst.bucket.name, &dst_path).await {
                Ok(verified) if verified.size == copied.size => {}
                Ok(_) => {
                    return Ok(crate::job_run::Step::failed_msg(
                        "target size mismatch after copy; source kept",
                    ));
                }
                Err(e) => {
                    return Ok(crate::job_run::Step::failed_msg(format!(
                        "target verification failed; source kept: {e}"
                    )));
                }
            }
            if let Err(e) = src.storage.delete(&src.bucket.name, src_path).await {
                return Ok(crate::job_run::Step::failed_msg(format!(
                    "copied, but delete failed: {e}"
                )));
            }
            self.mutations
                .settle_delete(src.bucket.connection_id, src.bucket.id, src_path, now)
                .await;
        }

        Ok(crate::job_run::Step::done(Outcome::Succeeded, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_domain::job::JobState;
    use rstore_storage_api::Storage as _;
    use rstore_test_support::FakeStorageFactory;

    type Svc = Arc<DefaultCopyService>;

    async fn setup() -> (Svc, Arc<FakeStorageFactory>) {
        let (app, factory) = crate::test_util::app_with(
            &["nas", "nas2"],
            &[("nas", "media"), ("nas", "backup"), ("nas2", "mirror")],
        )
        .await;
        (app.copy_service(), factory)
    }

    fn options(conflict: ConflictPolicy, dry_run: bool) -> CopyOptions {
        CopyOptions {
            dry_run,
            conflict,
            recursive: false,
            keep_tags: false,
        }
    }

    fn pair(source: &str, target: &str) -> CopyPair {
        CopyPair {
            source: source.into(),
            target: target.into(),
        }
    }

    #[test]
    fn copy_target_validation_is_shared_by_copy_and_move() {
        let pairs = [
            pair("nas/media/a.txt", "nas/media/archive/a.txt"),
            pair("nas/media/b.txt", "nas/media/archive/a.txt"),
        ];
        let error = validate_copy_target(&pairs, false).unwrap_err();
        assert_eq!(error.code, rstore_domain::ErrorCode::InvalidInput);

        assert!(validate_copy_target(
            &[
                pair("nas/media/a.txt", "nas/media/archive/"),
                pair("nas/media/b.txt", "nas/media/archive/"),
            ],
            false,
        )
        .is_ok());
        assert!(validate_copy_target(
            &[pair("nas/media/source/", "nas/media/archive/a.txt")],
            true,
        )
        .is_err());
    }

    #[tokio::test]
    async fn copy_same_bucket_renames() {
        let (svc, factory) = setup().await;
        factory
            .storage()
            .seed("media", "docs/a.txt", b"alpha", None);
        let report = svc
            .run_copy(
                &[pair("nas/media/docs/a.txt", "nas/media/docs/b.txt")],
                &options(ConflictPolicy::Overwrite, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1);
        assert_eq!(report.state, "succeeded");
        // Target landed; directory-style target keeps the file name below.
        let target = factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("docs/b.txt").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(target.size, 5);
    }

    #[tokio::test]
    async fn copy_directory_target_keeps_file_name() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "a.txt", b"x", None);
        let report = svc
            .run_copy(
                &[pair("nas/media/a.txt", "nas/media/sub/")],
                &options(ConflictPolicy::Overwrite, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1);
        factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("sub/a.txt").unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn copy_cross_bucket() {
        let (svc, factory) = setup().await;
        factory
            .storage()
            .seed("media", "keep.bin", b"payload", None);
        let report = svc
            .run_copy(
                &[pair("nas/media/keep.bin", "nas/backup/keep.bin")],
                &options(ConflictPolicy::Overwrite, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1);
        factory
            .storage()
            .stat(
                "backup",
                &rstore_domain::remote::ObjectPath::new("keep.bin").unwrap(),
            )
            .await
            .unwrap();
        // A copy never removes the source.
        factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("keep.bin").unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn move_copies_verifies_then_deletes_source() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "old.txt", b"data", None);
        let report = svc
            .run_move(
                &[pair("nas/media/old.txt", "nas/backup/renamed.txt")],
                &options(ConflictPolicy::Overwrite, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1);
        assert_eq!(report.state, "succeeded");
        factory
            .storage()
            .stat(
                "backup",
                &rstore_domain::remote::ObjectPath::new("renamed.txt").unwrap(),
            )
            .await
            .unwrap();
        assert!(factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("old.txt").unwrap()
            )
            .await
            .is_err());

        // The move is a persisted job and the catalog rows follow: the source
        // is tombstoned, the target indexed.
        let jobs = svc.jobs.list().await.unwrap();
        assert!(jobs
            .iter()
            .any(|j| j.kind == JobKind::Move && j.state == JobState::Succeeded));
    }

    #[tokio::test]
    async fn move_missing_source_fails_and_deletes_nothing() {
        let (svc, _factory) = setup().await;
        let report = svc
            .run_move(
                &[pair("nas/media/absent.txt", "nas/backup/absent.txt")],
                &options(ConflictPolicy::Overwrite, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.failed, 1);
        assert_eq!(report.state, "failed");
    }

    #[tokio::test]
    async fn conflict_skip_leaves_target_untouched() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "src.txt", b"new", None);
        factory
            .storage()
            .seed("media", "dst.txt", b"original", None);
        let report = svc
            .run_copy(
                &[pair("nas/media/src.txt", "nas/media/dst.txt")],
                &options(ConflictPolicy::Skip, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.skipped, 1);
        let target = factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("dst.txt").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(target.size, 8);
    }

    #[tokio::test]
    async fn conflict_rename_derives_free_name() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "src.txt", b"new", None);
        factory
            .storage()
            .seed("media", "doc.txt", b"original", None);
        let report = svc
            .run_copy(
                &[pair("nas/media/src.txt", "nas/media/doc.txt")],
                &options(ConflictPolicy::Rename, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1);
        // Rename derives `doc-<8 hex>.txt` with a random suffix.
        let page = factory
            .storage()
            .list(
                "media",
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
    async fn dry_run_sends_no_write_request() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "x.txt", b"x", None);
        let report = svc
            .run_copy(
                &[pair("nas/media/x.txt", "nas/backup/y.txt")],
                &options(ConflictPolicy::Overwrite, true),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert!(report.dry_run);
        assert_eq!(report.succeeded, 1);
        assert!(factory
            .storage()
            .stat(
                "backup",
                &rstore_domain::remote::ObjectPath::new("y.txt").unwrap()
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn unresolved_ask_conflict_on_existing_target_is_blocked() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "src.txt", b"new", None);
        factory
            .storage()
            .seed("media", "dst.txt", b"original", None);
        // `ask` reaching the service unresolved (the CLI resolves it before
        // calling) blocks the item rather than silently overwriting.
        let report = svc
            .run_copy(
                &[pair("nas/media/src.txt", "nas/media/dst.txt")],
                &options(ConflictPolicy::Ask, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.blocked, 1);
        let target = factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("dst.txt").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(target.size, 8);
    }
    fn ropts(recursive: bool, keep_tags: bool) -> CopyOptions {
        CopyOptions {
            dry_run: false,
            conflict: ConflictPolicy::Overwrite,
            recursive,
            keep_tags,
        }
    }

    #[tokio::test]
    async fn recursive_copy_expands_prefix_and_mirrors_tree() {
        let (svc, factory) = setup().await;
        let fake = factory.storage();
        fake.seed("media", "docs/a.txt", b"a", None);
        fake.seed("media", "docs/nested/b.txt", b"b", None);
        fake.seed("media", "other/c.txt", b"c", None);
        let report = svc
            .run_copy(
                &[pair("nas/media/docs/", "nas/backup/arc/")],
                &ropts(true, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 2);
        fake.stat(
            "backup",
            &rstore_domain::remote::ObjectPath::new("arc/a.txt").unwrap(),
        )
        .await
        .unwrap();
        fake.stat(
            "backup",
            &rstore_domain::remote::ObjectPath::new("arc/nested/b.txt").unwrap(),
        )
        .await
        .unwrap();
        // Outside the prefix: not copied (the source itself is untouched).
        assert!(fake
            .stat(
                "backup",
                &rstore_domain::remote::ObjectPath::new("arc/c.txt").unwrap()
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn directory_copy_requires_recursive() {
        let (svc, _) = setup().await;
        let err = svc
            .run_copy(
                &[pair("nas/media/docs/", "nas/backup/arc/")],
                &ropts(false, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
    }

    #[tokio::test]
    async fn recursive_flag_keeps_an_unslashed_key_exact() {
        let (svc, factory) = setup().await;
        let fake = factory.storage();
        fake.seed("media", "docs", b"exact", None);
        fake.seed("media", "docs/child.txt", b"child", None);

        let report = svc
            .run_copy(
                &[pair("nas/media/docs", "nas/backup/copied")],
                &ropts(true, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();

        assert_eq!(report.succeeded, 1);
        assert_eq!(
            fake.stat(
                "backup",
                &rstore_domain::remote::ObjectPath::new("copied").unwrap()
            )
            .await
            .unwrap()
            .size,
            5
        );
        assert!(fake
            .stat(
                "backup",
                &rstore_domain::remote::ObjectPath::new("child.txt").unwrap()
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn batch_copy_resolves_each_shared_endpoint_once() {
        let (svc, factory) = setup().await;
        let fake = factory.storage();
        fake.seed("media", "one.txt", b"1", None);
        fake.seed("media", "two.txt", b"2", None);
        let before = factory.built();

        let report = svc
            .run_copy(
                &[
                    pair("nas/media/one.txt", "nas/media/archive/"),
                    pair("nas/media/two.txt", "nas/media/archive/"),
                ],
                &options(ConflictPolicy::Overwrite, true),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();

        assert_eq!(report.succeeded, 2);
        assert_eq!(factory.built() - before, 1);
    }

    /// The shared arrangement for the keep-tags pair (MOVE-015): media and
    /// backup registered, a workspace spanning both, the source indexed into
    /// the catalog and one tag attached through the production paths.
    async fn tagged_pair_app() -> (crate::app::Application, Arc<FakeStorageFactory>) {
        let (app, factory) =
            crate::test_util::app_with(&["nas"], &[("nas", "media"), ("nas", "backup")]).await;
        factory.storage().seed("media", "tagged.txt", b"t", None);
        let wsp = crate::workspace::WorkspaceRef::parse("main");
        let workspaces = app.api().catalog().workspaces();
        workspaces
            .create(crate::workspace::CreateWorkspaceInput {
                name: "main".into(),
                description: None,
            })
            .await
            .unwrap();
        for bucket in ["media", "backup"] {
            workspaces
                .add_bucket(
                    &wsp,
                    &crate::bucket::BucketRef::parse(&format!("nas/{bucket}")),
                )
                .await
                .unwrap();
        }
        app.api()
            .transfer()
            .remote()
            .ls("nas/media/", None, 50, true)
            .await
            .unwrap();
        let tags = app.api().catalog().tags();
        tags.create(crate::tag::CreateTagInput {
            workspace: wsp.clone(),
            name: "keep".into(),
            color: None,
            description: None,
        })
        .await
        .unwrap();
        tags.attach(&wsp, "nas/media/tagged.txt", &["keep".to_string()], false)
            .await
            .unwrap();
        (app, factory)
    }

    #[tokio::test]
    async fn keep_tags_carries_associations_to_the_target() {
        let (app, factory) = tagged_pair_app().await;
        let command = crate::commands::CopyObjectsCommand {
            pairs: vec![pair("nas/media/tagged.txt", "nas/backup/tagged.txt")],
            options: ropts(false, true),
        };
        let report = app
            .api()
            .transfer()
            .run_copy(
                &command,
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1);

        // MOVE-015: the association landed on the target (object_count is 2 —
        // source and target both tagged); a plain copy without `keep_tags`
        // leaves it at 1.
        let list = app
            .api()
            .catalog()
            .tags()
            .list(&crate::workspace::WorkspaceRef::parse("main"))
            .await
            .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].object_count, 2);
        factory
            .storage()
            .stat(
                "backup",
                &rstore_domain::remote::ObjectPath::new("tagged.txt").unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn copy_without_keep_tags_leaves_associations_behind() {
        let (app, _factory) = tagged_pair_app().await;
        let command = crate::commands::CopyObjectsCommand {
            pairs: vec![pair("nas/media/tagged.txt", "nas/backup/tagged.txt")],
            options: ropts(false, false),
        };
        let report = app
            .api()
            .transfer()
            .run_copy(
                &command,
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1);
        let list = app
            .api()
            .catalog()
            .tags()
            .list(&crate::workspace::WorkspaceRef::parse("main"))
            .await
            .unwrap();
        assert_eq!(list[0].object_count, 1);
    }

    #[tokio::test]
    async fn cross_connection_relay_streams_large_objects() {
        let (svc, factory) = setup().await;
        let fake = factory.storage();
        let big: Vec<u8> = vec![7u8; 9 * 1024 * 1024]; // above the relay multipart threshold
        fake.seed("media", "big.bin", &big, None);
        let report = svc
            .run_copy(
                &[pair("nas/media/big.bin", "nas2/mirror/big.bin")],
                &ropts(false, false),
                &rstore_transfer::CancellationToken::new(),
                &rstore_transfer::NullProgressSink,
            )
            .await
            .unwrap();
        assert_eq!(report.succeeded, 1);
        let landed = fake
            .stat(
                "mirror",
                &rstore_domain::remote::ObjectPath::new("big.bin").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(landed.size, big.len() as u64);
    }
}
