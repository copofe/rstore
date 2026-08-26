//! Copy/move orchestration: expands source references into per-item work,
//! issues guard adapters before the first item runs (and executes through
//! them), runs items through the transfer copy entry, and settles catalog
//! state (including move completion with verification).

use std::collections::HashMap;
use std::sync::Arc;

use rstore_application_ports::JobRepository;
use rstore_domain::job::JobKind;
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Clock, Error, Result};
use rstore_transfer::{CancellationToken, ProgressSink};

use crate::gateway::RemoteGateway;
use crate::reference::RemoteUri;
use crate::transfer::TransferReport;

/// One source/target remote reference pair.
#[derive(Debug, Clone)]
pub struct CopyPair {
    pub source: String,
    pub target: String,
}

/// Whether an expanded pair copies or moves: moves verify the target and
/// delete the source after the copy lands. The kind and the item's delete
/// adapter are issued together by expansion, so a `Move` always carries one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ItemKind {
    Copy,
    Move,
}

#[derive(Clone)]
pub(crate) struct Item {
    pub(crate) kind: ItemKind,
    pub(crate) src: Arc<crate::batch_plan::Endpoint>,
    pub(crate) src_path: ObjectPath,
    pub(crate) dst: Arc<crate::batch_plan::Endpoint>,
    pub(crate) dst_path: ObjectPath,
    /// The guard-issued mutation adapter for the target bucket: the copy
    /// executes through it, so the guard travels with the item.
    pub(crate) dst_storage: Arc<dyn rstore_storage_api::Storage>,
    /// The delete-guarded adapter for a move's source bucket; present
    /// exactly when `kind` is [`ItemKind::Move`].
    pub(crate) src_delete: Option<Arc<dyn rstore_storage_api::Storage>>,
    source_label: String,
    target_label: String,
}

#[cfg(test)]
impl Item {
    /// Test-only direct assembly (labels are empty): settlement tests build
    /// items without the expansion pipeline.
    pub(crate) fn for_test(
        kind: ItemKind,
        src: Arc<crate::batch_plan::Endpoint>,
        src_path: ObjectPath,
        dst: Arc<crate::batch_plan::Endpoint>,
        dst_path: ObjectPath,
        dst_storage: Arc<dyn rstore_storage_api::Storage>,
        src_delete: Option<Arc<dyn rstore_storage_api::Storage>>,
    ) -> Self {
        debug_assert_eq!(
            (kind == ItemKind::Move),
            src_delete.is_some(),
            "a move carries the delete-guarded adapter, a copy does not"
        );
        Self {
            kind,
            src,
            src_path,
            dst,
            dst_path,
            dst_storage,
            src_delete,
            source_label: String::new(),
            target_label: String::new(),
        }
    }
}

/// Options for copy and move runs.
#[derive(Debug, Clone, Copy, Default)]
pub struct CopyOptions {
    pub dry_run: bool,
    pub conflict: ConflictPolicy,

    /// Expand directory-style sources into their object trees.
    pub recursive: bool,

    pub keep_tags: bool,

    pub config: rstore_transfer::TransferConfig,
}
/// Validates that every pair has a legal target shape before any work starts.
///
/// Multiple sources, or any expanding directory source, require every target
/// to be a bucket or slash-terminated prefix so items cannot overwrite each
/// other.
pub fn validate_copy_target(pairs: &[CopyPair], recursive: bool) -> Result<()> {
    if pairs.is_empty() {
        return Err(Error::invalid_input("nothing to copy"));
    }
    let expands_directory = recursive
        && pairs.iter().try_fold(false, |expanded, pair| {
            Ok::<_, rstore_domain::Error>(
                expanded || RemoteUri::parse(&pair.source)?.is_directory_ref(),
            )
        })?;
    if pairs.len() > 1 || expands_directory {
        for pair in pairs {
            let target = RemoteUri::parse(&pair.target)?;
            if !target.key.is_empty() && !target.is_directory_ref() {
                return Err(Error::invalid_input(
                    "multiple sources need a target bucket or prefix (end it with `/`)",
                ));
            }
        }
    }
    Ok(())
}

/// Gateway guard-issuance function: [`RemoteGateway::writer`] or
/// [`RemoteGateway::deleter`].
type GuardIssue = fn(
    &crate::gateway::RemoteGateway,
    Arc<dyn rstore_storage_api::Storage>,
    &rstore_domain::Bucket,
) -> rstore_domain::Result<Arc<dyn rstore_storage_api::Storage>>;

/// Issue a guard adapter once per unique bucket, memoized for the batch.
fn guarded_once(
    issued: &mut HashMap<rstore_domain::BucketId, Arc<dyn rstore_storage_api::Storage>>,
    gateway: &crate::gateway::RemoteGateway,
    endpoint: &crate::batch_plan::Endpoint,
    issue: GuardIssue,
) -> rstore_domain::Result<Arc<dyn rstore_storage_api::Storage>> {
    use std::collections::hash_map::Entry;
    Ok(match issued.entry(endpoint.bucket.id) {
        Entry::Occupied(occupied) => occupied.get().clone(),
        Entry::Vacant(vacant) => vacant
            .insert(issue(gateway, endpoint.storage.clone(), &endpoint.bucket)?)
            .clone(),
    })
}

/// Settle hook for copy/move runs: hands each succeeded item to the
/// copy/move settlement module.
struct CopySettle {
    settlement: crate::settlement::Settlement,
    keep_tags: bool,
}

#[async_trait::async_trait]
impl crate::job_run::SettleHook<Item> for CopySettle {
    async fn settle(
        &self,
        item: Item,
        verdict: crate::job_run::ItemVerdict,
    ) -> crate::job_run::Step {
        self.settlement.settle(&item, verdict, self.keep_tags).await
    }
}

pub struct DefaultCopyService {
    gateway: crate::gateway::RemoteGateway,
    locator: crate::locator::CatalogLocator,
    jobs: Arc<dyn JobRepository>,
    clock: Arc<dyn Clock>,
    settlement: crate::settlement::Settlement,
}

impl DefaultCopyService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        gateway: RemoteGateway,
        locator: crate::locator::CatalogLocator,
        jobs: Arc<dyn JobRepository>,
        clock: Arc<dyn Clock>,
        settlement: crate::settlement::Settlement,
    ) -> Self {
        Self {
            gateway,
            locator,
            jobs,
            clock,
            settlement,
        }
    }

    /// Expands each pair into concrete items against shared endpoints.
    ///
    /// Endpoints are memoized through `endpoints` so a batch touching one
    /// connection/bucket combination builds its storage adapter only once.
    /// Guard adapters are issued the same way — one per unique bucket — and
    /// carried on every item: a read-only target or an undeletable move
    /// source fails the run here, before any item executes, and the
    /// adapter the item runs through is the one the guard issued.
    async fn expand(
        &self,
        pairs: &[CopyPair],
        recursive: bool,
        delete_source: bool,
        endpoints: &mut crate::batch_plan::Endpoints,
    ) -> Result<Vec<Item>> {
        let mut items = Vec::new();
        let mut writers = HashMap::new();
        let mut deleters = HashMap::new();
        for pair in pairs {
            let src_uri = RemoteUri::parse(&pair.source)?;
            let dst_uri = RemoteUri::parse(&pair.target)?;
            crate::expand::require_recursive(&pair.source, &src_uri.key, recursive)?;
            let dst = endpoints.resolve(&self.locator, &dst_uri).await?;
            for entry in
                crate::batch_plan::remote_items(&self.locator, endpoints, &pair.source, recursive)
                    .await?
            {
                // A directory target mirrors each entry's relative path under
                // it; an exact key target receives every entry verbatim.
                let dst_key = if dst_uri.is_directory_ref() {
                    format!("{}{}", dst_uri.key, entry.relative)
                } else {
                    dst_uri.key.clone()
                };
                let dst_path = ObjectPath::new(dst_key)?;
                let dst_storage =
                    guarded_once(&mut writers, &self.gateway, &dst, RemoteGateway::writer)?;
                let (kind, src_delete) = if delete_source {
                    (
                        ItemKind::Move,
                        Some(guarded_once(
                            &mut deleters,
                            &self.gateway,
                            &entry.endpoint,
                            RemoteGateway::deleter,
                        )?),
                    )
                } else {
                    (ItemKind::Copy, None)
                };
                items.push(Item {
                    kind,
                    source_label: entry.label.clone(),
                    target_label: dst.label(dst_path.as_str()),
                    src: entry.endpoint.clone(),
                    dst: dst.clone(),
                    src_path: entry.path,
                    dst_path,
                    dst_storage,
                    src_delete,
                });
            }
        }
        if items.is_empty() {
            return Err(Error::not_found("recursive copy matched no objects"));
        }
        Ok(items)
    }

    /// Runs a copy job and returns the aggregated transfer report.
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

    /// Runs a move job: copy first, verify, then delete the source.
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
            .expand(pairs, options.recursive, delete_source, &mut endpoints)
            .await?;

        let items = items
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
                    let result = self
                        .execute_item(&item, options, &item_cancel, item_progress)
                        .await;
                    crate::job_run::ItemVerdict::from_engine_result(result)
                }
            },
            &CopySettle {
                settlement: self.settlement.clone(),
                keep_tags: options.keep_tags,
            },
        )
        .await
    }

    /// Executes one item through the transfer copy entry. The entry owns the
    /// source stat, the conflict decision, and the same-connection/relay
    /// routing; this side only annotates a move's preview.
    async fn execute_item(
        &self,
        item: &Item,
        options: &CopyOptions,
        cancel: &rstore_transfer::CancellationToken,
        progress: &dyn rstore_transfer::ProgressSink,
    ) -> Result<rstore_transfer::ItemOutcome> {
        let Item {
            src,
            dst,
            src_path,
            dst_path,
            dst_storage,
            ..
        } = item;

        // The target runs through the guard-issued adapter; the source
        // stays raw because a copy only reads from it.
        let source = rstore_transfer::CopyEndpoint::new(
            src.storage.clone(),
            src.bucket.connection_id,
            &src.bucket.name,
        );
        let target = rstore_transfer::CopyEndpoint::new(
            dst_storage.clone(),
            dst.bucket.connection_id,
            &dst.bucket.name,
        );
        let request = rstore_transfer::CopyRequest::new(src_path, dst_path, options.conflict);

        let ctx = rstore_transfer::RunContext::new(cancel, options.config, progress);
        let mut outcome =
            rstore_transfer::copy(source, target, &request, options.dry_run, &ctx).await?;

        // A move's preview names the delete that would follow it.
        if outcome.disposition.is_not_run() && item.kind == ItemKind::Move {
            if let Some(detail) = &mut outcome.detail {
                detail.push_str(" (then delete source)");
            }
        }
        Ok(outcome)
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
            config: rstore_transfer::TransferConfig::default(),
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
        // Every pair's target is checked, not just the first: a later
        // exact-key target would let expanded items overwrite each other.
        assert!(validate_copy_target(
            &[
                pair("nas/media/a.txt", "nas/media/archive/"),
                pair("nas/media/b.txt", "nas/media/archive/exact.txt"),
            ],
            false,
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
        assert_eq!(report.state, JobState::Succeeded);

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
        assert_eq!(report.state, JobState::Succeeded);
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
        assert_eq!(report.state, JobState::Failed);
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
    async fn rename_policy_reports_success_through_the_service() {
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
        // Rename mechanics (suffix shape, free-name derivation) are the
        // transfer entry's contract; the service reports the landing.
        assert_eq!(report.succeeded, 1);
        assert_eq!(report.failed, 0);
    }

    #[tokio::test]
    async fn unresolved_ask_conflict_on_existing_target_is_blocked() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "src.txt", b"new", None);
        factory
            .storage()
            .seed("media", "dst.txt", b"original", None);

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
            config: rstore_transfer::TransferConfig::default(),
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

        // Dry-run previews count as skipped, not succeeded.
        assert_eq!(report.skipped, 2);
        assert_eq!(factory.built() - before, 1);
    }

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
        let big: Vec<u8> = vec![7u8; 9 * 1024 * 1024];
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
