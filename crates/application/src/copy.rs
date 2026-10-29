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
use rstore_domain::{Bucket, Clock, Error, Result};
use rstore_transfer::{CancellationToken, ProgressSink};

use crate::gateway::RemoteGateway;
use crate::reference::RemoteUri;
use crate::transfer::TransferReport;

/// One source/target remote reference pair.
#[derive(Debug, Clone)]
pub struct CopyPair {
    pub source: RemoteUri,
    pub target: RemoteUri,
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
    pub(crate) dst_bucket: Bucket,
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
    let expands_directory = recursive && pairs.iter().any(|pair| pair.source.is_directory_ref());
    if pairs.len() > 1 || expands_directory {
        for pair in pairs {
            let target = &pair.target;
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
            let src_uri = &pair.source;
            let dst_uri = &pair.target;
            crate::expand::require_recursive(&src_uri.to_string(), &src_uri.key, recursive)?;
            let dst = endpoints.resolve(&self.locator, dst_uri).await?;
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
                    dst_bucket: dst.bucket.clone(),
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
        let context = rstore_transfer::RunContext::new(cancel, options.config, progress);
        crate::job_run::run_items(
            self.jobs.as_ref(),
            self.clock.as_ref(),
            kind,
            items,
            options.dry_run,
            progress,
            cancel,
            |item| {
                let context = &context;
                async move {
                    let result = self.execute_item(&item, options, context).await;
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
        context: &rstore_transfer::RunContext<'_>,
    ) -> Result<rstore_transfer::ItemOutcome> {
        let Item {
            src,
            dst_bucket,
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
            dst_bucket.connection_id,
            &dst_bucket.name,
        );
        let request = rstore_transfer::CopyRequest::new(src_path, dst_path, options.conflict);

        let mut outcome =
            rstore_transfer::copy(source, target, &request, options.dry_run, context).await?;

        // A move's preview names the delete that would follow it.
        if outcome.disposition.is_not_run() && item.kind == ItemKind::Move {
            if let Some(detail) = &mut outcome.detail {
                detail.push_str(" (then delete source)");
            }
        }
        Ok(outcome)
    }
}
