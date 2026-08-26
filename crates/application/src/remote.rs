//! Live remote operations over resolved storage endpoints: paginated listing
//! with optional catalog refresh, object stat, and planned removal runs that
//! honor guards, dry-run and cancellation.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::job::JobState;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Clock, Error, ErrorCode, Result};
use rstore_storage_api::model::{ListOptions, ObjectMetadata};
use rstore_storage_api::Storage;

use crate::batch_outcome::{
    BatchOutcome, BlockReason, Disposition, Failure, ItemOutcome, SkipReason,
};
use crate::reference::RemoteUri;

/// One entry of a remote listing: an object or a common-prefix directory.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LsEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_modified: Option<time::OffsetDateTime>,
}

/// A single page of `ls` output plus pagination and catalog-sync flags.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LsResult {
    pub uri: String,
    pub entries: Vec<LsEntry>,
    pub next_cursor: Option<String>,
    pub is_truncated: bool,
    /// Whether this page refreshed confirmed objects in the catalog.
    pub catalog_updated: bool,
}

/// Aggregated outcome of a removal run: per-item results in the unified
/// batch vocabulary plus the run-level job facts.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RemoveReport {
    pub outcome: BatchOutcome<()>,
    pub state: JobState,
    pub dry_run: bool,
}

impl RemoveReport {
    /// Whether the run stopped early due to cancellation.
    pub fn is_interrupted(&self) -> bool {
        self.state.is_interrupted()
    }

    /// Projects the run report onto removal semantics, one item at a time.
    /// Dry-run previews arrive as [`SkipReason::DryRun`] skips; rm has no
    /// conflict step, so any other non-run classification is defensive.
    fn from_run(report: crate::transfer_report::TransferReport, dry_run: bool) -> Self {
        let items = report
            .items
            .iter()
            .map(|item| {
                let disposition = match &item.disposition {
                    Disposition::Succeeded => Disposition::Succeeded,
                    Disposition::Skipped(SkipReason::DryRun) => {
                        Disposition::Skipped(SkipReason::DryRun)
                    }
                    Disposition::Blocked(BlockReason::DeleteForbidden) => {
                        Disposition::Blocked(BlockReason::DeleteForbidden)
                    }
                    Disposition::Failed(f) => Disposition::Failed(f.clone()),
                    other => Disposition::Failed(Failure {
                        code: ErrorCode::Unknown,
                        message: format!("unexpected classification {other:?}"),
                    }),
                };
                ItemOutcome {
                    target: item.source.clone(),
                    disposition,
                    detail: item.detail.clone(),
                    value: None,
                }
            })
            .collect();
        Self {
            outcome: BatchOutcome::new(items),
            state: report.state,
            dry_run,
        }
    }
}

/// Live inspection and removal against remote storage.
#[async_trait]
pub trait RemoteService: Send + Sync {
    /// Lists one page of a directory reference.
    async fn ls(
        &self,
        uri: &str,
        cursor: Option<String>,
        limit: u32,
        update_catalog: bool,
    ) -> Result<LsResult>;

    /// Fetches metadata for one object reference.
    async fn stat(&self, uri: &str) -> Result<ObjectMetadata>;

    /// Expands references into the concrete items a removal would touch.
    async fn plan_removals(&self, uris: &[String], recursive: bool) -> Result<RemovalPlan>;

    /// Executes a previously built removal plan as a tracked job.
    async fn remove_planned(
        &self,
        plan: RemovalPlan,
        dry_run: bool,
        cancel: &rstore_transfer::CancellationToken,
    ) -> Result<RemoveReport>;

    /// Plans and executes a removal in one call.
    async fn remove(
        &self,
        uris: &[String],
        recursive: bool,
        dry_run: bool,
        cancel: &rstore_transfer::CancellationToken,
    ) -> Result<RemoveReport> {
        let plan = self.plan_removals(uris, recursive).await?;
        self.remove_planned(plan, dry_run, cancel).await
    }
}

/// The concrete set of items a removal run will process.
///
/// Splitting planning from execution lets callers preview counts and keep the
/// expansion stable across the run.
#[derive(Default)]
pub struct RemovalPlan {
    items: Vec<crate::job_run::RunItem<crate::batch_plan::RemoteItem>>,
}

impl RemovalPlan {
    fn push(&mut self, item: crate::job_run::RunItem<crate::batch_plan::RemoteItem>) {
        self.items.push(item);
    }

    /// Number of objects the plan would remove.
    pub fn count(&self) -> usize {
        self.items.len()
    }
}

/// Settle hook for removal runs: settles each succeeded removal's delete;
/// delete settlement needs no engine metadata.
struct DeleteSettle {
    mutations: Arc<crate::mutation::MutationCoordinator>,
    clock: Arc<dyn Clock>,
}

#[async_trait]
impl crate::job_run::SettleHook<crate::batch_plan::RemoteItem> for DeleteSettle {
    async fn settle(
        &self,
        item: crate::batch_plan::RemoteItem,
        verdict: crate::job_run::ItemVerdict,
    ) -> crate::job_run::Step {
        self.mutations
            .settle_delete(
                item.endpoint.bucket.connection_id,
                item.endpoint.bucket.id,
                &item.path,
                self.clock.now(),
            )
            .await;
        verdict.step
    }
}

pub struct DefaultRemoteService {
    gateway: crate::gateway::RemoteGateway,
    locator: crate::locator::CatalogLocator,
    catalog: Arc<dyn RemoteObjectRepository>,
    jobs: Arc<dyn rstore_application_ports::JobRepository>,
    clock: Arc<dyn Clock>,
    mutations: Arc<crate::mutation::MutationCoordinator>,
}

impl DefaultRemoteService {
    pub(crate) fn new(
        gateway: crate::gateway::RemoteGateway,
        locator: crate::locator::CatalogLocator,
        catalog: Arc<dyn RemoteObjectRepository>,
        jobs: Arc<dyn rstore_application_ports::JobRepository>,
        clock: Arc<dyn Clock>,
        mutations: Arc<crate::mutation::MutationCoordinator>,
    ) -> Self {
        Self {
            gateway,
            locator,
            catalog,
            jobs,
            clock,
            mutations,
        }
    }

    /// Parses a URI and resolves its connection, bucket and storage adapter.
    async fn resolve_target(
        &self,
        uri: &str,
    ) -> Result<(rstore_domain::Bucket, Arc<dyn Storage>, RemoteUri)> {
        let parsed = RemoteUri::parse(uri)?;
        let (_config, bucket, storage) = self.locator.resolve_target(&parsed).await?;
        Ok((bucket, storage, parsed))
    }
}

#[async_trait]
impl RemoteService for DefaultRemoteService {
    async fn ls(
        &self,
        uri: &str,
        cursor: Option<String>,
        limit: u32,
        update_catalog: bool,
    ) -> Result<LsResult> {
        let (bucket, storage, parsed) = self.resolve_target(uri).await?;
        if !parsed.is_directory_ref() {
            return Err(Error::invalid_input(format!(
                "`remote ls` needs a directory reference; write `{uri}/`"
            )));
        }

        let prefix = crate::expand::listing_prefix(&parsed.key);

        let page = storage
            .list(
                &bucket.name,
                &ListOptions {
                    prefix: Some(prefix.clone()),
                    delimiter: Some("/".into()),
                    continuation_token: cursor.clone(),
                    max_keys: Some(limit),
                },
            )
            .await?;

        let mut entries: Vec<LsEntry> = page
            .common_prefixes
            .iter()
            .map(|p| LsEntry {
                name: trim_dir_name(&prefix, p),
                is_dir: true,
                size: 0,
                last_modified: None,
            })
            .collect();
        entries.extend(page.objects.iter().map(|o| LsEntry {
            name: o.metadata.key[prefix.len()..].to_string(),
            is_dir: false,
            size: o.metadata.size,
            last_modified: Some(o.metadata.last_modified),
        }));

        entries.sort_by(|a, b| b.is_dir.cmp(&a.is_dir).then_with(|| a.name.cmp(&b.name)));
        entries.truncate(limit as usize);

        let mut catalog_updated = false;
        if update_catalog {
            let confirmed_at = self.clock.now();
            crate::catalog_follow::sync_page(
                self.catalog.as_ref(),
                bucket.connection_id,
                bucket.id,
                &page.objects,
                confirmed_at,
            )
            .await?;
            catalog_updated = true;
        }

        Ok(LsResult {
            uri: uri.to_string(),
            entries,
            next_cursor: page.continuation_token,
            is_truncated: page.is_truncated,
            catalog_updated,
        })
    }

    async fn stat(&self, uri: &str) -> Result<ObjectMetadata> {
        if uri.ends_with('/') {
            return Err(Error::invalid_input(
                "`remote stat` targets an object; the URI must not end with `/`",
            ));
        }
        let (bucket, storage, parsed) = self.resolve_target(uri).await?;
        storage.stat(&bucket.name, &parsed.object_path()?).await
    }

    async fn plan_removals(&self, uris: &[String], recursive: bool) -> Result<RemovalPlan> {
        let mut endpoints = crate::batch_plan::Endpoints::default();
        let mut plan = RemovalPlan::default();
        for uri in uris {
            for item in
                crate::batch_plan::remote_items(&self.locator, &mut endpoints, uri, recursive)
                    .await?
            {
                plan.push(crate::job_run::RunItem::new(
                    item.label.clone(),
                    item.label.clone(),
                    String::new(),
                    item,
                ));
            }
        }
        Ok(plan)
    }

    async fn remove_planned(
        &self,
        plan: RemovalPlan,
        dry_run: bool,
        cancel: &rstore_transfer::CancellationToken,
    ) -> Result<RemoveReport> {
        let report = crate::job_run::run_items(
            self.jobs.as_ref(),
            self.clock.as_ref(),
            rstore_domain::job::JobKind::Delete,
            plan.items,
            dry_run,
            &rstore_transfer::NullProgressSink,
            cancel,
            |item| async move {
                self.remove_one(
                    &item.endpoint.bucket,
                    &item.endpoint.storage,
                    &item.path,
                    dry_run,
                )
                .await
                .map_or_else(
                    |e| crate::job_run::ItemVerdict::from_engine_result(Err(e)),
                    crate::job_run::ItemVerdict::from_step,
                )
            },
            &DeleteSettle {
                mutations: self.mutations.clone(),
                clock: self.clock.clone(),
            },
        )
        .await?;
        Ok(RemoveReport::from_run(report, dry_run))
    }
}

impl DefaultRemoteService {
    /// Removes one object after the delete guard, stat re-verification and
    /// dry-run. The guard rides the gateway's delete adapter — reads pass
    /// it, the delete re-checks the bits — so a refused bucket becomes a
    /// per-item block, which is what a mixed-bucket batch reports.
    async fn remove_one(
        &self,
        bucket: &rstore_domain::Bucket,
        storage: &Arc<dyn Storage>,
        path: &ObjectPath,
        dry_run: bool,
    ) -> Result<crate::job_run::Step> {
        let storage = match self.gateway.deleter(storage.clone(), bucket) {
            Ok(guarded) => guarded,
            Err(e) => {
                return Ok(crate::job_run::Step::done(
                    Disposition::Blocked(BlockReason::DeleteForbidden),
                    Some(e.to_string()),
                ));
            }
        };

        if dry_run {
            return Ok(crate::job_run::Step::preview(format!(
                "would delete {}/{}",
                bucket.name,
                path.as_str()
            )));
        }

        // Re-stat right before deletion so a vanished or changed target stops
        // the run instead of silently succeeding on a stale plan.
        if let Err(e) = storage.stat(&bucket.name, path).await {
            return Ok(crate::job_run::Step::failed_msg(format!(
                "target changed or missing, delete stopped: {e}"
            )));
        }

        storage.delete(&bucket.name, path).await?;
        Ok(crate::job_run::Step::succeeded())
    }
}

fn trim_dir_name(prefix: &str, common_prefix: &str) -> String {
    let name = common_prefix.strip_prefix(prefix).unwrap_or(common_prefix);
    name.trim_end_matches('/').to_string()
}
