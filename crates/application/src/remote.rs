//! Live remote listing and stat (REM-) plus object removal (DEL-).

use std::sync::Arc;

use async_trait::async_trait;
use rstore_catalog::RemoteObjectRepository;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Clock, Error, Result};
use rstore_storage_api::model::{ListOptions, ObjectMetadata};
use rstore_storage_api::Storage;

use crate::bucket::BucketService;
use crate::connection::{ConnectionRef, ConnectionService};

/// Split a short remote reference into non-empty path segments — the pure
/// half of [`expand_reference`]; which leading segment (if any) is a
/// connection is a registry question answered by the caller.
fn short_reference_segments(reference: &str) -> Vec<String> {
    reference
        .split('/')
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect()
}

/// Expand a user remote reference (short or canonical) into the canonical
/// `<connection-id>/<bucket>/[key]` form.
///
/// Short forms: `<bucket>/[prefix/]` with a bare (globally unambiguous)
/// bucket name, or `<connection>/<bucket>/[prefix/]`. A first segment only
/// counts as a connection when one is registered under that name (or id), so
/// `media/docs/` with no connection called `media` means bucket `media`,
/// prefix `docs/`. The rule lives here — beside [`RemoteUri`], below any
/// entry point — so the CLI and the future desktop expand identically.
pub async fn expand_reference(
    connections: &dyn ConnectionService,
    buckets: &dyn BucketService,
    reference: &str,
) -> Result<String> {
    let known = registered_connections(connections).await?;
    expand_known(buckets, &known, reference).await
}

/// Expand a batch through the same short-reference rule, preserving order.
/// One registry read answers every reference's connection question — a
/// 100-URI batch pays one connection listing, not a probe per reference.
pub async fn expand_references(
    connections: &dyn ConnectionService,
    buckets: &dyn BucketService,
    references: &[String],
) -> Result<Vec<String>> {
    let known = registered_connections(connections).await?;
    let mut expanded = Vec::with_capacity(references.len());
    for reference in references {
        expanded.push(expand_known(buckets, &known, reference).await?);
    }
    Ok(expanded)
}

/// The registered connection names and ids as one set — the disambiguation
/// vocabulary for short references.
async fn registered_connections(
    connections: &dyn ConnectionService,
) -> Result<std::collections::HashSet<String>> {
    Ok(connections
        .list()
        .await?
        .into_iter()
        .flat_map(|c| [c.name, c.id.to_string()])
        .collect())
}

async fn expand_known(
    buckets: &dyn BucketService,
    known: &std::collections::HashSet<String>,
    reference: &str,
) -> Result<String> {
    use crate::bucket::BucketRef;
    let directory_ref = reference.ends_with('/');
    let segments = short_reference_segments(reference);
    let (bucket, mut prefix): (rstore_domain::Bucket, String) = match segments.as_slice() {
        [] => return Err(Error::invalid_input("empty remote reference")),
        [connection, name, rest @ ..] if known.contains(connection) => {
            let reference = BucketRef::Scoped(ConnectionRef::parse(connection), name.clone());
            let bucket = buckets.resolve(&reference).await?;
            (bucket, rest.join("/"))
        }
        [name, rest @ ..] => {
            let bucket = buckets.resolve(&BucketRef::Name(name.clone())).await?;
            (bucket, rest.join("/"))
        }
    };
    if directory_ref && !prefix.is_empty() {
        prefix.push('/');
    }
    Ok(format!(
        "{}/{}/{}",
        bucket.connection_id, bucket.name, prefix
    ))
}

/// A parsed `<connection>/<bucket>/[key]` remote reference (REM-).
///
/// The connection segment is an id (`conn_…`) or a registered name. `key` is
/// the bucket-relative remainder; empty means "the bucket root" (a listing,
/// never a stat target).
#[derive(Debug, Clone)]
pub struct RemoteUri {
    pub connection: ConnectionRef,
    pub bucket: String,
    pub key: String,
}

impl RemoteUri {
    /// Parse a canonical remote reference, preserving a trailing `/` because
    /// it distinguishes a directory reference from an exact key.
    pub fn parse(uri: &str) -> Result<Self> {
        let mut segments = uri.splitn(3, '/');
        let connection = segments.next().unwrap_or_default();
        let bucket = segments.next().unwrap_or_default();
        let key = segments.next().unwrap_or_default().trim_start_matches('/');
        if connection.is_empty() {
            return Err(Error::invalid_input(format!(
                "a remote reference needs a connection: `<connection>/<bucket>/[key]`, got `{uri}`"
            )));
        }
        if bucket.is_empty() {
            return Err(Error::invalid_input(format!(
                "a remote reference needs a bucket: `<connection>/<bucket>/[key]`, got `{uri}`"
            )));
        }
        Ok(Self {
            connection: ConnectionRef::parse(connection),
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    /// The key as a strict [`ObjectPath`] (stat/delete targets).
    pub fn object_path(&self) -> Result<ObjectPath> {
        ObjectPath::new(&self.key)
    }

    /// Whether this reference denotes a directory rather than an exact key.
    pub fn is_directory_ref(&self) -> bool {
        crate::expand::is_directory_ref(&self.key)
    }

    /// Split a reference into a transfer-plan base while preserving the key's
    /// trailing `/` directory marker.
    pub fn parse_base(uri: &str) -> Result<(ConnectionRef, String, String)> {
        let parsed = Self::parse(uri)?;
        Ok((parsed.connection, parsed.bucket, parsed.key))
    }
}

/// One row of a live listing.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LsEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_modified: Option<time::OffsetDateTime>,
}

/// A page of live listing results (REM-003).
#[derive(Debug, Clone, serde::Serialize)]
pub struct LsResult {
    pub uri: String,
    pub entries: Vec<LsEntry>,
    pub next_cursor: Option<String>,
    pub is_truncated: bool,
    pub catalog_updated: bool,
}

/// Removal report (DEL-001/002/004). The serialized shape is a recorded CLI
/// contract; the bookkeeping behind it is [`crate::job_run`]'s — `remove`
/// runs as a `delete` job and projects the run's report into this view.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RemoveItem {
    pub uri: String,
    pub outcome: RemoveOutcome,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveOutcome {
    Deleted,
    WouldDelete,
    GuardBlocked,
    Failed,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RemoveReport {
    pub items: Vec<RemoveItem>,
    pub deleted: usize,
    pub blocked: usize,
    pub failed: usize,
    /// Run-terminal state (`succeeded` / `partial_success` / `failed` /
    /// `interrupted`) — the CLI maps `interrupted` to exit 130 (CLI-013).
    pub state: &'static str,
    pub dry_run: bool,
}

impl RemoveReport {
    /// The run ended via Ctrl+C — the exit-130 trigger (CLI-013). The one
    /// interpretation of the state string, so a spelling change on the
    /// job-run side cannot silently break the exit code.
    pub fn is_interrupted(&self) -> bool {
        self.state == "interrupted"
    }

    /// The rm JSON view of a job-run report: same envelope as before rm
    /// joined the job-run family, one derivation.
    fn from_run(report: crate::transfer_report::TransferReport, dry_run: bool) -> Self {
        let items = report
            .items
            .iter()
            .map(|item| {
                let outcome = match (item.outcome, dry_run) {
                    (crate::transfer_report::Outcome::Succeeded, true) => {
                        RemoveOutcome::WouldDelete
                    }
                    (crate::transfer_report::Outcome::Succeeded, false) => RemoveOutcome::Deleted,
                    (crate::transfer_report::Outcome::Blocked, _) => RemoveOutcome::GuardBlocked,
                    (crate::transfer_report::Outcome::Failed, _) => RemoveOutcome::Failed,
                    // Removal has no skip path; the arm exists for totality.
                    (crate::transfer_report::Outcome::Skipped, _) => RemoveOutcome::Failed,
                };
                RemoveItem {
                    uri: item.source.clone(),
                    outcome,
                    detail: item.detail.clone(),
                }
            })
            .collect();
        Self {
            deleted: if dry_run { 0 } else { report.succeeded },
            blocked: report.blocked,
            failed: report.failed,
            state: report.state,
            items,
            dry_run,
        }
    }
}

/// Live remote operations (REM-/DEL-).
#[async_trait]
pub trait RemoteService: Send + Sync {
    /// List one directory page (REM-001/002/003). `update_catalog` upserts the
    /// listed objects into the local catalog (REM-007 / IDX-010).
    async fn ls(
        &self,
        uri: &str,
        cursor: Option<String>,
        limit: u32,
        update_catalog: bool,
    ) -> Result<LsResult>;

    /// Stat one object (REM-005).
    async fn stat(&self, uri: &str) -> Result<ObjectMetadata>;

    /// Plan a removal (DEL-003): expand every reference once into typed
    /// items with resolved endpoints and canonical labels. The plan is the
    /// unit the destructive confirmation counts *and* the removal executes —
    /// one expansion serves both, so the confirmed count is exactly what the
    /// run deletes and the prefix is listed once.
    async fn plan_removals(&self, uris: &[String], recursive: bool) -> Result<RemovalPlan>;

    /// Remove objects (DEL-001/002/003). Guards run per target
    /// (DEL-006/007), each target is re-statted right before its delete so a
    /// changed or vanished object stops the removal (DEL-008), deletions
    /// tombstone the catalog (DEL-009), `dry_run` previews only. The run
    /// persists as a `delete` job through [`crate::job_run`] — cancellable,
    /// with the one terminal-state rule.
    async fn remove_planned(
        &self,
        plan: RemovalPlan,
        dry_run: bool,
        cancel: &rstore_transfer::CancellationToken,
    ) -> Result<RemoveReport>;

    /// Remove objects by reference — plan then execute. Callers that need
    /// the exact confirmation count first should [`plan_removals`] once and
    /// pass the same plan to [`remove_planned`](RemoteService::remove_planned)
    /// instead of re-expanding through this method.
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

/// A removal expanded once: typed items, resolved endpoints, canonical
/// labels — the exact object set a confirmation counts and a run deletes.
#[derive(Default)]
pub struct RemovalPlan {
    items: Vec<crate::job_run::RunItem<crate::batch_plan::RemoteItem>>,
}

impl RemovalPlan {
    fn push(&mut self, item: crate::job_run::RunItem<crate::batch_plan::RemoteItem>) {
        self.items.push(item);
    }

    /// The exact number of objects this plan removes (DEL-003).
    pub fn count(&self) -> usize {
        self.items.len()
    }
}

/// Default implementation over the locator + registered buckets.
pub struct DefaultRemoteService {
    buckets: Arc<dyn BucketService>,
    locator: crate::locator::CatalogLocator,
    catalog: Arc<dyn RemoteObjectRepository>,
    jobs: Arc<dyn rstore_domain::JobRepository>,
    clock: Arc<dyn Clock>,
    mutations: Arc<crate::mutation::MutationCoordinator>,
}

impl DefaultRemoteService {
    pub(crate) fn new(
        buckets: Arc<dyn BucketService>,
        locator: crate::locator::CatalogLocator,
        catalog: Arc<dyn RemoteObjectRepository>,
        jobs: Arc<dyn rstore_domain::JobRepository>,
        clock: Arc<dyn Clock>,
        mutations: Arc<crate::mutation::MutationCoordinator>,
    ) -> Self {
        Self {
            buckets,
            locator,
            catalog,
            jobs,
            clock,
            mutations,
        }
    }

    /// Resolve a URI to (registered bucket, storage adapter, parsed URI) via
    /// the tier-1 locator path. The key stays on the [`RemoteUri`] —
    /// listings take it as a prefix (possibly empty at bucket root), object
    /// operations parse it strictly.
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
        // S3 directory semantics, one derivation rule: a listing prefix keeps
        // its trailing `/` (see `expand::listing_prefix`).
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
        // Directories first, then names — deterministic without terminal width.
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
        // DEL-003: recursive references expand to concrete objects first, so
        // both the confirmation count and the report are exact. The shared
        // prelude resolves and labels; nothing here parses.
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
                match self
                    .remove_one(
                        &item.endpoint.bucket,
                        item.endpoint.storage.as_ref(),
                        &item.path,
                        dry_run,
                    )
                    .await
                {
                    Ok(step) => step,
                    Err(e) => crate::job_run::Step::failed(&e),
                }
            },
        )
        .await?;
        Ok(RemoveReport::from_run(report, dry_run))
    }
}

impl DefaultRemoteService {
    /// Delete one object. The endpoint is already resolved — `remove` owns
    /// expansion and resolution; nothing here parses. Outcomes speak the
    /// report vocabulary: `job_run` owns the bookkeeping.
    async fn remove_one(
        &self,
        bucket: &rstore_domain::Bucket,
        storage: &dyn Storage,
        path: &ObjectPath,
        dry_run: bool,
    ) -> Result<crate::job_run::Step> {
        use crate::transfer::Outcome;

        // Core-level guards before any adapter call (DEL-006/007, SEC-004/005).
        // A blocked guard is a per-item outcome, not a batch failure (DEL-002).
        if let Some(detail) =
            crate::guards::item_guards(self.buckets.as_ref(), None, Some(bucket.id)).await
        {
            return Ok(crate::job_run::Step::done(Outcome::Blocked, Some(detail)));
        }

        if dry_run {
            return Ok(crate::job_run::Step::preview(format!(
                "would delete {}/{}",
                bucket.name,
                path.as_str()
            )));
        }

        // DEL-008: re-stat right before the delete; an object that changed
        // or vanished since the caller planned the removal stops here.
        if let Err(e) = storage.stat(&bucket.name, path).await {
            return Ok(crate::job_run::Step::failed_msg(format!(
                "target changed or missing, delete stopped: {e}"
            )));
        }

        storage.delete(&bucket.name, path).await?;
        // DEL-009: tombstone so a later index refresh reconciles the removal.
        self.mutations
            .settle_delete(bucket.connection_id, bucket.id, path, self.clock.now())
            .await;
        Ok(crate::job_run::Step::done(Outcome::Succeeded, None))
    }
}

/// `img/` given prefix `img/` → display name `img` for a directory entry.
fn trim_dir_name(prefix: &str, common_prefix: &str) -> String {
    let name = common_prefix.strip_prefix(prefix).unwrap_or(common_prefix);
    name.trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_storage_api::model::PutOptions;
    use rstore_test_support::FakeStorageFactory;

    async fn setup() -> (Arc<dyn RemoteService>, Arc<FakeStorageFactory>) {
        let (app, factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;

        // Seed the fake with a small tree.
        let storage = factory.storage();
        for key in [
            "img/logo.png",
            "img/photo.jpg",
            "docs/readme.md",
            "root.txt",
        ] {
            storage
                .put(
                    "media",
                    &rstore_domain::remote::ObjectPath::new(key).unwrap(),
                    bytes::Bytes::from_static(b"x"),
                    &PutOptions::default(),
                )
                .await
                .unwrap();
        }
        (app.api().transfer().remote(), factory)
    }

    #[test]
    fn remote_uri_preserves_the_directory_marker() {
        let canonical = RemoteUri::parse("nas/media/docs/").unwrap();
        assert_eq!(canonical.key, "docs/");
        assert_eq!(RemoteUri::parse_base("nas/media/docs/").unwrap().2, "docs/");
    }

    /// The short-reference rule: the trailing
    /// `/` survives expansion and the canonical form carries the connection
    /// id.
    #[tokio::test]
    async fn short_reference_expansion_preserves_the_directory_marker() {
        let (app, _factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;
        let directory = expand_reference(
            app.api().registry().connections().as_ref(),
            app.api().registry().buckets().as_ref(),
            "media/docs/",
        )
        .await
        .unwrap();
        let exact = expand_reference(
            app.api().registry().connections().as_ref(),
            app.api().registry().buckets().as_ref(),
            "media/docs",
        )
        .await
        .unwrap();

        assert!(directory.ends_with("/media/docs/"), "{directory}");
        assert!(exact.ends_with("/media/docs"), "{exact}");
    }

    #[tokio::test]
    async fn ls_stat_and_catalog_update() {
        let (svc, _factory) = setup().await;

        // Bucket root: dirs first, then files.
        let page = svc.ls("nas/media/", None, 50, false).await.unwrap();
        let names: Vec<&str> = page.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["docs", "img", "root.txt"]);
        assert!(page.entries[0].is_dir);

        // Subdirectory prefix.
        let page = svc.ls("nas/media/img/", None, 50, true).await.unwrap();
        let names: Vec<&str> = page.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["logo.png", "photo.jpg"]);
        assert!(page.catalog_updated);

        // Stat needs a non-directory URI.
        let meta = svc.stat("nas/media/root.txt").await.unwrap();
        assert_eq!(meta.key, "root.txt");
        assert!(svc.stat("nas/media/root.txt/").await.is_err());
    }

    #[tokio::test]
    async fn recursive_remove_expands_prefix_and_stops_on_missing() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "dir/one.txt", b"1", None);
        factory.storage().seed("media", "dir/two.txt", b"2", None);

        // Dry run lists the prefix and reports every object (DEL-003).
        let before = factory.built();
        let report = svc
            .remove(
                &["nas/media/dir/".into()],
                true,
                true,
                &rstore_transfer::CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(factory.built() - before, 1);
        let outcomes: Vec<(String, RemoveOutcome)> = report
            .items
            .iter()
            .map(|i| (i.uri.clone(), i.outcome))
            .collect();
        assert_eq!(
            outcomes,
            vec![
                ("nas/media/dir/one.txt".into(), RemoveOutcome::WouldDelete),
                ("nas/media/dir/two.txt".into(), RemoveOutcome::WouldDelete),
            ]
        );

        // The real run deletes the prefix contents (DEL-008 re-stats first).
        let report = svc
            .remove(
                &["nas/media/dir/".into()],
                true,
                false,
                &rstore_transfer::CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(report.deleted, 2);
        assert!(factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("dir/one.txt").unwrap()
            )
            .await
            .is_err());
        assert!(factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("dir/two.txt").unwrap()
            )
            .await
            .is_err());

        // A vanished target no longer deletes blindly (DEL-008).
        let report = svc
            .remove(
                &["nas/media/dir/one.txt".into()],
                false,
                false,
                &rstore_transfer::CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(report.failed, 1);
    }

    #[tokio::test]
    async fn recursive_remove_keeps_an_unslashed_key_exact() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "dir", b"exact", None);
        factory
            .storage()
            .seed("media", "dir/child.txt", b"child", None);

        let report = svc
            .remove(
                &["nas/media/dir".into()],
                true,
                false,
                &rstore_transfer::CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(report.deleted, 1);
        assert!(factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("dir").unwrap()
            )
            .await
            .is_err());
        assert!(factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("dir/child.txt").unwrap()
            )
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn planned_count_matches_what_the_run_deletes() {
        let (svc, factory) = setup().await;
        factory.storage().seed("media", "img/a.png", b"a", None);
        factory.storage().seed("media", "img/b.png", b"b", None);
        factory.storage().seed("media", "docs/c.md", b"c", None);

        // One plan serves the confirmation count and the execution: the
        // recursive prefix is listed once and both see the same item set
        // (the setup tree's logo/photo plus two seeded here).
        let plan = svc
            .plan_removals(&["nas/media/img/".into()], true)
            .await
            .unwrap();
        assert_eq!(plan.count(), 4);
        let report = svc
            .remove_planned(plan, false, &rstore_transfer::CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.deleted, 4);
        assert!(factory
            .storage()
            .stat(
                "media",
                &rstore_domain::remote::ObjectPath::new("docs/c.md").unwrap()
            )
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn remove_dry_run_guard_and_delete() {
        let (svc, _) = setup().await;

        let report = svc
            .remove(
                &["nas/media/root.txt".into()],
                false,
                true,
                &rstore_transfer::CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(report.items[0].outcome, RemoveOutcome::WouldDelete);

        let report = svc
            .remove(
                &["nas/media/root.txt".into()],
                false,
                false,
                &rstore_transfer::CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(report.items[0].outcome, RemoveOutcome::Deleted);
        assert!(svc.stat("nas/media/root.txt").await.is_err());

        // The tombstone was written and the object vanished from listings.
        let page = svc.ls("nas/media/", None, 50, false).await.unwrap();
        assert!(!page.entries.iter().any(|e| e.name == "root.txt"));
    }

    #[tokio::test]
    async fn bad_uris_are_input_errors() {
        let (svc, _) = setup().await;
        let err = svc.ls("http://x/y/", None, 50, false).await.unwrap_err();
        assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
        let err = svc.ls("nas/", None, 50, false).await.unwrap_err();
        assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
    }
}
