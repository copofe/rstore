//! Persistence ports owned by the application core.
//!
//! Concrete persistence implementations depend on these contracts; the
//! application core never depends on SQLite or any concrete adapter.
//! Provider-facing remote object-store ports live in `rstore-storage-api`
//! because they describe remote object-store semantics, not local records.

pub mod deps;

use async_trait::async_trait;
pub use deps::{
    ApplicationDependencies, CatalogDependencies, RegistryDependencies, RuntimeDependencies,
    TransferDependencies,
};
use rstore_domain::connection::{ConnectionConfig, ConnectionTestStatus};
use rstore_domain::id::{
    BucketId, ConnectionId, EventId, IndexRunId, JobId, ReceiptId, RemoteObjectId, RepairId, TagId,
    WorkspaceId,
};
use rstore_domain::job::{Job, JobItem, JobState};
use rstore_domain::remote::RemoteObject;
use rstore_domain::workspace::{IgnoreRule, IndexRun, IndexRunState, Tag, Workspace};
use rstore_domain::{BoundedContext, Bucket, MutationOperation, RemoteLocator, Result};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// A workspace row with an application-facing summary, not a SQLite row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSummary {
    pub workspace: Workspace,
    pub bucket_count: u64,
}

/// A tag row with an application-facing association count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagSummary {
    pub tag: Tag,
    pub object_count: u64,
}

/// A resumable index walk checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexCheckpoint {
    pub continuation_token: Option<String>,
    pub updated_at: OffsetDateTime,
}

/// One atomic index-page publication request.
#[derive(Debug, Clone)]
pub struct IndexPageCommit {
    pub run: IndexRunId,
    pub bucket: BucketId,
    pub objects: Vec<RemoteObject>,
    pub continuation_token: Option<String>,
    pub at: OffsetDateTime,
    pub objects_seen: u64,
    pub error_count: u64,
}

/// One atomic index-run completion request.
#[derive(Debug, Clone)]
pub struct IndexRunCommit {
    pub run: IndexRunId,
    pub bucket: BucketId,
    pub state: IndexRunState,
    pub finished_at: Option<OffsetDateTime>,
    pub objects_seen: u64,
    pub error_count: u64,
    pub at: OffsetDateTime,
}

/// Registry persistence port for connections.
#[async_trait]
pub trait ConnectionStore: Send + Sync {
    async fn insert(&self, config: &ConnectionConfig) -> Result<()>;
    async fn list(&self) -> Result<Vec<ConnectionConfig>>;
    async fn get(&self, id: ConnectionId) -> Result<Option<ConnectionConfig>>;
    async fn get_by_name(&self, name: &str) -> Result<Option<ConnectionConfig>>;
    async fn update(&self, config: &ConnectionConfig) -> Result<()>;
    async fn record_test(
        &self,
        id: ConnectionId,
        status: ConnectionTestStatus,
        at: OffsetDateTime,
    ) -> Result<()>;
    async fn delete(&self, id: ConnectionId) -> Result<bool>;
}

/// Registry persistence port for local Bucket registrations.
#[async_trait]
pub trait BucketStore: Send + Sync {
    async fn insert(&self, bucket: &Bucket) -> Result<()>;
    async fn list(&self) -> Result<Vec<Bucket>>;
    async fn get(&self, id: BucketId) -> Result<Option<Bucket>>;
    async fn get_by_name(&self, connection: ConnectionId, name: &str) -> Result<Option<Bucket>>;
    async fn list_by_connection(&self, connection: ConnectionId) -> Result<Vec<Bucket>>;
    async fn update(&self, bucket: &Bucket) -> Result<()>;
    async fn delete(&self, id: BucketId) -> Result<bool>;
}

/// Workspace aggregate persistence port.
#[async_trait]
pub trait WorkspaceStore: Send + Sync {
    async fn insert(&self, workspace: &Workspace) -> Result<()>;
    async fn list(&self) -> Result<Vec<WorkspaceSummary>>;
    async fn object_counts_by_bucket(&self, workspace: WorkspaceId)
        -> Result<Vec<(BucketId, u64)>>;
    async fn get(&self, id: WorkspaceId) -> Result<Option<Workspace>>;
    async fn get_by_name(&self, name: &str) -> Result<Option<Workspace>>;
    async fn update(
        &self,
        id: WorkspaceId,
        name: Option<&str>,
        description: Option<Option<&str>>,
        at: OffsetDateTime,
    ) -> Result<()>;
    async fn delete(&self, id: WorkspaceId) -> Result<bool>;
    async fn buckets(&self, id: WorkspaceId) -> Result<Vec<BucketId>>;
    async fn add_bucket(
        &self,
        workspace: WorkspaceId,
        bucket: BucketId,
        at: OffsetDateTime,
    ) -> Result<()>;
    async fn remove_bucket(&self, workspace: WorkspaceId, bucket: BucketId) -> Result<bool>;
}

/// Bucket-scoped ignore-rule port.
#[async_trait]
pub trait IgnoreRuleStore: Send + Sync {
    async fn list(&self, bucket: BucketId) -> Result<Vec<IgnoreRule>>;
    async fn insert(&self, rule: &IgnoreRule) -> Result<()>;
    async fn delete(&self, bucket: BucketId, directory: &str) -> Result<bool>;
}

/// Long-running index aggregate persistence port.
#[async_trait]
pub trait IndexRunStore: Send + Sync {
    async fn create(&self, run: &IndexRun) -> Result<()>;
    async fn update(
        &self,
        id: IndexRunId,
        state: Option<rstore_domain::workspace::IndexRunState>,
        finished_at: Option<OffsetDateTime>,
        objects_seen: u64,
        error_count: u64,
    ) -> Result<()>;
    async fn get(&self, id: IndexRunId) -> Result<Option<IndexRun>>;
    async fn latest_per_bucket(&self) -> Result<Vec<IndexRun>>;
    async fn checkpoint(&self, bucket: BucketId) -> Result<Option<IndexCheckpoint>>;
    async fn save_checkpoint(
        &self,
        bucket: BucketId,
        continuation_token: Option<&str>,
        at: OffsetDateTime,
    ) -> Result<()>;
    /// Atomically publish one catalog page, its checkpoint and run counters.
    async fn publish_page(&self, commit: &IndexPageCommit) -> Result<()>;
    async fn clear_checkpoint(&self, bucket: BucketId) -> Result<()>;
    async fn revision(&self, bucket: BucketId) -> Result<Option<(i64, OffsetDateTime)>>;
    /// Commit a run's new state and, for a successful run, its catalog
    /// revision in one local transaction. Returns the new revision when one
    /// was bumped.
    async fn finish(&self, commit: &IndexRunCommit) -> Result<Option<i64>>;
}

/// Key/value settings needed by application services, without exposing a DB.
#[async_trait]
pub trait SettingsStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<String>>;
    async fn set(&self, key: &str, value: &str) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<bool>;
    async fn get_json(&self, key: &str) -> Result<Option<serde_json::Value>>;
    async fn set_json(&self, key: &str, value: &serde_json::Value) -> Result<()>;
}

/// Per-bucket link-profile persistence port.
#[async_trait]
pub trait LinkProfileStore: Send + Sync {
    async fn get(&self, bucket: BucketId) -> Result<rstore_domain::LinkProfile>;
    async fn upsert(&self, bucket: BucketId, profile: &rstore_domain::LinkProfile) -> Result<()>;
    async fn clear(&self, bucket: BucketId) -> Result<()>;
}

/// Workspace tag definitions and association port.
///
/// A subtrait of the catalog seam's narrow [`rstore_catalog::TagStore`]:
/// `attach` and `associations_for_object` are declared once there, not
/// duplicated here — every wide-port implementor is automatically a
/// catalog-seam adapter. Callers of the inherited methods need
/// `rstore_catalog::TagStore` in scope.
#[async_trait]
pub trait TagStore: Send + Sync + rstore_catalog::TagStore {
    async fn insert(&self, tag: &Tag) -> Result<()>;
    async fn list(&self, workspace: WorkspaceId) -> Result<Vec<TagSummary>>;
    async fn get(&self, workspace: WorkspaceId, name: &str) -> Result<Option<Tag>>;
    async fn get_by_id(&self, id: TagId) -> Result<Option<Tag>>;
    async fn update(
        &self,
        id: TagId,
        name: Option<&str>,
        color: Option<&str>,
        description: Option<Option<&str>>,
    ) -> Result<()>;
    async fn delete(&self, id: TagId) -> Result<bool>;
    async fn detach(
        &self,
        workspace: WorkspaceId,
        object_id: RemoteObjectId,
        tag: TagId,
    ) -> Result<bool>;
    async fn tags_for_object(
        &self,
        workspace: WorkspaceId,
        object_id: RemoteObjectId,
    ) -> Result<Vec<String>>;
    async fn merge_associations(
        &self,
        workspace: WorkspaceId,
        source: TagId,
        target: TagId,
        at: OffsetDateTime,
    ) -> Result<u64>;
    async fn clear_object(&self, workspace: WorkspaceId, object_id: RemoteObjectId) -> Result<()>;
}

/// Database-derived diagnostics and catalog statistics port. Counts stay
/// counts: callers never load full lists to answer "how many".
#[async_trait]
pub trait RuntimeStats: Send + Sync {
    async fn count_remote_objects(&self) -> Result<u64>;
    async fn count_stale_objects(&self) -> Result<u64>;
    async fn count_tombstones(&self) -> Result<u64>;
    async fn count_pending_outbox(&self) -> Result<u64>;
    async fn count_pending_repairs(&self) -> Result<u64>;
    /// Registry/health counters (doctor).
    async fn count_connections(&self) -> Result<u64>;
    async fn count_untested_connections(&self) -> Result<u64>;
    async fn count_interrupted_jobs(&self) -> Result<u64>;
    async fn size_bytes(&self) -> Result<u64>;
    /// Where the catalog is stored (a file path, or `:memory:`), for diagnostics.
    fn location(&self) -> String;
}

/// Persists jobs and their item rows. Implemented over SQLite by
/// `rstore-persistence`; consumers hold `Arc<dyn …>` so job services stay
/// testable without a database.
#[async_trait]
pub trait JobRepository: Send + Sync {
    async fn create(&self, job: &Job) -> Result<()>;
    async fn get(&self, id: JobId) -> Result<Option<Job>>;
    /// Apply a state transition and persist it.
    async fn transition(&self, id: JobId, target: JobState, at: OffsetDateTime) -> Result<Job>;
    /// Atomically replace terminal items and apply a validated terminal state
    /// transition. A report must never outrun this local commit.
    async fn commit_terminal(
        &self,
        id: JobId,
        items: &[JobItem],
        target: JobState,
        at: OffsetDateTime,
    ) -> Result<Job>;
    async fn list(&self) -> Result<Vec<Job>>;
    /// Replace a job's item rows wholesale (checkpoints / interruptions).
    async fn replace_items(&self, id: JobId, items: &[JobItem]) -> Result<()>;
    async fn delete(&self, id: JobId) -> Result<bool>;
    /// Jobs orphaned by a dead process (pending/running) transition
    /// to `Interrupted`; returns how many. Called once at startup
    /// (`Application::initialize`) — there is no daemon, no job survives its
    /// owner.
    async fn mark_stale_jobs_interrupted(&self, at: OffsetDateTime) -> Result<u64>;
}

/// The kind of follow-up that failed, as recorded by its producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairKind {
    /// A catalog projection step failed after a remote mutation.
    CatalogProjection,
    /// The remote mutation's local settlement (receipt + outbox) failed.
    RemoteMutation,
    /// A move's post-copy verification or source deletion failed.
    MoveFollowup,
    /// Outbox delivery exhausted its attempts and was dead-lettered.
    OutboxDelivery,
}

impl RepairKind {
    /// Canonical stored token for this kind, as written by
    /// [`RepairKind::parse`].
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CatalogProjection => "catalog_projection",
            Self::RemoteMutation => "remote_mutation",
            Self::MoveFollowup => "move_followup",
            Self::OutboxDelivery => "outbox_delivery",
        }
    }

    /// Decode the stored token of a persisted repair row.
    pub fn parse(token: &str) -> Result<Self> {
        match token {
            "catalog_projection" => Ok(Self::CatalogProjection),
            "remote_mutation" => Ok(Self::RemoteMutation),
            "move_followup" => Ok(Self::MoveFollowup),
            "outbox_delivery" => Ok(Self::OutboxDelivery),
            other => Err(rstore_domain::Error::database(format!(
                "unknown repair kind `{other}`"
            ))),
        }
    }
}

impl std::fmt::Display for RepairKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lifecycle state of a queryable repair item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairStatus {
    /// The follow-up failed and waits for repair tooling.
    Required,
    /// The follow-up has been re-applied successfully.
    Resolved,
}

impl RepairStatus {
    /// Canonical stored token for this status, as written by
    /// [`RepairStatus::parse`].
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Required => "repair_required",
            Self::Resolved => "resolved",
        }
    }

    /// Decode the stored token of a persisted repair row.
    pub fn parse(token: &str) -> Result<Self> {
        match token {
            "repair_required" => Ok(Self::Required),
            "resolved" => Ok(Self::Resolved),
            other => Err(rstore_domain::Error::database(format!(
                "unknown repair status `{other}`"
            ))),
        }
    }
}

/// Small structured repair record exposed to diagnostics and job views.
///
/// Identity is deterministic: event-driven repairs (delivery dead-letters,
/// verification divergences) carry the outbox [`Self::event_id`],
/// receipt-driven repairs (settlement/projection failures) carry
/// [`Self::receipt_id`]; re-recording the same cause upserts the same row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairItem {
    pub id: RepairId,
    /// The outbox event this repair derives from, when event-driven.
    pub event_id: Option<EventId>,
    /// The durable mutation receipt this repair follows, when
    /// receipt-driven.
    pub receipt_id: Option<ReceiptId>,
    pub kind: RepairKind,
    pub status: RepairStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub next_attempt_at: OffsetDateTime,
    /// The object the failed follow-up concerned, when known.
    pub locator: Option<RemoteLocator>,
    /// The remote mutation the follow-up belonged to, when known.
    pub operation: Option<MutationOperation>,
}

/// Persistence port for repair records. [`RepairStore::insert`] is the
/// single write path — a plain upsert keyed by the deterministic identity —
/// shared by every producer, including the outbox's own dead-letter.
#[async_trait]
pub trait RepairStore: Send + Sync {
    async fn insert(&self, item: &RepairItem) -> Result<()>;
    async fn list(&self, limit: u32) -> Result<Vec<RepairItem>>;
    async fn resolve(&self, id: RepairId, at: OffsetDateTime) -> Result<bool>;
}

/// Durable record that a remote mutation settled locally.
///
/// Written only through [`UnitOfWork::commit`]; read back for repair
/// tooling and diagnostics. `remote_version`/`remote_etag` capture the
/// provider state observed at completion when the provider reported one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteMutationReceipt {
    pub receipt_id: ReceiptId,
    pub job_id: Option<JobId>,
    pub operation: MutationOperation,
    pub locator: RemoteLocator,
    pub remote_version: Option<String>,
    pub remote_etag: Option<String>,
    pub completed_at: OffsetDateTime,
    pub payload: Option<serde_json::Value>,
}

/// Reliable outbox-delivery port. The first implementation is SQLite-backed;
/// the interface is intentionally independent of a worker or UI runtime.
///
/// Delivery policy lives here in its entirety: [`OutboxStore::retry`] owns
/// the backoff schedule and the exhausted-attempts dead-letter decision, so
/// no dispatcher can drift from it. Repair records are a separate lifecycle
/// behind [`RepairStore`].
#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn append(&self, event: &OutboxEvent) -> Result<()>;
    async fn claim_due(&self, now: OffsetDateTime, limit: u32) -> Result<Vec<OutboxEvent>>;
    async fn recover_processing(&self, now: OffsetDateTime) -> Result<u64>;
    /// Return claimed events that were never handed to a handler to the due
    /// queue, undoing the attempt their claim counted.
    async fn release(&self, event_ids: &[EventId]) -> Result<()>;
    async fn acknowledge(&self, event_id: EventId, at: OffsetDateTime) -> Result<()>;
    /// Schedules the next delivery attempt under the store's backoff
    /// policy, or — once attempts are exhausted — dead-letters the event
    /// into a repair row derived from the event's identity.
    async fn retry(&self, event_id: EventId, error: &str, now: OffsetDateTime) -> Result<()>;
}

/// Versioned event envelope stored by the outbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub event_id: EventId,
    pub context: BoundedContext,
    pub aggregate_type: String,
    pub aggregate_id: String,
    /// Ordering position within the event's aggregate. The value `0` is a
    /// sentinel asking the store to allocate the next version inside the
    /// append transaction; any non-zero value is stored as given and must not
    /// move backwards. Allocation lives in persistence so per-aggregate
    /// ordering is a storage guarantee rather than a producer-side counter.
    pub aggregate_version: u64,
    pub event_type: String,
    pub schema_version: u32,
    pub occurred_at: OffsetDateTime,
    pub payload: serde_json::Value,
    /// Delivery attempt count after the event was claimed.
    pub attempts: u32,
}

/// Unit-of-work boundary between the application core and durable storage.
///
/// Implementations must commit the remote-mutation receipt and its outbox
/// event atomically, so a receipt can never exist without the domain fact that
/// lets projection and repair workers find it. The core never receives a SQL
/// transaction object.
#[async_trait]
pub trait UnitOfWork: Send + Sync {
    async fn commit(&self, receipt: RemoteMutationReceipt, event: OutboxEvent) -> Result<()>;
}
