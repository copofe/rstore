//! Ports owned by the application core.
//!
//! Concrete persistence implementations depend on this crate; the application
//! core depends on these contracts and never on SQLite or a concrete adapter.
//! The provider-facing storage ports remain in `rstore-storage-api` because
//! they describe remote object-store semantics rather than local application
//! records.

use async_trait::async_trait;
use rstore_domain::connection::{ConnectionConfig, ConnectionTestStatus};
use rstore_domain::id::{BucketId, ConnectionId, IndexRunId, RemoteObjectId, TagId, WorkspaceId};
use rstore_domain::remote::RemoteObject;
use rstore_domain::workspace::{IgnoreRule, IndexRun, IndexRunState, Tag, Workspace};
use rstore_domain::{Bucket, Result};
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

/// Counts of references held by the configured credential sources.
#[derive(Debug, Clone, Copy, Default)]
pub struct SecretSourceCounts {
    pub env_refs: usize,
    pub file_refs: usize,
}

/// Read-only inventory seam used by `doctor` without coupling the application
/// core to a concrete environment or file-backed secret store.
#[async_trait]
pub trait SecretSourceStats: Send + Sync {
    async fn counts(&self) -> Result<SecretSourceCounts>;
}

/// Empty source inventory for adapters and tests that do not expose
/// environment/file credential counts.
pub struct EmptySecretSourceStats;

#[async_trait]
impl SecretSourceStats for EmptySecretSourceStats {
    async fn counts(&self) -> Result<SecretSourceCounts> {
        Ok(SecretSourceCounts::default())
    }
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
    async fn count(&self) -> Result<i64>;
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
        description: Option<&str>,
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
#[async_trait]
pub trait TagStore: Send + Sync {
    async fn insert(&self, tag: &Tag) -> Result<()>;
    async fn list(&self, workspace: WorkspaceId) -> Result<Vec<TagSummary>>;
    async fn get(&self, workspace: WorkspaceId, name: &str) -> Result<Option<Tag>>;
    async fn get_by_id(&self, id: TagId) -> Result<Option<Tag>>;
    async fn update(
        &self,
        id: TagId,
        name: Option<&str>,
        color: Option<&str>,
        description: Option<&str>,
    ) -> Result<()>;
    async fn delete(&self, id: TagId) -> Result<bool>;
    async fn attach(
        &self,
        workspace: WorkspaceId,
        object_id: RemoteObjectId,
        tag: TagId,
        at: OffsetDateTime,
    ) -> Result<()>;
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
    async fn associations_for_object(
        &self,
        object_id: RemoteObjectId,
    ) -> Result<Vec<(WorkspaceId, TagId)>>;
    async fn merge_associations(
        &self,
        workspace: WorkspaceId,
        source: TagId,
        target: TagId,
        at: OffsetDateTime,
    ) -> Result<u64>;
    async fn clear_object(&self, workspace: WorkspaceId, object_id: RemoteObjectId) -> Result<()>;
}

/// Database-derived diagnostics and catalog statistics port.
#[async_trait]
pub trait RuntimeStats: Send + Sync {
    async fn count_remote_objects(&self) -> Result<u64>;
    async fn count_stale_objects(&self) -> Result<u64>;
    async fn count_tombstones(&self) -> Result<u64>;
    async fn count_pending_outbox(&self) -> Result<u64>;
    async fn count_pending_repairs(&self) -> Result<u64>;
    async fn size_bytes(&self) -> Result<u64>;
}

/// Small structured repair record exposed to diagnostics and job views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairItem {
    pub id: String,
    pub receipt_id: Option<String>,
    pub kind: String,
    pub status: String,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub next_attempt_at: OffsetDateTime,
    pub locator: Option<String>,
    pub operation: Option<String>,
}

#[async_trait]
pub trait RepairRepository: Send + Sync {
    async fn insert(&self, item: &RepairItem) -> Result<()>;
    async fn list(&self, limit: u32) -> Result<Vec<RepairItem>>;
    async fn resolve(&self, id: &str, at: OffsetDateTime) -> Result<bool>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteMutationReceipt {
    pub receipt_id: String,
    pub job_id: Option<String>,
    pub operation: String,
    pub locator_json: String,
    pub remote_version: Option<String>,
    pub remote_etag: Option<String>,
    pub completed_at: OffsetDateTime,
    pub payload: Option<serde_json::Value>,
}

#[async_trait]
pub trait RemoteMutationRepository: Send + Sync {
    async fn record(&self, receipt: &RemoteMutationReceipt) -> Result<()>;
    async fn get(&self, receipt_id: &str) -> Result<Option<RemoteMutationReceipt>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerCheckpoint {
    pub consumer_id: String,
    pub last_event_id: Option<String>,
    pub last_event_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
}

#[async_trait]
pub trait ConsumerCheckpointStore: Send + Sync {
    async fn checkpoint(&self, consumer_id: &str) -> Result<Option<ConsumerCheckpoint>>;
    async fn save_checkpoint(&self, checkpoint: &ConsumerCheckpoint) -> Result<()>;
}

/// Reliable outbox/repair port. The first implementation is SQLite-backed;
/// the interface is intentionally independent of a worker or UI runtime.
#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn append(&self, event: &OutboxEvent) -> Result<()>;
    async fn claim_due(&self, now: OffsetDateTime, limit: u32) -> Result<Vec<OutboxEvent>>;
    async fn recover_processing(&self, now: OffsetDateTime) -> Result<u64>;
    async fn acknowledge(&self, event_id: &str) -> Result<()>;
    async fn retry(
        &self,
        event_id: &str,
        next_attempt_at: OffsetDateTime,
        error: &str,
    ) -> Result<()>;
    async fn repairs(&self, limit: u32) -> Result<Vec<RepairItem>>;
}

/// Versioned event envelope stored by the outbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub event_id: String,
    pub context: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub event_type: String,
    pub schema_version: u32,
    pub occurred_at: OffsetDateTime,
    pub payload: serde_json::Value,
    /// Delivery attempt count after the event was claimed.
    pub attempts: u32,
}

/// Unit-of-work boundary. Implementations must commit the supplied aggregate
/// changes and outbox append atomically; the core never receives a SQL
/// transaction object.
#[async_trait]
pub trait UnitOfWork: Send + Sync {
    async fn commit(&self, operation: UnitOfWorkOperation) -> Result<()>;
}

/// Minimal serialisable write set used by the first outbox migration. More
/// context-specific operations can be added without exposing SQLite types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnitOfWorkOperation {
    OutboxOnly {
        event: OutboxEvent,
    },
    /// A completed remote mutation and its durable domain fact. Persistence
    /// must write both rows in one local transaction so a receipt can never
    /// exist without the event that lets a projection/repair worker find it.
    RemoteMutation {
        receipt: RemoteMutationReceipt,
        event: OutboxEvent,
    },
    RepairOnly {
        event: OutboxEvent,
        error: String,
    },
}
