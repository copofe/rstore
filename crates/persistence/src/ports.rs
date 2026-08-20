//! Implementations of the infrastructure-independent application ports.
//!
//! This module is the only place where the SQLite repository types are
//! connected to `rstore-application-ports`. The application crate depends on
//! the traits, never on this module or on SQLx.

use async_trait::async_trait;
use rstore_application_ports::{
    BucketStore, ConnectionStore, IgnoreRuleStore, IndexCheckpoint, IndexPageCommit,
    IndexRunCommit, IndexRunStore, LinkProfileStore, OutboxStore, RuntimeStats, SettingsStore,
    TagStore, UnitOfWork, UnitOfWorkOperation, WorkspaceStore,
};
use rstore_domain::connection::{ConnectionConfig, ConnectionTestStatus};
use rstore_domain::id::{BucketId, ConnectionId, IndexRunId, RemoteObjectId, TagId, WorkspaceId};
use rstore_domain::workspace::{IgnoreRule, IndexRun, Tag, Workspace};
use rstore_domain::{Bucket, LinkProfile, Result};
use time::OffsetDateTime;

use crate::repo::index::IndexRunRepository;
use crate::repo::index::ScanCheckpoint;
use crate::repo::workspace::WorkspaceRepository;
use crate::Database;
use crate::{
    BucketRepository, ConnectionRepository, IgnoreRuleRepository, LinkRepository,
    SettingsRepository, TagRepository,
};

#[async_trait]
impl ConnectionStore for ConnectionRepository {
    async fn insert(&self, config: &ConnectionConfig) -> Result<()> {
        ConnectionRepository::insert(self, config).await
    }

    async fn list(&self) -> Result<Vec<ConnectionConfig>> {
        ConnectionRepository::list(self).await
    }

    async fn get(&self, id: ConnectionId) -> Result<Option<ConnectionConfig>> {
        ConnectionRepository::get(self, id).await
    }

    async fn get_by_name(&self, name: &str) -> Result<Option<ConnectionConfig>> {
        ConnectionRepository::get_by_name(self, name).await
    }

    async fn update(&self, config: &ConnectionConfig) -> Result<()> {
        ConnectionRepository::update(self, config).await
    }

    async fn record_test(
        &self,
        id: ConnectionId,
        status: ConnectionTestStatus,
        at: OffsetDateTime,
    ) -> Result<()> {
        ConnectionRepository::record_test(self, id, status, at).await
    }

    async fn delete(&self, id: ConnectionId) -> Result<bool> {
        ConnectionRepository::delete(self, id).await
    }

    async fn count(&self) -> Result<i64> {
        ConnectionRepository::count(self).await
    }
}

#[async_trait]
impl BucketStore for BucketRepository {
    async fn insert(&self, bucket: &Bucket) -> Result<()> {
        BucketRepository::insert(self, bucket).await
    }

    async fn list(&self) -> Result<Vec<Bucket>> {
        BucketRepository::list(self).await
    }

    async fn get(&self, id: BucketId) -> Result<Option<Bucket>> {
        BucketRepository::get(self, id).await
    }

    async fn get_by_name(&self, connection: ConnectionId, name: &str) -> Result<Option<Bucket>> {
        BucketRepository::get_by_name(self, connection, name).await
    }

    async fn list_by_connection(&self, connection: ConnectionId) -> Result<Vec<Bucket>> {
        BucketRepository::list_by_connection(self, connection).await
    }

    async fn update(&self, bucket: &Bucket) -> Result<()> {
        BucketRepository::update(self, bucket).await
    }

    async fn delete(&self, id: BucketId) -> Result<bool> {
        BucketRepository::delete(self, id).await
    }
}

#[async_trait]
impl WorkspaceStore for WorkspaceRepository {
    async fn insert(&self, workspace: &Workspace) -> Result<()> {
        WorkspaceRepository::insert(self, workspace).await
    }

    async fn list(&self) -> Result<Vec<rstore_application_ports::WorkspaceSummary>> {
        WorkspaceRepository::list(self).await.map(|rows| {
            rows.into_iter()
                .map(|row| rstore_application_ports::WorkspaceSummary {
                    workspace: row.workspace,
                    bucket_count: row.bucket_count,
                })
                .collect()
        })
    }

    async fn object_counts_by_bucket(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Vec<(BucketId, u64)>> {
        WorkspaceRepository::object_counts_by_bucket(self, workspace).await
    }

    async fn get(&self, id: WorkspaceId) -> Result<Option<Workspace>> {
        WorkspaceRepository::get(self, id).await
    }

    async fn get_by_name(&self, name: &str) -> Result<Option<Workspace>> {
        WorkspaceRepository::get_by_name(self, name).await
    }

    async fn update(
        &self,
        id: WorkspaceId,
        name: Option<&str>,
        description: Option<&str>,
        at: OffsetDateTime,
    ) -> Result<()> {
        WorkspaceRepository::update(self, id, name, description, at).await
    }

    async fn delete(&self, id: WorkspaceId) -> Result<bool> {
        WorkspaceRepository::delete(self, id).await
    }

    async fn buckets(&self, id: WorkspaceId) -> Result<Vec<BucketId>> {
        WorkspaceRepository::buckets(self, id).await
    }

    async fn add_bucket(
        &self,
        workspace: WorkspaceId,
        bucket: BucketId,
        at: OffsetDateTime,
    ) -> Result<()> {
        WorkspaceRepository::add_bucket(self, workspace, bucket, at).await
    }

    async fn remove_bucket(&self, workspace: WorkspaceId, bucket: BucketId) -> Result<bool> {
        WorkspaceRepository::remove_bucket(self, workspace, bucket).await
    }
}

#[async_trait]
impl IgnoreRuleStore for IgnoreRuleRepository {
    async fn list(&self, bucket: BucketId) -> Result<Vec<IgnoreRule>> {
        IgnoreRuleRepository::list(self, bucket).await
    }

    async fn insert(&self, rule: &IgnoreRule) -> Result<()> {
        IgnoreRuleRepository::insert(self, rule).await
    }

    async fn delete(&self, bucket: BucketId, directory: &str) -> Result<bool> {
        IgnoreRuleRepository::delete(self, bucket, directory).await
    }
}

#[async_trait]
impl IndexRunStore for IndexRunRepository {
    async fn create(&self, run: &IndexRun) -> Result<()> {
        IndexRunRepository::create(self, run).await
    }

    async fn update(
        &self,
        id: IndexRunId,
        state: Option<rstore_domain::workspace::IndexRunState>,
        finished_at: Option<OffsetDateTime>,
        objects_seen: u64,
        error_count: u64,
    ) -> Result<()> {
        IndexRunRepository::update(self, id, state, finished_at, objects_seen, error_count).await
    }

    async fn get(&self, id: IndexRunId) -> Result<Option<IndexRun>> {
        IndexRunRepository::get(self, id).await
    }

    async fn latest_per_bucket(&self) -> Result<Vec<IndexRun>> {
        IndexRunRepository::latest_per_bucket(self).await
    }

    async fn checkpoint(&self, bucket: BucketId) -> Result<Option<IndexCheckpoint>> {
        IndexRunRepository::checkpoint(self, bucket)
            .await
            .map(|checkpoint| checkpoint.map(map_checkpoint))
    }

    async fn save_checkpoint(
        &self,
        bucket: BucketId,
        continuation_token: Option<&str>,
        at: OffsetDateTime,
    ) -> Result<()> {
        IndexRunRepository::save_checkpoint(self, bucket, continuation_token, at).await
    }

    async fn publish_page(&self, commit: &IndexPageCommit) -> Result<()> {
        IndexRunRepository::publish_page(self, commit).await
    }

    async fn clear_checkpoint(&self, bucket: BucketId) -> Result<()> {
        IndexRunRepository::clear_checkpoint(self, bucket).await
    }

    async fn revision(&self, bucket: BucketId) -> Result<Option<(i64, OffsetDateTime)>> {
        IndexRunRepository::revision(self, bucket).await
    }

    async fn finish(&self, commit: &IndexRunCommit) -> Result<Option<i64>> {
        IndexRunRepository::finish(self, commit).await
    }
}

fn map_checkpoint(checkpoint: ScanCheckpoint) -> IndexCheckpoint {
    IndexCheckpoint {
        continuation_token: checkpoint.continuation_token,
        updated_at: checkpoint.updated_at,
    }
}

#[async_trait]
impl SettingsStore for SettingsRepository {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        SettingsRepository::get(self, key).await
    }

    async fn set(&self, key: &str, value: &str) -> Result<()> {
        SettingsRepository::set(self, key, value).await
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        SettingsRepository::delete(self, key).await
    }

    async fn get_json(&self, key: &str) -> Result<Option<serde_json::Value>> {
        SettingsRepository::get_json(self, key).await
    }

    async fn set_json(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        SettingsRepository::set_json(self, key, value).await
    }
}

#[async_trait]
impl LinkProfileStore for LinkRepository {
    async fn get(&self, bucket: BucketId) -> Result<LinkProfile> {
        LinkRepository::get(self, bucket).await
    }

    async fn upsert(&self, bucket: BucketId, profile: &LinkProfile) -> Result<()> {
        LinkRepository::upsert(self, bucket, profile).await
    }

    async fn clear(&self, bucket: BucketId) -> Result<()> {
        LinkRepository::clear(self, bucket).await
    }
}

#[async_trait]
impl TagStore for TagRepository {
    async fn insert(&self, tag: &Tag) -> Result<()> {
        TagRepository::insert(self, tag).await
    }

    async fn list(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Vec<rstore_application_ports::TagSummary>> {
        TagRepository::list(self, workspace).await.map(|rows| {
            rows.into_iter()
                .map(|row| rstore_application_ports::TagSummary {
                    tag: row.tag,
                    object_count: row.object_count,
                })
                .collect()
        })
    }

    async fn get(&self, workspace: WorkspaceId, name: &str) -> Result<Option<Tag>> {
        TagRepository::get(self, workspace, name).await
    }

    async fn get_by_id(&self, id: TagId) -> Result<Option<Tag>> {
        TagRepository::get_by_id(self, id).await
    }

    async fn update(
        &self,
        id: TagId,
        name: Option<&str>,
        color: Option<&str>,
        description: Option<&str>,
    ) -> Result<()> {
        TagRepository::update(self, id, name, color, description).await
    }

    async fn delete(&self, id: TagId) -> Result<bool> {
        TagRepository::delete(self, id).await
    }

    async fn attach(
        &self,
        workspace: WorkspaceId,
        object_id: RemoteObjectId,
        tag: TagId,
        at: OffsetDateTime,
    ) -> Result<()> {
        TagRepository::attach(self, workspace, object_id, tag, at).await
    }

    async fn detach(
        &self,
        workspace: WorkspaceId,
        object_id: RemoteObjectId,
        tag: TagId,
    ) -> Result<bool> {
        TagRepository::detach(self, workspace, object_id, tag).await
    }

    async fn tags_for_object(
        &self,
        workspace: WorkspaceId,
        object_id: RemoteObjectId,
    ) -> Result<Vec<String>> {
        TagRepository::tags_for_object(self, workspace, object_id).await
    }

    async fn associations_for_object(
        &self,
        object_id: RemoteObjectId,
    ) -> Result<Vec<(WorkspaceId, TagId)>> {
        TagRepository::associations_for_object(self, object_id).await
    }

    async fn merge_associations(
        &self,
        workspace: WorkspaceId,
        source: TagId,
        target: TagId,
        at: OffsetDateTime,
    ) -> Result<u64> {
        TagRepository::merge_associations(self, workspace, source, target, at).await
    }

    async fn clear_object(&self, workspace: WorkspaceId, object_id: RemoteObjectId) -> Result<()> {
        TagRepository::clear_object(self, workspace, object_id).await
    }
}

#[async_trait]
impl RuntimeStats for Database {
    async fn count_remote_objects(&self) -> Result<u64> {
        Database::count_remote_objects(self).await
    }

    async fn count_stale_objects(&self) -> Result<u64> {
        Database::count_stale_objects(self).await
    }

    async fn count_tombstones(&self) -> Result<u64> {
        Database::count_tombstones(self).await
    }

    async fn count_pending_outbox(&self) -> Result<u64> {
        Database::count_pending_outbox(self).await
    }

    async fn count_pending_repairs(&self) -> Result<u64> {
        Database::count_pending_repairs(self).await
    }

    async fn size_bytes(&self) -> Result<u64> {
        Database::size_bytes(self).await
    }
}

#[async_trait]
impl UnitOfWork for Database {
    async fn commit(&self, operation: UnitOfWorkOperation) -> Result<()> {
        let outbox = self.outbox();
        match operation {
            UnitOfWorkOperation::OutboxOnly { event } => outbox.append(&event).await,
            UnitOfWorkOperation::RemoteMutation { receipt, event } => {
                outbox.append_mutation(&receipt, &event).await
            }
            UnitOfWorkOperation::RepairOnly { event, error } => {
                outbox.append_and_repair(&event, &error).await
            }
        }
    }
}
