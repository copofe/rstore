//! Domain facts emitted by aggregates.
//!
//! These facts are intentionally independent of delivery. Persistence may
//! store the envelope in an outbox, while the application runtime may publish
//! progress notifications separately. A domain event never contains a UI
//! handle, storage client, or database row.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::contexts::BoundedContext;
use crate::id::{BucketId, ConnectionId, JobId, RemoteObjectId, TagId, TypedId, WorkspaceId};
use crate::locator::RemoteLocator;

/// Version of the serialized domain-event payload contract.
pub const DOMAIN_EVENT_SCHEMA_VERSION: u32 = 1;

/// A business fact produced by an aggregate or an application orchestration
/// that has completed an aggregate-level mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum DomainEvent {
    ConnectionRegistered {
        connection_id: ConnectionId,
    },
    BucketUpdated {
        bucket_id: BucketId,
    },
    WorkspaceBucketAdded {
        workspace_id: WorkspaceId,
        bucket_id: BucketId,
    },
    TagCreated {
        workspace_id: WorkspaceId,
        tag_id: TagId,
    },
    RemoteObjectConfirmed {
        locator: RemoteLocator,
        object_id: RemoteObjectId,
    },
    RemoteObjectTombstoned {
        locator: RemoteLocator,
        object_id: RemoteObjectId,
    },
    /// Application orchestration fact for a remote mutation whose durable
    /// receipt is ready for projection/repair handling.
    RemoteMutationCompleted {
        locator: RemoteLocator,
        operation: String,
    },
    JobStarted {
        job_id: JobId,
    },
    JobInterrupted {
        job_id: JobId,
    },
    JobCompleted {
        job_id: JobId,
    },
}

impl DomainEvent {
    pub fn context(&self) -> BoundedContext {
        match self {
            Self::ConnectionRegistered { .. } | Self::BucketUpdated { .. } => {
                BoundedContext::Registry
            }
            Self::WorkspaceBucketAdded { .. }
            | Self::TagCreated { .. }
            | Self::RemoteObjectConfirmed { .. }
            | Self::RemoteObjectTombstoned { .. } => BoundedContext::Catalog,
            Self::RemoteMutationCompleted { .. } => BoundedContext::Transfer,
            Self::JobStarted { .. } | Self::JobInterrupted { .. } | Self::JobCompleted { .. } => {
                BoundedContext::Transfer
            }
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            Self::ConnectionRegistered { .. } => "connection_registered",
            Self::BucketUpdated { .. } => "bucket_updated",
            Self::WorkspaceBucketAdded { .. } => "workspace_bucket_added",
            Self::TagCreated { .. } => "tag_created",
            Self::RemoteObjectConfirmed { .. } => "remote_object_confirmed",
            Self::RemoteObjectTombstoned { .. } => "remote_object_tombstoned",
            Self::RemoteMutationCompleted { .. } => "remote_mutation_completed",
            Self::JobStarted { .. } => "job_started",
            Self::JobInterrupted { .. } => "job_interrupted",
            Self::JobCompleted { .. } => "job_completed",
        }
    }

    /// Build the stable aggregate-facing envelope written to the outbox.
    pub fn envelope(
        &self,
        event_id: uuid::Uuid,
        aggregate_version: u64,
        occurred_at: OffsetDateTime,
    ) -> EventEnvelope {
        EventEnvelope {
            event_id,
            context: self.context(),
            aggregate_type: self.aggregate_type(),
            aggregate_id: self.aggregate_id().to_string(),
            aggregate_version,
            event_type: self.event_type().to_owned(),
            schema_version: DOMAIN_EVENT_SCHEMA_VERSION,
            occurred_at,
            payload: serde_json::to_value(self)
                .expect("domain events contain only serializable values"),
        }
    }

    fn aggregate_type(&self) -> &'static str {
        match self {
            Self::ConnectionRegistered { .. } => "connection",
            Self::BucketUpdated { .. } => "bucket",
            Self::WorkspaceBucketAdded { .. } | Self::TagCreated { .. } => "workspace",
            Self::RemoteObjectConfirmed { .. } | Self::RemoteObjectTombstoned { .. } => {
                "remote_object"
            }
            Self::RemoteMutationCompleted { .. } => "remote_mutation",
            Self::JobStarted { .. } | Self::JobInterrupted { .. } | Self::JobCompleted { .. } => {
                "job"
            }
        }
    }

    fn aggregate_id(&self) -> String {
        match self {
            Self::ConnectionRegistered { connection_id } => connection_id.as_uuid().to_string(),
            Self::BucketUpdated { bucket_id } => bucket_id.as_uuid().to_string(),
            Self::WorkspaceBucketAdded { workspace_id, .. }
            | Self::TagCreated { workspace_id, .. } => workspace_id.as_uuid().to_string(),
            Self::RemoteObjectConfirmed { object_id, .. }
            | Self::RemoteObjectTombstoned { object_id, .. } => object_id.as_uuid().to_string(),
            Self::RemoteMutationCompleted { locator, .. } => locator.to_string(),
            Self::JobStarted { job_id }
            | Self::JobInterrupted { job_id }
            | Self::JobCompleted { job_id } => job_id.as_uuid().to_string(),
        }
    }
}

/// Persistence-neutral event envelope. Outbox adapters may add delivery
/// metadata, but must preserve these fields and their ordering semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: uuid::Uuid,
    pub context: BoundedContext,
    pub aggregate_type: &'static str,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub event_type: String,
    pub schema_version: u32,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
    pub payload: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_contains_ordering_and_context_metadata() {
        let connection_id = ConnectionId::new();
        let event = DomainEvent::ConnectionRegistered { connection_id };
        let envelope = event.envelope(uuid::Uuid::new_v4(), 3, OffsetDateTime::UNIX_EPOCH);

        assert_eq!(envelope.context, BoundedContext::Registry);
        assert_eq!(envelope.aggregate_type, "connection");
        assert_eq!(envelope.aggregate_id, connection_id.as_uuid().to_string());
        assert_eq!(envelope.aggregate_version, 3);
        assert_eq!(envelope.event_type, "connection_registered");
        assert_eq!(envelope.schema_version, DOMAIN_EVENT_SCHEMA_VERSION);
    }
}
