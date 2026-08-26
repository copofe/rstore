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
use crate::locator::RemoteLocator;

/// Version of the serialized domain-event payload contract.
pub const DOMAIN_EVENT_SCHEMA_VERSION: u32 = 1;

/// A business fact produced by an aggregate or an application orchestration
/// that has completed an aggregate-level mutation.
///
/// Variants are admitted only once a producer exists: an unwitnessed variant
/// is dead vocabulary, so the enum currently carries exactly one fact — the
/// durable remote-mutation completion written to the outbox alongside its
/// receipt. Registry/catalog/job facts return with their projection
/// consumers (the resident dispatch loop), not before.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum DomainEvent {
    /// Application orchestration fact for a remote mutation whose durable
    /// receipt is ready for projection/repair handling.
    RemoteMutationCompleted {
        locator: RemoteLocator,
        operation: String,
    },
}

impl DomainEvent {
    /// The bounded context owning this fact.
    pub fn context(&self) -> BoundedContext {
        match self {
            Self::RemoteMutationCompleted { .. } => BoundedContext::Transfer,
        }
    }

    /// Stable snake-case event-type token.
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::RemoteMutationCompleted { .. } => "remote_mutation_completed",
        }
    }

    /// Build the stable aggregate-facing envelope written to the outbox.
    ///
    /// # Panics
    /// Panics if the event payload cannot serialise; every variant is built
    /// from serializable values, so this is unreachable by construction.
    pub fn envelope(
        &self,
        event_id: uuid::Uuid,
        aggregate_version: u64,
        occurred_at: OffsetDateTime,
    ) -> EventEnvelope {
        EventEnvelope {
            event_id,
            context: self.context(),
            aggregate_type: self.aggregate_type().to_owned(),
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
            Self::RemoteMutationCompleted { .. } => "remote_mutation",
        }
    }

    fn aggregate_id(&self) -> String {
        match self {
            Self::RemoteMutationCompleted { locator, .. } => locator.to_string(),
        }
    }
}

/// Persistence-neutral event envelope. Outbox adapters may add delivery
/// metadata, but must preserve these fields and their ordering semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: uuid::Uuid,
    pub context: BoundedContext,
    pub aggregate_type: String,
    pub aggregate_id: String,
    /// Monotonic per-aggregate sequence establishing the ordering guarantee.
    pub aggregate_version: u64,
    pub event_type: String,
    pub schema_version: u32,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
    pub payload: Value,
}
