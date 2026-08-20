//! Transfer bounded context: durable job lifecycle and conflict policy.

pub use crate::conflict::{ConflictDecision, ConflictProbe, Resolved};
pub use crate::events::DomainEvent;
pub use crate::job::{ItemState, Job, JobItem, JobKind, JobState};
pub use crate::policy::ConflictPolicy;
