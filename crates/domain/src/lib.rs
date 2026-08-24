//! Domain core: entities, value objects, strongly-typed IDs, the unified
//! error model, an injectable clock and guarded state machines.
//!
//! Hard dependency rule: this crate MUST NOT depend on any UI toolkit, cloud
//! SDK or database driver. Everything in here is pure Rust + serde.

pub mod bucket;
pub mod clock;
pub mod conflict;
pub mod connection;
pub mod contexts;
pub mod duration;
pub mod error;
pub mod events;
pub mod id;
pub mod job;
pub mod link;
pub mod locator;
pub mod policy;
pub mod remote;
pub mod secret;
pub mod workspace;

pub use bucket::{Bucket, BucketConfig};
pub use clock::{Clock, SystemClock};
pub use conflict::{ConflictDecision, ConflictProbe, Resolved};
pub use connection::{AddressingStyle, ConnectionConfig, ConnectionTestStatus, Region};
pub use contexts::BoundedContext;
pub use error::{Error, ErrorCode, ErrorContext, Result};
pub use events::{DomainEvent, EventEnvelope, DOMAIN_EVENT_SCHEMA_VERSION};
pub use id::{
    BucketId, ConnectionId, IndexRunId, JobId, ReceiptId, RemoteObjectId, RepairId, TagId, TypedId,
    WorkspaceId,
};
pub use job::{Job, JobItem, JobKind, JobState};
pub use link::LinkProfile;
pub use locator::RemoteLocator;
pub use policy::ConflictPolicy;
pub use remote::{FileType, ObjectPath, RemoteObject};
pub use secret::{mask_secret, Secret};
pub use workspace::{
    normalize_tag_name, IgnoreRule, IgnoreRuleId, IndexRun, IndexRunState, ObjectState, Tag,
    Workspace, WorkspaceBucket,
};

/// Crate-wide version string surfaced by `rstore --version`.
pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");
