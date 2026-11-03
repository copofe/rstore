//! Domain core: entities, value objects, strongly-typed IDs, the unified
//! error model, an injectable clock and guarded state machines.
//!
//! Hard dependency rule: this crate MUST NOT depend on any UI toolkit, cloud
//! SDK or database driver. Everything in here is pure Rust + serde.

pub mod bucket;
pub mod clock;
pub mod conflict;
pub mod connection;
pub mod duration;
pub mod error;
pub mod id;
pub mod job;
pub mod link;
pub mod outcome;
pub mod policy;
pub mod remote;
pub mod secret;
pub mod workspace;

pub use bucket::{Bucket, BucketConfig};
pub use clock::{Clock, SystemClock};
pub use conflict::{ConflictDecision, ConflictProbe, Resolved};
pub use connection::{AddressingStyle, ConnectionConfig, ConnectionTestStatus, Region};
pub use error::{Error, ErrorCode, ErrorContext, Result};
pub use id::{
    BucketId, ConnectionId, IndexRunId, JobId, RemoteObjectId, TagId, TypedId, WorkspaceId,
};
pub use job::{Job, JobItem, JobKind, JobState};
pub use link::LinkProfile;
pub use policy::ConflictPolicy;
pub use remote::{FileType, ObjectPath, RemoteObject};
pub use secret::{mask_secret, Secret};
pub use workspace::{
    normalize_tag_name, IgnoreRule, IgnoreRuleId, IndexRun, IndexRunState, ObjectState, Tag,
    Workspace, WorkspaceBucket,
};

/// Crate-wide version string surfaced by `rstore --version`.
pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");
