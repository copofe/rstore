//! Catalog bounded context: workspace index and local catalog aggregates.

pub use crate::remote::{FileType, ObjectPath, RemoteObject};
pub use crate::workspace::{
    IgnoreRule, IndexRun, IndexRunState, ObjectState, Tag, Workspace, WorkspaceBucket,
};
