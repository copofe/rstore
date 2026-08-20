//! Tag association port.
//!
//! The narrow slice of tag bookkeeping a service needs when it follows
//! objects across copies: list a source's associations and attach them to
//! the target (MOVE-015). `rstore_persistence::TagRepository` implements
//! this over SQLite; `rstore_test_support::InMemoryTagStore` is the second
//! adapter, so services can hold `Arc<dyn TagStore>` instead of the
//! concrete repository (CORE-003 — no per-service SQLite).

use async_trait::async_trait;
use rstore_domain::id::{TagId, WorkspaceId};
use rstore_domain::Result;
use time::OffsetDateTime;

/// Tag associations a copy/move carries from source to target.
#[async_trait]
pub trait TagStore: Send + Sync {
    /// Attach `tag` to `object` in `workspace` (idempotent; membership is
    /// enforced by the adapter).
    async fn attach(
        &self,
        workspace: WorkspaceId,
        object: rstore_domain::RemoteObjectId,
        tag: TagId,
        at: OffsetDateTime,
    ) -> Result<()>;

    /// Every `(workspace, tag)` association of an object, across all
    /// workspaces — the source list for tag preservation on copy/move
    /// (MOVE-015).
    async fn associations_for_object(
        &self,
        object: rstore_domain::RemoteObjectId,
    ) -> Result<Vec<(WorkspaceId, TagId)>>;
}
