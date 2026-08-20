//! In-memory [`TagStore`] adapter for service tests that do not need SQLite.
//! Membership is not enforced here; tests arrange what they need.

use std::sync::Mutex;

use async_trait::async_trait;
use rstore_catalog::TagStore;
use rstore_domain::id::{TagId, WorkspaceId};
use rstore_domain::Result;
use time::OffsetDateTime;

/// Records of `(workspace, object, tag)` associations.
#[derive(Debug, Default)]
pub struct InMemoryTagStore {
    rows: Mutex<Vec<(WorkspaceId, rstore_domain::RemoteObjectId, TagId)>>,
}

impl InMemoryTagStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every recorded association, for assertions.
    pub fn associations(&self) -> Vec<(WorkspaceId, rstore_domain::RemoteObjectId, TagId)> {
        self.rows.lock().expect("tag store lock poisoned").clone()
    }
}

#[async_trait]
impl TagStore for InMemoryTagStore {
    async fn attach(
        &self,
        workspace: WorkspaceId,
        object: rstore_domain::RemoteObjectId,
        tag: TagId,
        _at: OffsetDateTime,
    ) -> Result<()> {
        let mut rows = self.rows.lock().expect("tag store lock poisoned");
        if !rows.contains(&(workspace, object, tag)) {
            rows.push((workspace, object, tag));
        }
        Ok(())
    }

    async fn associations_for_object(
        &self,
        object: rstore_domain::RemoteObjectId,
    ) -> Result<Vec<(WorkspaceId, TagId)>> {
        Ok(self
            .rows
            .lock()
            .expect("tag store lock poisoned")
            .iter()
            .filter(|(_, o, _)| *o == object)
            .map(|(w, _, t)| (*w, *t))
            .collect())
    }
}
