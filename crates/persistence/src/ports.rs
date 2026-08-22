//! `Database`-level port implementations.
//!
//! The per-repository ports (`ConnectionStore`, `BucketStore`, …) are
//! implemented directly in their repository modules under `repo/`; this
//! module keeps only the ports whose subject is the opened database itself.

use async_trait::async_trait;
use rstore_application_ports::{RuntimeStats, UnitOfWork, UnitOfWorkOperation};
use rstore_domain::Result;

use crate::Database;

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
        let UnitOfWorkOperation::RemoteMutation { receipt, event } = operation;
        self.outbox().append_mutation(&receipt, &event).await
    }
}
