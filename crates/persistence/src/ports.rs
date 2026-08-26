//! Ports implemented directly on `Database`.
//!
//! Per-repository ports live in their repository modules under `repo/`; this
//! module holds only the ports whose subject is the opened database itself.
use async_trait::async_trait;
use rstore_application_ports::{OutboxEvent, RemoteMutationReceipt, UnitOfWork};
use rstore_domain::Result;

use crate::Database;

#[async_trait]
impl UnitOfWork for Database {
    /// Commit one remote mutation: receipt and outbox event land in a single transaction, so a crash
    /// cannot record one without the other.
    async fn commit(&self, receipt: RemoteMutationReceipt, event: OutboxEvent) -> Result<()> {
        self.outbox().append_mutation(&receipt, &event).await
    }
}
