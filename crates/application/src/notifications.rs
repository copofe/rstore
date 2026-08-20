//! Runtime-only notifications for CLI/Desktop subscribers.
//!
//! These are intentionally separate from durable domain events. Dropping a
//! progress notification is acceptable; dropping an outbox event is not.

use rstore_domain::{JobId, JobState};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressEvent {
    pub job_id: Option<JobId>,
    pub items_done: u64,
    pub items_total: u64,
    pub bytes_done: u64,
    pub bytes_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobNotification {
    StateChanged {
        job_id: JobId,
        state: JobState,
    },
    Progress(ProgressEvent),
    RepairRequired {
        job_id: Option<JobId>,
        repair_id: String,
    },
}
