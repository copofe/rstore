//! Resumable, cancellable upload/download engine over a
//! [`rstore_storage_api::Storage`] adapter.
//!
//! Retries live at the storage seam ([`RetryingStorage`]); cooperative
//! cancellation is shared with copy, move, relay and indexing callers.

pub mod cancel;
mod conflict;
pub mod engine;
mod multipart;
mod part_file;
pub mod progress;
mod pump;
mod relay;
pub mod retry;
pub mod retrying_storage;

pub use cancel::CancellationToken;
pub use conflict::{decide_target, stat_probe, TargetDecision};
pub use engine::{DownloadRequest, ItemOutcome, TransferConfig, TransferEngine, UploadRequest};
pub use progress::{NullProgressSink, Progress, ProgressSink};
pub use relay::{relay, RelayRequest};
pub use retry::RetryPolicy;
pub use retrying_storage::RetryingStorage;

// The checkpoint port (`MultipartStore`, `PendingUpload`, `FileFingerprint`)
// lives at the storage seam (`rstore_storage_api::multipart`) so persistence
// never depends on this crate; it is re-exported here for the engine's callers.
pub use rstore_storage_api::{FileFingerprint, MultipartStore, NoopMultipartStore, PendingUpload};
