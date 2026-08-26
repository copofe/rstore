//! Resumable, cancellable upload/download engine and remote-to-remote copy
//! entry over [`rstore_storage_api::Storage`] adapters.
//!
//! Retries live at the storage seam ([`RetryingStorage`]); cooperative
//! cancellation is shared by the engine, the copy entry, and indexing
//! callers.

pub mod cancel;
mod conflict;
mod copy;
pub mod engine;
mod multipart;
mod part_file;
pub mod progress;
mod pump;
mod relay;
pub mod retry;
pub mod retrying_storage;
pub mod run;

pub use cancel::CancellationToken;
pub use copy::{copy, CopyEndpoint, CopyRequest};
pub use engine::{DownloadRequest, TransferEngine, UploadRequest};
pub use progress::{NullProgressSink, Progress, ProgressSink};
pub use retry::RetryPolicy;
pub use retrying_storage::RetryingStorage;
pub use run::{ItemOutcome, RunContext, TransferConfig};

// The checkpoint port (`MultipartStore`, `PendingUpload`, `FileFingerprint`)
// lives at the storage seam (`rstore_storage_api::multipart`) so persistence
// never depends on this crate; it is re-exported here for the engine's callers.
pub use rstore_storage_api::{FileFingerprint, MultipartStore, NoopMultipartStore, PendingUpload};
