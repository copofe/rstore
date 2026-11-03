//! Resumable, cancellable upload/download engine and remote-to-remote copy
//! entry over [`rstore_storage::Storage`] adapters.
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
