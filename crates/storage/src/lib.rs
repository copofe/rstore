//! Storage abstraction and the S3 adapter.
//!
//! The [`Storage`] facade is the one seam between the engine and an object
//! store: provider quirks live behind it, and it is the seam a fake plugs into.
//!
//! Dependency rule: this crate depends only on `domain` and the provider SDK —
//! never on GPUI or SQLite.

pub mod factory;
pub mod model;
pub mod multipart;
pub mod s3;
pub mod storage;

pub use factory::{StorageCredentials, StorageFactory};
pub use model::{
    guess_content_type, BucketSummary, CopyRequest, DeleteOutcome, ListOptions, ListPage,
    ObjectEntry, ObjectMetadata, PresignRequest, PresignedUrl, PutOptions, ReadRange, UploadedPart,
};
pub use multipart::MultipartSession;
pub use storage::Storage;
