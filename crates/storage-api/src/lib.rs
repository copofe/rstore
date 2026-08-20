//! Storage abstraction (CORE-004) and provider capability model (STORE-021).
//!
//! The [`Storage`] facade is the seam between the application core and an
//! object store. Production wiring uses the S3 adapter; `FakeStorage` is the
//! deterministic contract-test implementation.
//!
//! Dependency rule (PRD §6): this crate depends only on `domain` and basic
//! crates — never on GPUI or a concrete SDK.

pub mod capability;
pub mod factory;
pub mod model;
pub mod multipart;
pub mod storage;

pub use capability::Capability;
pub use factory::{StorageCredentials, StorageFactory};
pub use model::{
    guess_content_type, BucketSummary, CopyRequest, DeleteOutcome, ListOptions, ListPage,
    ObjectEntry, ObjectMetadata, PresignRequest, PresignedUrl, PutOptions, ReadRange, UploadedPart,
};
pub use multipart::{FileFingerprint, MultipartStore, NoopMultipartStore, PendingUpload};
pub use storage::Storage;
