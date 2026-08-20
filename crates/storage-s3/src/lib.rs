//! S3-compatible storage adapter.
//!
//! Implements [`rstore_storage_api::Storage`] using the AWS SDK for Rust,
//! including multipart transfer, server-side copy and presigning.

mod adapter;
mod error;
pub mod factory;

pub use adapter::S3Storage;
pub use factory::{build_client, S3StorageFactory};
