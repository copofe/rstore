//! S3-compatible storage adapter.
//!
//! Implements [`crate::Storage`] using the AWS SDK for Rust,
//! including multipart transfer, server-side copy and presigning.

mod adapter;
mod error;
pub mod factory;
mod multipart;

pub use adapter::S3Storage;
pub use factory::S3StorageFactory;
