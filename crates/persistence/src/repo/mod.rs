//! SQLite repository implementations backing the application port traits.
//!
//! Each child module maps one aggregate or store contract onto the shared
//! connection pool; SQL construction for dynamic catalog queries lives in
//! `catalog_sql`.

/// Bucket registration and conflict-policy storage.
pub mod bucket;
pub(crate) mod catalog_sql;
/// Local connection configuration, including credentials.
pub mod connection;
/// Per-bucket ignore rules.
pub mod ignore;
/// Index runs, scan checkpoints and catalog revisions.
pub mod index;
/// Transfer-job records with per-item state.
pub mod job;
/// Per-bucket link profiles.
pub mod link;
/// Multipart upload checkpoints for resumable transfers.
pub mod multipart;
/// Transactional event outbox with dead-lettering and repair items.
pub mod outbox;
/// Durable receipts for completed remote mutations.
pub mod receipt;
/// Indexed remote objects and deletion tombstones.
pub mod remote_object;
/// Repair items: the queryable record of follow-up failures.
pub mod repair;
/// Key/value app settings such as the capability cache.
pub mod settings;
/// Tag definitions and object associations.
pub mod tag;
/// Workspaces and their bucket memberships.
pub mod workspace;
