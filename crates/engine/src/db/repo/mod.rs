//! SQLite repositories, one per aggregate or store.
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
/// Cached remote objects.
pub mod remote_object;
/// Key/value app settings such as the current workspace.
pub mod settings;
/// Tag definitions and object associations.
pub mod tag;
/// Workspaces and their bucket memberships.
pub mod workspace;
