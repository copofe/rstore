//! Repository implementations over SQLite.

pub mod bucket;
pub(crate) mod catalog_sql;
pub mod connection;
pub mod ignore;
pub mod index;
pub mod job;
pub mod link;
pub mod multipart;
pub mod outbox;
pub mod receipt;
pub mod remote_object;
pub mod settings;
pub mod tag;
pub mod workspace;
