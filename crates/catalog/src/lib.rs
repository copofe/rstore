//! Local catalog read/write contracts.
//!
//! The catalog is the rebuildable local index over remote objects. This crate
//! defines the repository and query interfaces plus the filter/sort/pagination
//! value types used by the mixed file list and search. A SQLite-backed and an
//! in-memory implementation share the same seam; shared contract tests pin
//! their observable behavior so the two adapters cannot drift.

pub mod order;
pub mod query;
pub mod repo;
pub mod tag_store;

pub use order::{cursor_of, decode, order_key, SortKey};
pub use query::{
    CatalogPage, CatalogPageItem, FileFilter, FileSort, Page, PageRequest, Scope, SortField,
    SortOrder,
};
pub use repo::{CatalogQuery, InMemoryCatalog, RemoteObjectRepository};
pub use tag_store::TagStore;
