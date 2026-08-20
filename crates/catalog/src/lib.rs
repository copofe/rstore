//! Local catalog read/write contracts (CORE-005).
//!
//! The catalog is the rebuildable local index over remote objects (PRD §4.1).
//! This crate defines the repository and query interfaces plus the filter/sort/
//! pagination value types used by the mixed file list (MIX-) and search. A
//! SQLite-backed and in-memory implementations share the same catalog seam;
//! contract tests pin their observable behavior (QA-003).

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
