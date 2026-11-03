//! Catalog query vocabulary: filters, sort keys, keyset cursors, and pages.
//!
//! The catalog is a rebuildable cache of remote listings plus the search index
//! for buckets that opt into indexing. The SQL lives in `db`; this module holds
//! only the value types use cases and entry points speak.

pub mod order;
pub mod query;

pub use query::{
    CatalogPage, CatalogPageItem, FileFilter, FileSort, Page, PageRequest, Scope, SortField,
    SortOrder,
};
