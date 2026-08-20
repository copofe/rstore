//! Query, filter, sort and pagination value types for the mixed file list
//! (MIX- / IDX-024 Keyset pagination).

use serde::{Deserialize, Serialize};

use rstore_domain::remote::FileType;
use rstore_domain::{BucketId, ConnectionId, TagId, WorkspaceId};

/// Sortable fields (MIX-023).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortField {
    Name,
    Size,
    Modified,
    Type,
    Bucket,
    Connection,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileSort {
    pub field: SortField,
    pub order: SortOrder,
}

impl Default for FileSort {
    fn default() -> Self {
        // PRD §10.7.4: default modified desc.
        Self {
            field: SortField::Modified,
            order: SortOrder::Desc,
        }
    }
}

/// Mixed-list filter expression (MIX-011..022).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FileFilter {
    pub name_contains: Option<String>,
    pub path_contains: Option<String>,
    pub buckets: Vec<BucketId>,
    pub connections: Vec<ConnectionId>,
    pub file_type: Option<FileType>,
    pub extension: Option<String>,
    pub min_size: Option<u64>,
    pub max_size: Option<u64>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub modified_after: Option<time::OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub modified_before: Option<time::OffsetDateTime>,
    /// Tags required to be present (default AND semantics — MIX-019).
    pub tags: Vec<TagId>,
    pub tag_mode_or: bool,
    pub untagged: bool,
    /// Catalog state filter (MIX-022); `None` = anything not deleted.
    pub state: Option<rstore_domain::ObjectState>,
}

/// Keyset pagination request (IDX-024): a stable cursor + page size.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageRequest {
    pub limit: u32,
    /// Opaque cursor from a previous page's `next_cursor`. Its encoding is
    /// private to the producing adapter: a cursor issued by the SQLite
    /// implementation must be replayed against the same adapter (the
    /// in-memory reference implementation encodes cursors differently).
    /// Never parse, construct, or forward one across adapters.
    pub after: Option<String>,
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            limit: 200,
            after: None,
        }
    }
}

/// A page of results and the cursor for the next page, if more remain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// One fully assembled row for the mixed files page.
///
/// The catalog adapter owns this read model so callers do not need to turn a
/// page of object ids into a sequence of registry and tag lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogPageItem {
    pub object: rstore_domain::remote::RemoteObject,
    pub bucket_name: String,
    pub connection_name: String,
    pub tags: Vec<String>,
}

/// A complete mixed files page.
///
/// `total` is the number of rows matching the filter before `after` is
/// applied. It therefore stays stable across every page of one cursor walk,
/// including an empty page after the final item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogPage {
    pub items: Vec<CatalogPageItem>,
    pub total: u64,
    pub next_cursor: Option<String>,
}

impl<T> Page<T> {
    pub fn map<U>(self, f: impl Fn(T) -> U) -> Page<U> {
        Page {
            items: self.items.into_iter().map(f).collect(),
            next_cursor: self.next_cursor,
        }
    }
}

/// Which workspace scope a query runs in (WSP-).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Scope {
    pub workspace: WorkspaceId,
}
