//! Query, filter, sort and pagination value types for the mixed file list.
//!
//! The keyset pagination contract lives here: a [`PageRequest`] carries an
//! opaque cursor issued by the producing adapter, and a stable total order
//! (`sort value`, `path`, `id` — see `crate::catalog::order`) makes cursor walks
//! repeatable across pages.

use serde::{Deserialize, Serialize};

use rstore_domain::remote::FileType;
use rstore_domain::{BucketId, ConnectionId, TagId, WorkspaceId};

/// A field of the mixed file list a page can be sorted by.
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

/// Direction of a sort; the default listing is most recently modified first.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// The sort clause of a mixed files query: one field plus one direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileSort {
    pub field: SortField,
    pub order: SortOrder,
}

impl Default for FileSort {
    fn default() -> Self {
        Self {
            field: SortField::Modified,
            order: SortOrder::Desc,
        }
    }
}

/// Filter expression narrowing the mixed file list. Every set field narrows
/// with AND semantics.
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
    /// Tags required to be present (AND semantics unless
    /// [`FileFilter::tag_mode_or`] is set).
    pub tags: Vec<TagId>,
    pub tag_mode_or: bool,
    pub untagged: bool,
    /// Catalog state filter; `None` matches anything not deleted.
    pub state: Option<rstore_domain::ObjectState>,
}

/// Keyset pagination request: a page size plus the cursor to resume from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageRequest {
    pub limit: u32,
    /// Opaque cursor from a previous page's `next_cursor`. Its encoding is
    /// private to the producing adapter: a cursor issued by one adapter must
    /// be replayed against that same adapter (implementations encode cursors
    /// differently). Never parse, construct, or forward one across adapters.
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

/// The workspace a query runs in; only buckets joined into the workspace are
/// visible to its queries.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Scope {
    pub workspace: WorkspaceId,
}
