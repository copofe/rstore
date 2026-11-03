//! Local bucket registration model.
//!
//! A [`Bucket`] is the local registration of a real remote bucket together
//! with its per-machine settings. It does not own remote data.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::id::{BucketId, ConnectionId};
use crate::policy::ConflictPolicy;

/// Locally-configured settings attached to a real bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketConfig {
    /// Display name override; defaults to the real bucket name.
    pub display_name: Option<String>,
    /// Core-level read-only guard.
    pub read_only: bool,
    /// Whether destructive operations are allowed.
    pub allow_delete: bool,
    /// Whether this bucket is indexed (searchable by name and attributes).
    /// Off by default: indexing is an explicit opt-in per bucket.
    pub index_enabled: bool,
    /// Default conflict policy for transfers into this bucket.
    pub default_conflict: ConflictPolicy,
    /// Per-bucket concurrency override; `None` falls back to the global limit.
    pub concurrency: Option<u32>,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self {
            display_name: None,
            read_only: false,
            allow_delete: true,
            index_enabled: false,
            default_conflict: ConflictPolicy::Ask,
            concurrency: None,
        }
    }
}

/// A locally registered bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub id: BucketId,
    pub connection_id: ConnectionId,
    /// The real remote bucket name. This is never rewritten locally.
    pub name: String,
    pub config: BucketConfig,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}
