//! Bucket domain model (BKT-).
//!
//! `Bucket` is the local registration of a real remote bucket plus its
//! per-machine settings (BKT-006..010). It does not own remote data.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::id::{BucketId, ConnectionId};
use crate::policy::ConflictPolicy;

/// Locally-configured settings attached to a real bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketConfig {
    /// Display name override (defaults to the real bucket name — BKT-006).
    pub display_name: Option<String>,
    /// Core-level read-only guard (BKT-007 / SEC-004).
    pub read_only: bool,
    /// Whether destructive operations are allowed (BKT-008 / SEC-005).
    pub allow_delete: bool,
    /// Whether this bucket participates in the index (BKT-009).
    pub index_enabled: bool,
    /// Default conflict policy for transfers into this bucket (BKT-010).
    pub default_conflict: ConflictPolicy,
    /// Per-bucket concurrency override (None = use global, BKT-012).
    pub concurrency: Option<u32>,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self {
            display_name: None,
            read_only: false,
            allow_delete: true,
            index_enabled: true,
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
