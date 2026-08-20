//! Per-bucket link settings (LINK-003..007): how public URLs are built for a
//! bucket and whether presigning is allowed at all.

/// Link generation settings for one bucket. Stored in `link_profiles`
/// (one row per bucket, defaults when absent).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct LinkProfile {
    /// Base for public URLs (no trailing slash), e.g.
    /// `https://cdn.example.com/assets` (LINK-003/004). `None` derives the
    /// URL from the connection endpoint instead.
    pub public_base_url: Option<String>,
    /// Optional CDN override, preferred over `public_base_url` when set.
    pub cdn_base_url: Option<String>,
    /// Whether plain public URLs may be produced for this bucket.
    pub allow_public_url: bool,
    /// Whether presigned URLs may be produced for this bucket.
    pub allow_presigned: bool,
    /// TTL used when `--signed` carries no explicit `--ttl` (LINK-006).
    pub default_ttl_seconds: u64,
}

impl Default for LinkProfile {
    fn default() -> Self {
        Self {
            public_base_url: None,
            cdn_base_url: None,
            allow_public_url: true,
            allow_presigned: true,
            default_ttl_seconds: 3600,
        }
    }
}
