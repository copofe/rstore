//! Per-bucket link settings: URL overrides and permissions.

/// Link generation settings for one bucket. Stored in `link_profiles`
/// (one row per bucket, defaults when absent).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct LinkProfile {
    /// Optional public-object base override for this bucket.
    pub public_base_url: Option<String>,
    /// Optional CDN/custom-domain delivery override for this bucket.
    pub cdn_base_url: Option<String>,
    /// Whether plain public URLs may be produced for this bucket.
    pub allow_public_url: bool,
    /// Whether presigned URLs may be produced for this bucket.
    pub allow_presigned: bool,
    /// TTL used when `--signed` carries no explicit `--ttl`.
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
