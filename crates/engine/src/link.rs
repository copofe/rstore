//! Link generation behind `rstore url …`.
//!
//! One service turns a remote reference into the requested representation — object path, S3
//! URI, public URL, presigned URL, Markdown, or HTML — using the connection's defaults and the
//! bucket's [`LinkProfile`] overrides for public/CDN bases and permissions. A public URL needs a
//! configured base; without one a URL-bearing format gets a presigned link, the only kind sure to
//! open (most providers refuse unsigned reads on the S3 endpoint). Presigning
//! delegates to the storage adapter; a bucket or adapter that disallows it surfaces
//! `CapabilityUnsupported` (CLI exit 11) rather than a silently degraded link. Presigned URLs
//! are results, never logs.

use std::sync::Arc;

use rstore_domain::link::LinkProfile;
use rstore_domain::{Clock, Error, Result};
use rstore_storage::model::{PresignMethod, PresignRequest};

use crate::batch_outcome::{BatchOutcome, ItemOutcome};
use crate::gateway::RemoteGateway;
use crate::reference::RemoteUri;

/// The requested representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkFormat {
    Path,
    S3,
    Url,
    Markdown,
    Html,
    Json,
}

impl LinkFormat {
    /// Parse a CLI format token (`md` is accepted as an alias for `markdown`).
    pub fn parse(token: &str) -> Result<Self> {
        match token {
            "path" => Ok(Self::Path),
            "s3" => Ok(Self::S3),
            "url" => Ok(Self::Url),
            "markdown" | "md" => Ok(Self::Markdown),
            "html" => Ok(Self::Html),
            "json" => Ok(Self::Json),
            other => Err(Error::invalid_input(format!(
                "unknown link format `{other}` (path, s3, url, markdown, html, json)"
            ))),
        }
    }
}

/// One generate request.
#[derive(Debug, Clone)]
pub struct LinkRequest {
    pub format: LinkFormat,
    pub signed: bool,
    /// Explicit TTL in seconds; `None` uses the profile default.
    pub ttl_seconds: Option<u64>,
}

/// The generated link in every representation; `requested` is what the caller prints and keeps
/// the JSON shape stable across releases.
#[derive(Debug, Clone, serde::Serialize)]
pub struct GeneratedLink {
    #[serde(skip)]
    pub uri: String,
    pub path: String,
    pub s3_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presigned_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(skip)]
    pub requested: String,
}

/// Parse a TTL like `30s`, `15m`, `1h`, `7d`, `1h30m`, or plain seconds.
///
/// S3 presigning caps at seven days; the parser enforces the same bound so every adapter gets
/// a valid request.
pub fn parse_ttl(raw: &str) -> Result<u64> {
    rstore_domain::duration::parse_span(raw, "smhd", 7 * 86_400, "TTL", "7d (S3 presign limit)")
}

/// Resolve the URL serving `bucket`'s root from connection defaults and bucket overrides.
///
/// Precedence: bucket CDN override, connection CDN default, bucket public override, then
/// connection public default; `None` when nothing is configured. A bucket override is that
/// bucket's root (a custom domain, an r2.dev address); a connection default serves every
/// bucket under `/<bucket>`. The S3 endpoint is never a fallback: R2, Supabase, and private
/// buckets everywhere refuse unsigned reads there. No trailing slash, so callers join with `/`.
pub fn resolve_link_base(
    config: &rstore_domain::ConnectionConfig,
    profile: &LinkProfile,
    bucket: &str,
) -> Option<String> {
    let own = |base: &str| base.trim_end_matches('/').to_string();
    let shared = |base: &str| format!("{}/{bucket}", base.trim_end_matches('/'));
    profile
        .cdn_base_url
        .as_deref()
        .map(own)
        .or_else(|| config.cdn_base_url.as_deref().map(shared))
        .or_else(|| profile.public_base_url.as_deref().map(own))
        .or_else(|| config.public_base_url.as_deref().map(shared))
}

/// Editable link-profile fields for the application access surface.
///
/// The outer `None` keeps the current value; the inner `None` (or an empty string, which
/// normalizes to it) clears a base override.
#[derive(Debug, Clone, Default)]
pub struct LinkProfileInput {
    pub public_base_url: Option<Option<String>>,
    pub cdn_base_url: Option<Option<String>>,
    pub allow_public_url: Option<bool>,
    pub allow_presigned: Option<bool>,
    pub default_ttl_seconds: Option<u64>,
}

/// Link generation service.
///
/// Resolves remote URIs through the gateway and merges connection defaults with
/// per-bucket [`LinkProfile`] overrides.
pub(crate) struct LinkService {
    gateway: RemoteGateway,
    buckets: Arc<crate::bucket::BucketService>,
    profiles: Arc<crate::db::LinkRepository>,
    clock: Arc<dyn Clock>,
}

impl LinkService {
    pub(crate) fn new(
        gateway: RemoteGateway,
        buckets: Arc<crate::bucket::BucketService>,
        profiles: Arc<crate::db::LinkRepository>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            gateway,
            buckets,
            profiles,
            clock,
        }
    }

    /// The bucket's link profile (defaults when never configured).
    pub(crate) async fn profile_for(
        &self,
        bucket: &crate::bucket::BucketRef,
    ) -> Result<LinkProfile> {
        let bucket = self.buckets.resolve(bucket).await?;
        self.profiles.get(bucket.id).await
    }

    /// Merge `input` into the bucket's link profile and persist it.
    ///
    /// Merge semantics follow the `Option<Option<T>>` convention: `None` fields keep their
    /// current values, `Some(None)` clears an override, and base-URL values pass through
    /// `ConnectionConfig::normalize_base_url`, so surrounding whitespace, trailing slashes,
    /// and empty strings all normalize away.
    pub(crate) async fn set_profile(
        &self,
        bucket: &crate::bucket::BucketRef,
        input: LinkProfileInput,
    ) -> Result<LinkProfile> {
        let resolved = self.buckets.resolve(bucket).await?;
        let mut profile = self.profiles.get(resolved.id).await?;
        if let Some(base) = input.public_base_url {
            profile.public_base_url = rstore_domain::ConnectionConfig::normalize_base_url(base);
        }
        if let Some(base) = input.cdn_base_url {
            profile.cdn_base_url = rstore_domain::ConnectionConfig::normalize_base_url(base);
        }
        if let Some(allow) = input.allow_public_url {
            profile.allow_public_url = allow;
        }
        if let Some(allow) = input.allow_presigned {
            profile.allow_presigned = allow;
        }
        if let Some(ttl) = input.default_ttl_seconds {
            profile.default_ttl_seconds = ttl;
        }
        self.profiles.upsert(resolved.id, &profile).await?;
        Ok(profile)
    }

    /// Generate links for a batch of URIs; never fails as a whole — each URI
    /// gets an [`ItemOutcome`] carrying either the generated link or the
    /// projected failure.
    pub(crate) async fn generate(
        &self,
        uris: &[RemoteUri],
        request: &LinkRequest,
    ) -> BatchOutcome<GeneratedLink> {
        let mut items = Vec::with_capacity(uris.len());
        for uri in uris {
            items.push(match self.one(uri, request).await {
                Ok(link) => ItemOutcome::succeeded(uri.to_string(), link),
                Err(e) => ItemOutcome::failed(uri.to_string(), &e),
            });
        }
        BatchOutcome::new(items)
    }

    async fn one(&self, uri: &RemoteUri, request: &LinkRequest) -> Result<GeneratedLink> {
        let parsed = uri;
        let (config, bucket) = self.gateway.resolve_bucket(parsed).await?;
        let key = &parsed.key;
        if key.is_empty() {
            return Err(Error::invalid_input(format!(
                "`{uri}` is a bucket, not an object"
            )));
        }
        let path = parsed.object_path()?;
        let profile = self.profiles.get(bucket.id).await?;

        // Delivery URL: bucket CDN override, connection CDN default, bucket public override,
        // then connection public default.
        let public_url = if profile.allow_public_url {
            resolve_link_base(&config, &profile, &bucket.name)
                .map(|root| format!("{root}/{}", encode_path(key)))
        } else {
            None
        };

        // Presigned URL: when asked for, and in place of a missing public URL for every format
        // that carries one. Gated on the profile's permission, TTL from the explicit request or
        // the profile default.
        let carries_url = !matches!(request.format, LinkFormat::Path | LinkFormat::S3);
        let fallback = carries_url && public_url.is_none() && profile.allow_presigned;
        let (presigned_url, expires_at) = if request.signed || fallback {
            if !profile.allow_presigned {
                return Err(Error::unsupported(format!(
                    "presigned links are disabled for bucket `{}`",
                    bucket.name
                )));
            }
            let ttl = match request.ttl_seconds {
                Some(t) => t,
                None => profile.default_ttl_seconds,
            };
            let storage = self.gateway.storage_for_config(&config).await?;
            let presigned = storage
                .presign(&PresignRequest {
                    bucket: bucket.name.clone(),
                    key: key.clone(),
                    method: PresignMethod::Get,
                    ttl_seconds: ttl,
                })
                .await?;
            let expires = self.clock.now() + time::Duration::seconds(ttl as i64);
            (Some(presigned.url), Some(expires))
        } else {
            (None, None)
        };

        let s3_uri = format!("s3://{}/{}", bucket.name, key);
        let name = path.file_name().to_string();
        let expires_iso = expires_at.map(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default()
        });

        // Derivation is demand-driven: a URL is load-bearing only for the formats that render
        // one, so `path`/`s3`/`json` succeed on a bucket with every URL flavour disabled.
        let requested = match request.format {
            LinkFormat::Path => path.as_str().to_string(),
            LinkFormat::S3 => s3_uri.clone(),
            LinkFormat::Json => serde_json::json!({
                "uri": uri.to_string(),
                "path": path.as_str(),
                "s3_uri": s3_uri,
                "url": public_url,
                "presigned_url": presigned_url,
                "expires_at": expires_iso,
            })
            .to_string(),
            format => {
                let url = presigned_url
                    .clone()
                    .or_else(|| public_url.clone())
                    .ok_or_else(|| {
                        Error::unsupported(format!(
                            "no link for bucket `{}`: presigned links are disabled and \
                             public links need a public base",
                            bucket.name
                        ))
                    })?;
                render_document(format, &name, &url)
            }
        };

        Ok(GeneratedLink {
            uri: uri.to_string(),
            path: path.as_str().to_string(),
            s3_uri,
            public_url,
            presigned_url,
            expires_at: expires_iso,
            requested,
        })
    }
}

/// Render a URL-bearing representation: `url` verbatim, or the image-aware markdown/HTML
/// document grammar.
fn render_document(format: LinkFormat, name: &str, url: &str) -> String {
    match format {
        LinkFormat::Markdown if is_image(name) => format!("![{name}]({url})"),
        LinkFormat::Markdown => format!("[{name}]({url})"),
        LinkFormat::Html if is_image(name) => {
            format!("<img src=\"{url}\" alt=\"{}\">", escape_html(name))
        }
        LinkFormat::Html => format!("<a href=\"{url}\">{}</a>", escape_html(name)),
        _ => url.to_string(),
    }
}

fn is_image(name: &str) -> bool {
    matches!(
        name.rsplit('.')
            .next()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("png" | "jpg" | "jpeg" | "gif" | "webp" | "svg" | "bmp" | "avif")
    )
}

/// Escape the HTML-significant characters in link text.
fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\"', "&quot;")
}

/// Percent-encode an object key for URL path segments: keep the unreserved set and `/` (a
/// legitimate path separator), escape the rest.
fn encode_path(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}
