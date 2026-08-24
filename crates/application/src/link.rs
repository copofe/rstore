//! Link generation behind `rstore url …`.
//!
//! One service turns a remote reference into the requested representation — object path, S3
//! URI, public URL, presigned URL, Markdown, or HTML — using the connection's defaults and the
//! bucket's [`LinkProfile`] overrides for public/CDN bases and permissions. Presigning
//! delegates to the storage adapter; a bucket or adapter that disallows it surfaces
//! `CapabilityUnsupported` (CLI exit 11) rather than a silently degraded link. Presigned URLs
//! are results, never logs.

use std::sync::Arc;

use rstore_application_ports::LinkProfileStore;
use rstore_domain::link::LinkProfile;
use rstore_domain::{Clock, Error, Result};
use rstore_storage_api::model::{PresignMethod, PresignRequest};

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

/// Resolve the effective object delivery base from connection defaults and bucket overrides.
///
/// Precedence: bucket CDN override, connection CDN default, bucket public override,
/// connection public default, then the unchanged S3 endpoint. The trailing slash is trimmed so
/// callers can join with `/`.
pub fn resolve_link_base(
    config: &rstore_domain::ConnectionConfig,
    profile: &LinkProfile,
) -> String {
    profile
        .cdn_base_url
        .as_deref()
        .or(config.cdn_base_url.as_deref())
        .or(profile.public_base_url.as_deref())
        .or(config.public_base_url.as_deref())
        .unwrap_or(config.endpoint.as_str())
        .trim_end_matches('/')
        .to_string()
}

/// Editable link-profile fields for [`DefaultLinkService::set_profile`].
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
/// Resolves remote URIs against the catalog locator and merges connection defaults with
/// per-bucket [`LinkProfile`] overrides.
pub struct DefaultLinkService {
    gateway: RemoteGateway,
    buckets: Arc<dyn crate::bucket::BucketService>,
    locator: crate::locator::CatalogLocator,
    profiles: Arc<dyn LinkProfileStore>,
    clock: Arc<dyn Clock>,
}

impl DefaultLinkService {
    pub fn new(
        gateway: RemoteGateway,
        buckets: Arc<dyn crate::bucket::BucketService>,
        locator: crate::locator::CatalogLocator,
        profiles: Arc<dyn LinkProfileStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            gateway,
            buckets,
            locator,
            profiles,
            clock,
        }
    }

    /// The bucket's link profile (defaults when never configured).
    pub async fn profile_for(&self, bucket: &crate::bucket::BucketRef) -> Result<LinkProfile> {
        let bucket = self.resolve(bucket).await?;
        self.profiles.get(bucket.id).await
    }

    /// Merge `input` into the bucket's link profile and persist it.
    ///
    /// Merge semantics follow the `Option<Option<T>>` convention: `None` fields keep their
    /// current values, `Some(None)` clears an override, and base-URL values pass through
    /// `ConnectionConfig::normalize_base_url`, so surrounding whitespace, trailing slashes,
    /// and empty strings all normalize away.
    pub async fn set_profile(
        &self,
        bucket: &crate::bucket::BucketRef,
        input: LinkProfileInput,
    ) -> Result<LinkProfile> {
        let resolved = self.resolve(bucket).await?;
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

    async fn resolve(&self, bucket: &crate::bucket::BucketRef) -> Result<rstore_domain::Bucket> {
        self.buckets.resolve(bucket).await
    }

    /// Generate links for a batch of URIs; never fails as a whole — each URI
    /// gets an [`ItemOutcome`] carrying either the generated link or the
    /// projected failure.
    pub async fn generate(
        &self,
        uris: &[String],
        request: &LinkRequest,
    ) -> BatchOutcome<GeneratedLink> {
        let mut items = Vec::with_capacity(uris.len());
        for uri in uris {
            items.push(match self.one(uri, request).await {
                Ok(link) => ItemOutcome::succeeded(uri.clone(), link),
                Err(e) => ItemOutcome::failed(uri.clone(), &e),
            });
        }
        BatchOutcome::new(items)
    }

    async fn one(&self, uri: &str, request: &LinkRequest) -> Result<GeneratedLink> {
        let parsed = RemoteUri::parse(uri)?;
        let (config, bucket) = self.locator.resolve_bucket(&parsed).await?;
        let key = &parsed.key;
        if key.is_empty() {
            return Err(Error::invalid_input(format!(
                "`{uri}` is a bucket, not an object"
            )));
        }
        let path = parsed.object_path()?;
        let profile = self.profiles.get(bucket.id).await?;

        // Delivery URL: bucket CDN override, connection CDN default, bucket public override,
        // connection public default, then the unchanged S3 endpoint.
        let public_url = if profile.allow_public_url {
            let base = resolve_link_base(&config, &profile);
            Some(format!("{}/{}/{}", base, bucket.name, encode_path(key)))
        } else {
            None
        };

        // Presigned URL: gated on the profile's permission, TTL from the explicit request or
        // the profile default.
        let (presigned_url, expires_at) = if request.signed {
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
                "uri": uri,
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
                            "public links are disabled for bucket `{}`; pass --signed",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch_outcome::Disposition;

    /// The projected failure message of one batch item, if it failed.
    fn failure_message(item: &ItemOutcome<GeneratedLink>) -> Option<String> {
        match &item.disposition {
            Disposition::Failed(f) => Some(f.message.clone()),
            _ => None,
        }
    }
    async fn setup() -> Arc<DefaultLinkService> {
        let (app, _factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;
        app.link_service()
    }

    async fn setup_with_public_base() -> Arc<DefaultLinkService> {
        let (app, _factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;
        let connection = app
            .api()
            .registry()
            .connections()
            .resolve(&crate::connection::ConnectionRef::Name("nas".into()))
            .await
            .unwrap();
        app.api()
            .registry()
            .connections()
            .update(
                connection.id,
                crate::connection::UpdateConnectionInput {
                    public_base_url: Some(Some("https://cdn.example.com/assets".into())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        app.link_service()
    }

    fn req(format: LinkFormat) -> LinkRequest {
        LinkRequest {
            format,
            signed: false,
            ttl_seconds: None,
        }
    }

    #[tokio::test]
    async fn formats_path_s3_and_encoded_url() {
        let svc = setup().await;
        let out = svc
            .generate(
                &["nas/media/docs/my file.txt".into()],
                &req(LinkFormat::Path),
            )
            .await
            .into_items()
            .remove(0);
        let link = out.value.unwrap();
        assert_eq!(link.requested, "docs/my file.txt");
        assert_eq!(link.s3_uri, "s3://media/docs/my file.txt");

        let out = svc
            .generate(
                &["nas/media/docs/my file.txt".into()],
                &req(LinkFormat::Url),
            )
            .await
            .into_items()
            .remove(0);
        let link = out.value.unwrap();
        // Derived from the endpoint, key percent-encoded.
        assert_eq!(
            link.requested,
            "http://localhost:9000/media/docs/my%20file.txt"
        );
    }

    #[tokio::test]
    async fn connection_public_base_url_overrides_endpoint() {
        let svc = setup_with_public_base().await;
        let out = svc
            .generate(&["nas/media/img/logo.png".into()], &req(LinkFormat::Url))
            .await
            .into_items()
            .remove(0);
        assert_eq!(
            out.value.unwrap().requested,
            "https://cdn.example.com/assets/media/img/logo.png"
        );
    }

    #[tokio::test]
    async fn bucket_and_connection_delivery_bases_follow_precedence() {
        let (app, _factory) = crate::test_util::app_with(&["nas"], &[("nas", "media")]).await;
        let connection = app
            .api()
            .registry()
            .connections()
            .resolve(&crate::connection::ConnectionRef::Name("nas".into()))
            .await
            .unwrap();
        let connection = app
            .api()
            .registry()
            .connections()
            .update(
                connection.id,
                crate::connection::UpdateConnectionInput {
                    public_base_url: Some(Some("https://connection-public.example.com".into())),
                    cdn_base_url: Some(Some("https://connection-cdn.example.com".into())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let bucket_override = LinkProfile {
            public_base_url: Some("https://bucket-public.example.com/".into()),
            cdn_base_url: Some("https://bucket-cdn.example.com/".into()),
            ..LinkProfile::default()
        };
        assert_eq!(
            resolve_link_base(&connection, &bucket_override),
            "https://bucket-cdn.example.com"
        );

        let public_only = LinkProfile {
            public_base_url: Some("https://bucket-public.example.com".into()),
            ..LinkProfile::default()
        };
        assert_eq!(
            resolve_link_base(&connection, &public_only),
            "https://connection-cdn.example.com"
        );

        let without_connection_cdn = rstore_domain::ConnectionConfig {
            cdn_base_url: None,
            ..connection.clone()
        };
        assert_eq!(
            resolve_link_base(&without_connection_cdn, &public_only),
            "https://bucket-public.example.com"
        );
        assert_eq!(
            resolve_link_base(&without_connection_cdn, &LinkProfile::default()),
            "https://connection-public.example.com"
        );
    }

    #[tokio::test]
    async fn bucket_delivery_base_override_normalizes_and_clears() {
        let svc = setup().await;
        let bucket = crate::bucket::BucketRef::Name("media".into());

        let profile = svc
            .set_profile(
                &bucket,
                LinkProfileInput {
                    public_base_url: Some(Some(" https://bucket-public.example.com/// ".into())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            profile.public_base_url.as_deref(),
            Some("https://bucket-public.example.com")
        );

        let profile = svc
            .set_profile(
                &bucket,
                LinkProfileInput {
                    public_base_url: Some(Some(String::new())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(profile.public_base_url.is_none());
    }

    async fn svc_for_profile(svc: &Arc<DefaultLinkService>) -> rstore_domain::BucketId {
        // Resolve through the same locator path the service uses.
        svc.locator
            .resolve_bucket(&RemoteUri {
                connection: crate::connection::ConnectionRef::Name("nas".into()),
                bucket: "media".into(),
                key: String::new(),
            })
            .await
            .unwrap()
            .1
            .id
    }

    #[tokio::test]
    async fn markdown_and_html_templates() {
        let svc = setup().await;
        let md = svc
            .generate(
                &["nas/media/img/shot.png".into()],
                &req(LinkFormat::Markdown),
            )
            .await
            .into_items()
            .remove(0)
            .value
            .unwrap()
            .requested;
        assert!(md.starts_with("![shot.png](http"));
        let html = svc
            .generate(&["nas/media/a<b>.txt".into()], &req(LinkFormat::Html))
            .await
            .into_items()
            .remove(0)
            .value
            .unwrap()
            .requested;
        // Link text is escaped.
        assert!(html.contains("&lt;b&gt;"));
        assert!(html.starts_with("<a href=\"http"));
    }

    #[tokio::test]
    async fn signed_uses_adapter_presign_and_ttl_default() {
        let svc = setup().await;
        let request = LinkRequest {
            format: LinkFormat::Url,
            signed: true,
            ttl_seconds: Some(90),
        };
        let out = svc
            .generate(&["nas/media/doc.pdf".into()], &request)
            .await
            .into_items()
            .remove(0);
        let link = out.value.unwrap();
        assert!(link
            .requested
            .starts_with("http://fake/media/doc.pdf?sig=fake"));
        assert!(link.expires_at.is_some());
    }

    #[tokio::test]
    async fn presign_disallowed_is_capability_unsupported() {
        let svc = setup().await;
        let bucket = svc_for_profile(&svc).await;
        svc.profiles
            .upsert(
                bucket,
                &LinkProfile {
                    allow_presigned: false,
                    ..LinkProfile::default()
                },
            )
            .await
            .unwrap();
        let request = LinkRequest {
            format: LinkFormat::Url,
            signed: true,
            ttl_seconds: None,
        };
        let out = svc
            .generate(&["nas/media/doc.pdf".into()], &request)
            .await
            .into_items()
            .remove(0);
        let err = failure_message(&out).expect("expected a per-item error");
        assert!(err.contains("presigned links are disabled"));
    }

    #[tokio::test]
    async fn public_url_disallowed_requires_signed() {
        let svc = setup().await;
        let bucket = svc_for_profile(&svc).await;
        svc.profiles
            .upsert(
                bucket,
                &LinkProfile {
                    allow_public_url: false,
                    ..LinkProfile::default()
                },
            )
            .await
            .unwrap();
        let out = svc
            .generate(&["nas/media/doc.pdf".into()], &req(LinkFormat::Url))
            .await
            .into_items()
            .remove(0);
        assert!(failure_message(&out)
            .unwrap()
            .contains("public links are disabled"));
    }

    #[tokio::test]
    async fn path_s3_and_json_survive_a_locked_down_bucket() {
        // A URL is load-bearing only for formats that render one: with every URL flavour
        // disabled, path/s3/json still succeed while url errors.
        let svc = setup().await;
        let bucket = svc_for_profile(&svc).await;
        svc.profiles
            .upsert(
                bucket,
                &LinkProfile {
                    allow_public_url: false,
                    allow_presigned: false,
                    ..LinkProfile::default()
                },
            )
            .await
            .unwrap();

        for format in [LinkFormat::Path, LinkFormat::S3, LinkFormat::Json] {
            let out = svc
                .generate(&["nas/media/doc.pdf".into()], &req(format))
                .await
                .into_items()
                .remove(0);
            let link = out.value.expect("{format:?} should not need a URL");
            assert!(link.public_url.is_none());
        }

        for format in [LinkFormat::Url, LinkFormat::Markdown] {
            let out = svc
                .generate(&["nas/media/doc.pdf".into()], &req(format))
                .await
                .into_items()
                .remove(0);
            assert!(
                failure_message(&out)
                    .unwrap()
                    .contains("public links are disabled"),
                "{format:?} renders a URL and must fail"
            );
        }
    }

    #[test]
    fn render_document_grammar() {
        assert_eq!(
            render_document(LinkFormat::Url, "a.txt", "http://x/a.txt"),
            "http://x/a.txt"
        );
        assert_eq!(
            render_document(LinkFormat::Markdown, "a.txt", "http://x/a.txt"),
            "[a.txt](http://x/a.txt)"
        );
        assert_eq!(
            render_document(LinkFormat::Markdown, "a.png", "http://x/a.png"),
            "![a.png](http://x/a.png)"
        );
        assert_eq!(
            render_document(LinkFormat::Html, "a.txt", "http://x/a.txt"),
            "<a href=\"http://x/a.txt\">a.txt</a>"
        );
        assert_eq!(
            render_document(LinkFormat::Html, "a<b>.png", "http://x/a.png"),
            "<img src=\"http://x/a.png\" alt=\"a&lt;b&gt;.png\">"
        );
    }

    #[tokio::test]
    async fn batch_is_per_item() {
        let svc = setup().await;
        let outcomes = svc
            .generate(
                &[
                    "nas/media/ok.txt".into(),
                    "nas/media".into(), // a bucket, not an object
                ],
                &req(LinkFormat::Path),
            )
            .await;
        assert!(outcomes.items()[0].value.is_some());
        assert!(failure_message(&outcomes.items()[1]).is_some());
    }

    #[test]
    fn ttl_parsing() {
        assert_eq!(parse_ttl("30").unwrap(), 30);
        assert_eq!(parse_ttl("15m").unwrap(), 900);
        assert_eq!(parse_ttl("1h30m").unwrap(), 5400);
        assert_eq!(parse_ttl("7d").unwrap(), 604_800);
        assert!(parse_ttl("8d").is_err());
        assert!(parse_ttl("0").is_err());
        assert!(parse_ttl("1x").is_err());
        assert!(parse_ttl("").is_err());
    }
}
