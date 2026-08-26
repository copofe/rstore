//! Storage connection configuration model.
//!
//! [`ConnectionConfig`] holds the complete local connection definition,
//! including credentials wrapped in [`Secret`]. Credential values are
//! persisted with the connection but skipped by serde, so they never enter
//! export JSON or logs. The wire formats for [`Region`] and
//! [`AddressingStyle`] live on the enums themselves (locality): the CLI, the
//! SQLite column and JSON all go through these methods instead of each leaf
//! re-implementing the mapping.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::id::ConnectionId;
use crate::Secret;

/// AWS region resolution.
///
/// Serialises as its token string (`"auto"` or an explicit region like
/// `"us-east-1"`) — the same token the CLI accepts and the SQLite column
/// stores.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "String", from = "String")]
pub enum Region {
    /// Let the SDK/endpoint resolve the region.
    #[default]
    Auto,
    /// An explicit region string (e.g. `us-east-1`).
    Specific(String),
}

impl Region {
    pub fn auto() -> Self {
        Self::Auto
    }
    pub fn specific(region: impl Into<String>) -> Self {
        Self::Specific(region.into())
    }

    /// Canonical token: `"auto"` or the explicit region string.
    pub fn as_token(&self) -> String {
        match self {
            Self::Auto => "auto".to_string(),
            Self::Specific(r) => r.clone(),
        }
    }

    /// Parse the canonical token (case-insensitive `auto`, else explicit).
    /// Never fails — any non-`auto` string becomes [`Region::Specific`].
    pub fn parse_token(token: &str) -> Self {
        let t = token.trim();
        if t.is_empty() || t.eq_ignore_ascii_case("auto") {
            Self::Auto
        } else {
            Self::Specific(t.to_string())
        }
    }
}

impl From<Region> for String {
    fn from(region: Region) -> Self {
        region.as_token()
    }
}

impl From<String> for Region {
    fn from(token: String) -> Self {
        Region::parse_token(&token)
    }
}

/// S3 bucket addressing style.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressingStyle {
    /// Let the client choose path vs. virtual-host.
    #[default]
    Auto,
    /// `https://endpoint/bucket/key`.
    Path,
    /// `https://bucket.endpoint/key`.
    VirtualHost,
}

impl AddressingStyle {
    /// Canonical token used by the SQLite column and serde (`"virtual_host"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Path => "path",
            Self::VirtualHost => "virtual_host",
        }
    }

    /// Parse a token, accepting common aliases (`virtual-host`, `vhost`, …).
    ///
    /// # Errors
    /// Returns [`Error::invalid_input`] for any token outside the accepted
    /// vocabulary.
    pub fn parse(token: &str) -> Result<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "path" => Ok(Self::Path),
            "virtual-host" | "virtualhost" | "vhost" | "virtual_host" => Ok(Self::VirtualHost),
            other => Err(Error::invalid_input(format!(
                "unknown addressing style `{other}` (expected auto|path|virtual-host)"
            ))),
        }
    }
}

/// Outcome of the last connection test, persisted so `connection list` can
/// surface health without re-probing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionTestStatus {
    /// All checks passed.
    Ok,
    /// One or more checks failed.
    Failed,
}

impl ConnectionTestStatus {
    /// Canonical token used by the SQLite column and serde.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }

    /// Parse a canonical token; `None` for anything unrecognised.
    pub fn parse(token: &str) -> Option<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "ok" => Some(Self::Ok),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Configuration for a storage connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub id: ConnectionId,
    /// Unique, human-friendly name on this machine.
    pub name: String,
    /// S3 API endpoint URL.
    pub endpoint: String,
    /// Base URL for public objects exposed by this connection, when it differs
    /// from the S3 API endpoint.
    pub public_base_url: Option<String>,
    /// Default accelerated/proxy delivery base for this connection.
    pub cdn_base_url: Option<String>,
    pub region: Region,
    pub addressing_style: AddressingStyle,
    /// TLS certificate verification; on unless explicitly disabled.
    pub tls_verify: bool,
    /// Access key stored with this connection. It is omitted from serde
    /// representations and must be explicitly masked for display.
    #[serde(skip)]
    pub access_key: Option<Secret<String>>,
    /// Secret key stored with this connection. It is omitted from serde
    /// representations and must be explicitly masked for display.
    #[serde(skip)]
    pub secret_key: Option<Secret<String>>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    /// When the connection was last tested, if ever.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub last_tested_at: Option<OffsetDateTime>,
    /// Result of that test.
    #[serde(default)]
    pub last_test_status: Option<ConnectionTestStatus>,
}

impl ConnectionConfig {
    /// Returns `true` if the name obeys the uniqueness/character rules.
    ///
    /// Names are case-sensitive and must be 1..=64 chars of printable,
    /// whitespace-trimmed text. This check is intentionally permissive;
    /// uniqueness itself is enforced by the registry port.
    pub fn is_valid_name(name: &str) -> bool {
        let trimmed = name.trim();
        !trimmed.is_empty() && trimmed.len() <= 64 && !trimmed.chars().any(|c| c.is_control())
    }

    /// Normalize a user-supplied base URL: trim whitespace and trailing
    /// slashes; an empty value normalizes to `None` ("no override"). The one
    /// home for this rule — connection fields and link profiles share it.
    pub fn normalize_base_url(value: Option<String>) -> Option<String> {
        value.and_then(|base| {
            let base = base.trim().trim_end_matches('/');
            (!base.is_empty()).then(|| base.to_string())
        })
    }
}
