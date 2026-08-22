//! Storage connection domain model (CON-/DATA-).
//!
//! ConnectionConfig holds the complete local connection configuration,
//! including credentials wrapped in Secret. Credential values are persisted
//! with the connection but skipped by serde so they never enter export JSON or
//! logs (SEC-001).
//!
//! The wire formats for [`Region`] and [`AddressingStyle`] live ON the enums
//! here (locality): the CLI, the SQLite column, and JSON all go through these
//! methods instead of each leaf re-implementing the mapping.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::id::ConnectionId;
use crate::Secret;

/// AWS region resolution.
///
/// Serialises as its token string (`"auto"` or an explicit region like
/// `"us-east-1"`) — the same token the CLI accepts and the SQLite column stores.
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

/// S3 bucket addressing style (STORE-004).
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

/// Outcome of the last connection test (CON-007/010), persisted so
/// `connection list` can surface health without re-probing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionTestStatus {
    /// All checks passed.
    Ok,
    /// One or more checks failed.
    Failed,
}

impl ConnectionTestStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }

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
    /// TLS certificate verification (default on — SEC-003).
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
    /// When the connection was last tested, if ever (CON-010).
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub last_tested_at: Option<OffsetDateTime>,
    /// Result of that test (CON-010).
    #[serde(default)]
    pub last_test_status: Option<ConnectionTestStatus>,
}

impl ConnectionConfig {
    /// Returns `true` if the name obeys the uniqueness/character rules.
    ///
    /// Names are case-sensitive and must be 1..=64 chars of printable,
    /// whitespace-trimmed text (CON-002). This is intentionally permissive.
    pub fn is_valid_name(name: &str) -> bool {
        let trimmed = name.trim();
        !trimmed.is_empty() && trimmed.len() <= 64 && !trimmed.chars().any(|c| c.is_control())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_token_round_trip() {
        assert_eq!(Region::Auto.as_token(), "auto");
        assert_eq!(Region::parse_token("AUTO"), Region::Auto);
        assert_eq!(
            Region::parse_token("us-east-1"),
            Region::specific("us-east-1")
        );
        assert_eq!(
            Region::specific("ap-southeast-2").as_token(),
            "ap-southeast-2"
        );
    }

    #[test]
    fn region_serde_is_token_string() {
        // The explicit representation preserves the Specific region string.
        let json = serde_json::to_string(&Region::specific("eu-west-1")).unwrap();
        assert_eq!(json, "\"eu-west-1\"");
        let back: Region = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Region::specific("eu-west-1"));
    }

    #[test]
    fn addressing_parse_accepts_aliases() {
        assert_eq!(
            AddressingStyle::parse("vhost").unwrap(),
            AddressingStyle::VirtualHost
        );
        assert_eq!(
            AddressingStyle::parse("virtual-host").unwrap(),
            AddressingStyle::VirtualHost
        );
        assert_eq!(AddressingStyle::VirtualHost.as_str(), "virtual_host");
        assert!(AddressingStyle::parse("weird").is_err());
    }
}
