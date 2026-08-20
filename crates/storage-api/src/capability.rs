//! Capability model (STORE-021).
//!
//! Every advanced storage feature is gated by an explicit capability check so
//! the product never silently assumes a provider supports something (PRD §10.2.2
//! — "不得仅根据 Provider 名称推断能力").

use serde::{Deserialize, Serialize};

/// The four-state result of probing a feature.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// The provider responded and the feature works.
    Supported,
    /// The provider responded and the feature is known not to work.
    Unsupported,
    /// Not yet probed (default until first connection test).
    #[default]
    Unknown,
    /// Probed but the configured credentials lack permission.
    Denied,
}

impl Capability {
    pub fn is_supported(self) -> bool {
        matches!(self, Self::Supported)
    }

    /// Canonical token used by the capability cache (STORE-022).
    pub fn as_token(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Unsupported => "unsupported",
            Self::Unknown => "unknown",
            Self::Denied => "denied",
        }
    }

    /// Parse the token; anything unrecognised reads as [`Capability::Unknown`]
    /// (a cache miss is always safe).
    pub fn parse_token(token: &str) -> Self {
        match token {
            "supported" => Self::Supported,
            "unsupported" => Self::Unsupported,
            "denied" => Self::Denied,
            _ => Self::Unknown,
        }
    }
}
