//! Provider capability model.
//!
//! Capabilities are probed against the live provider, never inferred from its
//! name; an operation a provider cannot perform fails with
//! `CapabilityUnsupported`.

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
