//! Bounded-context identity used by domain events and diagnostics.
//!
//! The enum is deliberately small. It describes ownership of business facts;
//! it is not a runtime registry and it does not introduce a dependency on any
//! adapter or transport.

use serde::{Deserialize, Serialize};

/// Access and diagnostics value-object surface.
pub mod access;
/// Catalog aggregate surface.
pub mod catalog;
/// Registry aggregate/value-object surface.
pub mod registry;
/// Transfer aggregate and policy surface.
pub mod transfer;

/// The four business contexts in the domain model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundedContext {
    Registry,
    Catalog,
    Transfer,
    AccessDiagnostics,
}

impl BoundedContext {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registry => "registry",
            Self::Catalog => "catalog",
            Self::Transfer => "transfer",
            Self::AccessDiagnostics => "access_diagnostics",
        }
    }
}

impl std::fmt::Display for BoundedContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
