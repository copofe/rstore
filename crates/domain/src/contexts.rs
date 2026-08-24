//! Bounded-context identity used by domain events and diagnostics.
//!
//! The enum is deliberately small. It describes ownership of business facts;
//! it is not a runtime registry and it does not introduce a dependency on any
//! adapter or transport.

use serde::{Deserialize, Serialize};

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
    /// Canonical snake-case token shared by serde and the persistence column.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registry => "registry",
            Self::Catalog => "catalog",
            Self::Transfer => "transfer",
            Self::AccessDiagnostics => "access_diagnostics",
        }
    }

    /// Decode the stored token of a persisted event's owning context.
    ///
    /// # Errors
    /// Returns [`crate::Error::database`] for an unknown token: a vocabulary miss in
    /// stored data is internal corruption, not user input.
    pub fn parse(token: &str) -> crate::Result<Self> {
        match token {
            "registry" => Ok(Self::Registry),
            "catalog" => Ok(Self::Catalog),
            "transfer" => Ok(Self::Transfer),
            "access_diagnostics" => Ok(Self::AccessDiagnostics),
            other => Err(crate::Error::database(format!(
                "unknown bounded context `{other}`"
            ))),
        }
    }
}

impl std::fmt::Display for BoundedContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
