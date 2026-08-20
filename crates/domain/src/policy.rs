//! Conflict policy shared by upload/download/copy/move (UP-/DOWN-/MOVE-).

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// How to handle an existing target object.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    /// Prompt the user interactively (the default).
    #[default]
    Ask,
    /// Leave the existing target untouched.
    Skip,
    /// Replace the existing target.
    Overwrite,
    /// Write to a derived, non-colliding name.
    Rename,
}

impl ConflictPolicy {
    /// Canonical token used by the SQLite column (`"rename"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::Skip => "skip",
            Self::Overwrite => "overwrite",
            Self::Rename => "rename",
        }
    }

    /// Parse a token, accepting the clap-style dash alias (`over-write`).
    pub fn parse(token: &str) -> Result<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "" | "ask" => Ok(Self::Ask),
            "skip" => Ok(Self::Skip),
            "overwrite" | "over-write" => Ok(Self::Overwrite),
            "rename" => Ok(Self::Rename),
            other => Err(Error::invalid_input(format!(
                "unknown conflict policy `{other}` (expected ask|skip|overwrite|rename)"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trip_and_alias() {
        assert_eq!(
            ConflictPolicy::parse("overwrite").unwrap(),
            ConflictPolicy::Overwrite
        );
        assert_eq!(
            ConflictPolicy::parse("Over-Write").unwrap(),
            ConflictPolicy::Overwrite
        );
        assert_eq!(ConflictPolicy::Skip.as_str(), "skip");
        assert!(ConflictPolicy::parse("nonsense").is_err());
    }
}
