//! Non-secret reference to credentials held by a [`SecretStore`](crate).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// A non-sensitive key into the secret store. The value is never credential
/// material; it is safe to persist and render as configuration metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CredentialRef(String);

impl CredentialRef {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(Error::invalid_input("credential reference cannot be empty"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CredentialRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for CredentialRef {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for CredentialRef {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl std::ops::Deref for CredentialRef {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_references() {
        assert!(CredentialRef::new(" ").is_err());
        let value = CredentialRef::new("conn_nas").unwrap();
        assert_eq!(value.as_str(), "conn_nas");
    }
}
