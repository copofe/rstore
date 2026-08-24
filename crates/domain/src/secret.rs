//! Secret wrapping and display masking for credentials.

use std::fmt;

/// A value whose Debug output never includes the underlying text.
///
/// Wrap every credential in this type so accidental `{:?}` logging of a
/// struct carrying it cannot leak the value; display paths must call
/// [`expose`](Secret::expose) explicitly and are expected to go through
/// [`mask_secret`].
pub struct Secret<T>(T);

impl<T> Secret<T> {
    /// Wrap a raw secret value.
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// Borrow the underlying value. Callers must not log or serialise it.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Unwrap into the raw value, consuming the wrapper.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(**REDACTED**)")
    }
}

impl<T: Clone> Clone for Secret<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Keep only the edges of a credential visible with a fixed four-character
/// mask, regardless of the original value's length.
pub fn mask_secret(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let len = chars.len();
    if len <= 4 {
        return "*".repeat(len.max(1));
    }

    let edge = if len <= 8 { 2 } else { 4 };
    let start: String = chars[..edge].iter().collect();
    let end: String = chars[len - edge..].iter().collect();
    format!("{start}****{end}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_value() {
        let secret = Secret::new("super-secret".to_string());
        assert_eq!(format!("{secret:?}"), "Secret(**REDACTED**)");
        assert!(!format!("{secret:?}").contains("super-secret"));
    }

    #[test]
    fn mask_keeps_edges_only_for_long_values() {
        assert_eq!(mask_secret("AKIAIOSFODNN7EXAMPLE"), "AKIA****MPLE");
        assert_eq!(mask_secret("abcdef"), "ab****ef");
        assert_eq!(mask_secret("abcd"), "****");
        assert_eq!(mask_secret(""), "*");
    }
}
