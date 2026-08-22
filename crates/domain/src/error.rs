//! Unified error model (CORE-007).
//!
//! Every layer maps its failures into [`Error`], which carries a stable
//! [`ErrorCode`], a human-readable message, structured [`ErrorContext`] pairs,
//! a `retryable` flag and an optional boxed source. The CLI converts `ErrorCode`
//! into a stable process exit code (CLI-011) and never surfaces a panic.

use std::backtrace::Backtrace;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable, serialisable error categories. The CLI maps these to exit codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Unclassified failure (CLI exit 1).
    Unknown,
    /// Argument or user input error (exit 2).
    InvalidInput,
    /// Configuration error (exit 3).
    Config,
    /// Authentication failure (exit 4).
    Unauthorized,
    /// Permission denied (exit 5).
    Forbidden,
    /// Network or endpoint unreachable (exit 6).
    NetworkUnreachable,
    /// Remote object does not exist (exit 7).
    NotFound,
    /// Conflict / precondition failed (exit 8).
    Conflict,
    /// Checksum / integrity failure (exit 9).
    Checksum,
    /// Local database or lock error (exit 10).
    Database,
    /// Capability not supported (exit 11).
    CapabilityUnsupported,
    /// Operation cancelled by the user (exit 12 / 130 on Ctrl+C).
    Cancelled,
    /// Operation timed out.
    Timeout,
    /// Another runtime holds the write lock.
    LockBusy,
    /// The bounded SQLite writer queue is full.
    DatabaseBusy,
    /// Local filesystem failure (exit 14) — staged files, upload sources,
    /// download targets. Distinct from [`ErrorCode::Database`]: a full disk is
    /// not a corrupt catalog.
    LocalIo,
}

impl ErrorCode {
    /// Whether a caller may reasonably retry an operation that yielded this code.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            ErrorCode::NetworkUnreachable
                | ErrorCode::Timeout
                | ErrorCode::LockBusy
                | ErrorCode::DatabaseBusy
        )
    }
}

/// A single structured key/value pair attached to an error (SEC-001 friendly:
/// never put secrets here).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorContext {
    pub key: String,
    pub value: String,
}

impl ErrorContext {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// The single error type used across the core.
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: Option<bool>,
    pub context: Vec<ErrorContext>,
    pub source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    backtrace: Backtrace,
}

impl Error {
    /// Construct with a code and message. `retryable` defaults to the code's own
    /// policy unless overridden via [`Error::retryable`].
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: None,
            context: Vec::new(),
            source: None,
            backtrace: Backtrace::capture(),
        }
    }

    /// Convenience constructors.
    pub fn unknown(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unknown, msg)
    }
    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidInput, msg)
    }
    pub fn config(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Config, msg)
    }
    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthorized, msg)
    }
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Forbidden, msg)
    }
    pub fn network(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::NetworkUnreachable, msg)
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, msg)
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, msg)
    }
    pub fn checksum(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Checksum, msg)
    }
    pub fn database(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Database, msg)
    }
    pub fn unsupported(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::CapabilityUnsupported, msg)
    }
    pub fn cancelled() -> Self {
        Self::new(ErrorCode::Cancelled, "operation cancelled")
    }
    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Timeout, msg)
    }
    pub fn lock_busy(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::LockBusy, msg)
    }
    pub fn database_busy(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::DatabaseBusy, msg).retryable(true)
    }
    pub fn io(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::LocalIo, msg)
    }

    /// Wrap an underlying error, preserving it as `source`.
    pub fn wrap(
        code: ErrorCode,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::new(code, message).with_source(source)
    }

    /// Attach a structured context pair.
    #[must_use]
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.push(ErrorContext::new(key, value));
        self
    }

    /// Attach the causal error.
    #[must_use]
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// Override the retryability decision.
    #[must_use]
    pub const fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = Some(retryable);
        self
    }

    /// Resolve the effective retryability flag.
    pub fn is_retryable(&self) -> bool {
        self.retryable.unwrap_or_else(|| self.code.is_retryable())
    }

    /// Captured backtrace for diagnostics (OBS-001). May be empty if backtraces
    /// are disabled at runtime (`RUST_BACKTRACE=0`).
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg = f.debug_struct("Error");
        dbg.field("code", &self.code)
            .field("message", &self.message)
            .field("retryable", &self.is_retryable());
        if !self.context.is_empty() {
            dbg.field("context", &self.context);
        }
        if let Some(src) = &self.source {
            dbg.field("source", &src.to_string());
        }
        dbg.finish()
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|s| &**s as &(dyn std::error::Error + 'static))
    }
}

// Ergonomic conversions from common error types.
impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::wrap(ErrorCode::InvalidInput, "serialization failed", err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        let code = if err.kind() == std::io::ErrorKind::NotFound {
            ErrorCode::NotFound
        } else {
            ErrorCode::LocalIo
        };
        Self::wrap(code, "io error", err)
    }
}

impl ErrorCode {
    /// Lowercase snake-case string used in JSON output.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::Unknown => "unknown",
            ErrorCode::InvalidInput => "invalid_input",
            ErrorCode::Config => "config",
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::Forbidden => "forbidden",
            ErrorCode::NetworkUnreachable => "network_unreachable",
            ErrorCode::NotFound => "not_found",
            ErrorCode::Conflict => "conflict",
            ErrorCode::Checksum => "checksum",
            ErrorCode::Database => "database",
            ErrorCode::CapabilityUnsupported => "capability_unsupported",
            ErrorCode::Cancelled => "cancelled",
            ErrorCode::Timeout => "timeout",
            ErrorCode::LockBusy => "lock_busy",
            ErrorCode::DatabaseBusy => "database_busy",
            ErrorCode::LocalIo => "local_io",
        }
    }
}

/// Convenience `Result` alias for the whole core.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    /// `ErrorCode::as_str` and the serde representation are two spellings of
    /// one contract (envelope `code` field ↔ exit-code mapping). The
    /// round-trip pins them together so a variant added to one side without
    /// the other fails here instead of in a mismatched CLI envelope.
    #[test]
    fn error_code_as_str_matches_the_serde_token_in_both_directions() {
        for code in [
            ErrorCode::Unknown,
            ErrorCode::InvalidInput,
            ErrorCode::Config,
            ErrorCode::Unauthorized,
            ErrorCode::Forbidden,
            ErrorCode::NetworkUnreachable,
            ErrorCode::NotFound,
            ErrorCode::Conflict,
            ErrorCode::Checksum,
            ErrorCode::Database,
            ErrorCode::CapabilityUnsupported,
            ErrorCode::Cancelled,
            ErrorCode::Timeout,
            ErrorCode::LockBusy,
            ErrorCode::DatabaseBusy,
            ErrorCode::LocalIo,
        ] {
            let json = serde_json::to_string(&code).unwrap();
            assert_eq!(
                json,
                format!("\"{}\"", code.as_str()),
                "as_str must equal the serde token"
            );
            let back: ErrorCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, code);
        }
    }
}
