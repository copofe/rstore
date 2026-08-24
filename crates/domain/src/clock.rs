//! Time access behind an injectable trait.
//!
//! All core code asks for the current time through [`Clock`] so tests can
//! inject a deterministic clock. Timestamps are UTC and serialise as ISO 8601.

use time::OffsetDateTime;

/// Current UTC instant. Inject in tests; default to [`SystemClock`].
pub trait Clock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

/// Reads the real system clock (UTC).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// Format a timestamp as ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`) for CLI output.
///
/// The RFC 3339 rendering carries an explicit timezone offset because every
/// stored time is UTC. Falls back to the default `Display` formatting if the
/// timestamp cannot be represented in that well-known format.
pub fn iso8601(ts: OffsetDateTime) -> String {
    ts.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| ts.to_string())
}
