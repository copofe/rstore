//! Time and clock abstraction (CORE-009).
//!
//! All core code asks for the current time through the [`Clock`] trait so tests
//! can inject a deterministic clock. Times are UTC and serialise as ISO 8601.

use time::OffsetDateTime;

/// Source of "now" in UTC. Inject in tests; default to [`SystemClock`].
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
pub fn iso8601(ts: OffsetDateTime) -> String {
    // `Rfc3339` includes the timezone offset; we always store UTC.
    ts.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| ts.to_string())
}
