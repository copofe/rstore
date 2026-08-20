//! A deterministic clock for tests (CORE-009).

use std::sync::Mutex;

use rstore_domain::Clock;
use time::OffsetDateTime;

/// Clock whose "now" is fixed and can be advanced manually.
pub struct TestClock {
    now: Mutex<OffsetDateTime>,
}

impl TestClock {
    pub fn fixed(at: OffsetDateTime) -> Self {
        Self {
            now: Mutex::new(at),
        }
    }

    /// Advance the clock by a number of seconds.
    pub fn advance_secs(&self, secs: i64) {
        let mut now = self.now.lock().expect("clock poisoned");
        *now = (*now)
            .checked_add(time::Duration::seconds(secs))
            .unwrap_or(*now);
    }
}

impl Clock for TestClock {
    fn now(&self) -> OffsetDateTime {
        *self.now.lock().expect("clock poisoned")
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::fixed(OffsetDateTime::UNIX_EPOCH)
    }
}
