//! A deterministic [`Clock`] for tests: frozen at construction, moved
//! forward only by explicit [`TestClock::advance_secs`] calls, so expiry,
//! staleness and timing logic can be asserted against known instants.

use std::sync::Mutex;

use rstore_domain::Clock;
use time::OffsetDateTime;

/// Clock whose reading is fixed until advanced manually.
pub struct TestClock {
    now: Mutex<OffsetDateTime>,
}

impl TestClock {
    /// Freeze the clock at `at`; every [`Clock::now`] returns it verbatim.
    pub fn fixed(at: OffsetDateTime) -> Self {
        Self {
            now: Mutex::new(at),
        }
    }

    /// Advance the reading by a number of seconds. An overflow leaves the
    /// reading unchanged — tests never need a clock past `time`'s bounds.
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
