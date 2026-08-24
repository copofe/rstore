//! Test-only SQLite composition helper.
//!
//! Keeping this in a separate crate means `rstore-application` itself has no
//! persistence dependency in its published build; the feature-gated fixture
//! inside the application crate serves its own unit tests. The split exists
//! because a support crate wrapping `Application` cannot be consumed by the
//! application's internal tests — the dev-cycle would put two distinct
//! `Application` types in one dependency graph.

use rstore_application::Application;
use rstore_persistence::Database;

pub fn application_from_db(db: Database) -> Application {
    rstore_application::test_util::application_from_db(db)
}
