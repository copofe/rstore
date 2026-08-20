//! Test-only SQLite composition helper.
//!
//! Keeping this in a separate crate means `rstore-application` itself has no
//! persistence dependency, even when integration tests want a real database.

use rstore_application::Application;
use rstore_persistence::Database;

pub fn application_from_db(db: Database) -> Application {
    rstore_application::test_util::application_from_db(db)
}
