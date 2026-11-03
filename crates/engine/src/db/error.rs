//! sqlx error classification onto domain errors.
use rstore_domain::{Error, ErrorCode};

/// Map a [`sqlx::Error`] onto a stable domain error: unique-constraint and foreign-key failures become
/// `Conflict`, a database another process kept locked past the busy timeout becomes the retryable
/// `DatabaseBusy`, a missing row becomes `NotFound`, everything else `Database`.
pub fn map_sqlx(e: sqlx::Error) -> Error {
    let code = match &e {
        sqlx::Error::RowNotFound => return Error::not_found("row not found"),
        sqlx::Error::Database(db) if db.is_unique_violation() => ErrorCode::Conflict,
        sqlx::Error::Database(db) if db.is_foreign_key_violation() => ErrorCode::Conflict,
        sqlx::Error::Database(db) if is_busy(db.code().as_deref()) => {
            return Error::database_busy("database is busy in another process").with_source(e)
        }
        _ => ErrorCode::Database,
    };
    Error::new(code, "database error").with_source(e)
}

/// `SQLITE_BUSY` (5) or `SQLITE_LOCKED` (6), in any extended form.
fn is_busy(code: Option<&str>) -> bool {
    code.and_then(|code| code.parse::<i32>().ok())
        .is_some_and(|code| matches!(code & 0xff, 5 | 6))
}

/// Re-map a unique-constraint `Conflict` with a domain message naming the duplicate; every other error
/// passes through unchanged. The single home for the remap every upsert shares.
pub(crate) fn with_conflict_message(error: Error, message: impl FnOnce() -> String) -> Error {
    if error.code == ErrorCode::Conflict {
        Error::conflict(message())
    } else {
        error
    }
}
