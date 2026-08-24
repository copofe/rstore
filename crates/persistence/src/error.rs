//! sqlx error classification onto domain errors.
use rstore_domain::{Error, ErrorCode};

/// Map a [`sqlx::Error`] onto a stable domain error: unique-constraint and foreign-key failures become
/// `Conflict`, a missing row becomes `NotFound`, everything else `Database`.
pub fn map_sqlx(e: sqlx::Error) -> Error {
    let code = match &e {
        sqlx::Error::RowNotFound => return Error::not_found("row not found"),
        sqlx::Error::Database(db) if db.is_unique_violation() => ErrorCode::Conflict,
        sqlx::Error::Database(db) if db.is_foreign_key_violation() => ErrorCode::Conflict,
        _ => ErrorCode::Database,
    };
    Error::new(code, "database error").with_source(e)
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
