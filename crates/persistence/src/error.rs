//! sqlx error → domain error mapping.

use rstore_domain::{Error, ErrorCode};

/// Map a [`sqlx::Error`] into a stable domain error. Unique-constraint failures
/// become `Conflict`, missing rows become `NotFound`, everything else `Database`.
pub fn map_sqlx(e: sqlx::Error) -> Error {
    let code = match &e {
        sqlx::Error::RowNotFound => return Error::not_found("row not found"),
        sqlx::Error::Database(db) if db.is_unique_violation() => ErrorCode::Conflict,
        sqlx::Error::Database(db) if db.is_foreign_key_violation() => ErrorCode::Conflict,
        _ => ErrorCode::Database,
    };
    Error::new(code, "database error").with_source(e)
}
