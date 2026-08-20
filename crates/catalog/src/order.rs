//! The one total-order vocabulary over catalog rows (IDX-024).
//!
//! Every adapter orders and keyset-paginates by the `(sort value, path, id)`
//! triple, with `id` components in bare-uuid text — the typed-ID `Display`
//! (`obj_<uuid>`) would order above every bare value and silently drop
//! tie-broken rows. The per-field value spelling and the cursor codec live
//! here once: the in-memory reference compares [`SortKey`] directly, the
//! SQLite adapter orders by the matching columns and binds the decoded key.
//! The contract suite keeps its own independent oracle on purpose — sharing
//! this module with it would let a bug hide behind itself.

use std::cmp::Ordering;

use rstore_domain::remote::RemoteObject;
use rstore_domain::{Error, Result, TypedId};

use crate::query::SortField;

/// The sort value of a row for one field. Numeric fields must compare
/// numerically (the SQL columns are INTEGER), text fields lexically — so the
/// key keeps the two apart instead of stringifying numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKey {
    Num(i64),
    Text(String),
}

impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (SortKey::Num(a), SortKey::Num(b)) => a.cmp(b),
            (SortKey::Text(a), SortKey::Text(b)) => a.cmp(b),
            // Unreachable per field: one sort field never mixes variants.
            (SortKey::Num(_), SortKey::Text(_)) => Ordering::Less,
            (SortKey::Text(_), SortKey::Num(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The total-order key of a row: `(sort value, path, id)` — exactly the
/// triple every adapter orders and keyset-paginates by. `Type` sorts by the
/// domain classifier (`file_type` column), matching the filter.
pub fn order_key(o: &RemoteObject, field: SortField) -> (SortKey, String, String) {
    let value = match field {
        SortField::Name => SortKey::Text(o.path.file_name().to_string()),
        SortField::Size => SortKey::Num(o.size as i64),
        SortField::Modified => SortKey::Num(o.last_modified.unix_timestamp()),
        SortField::Type => SortKey::Text(o.type_hint().as_str().to_string()),
        SortField::Bucket => SortKey::Text(o.bucket_id.as_uuid().to_string()),
        SortField::Connection => SortKey::Text(o.connection_id.as_uuid().to_string()),
    };
    (
        value,
        o.path.as_str().to_string(),
        o.id.as_uuid().to_string(),
    )
}

/// The cursor encoding a walk resumes from: the last served row's order key.
pub fn cursor_of(o: &RemoteObject, field: SortField) -> String {
    let (value, path, id) = order_key(o, field);
    encode(&value, &path, &id)
}

fn encode(value: &SortKey, path: &str, id: &str) -> String {
    let value = match value {
        SortKey::Num(n) => n.to_string(),
        SortKey::Text(t) => t.clone(),
    };
    format!("{value}\u{1f}{path}\u{1f}{id}")
}

/// Decode a cursor into its order key, parsing the value numerically for
/// numeric fields (the same classification both adapters bind by). A cursor
/// is opaque to callers; only this module gives it structure.
pub fn decode(cursor: &str, field: SortField) -> Result<(SortKey, String, String)> {
    let mut parts = cursor.splitn(3, '\u{1f}');
    let (value, path, id) = match (parts.next(), parts.next(), parts.next()) {
        (Some(v), Some(p), Some(i)) => (v.to_string(), p.to_string(), i.to_string()),
        _ => return Err(bad_cursor()),
    };
    let value = match field {
        SortField::Size | SortField::Modified => {
            SortKey::Num(value.parse().map_err(|_| bad_cursor())?)
        }
        _ => SortKey::Text(value),
    };
    Ok((value, path, id))
}

fn bad_cursor() -> Error {
    Error::invalid_input("invalid pagination cursor")
}
