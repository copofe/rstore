//! The one total-order vocabulary over catalog rows.
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

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
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
///
/// The payload is URL-safe Base64 so a cursor can be copied from the human CLI
/// output without losing the field separators to terminal control handling.
pub fn cursor_of(o: &RemoteObject, field: SortField) -> String {
    let (value, path, id) = order_key(o, field);
    encode(&value, &path, &id)
}

fn encode(value: &SortKey, path: &str, id: &str) -> String {
    let value = match value {
        SortKey::Num(n) => n.to_string(),
        SortKey::Text(t) => t.clone(),
    };
    URL_SAFE_NO_PAD.encode(format!("{value}\u{1f}{path}\u{1f}{id}"))
}

/// Decode a cursor into its order key, parsing the value numerically for
/// numeric fields (the same classification both adapters bind by). A cursor
/// is opaque to callers; only this module gives it structure.
///
/// # Errors
/// Fails with `ErrorCode::InvalidInput` when the cursor is not valid
/// URL-safe Base64, not decodable UTF-8, or does not carry the three
/// field separators.
pub fn decode(cursor: &str, field: SortField) -> Result<(SortKey, String, String)> {
    let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| bad_cursor())?;
    let raw = String::from_utf8(bytes).map_err(|_| bad_cursor())?;
    let mut parts = raw.splitn(3, '\u{1f}');
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_is_printable_and_round_trips() {
        let cursor = encode(
            &SortKey::Num(1_746_109_278),
            "upload/IMG-20250501220719790.webp",
            "d79b45a2-473f-4ef4-a5a1-32797e00a676",
        );

        assert!(!cursor.chars().any(char::is_control));
        assert_eq!(
            decode(&cursor, SortField::Modified).unwrap(),
            (
                SortKey::Num(1_746_109_278),
                "upload/IMG-20250501220719790.webp".to_string(),
                "d79b45a2-473f-4ef4-a5a1-32797e00a676".to_string(),
            )
        );
        assert!(decode(
            "1746109278\u{1f}upload/file.webp\u{1f}d79b45a2-473f-4ef4-a5a1-32797e00a676",
            SortField::Modified,
        )
        .is_err());
    }
}
