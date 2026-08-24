//! Strongly-typed identifiers.
//!
//! Each entity owns a distinct newtype over [`uuid::Uuid`]. They never share
//! a bare `String`/`Uuid` type, so the compiler prevents connection IDs being
//! passed where a bucket ID is expected. JSON output is a stable prefixed
//! string (`conn_<uuid>`, `bkt_<uuid>`, ...) so it is human-readable and
//! still round-trips through `FromStr`.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, ErrorCode};

/// Behaviour shared by every typed ID.
pub trait TypedId:
    Copy + Eq + std::hash::Hash + fmt::Display + Serialize + for<'a> Deserialize<'a> + Send + Sync
{
    /// Short prefix used in the string form (e.g. `conn`, `bkt`).
    const PREFIX: &'static str;
    /// Underlying UUID.
    fn as_uuid(&self) -> uuid::Uuid;
    /// Generate a fresh random ID.
    fn new_random() -> Self;
}

/// Defines a typed ID newtype.
///
/// The string contract is `<prefix>_<uuid>` in both directions: [`Display`]
/// and serde serialize with the prefix, `FromStr` and serde deserialize only
/// the matching prefix — a foreign prefix or missing underscore is an
/// [`ErrorCode::InvalidInput`] error, never a silent reinterpretation.
macro_rules! define_id {
    ($(#[$m:meta])* $name:ident, $prefix:literal) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(pub uuid::Uuid);

        impl $name {
            /// Generate a fresh random identity.
            pub fn new_random() -> Self {
                Self(uuid::Uuid::new_v4())
            }

            /// Wrap an existing UUID.
            pub const fn from_uuid(id: uuid::Uuid) -> Self {
                Self(id)
            }
        }




        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}_{}", $prefix, self.0)
            }
        }

        impl FromStr for $name {
            type Err = Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let Some((prefix, rest)) = s.split_once('_') else {
                    return Err(Error::invalid_input(format!(
                        "invalid id `{s}`: expected prefix `{}`",
                        $prefix
                    )));
                };
                if prefix != $prefix {
                    return Err(Error::invalid_input(format!(
                        "invalid id `{s}`: wrong prefix (expected `{}`)",
                        $prefix
                    )));
                }
                let uuid =
                    uuid::Uuid::parse_str(rest).map_err(|e| Error::wrap(ErrorCode::InvalidInput, format!("invalid id `{s}`"), e))?;
                Ok(Self(uuid))
            }
        }

        impl From<uuid::Uuid> for $name {
            fn from(id: uuid::Uuid) -> Self {
                Self(id)
            }
        }

        impl $crate::id::TypedId for $name {
            const PREFIX: &'static str = $prefix;
            fn as_uuid(&self) -> uuid::Uuid {
                self.0
            }
            fn new_random() -> Self {
                Self(uuid::Uuid::new_v4())
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = String::deserialize(deserializer)?;
                s.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

define_id!(
    /// Identifies a configured storage connection.
    ConnectionId,
    "conn"
);
define_id!(
    /// Identifies a locally registered bucket.
    BucketId,
    "bkt"
);
define_id!(
    /// Identifies a workspace.
    WorkspaceId,
    "wsp"
);
define_id!(
    /// Stable identity for an indexed remote object.
    RemoteObjectId,
    "obj"
);
define_id!(
    /// Identifies a workspace-scoped tag.
    TagId,
    "tag"
);
define_id!(
    /// Identifies an index run (one scan of one bucket).
    IndexRunId,
    "idx"
);

define_id!(
    /// Identifies a transfer / index job.
    JobId,
    "job"
);

define_id!(
    /// Correlates one durable remote-mutation receipt with its repair items.
    ReceiptId,
    "rcpt"
);

define_id!(
    /// Identifies one queryable catalog follow-up failure.
    RepairId,
    "rep"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_prefixed_string() {
        let id = ConnectionId::new_random();
        let s = id.to_string();
        assert!(s.starts_with("conn_"));
        let parsed: ConnectionId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn wrong_prefix_rejected() {
        assert!(ConnectionId::from_str("bkt_deadbeef").is_err());
    }

    #[test]
    fn types_do_not_mix() {
        // Compile-time guarantee: these are distinct types.
        fn takes_conn(_: ConnectionId) {}
        takes_conn(ConnectionId::new_random());
    }

    #[test]
    fn serde_round_trip() {
        let id = BucketId::new_random();
        let json = serde_json::to_string(&id).unwrap();
        let back: BucketId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }
}
