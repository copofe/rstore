//! Canonical remote address used after an entry point has parsed a URI.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::id::{BucketId, ConnectionId};
use crate::remote::ObjectPath;

/// A fully resolved, typed address of one remote object.
///
/// CLI and desktop input may use several URI spellings, but application
/// workflows should exchange this value instead of reparsing strings. The
/// bucket-relative path is kept as [`ObjectPath`], so a locator cannot carry
/// a second, conflicting bucket or connection name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoteLocator {
    pub connection_id: ConnectionId,
    pub bucket_id: BucketId,
    pub path: ObjectPath,
}

impl RemoteLocator {
    /// Build a locator from its typed components.
    pub fn new(connection_id: ConnectionId, bucket_id: BucketId, path: ObjectPath) -> Self {
        Self {
            connection_id,
            bucket_id,
            path,
        }
    }
}

impl fmt::Display for RemoteLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.connection_id,
            self.bucket_id,
            self.path.as_str()
        )
    }
}
