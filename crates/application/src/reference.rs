//! Remote reference syntax: short-form expansion and canonical parsing.
//!
//! One home for the `<connection>/<bucket>/[key]` vocabulary — expansion of
//! user short forms and canonical parsing ([`RemoteUri`]) — so every
//! consumer (CLI, copy, link, transfer, removal) speaks identical reference
//! grammar. Live listing/stat and removal execution live in
//! [`crate::remote`]; this module is pure address syntax.

use crate::bucket::BucketService;
use crate::connection::{ConnectionRef, ConnectionService};
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, Result};

/// Split a short remote reference into non-empty path segments — the pure
/// half of [`expand_reference`]; whether a leading segment is a connection
/// is a registry question answered by the caller.
fn short_reference_segments(reference: &str) -> Vec<String> {
    reference
        .split('/')
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect()
}

/// Expand a user remote reference (short or canonical) into the canonical
/// `<connection-id>/<bucket>/[key]` form.
///
/// Short forms: `<bucket>/[prefix/]` with a bare (globally unambiguous)
/// bucket name, or `<connection>/<bucket>/[prefix/]`. A first segment only
/// counts as a connection when one is registered under that name or id, so
/// `media/docs/` with no connection called `media` means bucket `media`,
/// prefix `docs/`. The disambiguation rule lives here, beside
/// [`RemoteUri`], so all entry points expand identically.
///
/// Errors when the bucket cannot be resolved under the chosen connection.
pub async fn expand_reference(
    connections: &dyn ConnectionService,
    buckets: &dyn BucketService,
    reference: &str,
) -> Result<String> {
    let known = registered_connections(connections).await?;
    expand_known(buckets, &known, reference).await
}

/// Expand a batch through the same short-reference rule, preserving order.
///
/// One registry read answers every reference's connection question: a
/// 100-URI batch pays one connection listing, not a probe per reference.
pub async fn expand_references(
    connections: &dyn ConnectionService,
    buckets: &dyn BucketService,
    references: &[String],
) -> Result<Vec<String>> {
    let known = registered_connections(connections).await?;
    let mut expanded = Vec::with_capacity(references.len());
    for reference in references {
        expanded.push(expand_known(buckets, &known, reference).await?);
    }
    Ok(expanded)
}

/// The registered connection names and ids as one set — the disambiguation
/// vocabulary for short references.
async fn registered_connections(
    connections: &dyn ConnectionService,
) -> Result<std::collections::HashSet<String>> {
    Ok(connections
        .list()
        .await?
        .into_iter()
        .flat_map(|c| [c.name, c.id.to_string()])
        .collect())
}

/// Apply [`expand_reference`]'s rule against a pre-fetched connection set.
/// Internal to this module's batch path; does not consult the registry.
async fn expand_known(
    buckets: &dyn BucketService,
    known: &std::collections::HashSet<String>,
    reference: &str,
) -> Result<String> {
    use crate::bucket::BucketRef;
    let directory_ref = reference.ends_with('/');
    let segments = short_reference_segments(reference);
    let (bucket, mut prefix): (rstore_domain::Bucket, String) = match segments.as_slice() {
        [] => return Err(Error::invalid_input("empty remote reference")),
        [connection, name, rest @ ..] if known.contains(connection) => {
            let reference = BucketRef::Scoped(ConnectionRef::parse(connection), name.clone());
            let bucket = buckets.resolve(&reference).await?;
            (bucket, rest.join("/"))
        }
        [name, rest @ ..] => {
            let bucket = buckets.resolve(&BucketRef::Name(name.clone())).await?;
            (bucket, rest.join("/"))
        }
    };
    if directory_ref && !prefix.is_empty() {
        prefix.push('/');
    }
    Ok(format!(
        "{}/{}/{}",
        bucket.connection_id, bucket.name, prefix
    ))
}

/// A parsed `<connection>/<bucket>/[key]` remote reference.
///
/// The connection segment is an id (`conn_…`) or a registered name. `key`
/// is the bucket-relative remainder; empty means "the bucket root" (a
/// listing target, never a stat target).
#[derive(Debug, Clone)]
pub struct RemoteUri {
    pub connection: ConnectionRef,
    pub bucket: String,
    pub key: String,
}

impl RemoteUri {
    /// Parse a canonical `<connection>/<bucket>/[key]` reference.
    ///
    /// Returns an invalid-input error when either the connection or the
    /// bucket segment is empty; leading slashes in `key` are tolerated and
    /// stripped.
    pub fn parse(uri: &str) -> Result<Self> {
        let mut segments = uri.splitn(3, '/');
        let connection = segments.next().unwrap_or_default();
        let bucket = segments.next().unwrap_or_default();
        let key = segments.next().unwrap_or_default().trim_start_matches('/');
        if connection.is_empty() {
            return Err(Error::invalid_input(format!(
                "a remote reference needs a connection: `<connection>/<bucket>/[key]`, got `{uri}`"
            )));
        }
        if bucket.is_empty() {
            return Err(Error::invalid_input(format!(
                "a remote reference needs a bucket: `<connection>/<bucket>/[key]`, got `{uri}`"
            )));
        }
        Ok(Self {
            connection: ConnectionRef::parse(connection),
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    /// Build the domain object path for this URI's key.
    ///
    /// Errors when `key` is empty (a bucket-root listing has no object
    /// path) or violates [`ObjectPath`] rules.
    pub fn object_path(&self) -> Result<ObjectPath> {
        ObjectPath::new(&self.key)
    }

    /// Whether this URI names a prefix/directory rather than a single
    /// object (trailing slash or empty-key convention).
    pub fn is_directory_ref(&self) -> bool {
        crate::expand::is_directory_ref(&self.key)
    }

    /// Destructure into `(connection, bucket, key)` without constructing
    /// the struct.
    pub fn parse_base(uri: &str) -> Result<(ConnectionRef, String, String)> {
        let parsed = Self::parse(uri)?;
        Ok((parsed.connection, parsed.bucket, parsed.key))
    }
}
