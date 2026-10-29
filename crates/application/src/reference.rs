//! Remote reference syntax: short-form expansion, transfer routing, and
//! canonical parsing.
//!
//! One home for the `[<connection>/]<bucket>/[key]` vocabulary — expansion of
//! user short forms and local/remote routing ([`References`]) plus canonical
//! parsing ([`RemoteUri`]) — so every consumer (CLI, copy, link, transfer,
//! removal) speaks identical reference grammar. Live listing/stat and removal
//! execution live in [`crate::remote`].

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::bucket::{BucketRef, BucketService};
use crate::connection::{ConnectionRef, ConnectionService};
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, Result};

/// Split a short remote reference into non-empty path segments — the pure
/// half of [`References::expand`]; whether a leading segment is a connection
/// is a registry question answered by the caller.
fn short_reference_segments(reference: &str) -> Vec<String> {
    reference
        .split('/')
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect()
}

/// Where a transfer's arguments point, decided once by [`References::route`].
/// Remote sides are expanded references bound to a connection id.
#[derive(Debug, Clone)]
pub enum TransferRoute {
    /// Local sources into a remote target.
    Upload {
        sources: Vec<PathBuf>,
        target: RemoteUri,
    },
    /// Remote sources into a local target.
    Download {
        sources: Vec<RemoteUri>,
        target: PathBuf,
    },
    /// Remote sources into a remote target.
    Copy {
        sources: Vec<RemoteUri>,
        target: RemoteUri,
    },
}

/// A bucket about to be registered, named by `[<connection>/]<name>`.
#[derive(Debug, Clone)]
pub struct NewBucket {
    pub connection: ConnectionRef,
    pub name: String,
}

/// One classified transfer argument.
enum Endpoint {
    Local(PathBuf),
    Remote(RemoteUri),
}

/// Registered names a short reference's first segment may match.
struct RegistryNames {
    /// Connection names and ids.
    connections: HashSet<String>,
    /// Registered bucket names across all connections.
    buckets: HashSet<String>,
}

impl RegistryNames {
    fn contains(&self, segment: &str) -> bool {
        self.connections.contains(segment) || self.buckets.contains(segment)
    }
}

/// Arguments that are local paths by their spelling alone, whatever the
/// registry holds.
fn is_explicit_local(argument: &str) -> bool {
    matches!(argument, "." | ".." | "~")
        || ["./", "../", "~/"]
            .iter()
            .any(|prefix| argument.starts_with(prefix))
        || Path::new(argument).is_absolute()
}

/// The reference grammar bound to the registry.
///
/// Each call reads the registered names once, so a batch pays one registry
/// listing rather than a probe per reference.
pub struct References {
    connections: Arc<dyn ConnectionService>,
    buckets: Arc<dyn BucketService>,
}

impl References {
    pub fn new(connections: Arc<dyn ConnectionService>, buckets: Arc<dyn BucketService>) -> Self {
        Self {
            connections,
            buckets,
        }
    }

    /// Expand a user remote reference (short or canonical) into a reference
    /// bound to the bucket's connection id.
    ///
    /// Short forms: `<bucket>/[prefix/]` with a bare (globally unambiguous)
    /// bucket name, or `<connection>/<bucket>/[prefix/]`. A first segment only
    /// counts as a connection when one is registered under that name or id, so
    /// `media/docs/` with no connection called `media` means bucket `media`,
    /// prefix `docs/`.
    ///
    /// Errors when the bucket cannot be resolved under the chosen connection.
    pub async fn expand(&self, reference: &str) -> Result<RemoteUri> {
        let known = self.connection_names().await?;
        self.expand_known(&known, reference).await
    }

    /// Expand a batch through the same short-reference rule, preserving order.
    pub async fn expand_many(&self, references: &[String]) -> Result<Vec<RemoteUri>> {
        let known = self.connection_names().await?;
        let mut expanded = Vec::with_capacity(references.len());
        for reference in references {
            expanded.push(self.expand_known(&known, reference).await?);
        }
        Ok(expanded)
    }

    /// Name a bucket that is not registered yet: `<connection>/<name>`, or a
    /// bare `<name>` when exactly one connection is registered.
    ///
    /// # Errors
    /// `invalid_input` for an empty or nested name, and for a bare name while
    /// zero or several connections are registered.
    pub async fn new_bucket(&self, reference: &str) -> Result<NewBucket> {
        let reference = reference.trim_end_matches('/');
        if reference.is_empty() {
            return Err(Error::invalid_input("bucket name must not be empty"));
        }
        if let Some((connection, name)) = reference.split_once('/') {
            if connection.is_empty() || name.is_empty() || name.contains('/') {
                return Err(Error::invalid_input(format!(
                    "expected `[<connection>/]<bucket>`, got `{reference}`"
                )));
            }
            return Ok(NewBucket {
                connection: ConnectionRef::parse(connection),
                name: name.to_string(),
            });
        }
        let connections = self.connections.list().await?;
        match connections.as_slice() {
            [only] => Ok(NewBucket {
                connection: ConnectionRef::Id(only.id),
                name: reference.to_string(),
            }),
            [] => Err(Error::invalid_input(
                "no connection is registered; add one before registering buckets",
            )),
            _ => Err(Error::invalid_input(format!(
                "several connections are registered; name the bucket as `<connection>/{reference}`"
            ))),
        }
    }

    /// Decide a transfer's direction from its source and target arguments.
    ///
    /// An argument is local when it starts with `./`, `../`, `~/` or is
    /// absolute; otherwise it is remote when its first segment names a
    /// registered connection or bucket, and local when it names neither. An
    /// argument whose first segment is both registered and present on the
    /// local filesystem is rejected as ambiguous rather than guessed.
    ///
    /// # Errors
    /// `invalid_input` for no sources, sources mixing local and remote
    /// arguments, a local-to-local transfer, or an ambiguous argument; a
    /// local I/O error when the filesystem cannot be probed.
    pub async fn route(&self, sources: &[String], target: &str) -> Result<TransferRoute> {
        if sources.is_empty() {
            return Err(Error::invalid_input("a transfer needs at least one source"));
        }
        let names = RegistryNames {
            connections: self.connection_names().await?,
            buckets: self
                .buckets
                .list()
                .await?
                .into_iter()
                .map(|bucket| bucket.name)
                .collect(),
        };
        let mut local = Vec::new();
        let mut remote = Vec::new();
        for source in sources {
            match self.classify(&names, source).await? {
                Endpoint::Local(path) => local.push(path),
                Endpoint::Remote(uri) => remote.push(uri),
            }
        }
        if !local.is_empty() && !remote.is_empty() {
            return Err(Error::invalid_input(
                "sources mix local paths and remote references; transfer them separately",
            ));
        }
        let target = self.classify(&names, target).await?;
        match (remote.is_empty(), target) {
            (true, Endpoint::Remote(target)) => Ok(TransferRoute::Upload {
                sources: local,
                target,
            }),
            (true, Endpoint::Local(_)) => Err(Error::invalid_input(
                "no argument names a registered bucket or connection; check the bucket name",
            )),
            (false, Endpoint::Local(target)) => Ok(TransferRoute::Download {
                sources: remote,
                target,
            }),
            (false, Endpoint::Remote(target)) => Ok(TransferRoute::Copy {
                sources: remote,
                target,
            }),
        }
    }

    /// The registered connection names and ids as one set — the
    /// disambiguation vocabulary for short references.
    async fn connection_names(&self) -> Result<HashSet<String>> {
        Ok(self
            .connections
            .list()
            .await?
            .into_iter()
            .flat_map(|c| [c.name, c.id.to_string()])
            .collect())
    }

    /// Classify one transfer argument per [`Self::route`]'s rule.
    async fn classify(&self, names: &RegistryNames, argument: &str) -> Result<Endpoint> {
        if argument.is_empty() {
            return Err(Error::invalid_input("empty transfer argument"));
        }
        if is_explicit_local(argument) {
            return Ok(Endpoint::Local(PathBuf::from(argument)));
        }
        let first = argument.split('/').next().unwrap_or(argument);
        if !names.contains(first) {
            return Ok(Endpoint::Local(PathBuf::from(argument)));
        }
        let shadowed = tokio::fs::try_exists(first)
            .await
            .map_err(|e| Error::io(format!("probe local path `{first}`")).with_source(e))?;
        if shadowed {
            return Err(Error::invalid_input(format!(
                "`{argument}` is ambiguous: `{first}` is both registered remotely and a local \
                 path; write `./{argument}` for the local path or qualify the remote with its \
                 connection"
            )));
        }
        Ok(Endpoint::Remote(
            self.expand_known(&names.connections, argument).await?,
        ))
    }

    /// Apply [`Self::expand`]'s rule against a pre-fetched connection set.
    async fn expand_known(&self, known: &HashSet<String>, reference: &str) -> Result<RemoteUri> {
        let directory_ref = reference.ends_with('/');
        let segments = short_reference_segments(reference);
        let (bucket, mut prefix): (rstore_domain::Bucket, String) = match segments.as_slice() {
            [] => return Err(Error::invalid_input("empty remote reference")),
            [connection, name, rest @ ..] if known.contains(connection) => {
                let reference = BucketRef::Scoped(ConnectionRef::parse(connection), name.clone());
                let bucket = self.buckets.resolve(&reference).await?;
                (bucket, rest.join("/"))
            }
            [name, rest @ ..] => {
                let bucket = self.buckets.resolve(&BucketRef::Name(name.clone())).await?;
                (bucket, rest.join("/"))
            }
        };
        if directory_ref && !prefix.is_empty() {
            prefix.push('/');
        }
        Ok(RemoteUri {
            connection: ConnectionRef::Id(bucket.connection_id),
            bucket: bucket.name,
            key: prefix,
        })
    }
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

/// The canonical spelling, `<connection>/<bucket>/<key>`: what
/// [`RemoteUri::parse`] reads back, and the label batch reports carry.
impl std::fmt::Display for RemoteUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.connection.display(),
            self.bucket,
            self.key
        )
    }
}
