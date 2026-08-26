//! Shared remote endpoint resolution and batch item planning.
//!
//! [`Endpoints`] caches one resolved adapter per connection/bucket pair so a
//! batch touching many keys under the same target pays the locator chain
//! once, and every item validates its URI shape before any resolution work
//! happens (`validate-before-resolve`).

use std::collections::HashMap;
use std::sync::Arc;

use rstore_domain::remote::ObjectPath;
use rstore_domain::{Bucket, Result};
use rstore_storage_api::Storage;

use crate::locator::CatalogLocator;
use crate::reference::RemoteUri;

/// One resolved batch target: the bucket plus an adapter connected to it.
pub struct Endpoint {
    pub bucket: Bucket,
    pub storage: Arc<dyn Storage>,

    /// `<connection>/<bucket>` prefix used to render per-item labels.
    /// Label prefix `<connection>/<bucket>` used to render per-item labels.
    pub(crate) base: String,
}

impl Endpoint {
    /// Render the full remote label for one object key under this target.
    pub fn label(&self, key: &str) -> String {
        format!("{}/{}", self.base, key)
    }
}

/// Cache of resolved [`Endpoint`]s keyed by `connection-display/bucket`.
#[derive(Default)]
pub struct Endpoints {
    cache: HashMap<String, Arc<Endpoint>>,
}

impl Endpoints {
    /// Resolve a URI's connection and bucket to a shared [`Endpoint`],
    /// reusing the cached adapter when this pair was already resolved this
    /// batch. Errors when the locator cannot resolve the target.
    pub async fn resolve(
        &mut self,
        locator: &CatalogLocator,
        uri: &RemoteUri,
    ) -> Result<Arc<Endpoint>> {
        let cache_key = format!("{}/{}", uri.connection.display(), uri.bucket);
        if let Some(endpoint) = self.cache.get(&cache_key) {
            return Ok(endpoint.clone());
        }
        let (_config, bucket, storage) = locator.resolve_target(uri).await?;
        let endpoint = Arc::new(Endpoint {
            base: format!("{}/{}", uri.connection.display(), bucket.name),
            bucket,
            storage,
        });
        self.cache.insert(cache_key, endpoint.clone());
        Ok(endpoint)
    }
}

/// One planned source item for a remote-side batch operation.
#[derive(Clone)]
pub struct RemoteItem {
    pub endpoint: Arc<Endpoint>,
    pub path: ObjectPath,

    /// Key relative to the requested prefix (empty for the prefix itself).
    pub relative: String,

    pub label: String,
}

/// List the objects under a remote URI as planned [`RemoteItem`]s.
///
/// Validates the URI and the recursive/prefix combination before any
/// resolution or listing; errors surface before side effects. Non-recursive
/// calls list a single flat level, recursive calls walk the whole subtree.
pub async fn remote_items(
    locator: &CatalogLocator,
    endpoints: &mut Endpoints,
    uri: &str,
    recursive: bool,
) -> Result<Vec<RemoteItem>> {
    let parsed = RemoteUri::parse(uri)?;
    crate::expand::require_recursive(uri, &parsed.key, recursive)?;
    let endpoint = endpoints.resolve(locator, &parsed).await?;
    let entries = crate::expand::source_entries(
        endpoint.storage.as_ref(),
        &endpoint.bucket.name,
        &parsed.key,
        recursive,
    )
    .await?;
    Ok(entries
        .into_iter()
        .map(|entry| RemoteItem {
            label: endpoint.label(entry.path.as_str()),
            path: entry.path,
            relative: entry.relative,
            endpoint: endpoint.clone(),
        })
        .collect())
}
