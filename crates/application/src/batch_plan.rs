//! The one prelude for remote batch operations.
//!
//! Recursive `cp`/`mv` sources and `rm` targets share one shape: resolve the
//! endpoint once per unique `conn/bucket`, expand directory references
//! through `expand::source_entries`, type the paths and label every item with
//! the canonical `conn/bucket/key` address. That assembly lives here, so the
//! orchestrators are parameter assemblies, and `job_run`'s interrupt,
//! truncation and terminal-state rules govern every batch.

use std::collections::HashMap;
use std::sync::Arc;

use rstore_domain::remote::ObjectPath;
use rstore_domain::{Bucket, Result};
use rstore_storage_api::Storage;

use crate::locator::CatalogLocator;
use crate::remote::RemoteUri;

/// One resolved `conn/bucket` endpoint, shared by every item referencing it —
/// adapter construction stays once per unique endpoint however many objects a
/// recursive expansion yields.
pub struct Endpoint {
    pub bucket: Bucket,
    pub storage: Arc<dyn Storage>,
    /// `conn/bucket` in the reference's own spelling — the base of the
    /// canonical address form.
    base: String,
}

impl Endpoint {
    /// The canonical `conn/bucket/key` address (the short-reference shape).
    pub fn label(&self, key: &str) -> String {
        format!("{}/{}", self.base, key)
    }
}

/// The endpoint cache a run resolves through.
#[derive(Default)]
pub struct Endpoints {
    cache: HashMap<String, Arc<Endpoint>>,
}

impl Endpoints {
    /// Resolve one endpoint, reusing an adapter that has already been resolved when
    /// the same `conn/bucket` repeats in a batch.
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

/// One typed item of a remote batch: endpoint shared, path typed, label
/// canonical, prefix-relative name kept for directory-target pairing.
pub struct RemoteItem {
    pub endpoint: Arc<Endpoint>,
    pub path: ObjectPath,
    /// The entry's name relative to the expanded prefix — what a
    /// directory-ish target joins under its own key.
    pub relative: String,
    /// Canonical `conn/bucket/key`.
    pub label: String,
}

/// Expand one URI reference into typed items — the shared prelude. The
/// directory-reference pairing rule (`require_recursive`), prefix expansion
/// (`source_entries`) and canonical labels happen here once; callers consume
/// typed items and never re-parse or re-resolve.
pub async fn remote_items(
    locator: &CatalogLocator,
    endpoints: &mut Endpoints,
    uri: &str,
    recursive: bool,
) -> Result<Vec<RemoteItem>> {
    let parsed = RemoteUri::parse(uri)?;
    let endpoint = endpoints.resolve(locator, &parsed).await?;
    crate::expand::require_recursive(uri, &parsed.key, recursive)?;
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
