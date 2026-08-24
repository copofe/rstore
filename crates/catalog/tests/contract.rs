//! The in-memory reference adapter runs the shared catalog contract: the
//! same suite the SQLite repositories must pass.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_catalog::{CatalogQuery, InMemoryCatalog, RemoteObjectRepository};
use rstore_domain::connection::ConnectionConfig;
use rstore_domain::id::{BucketId, WorkspaceId};
use rstore_domain::{Bucket, RemoteObjectId, TagId, Workspace};
use rstore_test_support::{catalog_contract, CatalogFixture};

struct InMemoryFixture {
    catalog: Arc<InMemoryCatalog>,
}

#[async_trait]
impl CatalogFixture for InMemoryFixture {
    fn objects(&self) -> Arc<dyn RemoteObjectRepository> {
        self.catalog.clone()
    }

    fn query(&self) -> Arc<dyn CatalogQuery> {
        self.catalog.clone()
    }

    // No foreign keys: rows live in the maps, membership via add_bucket.
    async fn seed(&self, _connection: &ConnectionConfig, _bucket: &Bucket, _workspace: &Workspace) {
    }

    async fn add_bucket_to_workspace(&self, workspace: WorkspaceId, bucket: BucketId) {
        self.catalog.add_bucket(workspace, bucket);
    }

    async fn tag_object(&self, workspace: WorkspaceId, object: RemoteObjectId, tag: TagId) {
        self.catalog.tag(workspace, object, tag);
    }
}

#[tokio::test]
async fn in_memory_catalog_meets_the_shared_contract() {
    let fx = InMemoryFixture {
        catalog: Arc::new(InMemoryCatalog::new()),
    };
    catalog_contract(&fx).await;
}
