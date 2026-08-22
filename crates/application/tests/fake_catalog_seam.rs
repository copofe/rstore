//! Seam test (QA-003 / CORE-005): the file list runs against the
//! in-memory catalog adapter through the composition root, proving
//! `with_catalog` reaches every service the catalog seam touches — no
//! hand-assembled wiring required.

use std::sync::Arc;

use rstore_application::WorkspaceRef;
use rstore_application_ports::{BucketStore as _, ConnectionStore as _, WorkspaceStore as _};
use rstore_catalog::{
    CatalogQuery as _, FileFilter, FileSort, InMemoryCatalog, PageRequest,
    RemoteObjectRepository as _,
};
use rstore_domain::bucket::BucketConfig;
use rstore_domain::id::{BucketId, WorkspaceId};
use rstore_domain::remote::{ObjectPath, RemoteObject};
use rstore_domain::workspace::Workspace;
use rstore_domain::{Bucket, ConnectionConfig, ObjectState, RemoteObjectId};
use rstore_persistence::Database;
use time::OffsetDateTime;

#[tokio::test]
async fn files_list_runs_over_the_in_memory_catalog() {
    let db = Database::open_in_memory().await.unwrap();
    let now = OffsetDateTime::now_utc();
    let conn = ConnectionConfig {
        id: rstore_domain::ConnectionId::new(),
        name: "nas".into(),
        endpoint: "http://localhost:9000".into(),
        public_base_url: None,
        cdn_base_url: None,
        region: rstore_domain::Region::Auto,
        addressing_style: rstore_domain::AddressingStyle::Path,
        tls_verify: true,
        access_key: None,
        secret_key: None,
        created_at: now,
        updated_at: now,
        last_tested_at: None,
        last_test_status: None,
    };
    db.connections().insert(&conn).await.unwrap();
    let bucket = Bucket {
        id: BucketId::new(),
        connection_id: conn.id,
        name: "media".into(),
        config: BucketConfig::default(),
        created_at: now,
        updated_at: now,
    };
    db.buckets().insert(&bucket).await.unwrap();
    let workspace = Workspace {
        id: WorkspaceId::new(),
        name: "w".into(),
        description: None,
        created_at: now,
        updated_at: now,
    };
    db.workspaces().insert(&workspace).await.unwrap();
    db.workspaces()
        .add_bucket(workspace.id, bucket.id, now)
        .await
        .unwrap();

    let catalog = Arc::new(InMemoryCatalog::new());
    catalog.set_connection_name(conn.id, "nas");
    catalog.set_bucket_name(bucket.id, "media");
    catalog.add_bucket(workspace.id, bucket.id);
    catalog
        .upsert(&RemoteObject {
            id: RemoteObjectId::new(),
            connection_id: conn.id,
            bucket_id: bucket.id,
            path: ObjectPath::new("photos/pic.bmp").unwrap(),
            size: 10,
            etag: None,
            content_type: None,
            last_modified: now,
            storage_class: None,
            version_id: None,
            confirmed_at: now,
            state: ObjectState::Confirmed,
        })
        .await
        .unwrap();

    let app = rstore_application::test_util::application_from_db(db).with_catalog(catalog.clone());

    let page = app
        .api()
        .catalog()
        .files()
        .list(
            &WorkspaceRef::Name("w".into()),
            &FileFilter::default(),
            FileSort::default(),
            &PageRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.items[0].name, "pic.bmp");
    assert_eq!(page.items[0].uri, "nas/media/photos/pic.bmp");

    // The same trait object also serves the widened repository contract:
    // locator resolution against the in-memory adapter.
    let object = catalog
        .find_by_locator(
            conn.id,
            bucket.id,
            &ObjectPath::new("photos/pic.bmp").unwrap(),
        )
        .await
        .unwrap()
        .expect("row present");
    assert_eq!(object.size, 10);

    // search/count stay usable through the CatalogQuery seam.
    assert_eq!(
        catalog
            .count(
                rstore_catalog::Scope {
                    workspace: workspace.id
                },
                &FileFilter::default()
            )
            .await
            .unwrap(),
        1
    );
}
