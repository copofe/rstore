//! Shared service-test fixture reachable from integration tests through the
//! `test-util` feature.
//!
//! One builder for "an `Application` with registered connections and buckets
//! over `FakeStorage`". Connections get fake endpoints (`localhost:9000+i`) and a
//! `conn_<name>` secret each; buckets are added with default config.

use std::sync::Arc;

use crate::app::Application;
use crate::bucket::AddBucketInput;
use crate::connection::{AddConnectionInput, ConnectionRef};
use rstore_domain::bucket::BucketConfig;
use rstore_domain::Clock;
use rstore_persistence::Database;
use rstore_secrets::{put_credentials, InMemorySecretStore};
use rstore_test_support::FakeStorageFactory;

/// Assemble the SQLite-backed test graph without exposing a database handle
/// from the production `Application` API.
pub fn application_from_db(db: Database) -> Application {
    let clock: Arc<dyn Clock> = Arc::new(rstore_domain::SystemClock);
    let catalog = Arc::new(db.remote_objects());
    let tags = Arc::new(db.tags());
    Application::from_dependencies(crate::ApplicationDependencies::new(
        crate::RegistryDependencies {
            connections: Arc::new(db.connections()),
            buckets: Arc::new(db.buckets()),
            workspaces: Arc::new(db.workspaces()),
            ignore_rules: Arc::new(db.ignore_rules()),
            settings: Arc::new(db.settings()),
            links: Arc::new(db.links()),
            tags: tags.clone(),
        },
        crate::CatalogDependencies {
            catalog: catalog.clone(),
            catalog_query: catalog,
            index_runs: Arc::new(db.index_runs()),
            catalog_tags: tags,
        },
        crate::TransferDependencies {
            jobs: Arc::new(db.jobs()),
            multipart: Arc::new(db.multipart(clock.clone())),
        },
        crate::RuntimeDependencies {
            stats: Arc::new(db.clone()),
            clock,
            secrets: Arc::new(rstore_secrets::NullSecretStore),
            secret_source_stats: Arc::new(rstore_application_ports::EmptySecretSourceStats),
            storage_factory: None,
            outbox: Some(Arc::new(db.outbox())),
            unit_of_work: Some(Arc::new(db.clone())),
            repairs: Some(Arc::new(db.outbox())),
            mutation_receipts: Some(Arc::new(db.mutation_receipts())),
        },
    ))
}

/// Build an [`Application`] over in-memory SQLite + fake storage with the
/// given topology: `conns` are connection names, `buckets` are
/// `(connection, bucket)` pairs registered against them.
pub async fn app_with(
    conns: &[&str],
    buckets: &[(&str, &str)],
) -> (Application, Arc<FakeStorageFactory>) {
    wire(conns, buckets, None).await
}

/// [`app_with`] with an injected clock, for expiry-sensitive behaviour
/// (tombstone GC, retention).
pub async fn app_with_clock(
    conns: &[&str],
    buckets: &[(&str, &str)],
    clock: Arc<dyn Clock>,
) -> (Application, Arc<FakeStorageFactory>) {
    wire(conns, buckets, Some(clock)).await
}

/// An app whose secret store carries credentials for `conns` but whose
/// registry is empty — the import-test shape (DATA-015/016), where a bundle
/// re-creates connections against pre-stored secrets.
pub async fn app_with_secrets_only(conns: &[&str]) -> Application {
    let factory = Arc::new(FakeStorageFactory::new());
    application_from_db(Database::open_in_memory().await.unwrap())
        .with_secrets(secret_store_for(conns).await)
        .with_storage_factory(factory)
}

async fn secret_store_for(conns: &[&str]) -> Arc<InMemorySecretStore> {
    let secrets = Arc::new(InMemorySecretStore::new());
    for name in conns {
        put_credentials(secrets.as_ref(), &format!("conn_{name}"), "AK", "SK")
            .await
            .unwrap();
    }
    secrets
}

async fn wire(
    conns: &[&str],
    buckets: &[(&str, &str)],
    clock: Option<Arc<dyn Clock>>,
) -> (Application, Arc<FakeStorageFactory>) {
    let secrets = secret_store_for(conns).await;
    let factory = Arc::new(FakeStorageFactory::new());
    let mut app = application_from_db(Database::open_in_memory().await.unwrap())
        .with_secrets(secrets)
        .with_storage_factory(factory.clone());
    if let Some(clock) = clock {
        app = app.with_clock(clock);
    }
    for (i, name) in conns.iter().enumerate() {
        app.api()
            .registry()
            .connections()
            .add(AddConnectionInput {
                name: (*name).into(),
                endpoint: format!("http://localhost:{}", 9000 + i),
                region: rstore_domain::Region::Auto,
                addressing_style: rstore_domain::AddressingStyle::Path,
                tls_verify: true,
                secret_ref: Some(format!("conn_{name}")),
                credentials: None,
            })
            .await
            .unwrap();
    }
    for (conn, bucket) in buckets {
        app.api()
            .registry()
            .buckets()
            .add(
                AddBucketInput {
                    connection: ConnectionRef::Name((*conn).into()),
                    name: (*bucket).into(),
                    config: BucketConfig::default(),
                },
                false,
            )
            .await
            .unwrap();
    }
    (app, factory)
}
