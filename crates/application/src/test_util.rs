//! Test fixtures for application-layer tests: assemble an [`Application`]
//! over an in-memory database backed by a fake storage factory, with
//! connections and buckets pre-registered.

use std::sync::Arc;

use crate::app::Application;
use crate::bucket::AddBucketInput;
use crate::connection::{AddConnectionInput, ConnectionRef};
use rstore_domain::bucket::BucketConfig;
use rstore_domain::Clock;
use rstore_persistence::Database;
use rstore_test_support::FakeStorageFactory;

/// Assemble an [`Application`] from an existing database using the
/// production dependency wiring.
pub fn application_from_db(db: Database) -> Application {
    Application::from_dependencies(rstore_persistence::assemble(&db, None))
}

/// An application over an in-memory database with a fake storage factory
/// and the given connections (`names`) and buckets (`connection, bucket`
/// pairs). Connections get distinct localhost endpoints and placeholder
/// credentials; every registration asserts success.
pub async fn app_with(
    conns: &[&str],
    buckets: &[(&str, &str)],
) -> (Application, Arc<FakeStorageFactory>) {
    wire(conns, buckets, None).await
}

/// [`app_with`] plus a caller-controlled clock, for tests that assert on
/// timestamps.
pub async fn app_with_clock(
    conns: &[&str],
    buckets: &[(&str, &str)],
    clock: Arc<dyn Clock>,
) -> (Application, Arc<FakeStorageFactory>) {
    wire(conns, buckets, Some(clock)).await
}

/// An application with a fake storage factory and an empty registry.
pub async fn app_with_empty_registry() -> Application {
    let factory = Arc::new(FakeStorageFactory::new());
    application_from_db(Database::open_in_memory().await.unwrap()).with_storage_factory(factory)
}

async fn wire(
    conns: &[&str],
    buckets: &[(&str, &str)],
    clock: Option<Arc<dyn Clock>>,
) -> (Application, Arc<FakeStorageFactory>) {
    let factory = Arc::new(FakeStorageFactory::new());
    let mut app = application_from_db(Database::open_in_memory().await.unwrap())
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
                public_base_url: None,
                cdn_base_url: None,
                region: rstore_domain::Region::Auto,
                addressing_style: rstore_domain::AddressingStyle::Path,
                tls_verify: true,
                credentials: Some(rstore_domain::Secret::new(
                    crate::connection::CredentialInput {
                        access_key: "AK".into(),
                        secret_key: "SK".into(),
                    },
                )),
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
