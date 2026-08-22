//! Shared service-test fixture reachable from integration tests through the
//! `test-util` feature.
//!
//! One builder for an Application with registered connections and buckets over
//! FakeStorage. Connections get fake endpoints and test credentials; buckets
//! are added with default config.

use std::sync::Arc;

use crate::app::Application;
use crate::bucket::AddBucketInput;
use crate::connection::{AddConnectionInput, ConnectionRef};
use rstore_domain::bucket::BucketConfig;
use rstore_domain::Clock;
use rstore_persistence::Database;
use rstore_test_support::FakeStorageFactory;

/// Assemble the SQLite-backed test graph without exposing a database handle
/// from the production `Application` API.
///
/// Deliberately routes through `rstore_persistence::assemble` — the same
/// assembly point `bootstrap::Runtime::open` uses — so a store added to the
/// production wiring is automatically visible to every service test.
/// Skips [`Application::initialize`](crate::app::Application::initialize):
/// construction stays deterministic; startup recovery rules are covered by
/// the bootstrap integration test instead.
pub fn application_from_db(db: Database) -> Application {
    let clock: Arc<dyn Clock> = Arc::new(rstore_domain::SystemClock);
    Application::from_dependencies(rstore_persistence::assemble(&db, None, clock))
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

/// An app with an empty registry, used by import tests.
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
