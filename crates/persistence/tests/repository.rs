//! Repository contract + migration tests (QA-003 / QA-017).

use rstore_domain::connection::{AddressingStyle, ConnectionConfig, Region};
use rstore_domain::{ConnectionId, ErrorCode};
use rstore_persistence::Database;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use time::OffsetDateTime;

fn sample(name: &str) -> ConnectionConfig {
    let now = OffsetDateTime::now_utc();
    ConnectionConfig {
        id: ConnectionId::new(),
        name: name.into(),
        endpoint: "https://s3.example.com".into(),
        region: Region::Specific("us-east-1".into()),
        addressing_style: AddressingStyle::Path,
        tls_verify: true,
        secret_ref: Some("conn_secret_ref".into()),
        created_at: now,
        updated_at: now,
        last_tested_at: None,
        last_test_status: None,
    }
}

#[tokio::test]
async fn connection_crud_round_trip() {
    let db = Database::open_in_memory().await.unwrap();

    let mut cfg = sample("prod-r2");
    db.connections().insert(&cfg).await.unwrap();
    assert_eq!(db.connections().count().await.unwrap(), 1);

    // Duplicate name must surface as a Conflict (CON-002).
    let dup = sample("prod-r2");
    let err = db.connections().insert(&dup).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);

    // Region/addressing survive a round trip.
    let fetched = db
        .connections()
        .get_by_name("prod-r2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.id, cfg.id);
    assert_eq!(fetched.endpoint, cfg.endpoint);
    assert!(matches!(&fetched.region, Region::Specific(r) if r == "us-east-1"));
    assert_eq!(fetched.addressing_style, AddressingStyle::Path);
    assert_eq!(fetched.secret_ref.as_deref(), Some("conn_secret_ref"));

    // Update mutates and persists.
    cfg.endpoint = "https://s3.new.example.com".into();
    db.connections().update(&cfg).await.unwrap();
    let after = db.connections().get(cfg.id).await.unwrap().unwrap();
    assert_eq!(after.endpoint, "https://s3.new.example.com");

    // Delete then missing.
    assert!(db.connections().delete(cfg.id).await.unwrap());
    assert!(db.connections().get(cfg.id).await.unwrap().is_none());
}

/// The connection test status columns round-trip (CON-007/010).
#[tokio::test]
async fn connection_test_status_round_trip() {
    use rstore_domain::connection::ConnectionTestStatus;

    let db = Database::open_in_memory().await.unwrap();
    let cfg = sample("tested");
    db.connections().insert(&cfg).await.unwrap();

    let at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    db.connections()
        .record_test(cfg.id, ConnectionTestStatus::Ok, at)
        .await
        .unwrap();
    let fetched = db
        .connections()
        .get_by_name("tested")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.last_test_status, Some(ConnectionTestStatus::Ok));
    assert_eq!(fetched.last_tested_at, Some(at));

    db.connections()
        .record_test(cfg.id, ConnectionTestStatus::Failed, at)
        .await
        .unwrap();
    let fetched = db
        .connections()
        .get_by_name("tested")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.last_test_status, Some(ConnectionTestStatus::Failed));
}

#[tokio::test]
async fn migrations_are_idempotent_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");

    {
        let _db = Database::open(&db_path).await.unwrap();
    }
    // Reopening the same file must not re-run / fail migrations.
    let _db = Database::open(&db_path).await.unwrap();
}

#[tokio::test]
async fn legacy_migration_history_is_upgraded() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");
    let setup = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE _sqlx_migrations (
             version BIGINT PRIMARY KEY,
             description TEXT NOT NULL,
             installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
             success BOOLEAN NOT NULL,
             checksum BLOB NOT NULL,
             execution_time BIGINT NOT NULL
         )",
    )
    .execute(&setup)
    .await
    .unwrap();
    for (version, description) in [
        (20260813000001_i64, "init"),
        (20260815000002, "connection test status"),
        (20260816000003, "remote object file type"),
        (20260816000004, "remote object fts"),
    ] {
        sqlx::query(
            "INSERT INTO _sqlx_migrations
             (version, description, success, checksum, execution_time)
             VALUES (?, ?, TRUE, ?, 1)",
        )
        .bind(version)
        .bind(description)
        .bind(vec![0_u8; 48])
        .execute(&setup)
        .await
        .unwrap();
    }
    sqlx::query(
        "CREATE TABLE storage_connections (
             id TEXT PRIMARY KEY,
             name TEXT NOT NULL UNIQUE,
             endpoint TEXT NOT NULL,
             region TEXT NOT NULL DEFAULT 'auto',
             addressing_style TEXT NOT NULL DEFAULT 'auto',
             tls_verify INTEGER NOT NULL DEFAULT 1,
             secret_ref TEXT,
             created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL,
             last_tested_at INTEGER,
             last_test_status TEXT
         )",
    )
    .execute(&setup)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE multipart_uploads (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             job_id TEXT,
             connection_id TEXT NOT NULL,
             bucket TEXT NOT NULL,
             key TEXT NOT NULL,
             upload_id TEXT NOT NULL,
             created_at INTEGER NOT NULL
         )",
    )
    .execute(&setup)
    .await
    .unwrap();
    setup.close().await;

    let db = Database::open(&db_path).await.unwrap();
    let source_size: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('multipart_uploads')
         WHERE name = 'source_size'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let source_modified_ns: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('multipart_uploads')
         WHERE name = 'source_modified_ns'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(source_size, 1);
    assert_eq!(source_modified_ns, 1);

    let outbox_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'outbox_events'
         )",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(outbox_exists, 1);
}

/// Corrupt stored state fails loud (see `CONTEXT.md`): the error names the
/// column and the raw value instead of silently coercing a default.
#[tokio::test]
async fn corrupt_rows_fail_loudly() {
    let db = Database::open_in_memory().await.unwrap();
    let cfg = sample("broken");
    db.connections().insert(&cfg).await.unwrap();

    // A closed-vocabulary token that no version ever wrote.
    sqlx::query("UPDATE storage_connections SET addressing_style = 'gibberish'")
        .execute(db.pool())
        .await
        .unwrap();
    let err = db.connections().list().await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Database);
    assert!(
        err.message.contains("addressing_style") && err.message.contains("gibberish"),
        "expected column and raw value in message, got: {}",
        err.message
    );

    // An out-of-range timestamp is equally loud.
    sqlx::query(
        "UPDATE storage_connections
         SET addressing_style = 'path', created_at = 99999999999999",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let err = db.connections().get_by_name("broken").await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Database);
    assert!(
        err.message.contains("created_at"),
        "expected column in message, got: {}",
        err.message
    );
}

// --- CatalogQuery seam contract (MIX-011/015, IDX-024, QA-003) -------------

use std::sync::Arc;

use async_trait::async_trait;
use rstore_catalog::{CatalogQuery, FileFilter, RemoteObjectRepository, Scope};
use rstore_domain::bucket::BucketConfig;
use rstore_domain::id::{BucketId, WorkspaceId};
use rstore_domain::remote::{ObjectPath, RemoteObject};
use rstore_domain::{Bucket, ObjectState, RemoteObjectId, Tag, TagId, TypedId, Workspace};
use rstore_test_support::{catalog_contract, CatalogFixture};

async fn catalog_fixture() -> (rstore_persistence::Database, Scope) {
    let db = Database::open_in_memory().await.unwrap();
    let now = OffsetDateTime::now_utc();
    let conn = sample("catalog-conn");
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
        .add_bucket(workspace.id, bucket.id, OffsetDateTime::now_utc())
        .await
        .unwrap();
    (
        db,
        Scope {
            workspace: workspace.id,
        },
    )
}

fn object(path: &str, size: u64) -> RemoteObject {
    let now = OffsetDateTime::now_utc();
    RemoteObject {
        id: RemoteObjectId::new(),
        connection_id: ConnectionId::new(), // corrected below per fixture
        bucket_id: BucketId::new(),
        path: ObjectPath::new(path).unwrap(),
        size,
        etag: None,
        content_type: None,
        last_modified: now,
        storage_class: None,
        version_id: None,
        confirmed_at: now,
        state: ObjectState::Confirmed,
    }
}

/// The SQLite repositories run the shared catalog contract (QA-003): the
/// same suite as the in-memory reference adapter — filters, workspace
/// scoping, state handling, total order, keyset-cursor stability under
/// mid-walk upserts, and the write face.
struct SqliteCatalogFixture {
    db: Database,
}

#[async_trait]
impl CatalogFixture for SqliteCatalogFixture {
    fn objects(&self) -> Arc<dyn RemoteObjectRepository> {
        Arc::new(self.db.remote_objects())
    }

    fn query(&self) -> Arc<dyn CatalogQuery> {
        Arc::new(self.db.remote_objects())
    }

    async fn seed(
        &self,
        connection: &rstore_domain::ConnectionConfig,
        bucket: &Bucket,
        workspace: &Workspace,
    ) {
        if self
            .db
            .connections()
            .get(connection.id)
            .await
            .unwrap()
            .is_none()
        {
            self.db.connections().insert(connection).await.unwrap();
        }
        if self.db.buckets().get(bucket.id).await.unwrap().is_none() {
            self.db.buckets().insert(bucket).await.unwrap();
        }
        if self
            .db
            .workspaces()
            .get(workspace.id)
            .await
            .unwrap()
            .is_none()
        {
            self.db.workspaces().insert(workspace).await.unwrap();
        }
    }

    async fn add_bucket_to_workspace(&self, workspace: WorkspaceId, bucket: BucketId) {
        self.db
            .workspaces()
            .add_bucket(workspace, bucket, OffsetDateTime::UNIX_EPOCH)
            .await
            .unwrap();
    }

    async fn tag_object(&self, workspace: WorkspaceId, object: RemoteObjectId, tag: TagId) {
        if self.db.tags().get_by_id(tag).await.unwrap().is_none() {
            let name = format!("tag-{}", tag.as_uuid().simple());
            self.db
                .tags()
                .insert(&Tag {
                    id: tag,
                    workspace_id: workspace,
                    normalized_name: name.to_lowercase(),
                    name,
                    color: "#0f172a".into(),
                    description: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                })
                .await
                .unwrap();
        }
        self.db
            .tags()
            .attach(workspace, object, tag, OffsetDateTime::UNIX_EPOCH)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn sqlite_catalog_meets_the_shared_contract() {
    let db = Database::open_in_memory().await.unwrap();
    let fx = SqliteCatalogFixture { db };
    catalog_contract(&fx).await;
}

#[tokio::test]
async fn fts_index_follows_deletes_and_updates() {
    let (db, scope) = catalog_fixture().await;
    let conn_id = db.connections().list().await.unwrap().remove(0).id;
    let bucket_id = db.workspaces().buckets(scope.workspace).await.unwrap()[0];
    let repo = db.remote_objects();
    let mut o = object("dir/vanishing.txt", 1);
    o.connection_id = conn_id;
    o.bucket_id = bucket_id;
    repo.upsert(&o).await.unwrap();

    async fn count(
        repo: &rstore_persistence::SqliteRemoteObjectRepository,
        scope: &rstore_catalog::Scope,
        needle: &str,
    ) -> u64 {
        repo.count(
            *scope,
            &FileFilter {
                name_contains: Some(needle.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }
    assert_eq!(count(&repo, &scope, "vanish").await, 1);

    // Deleting the row must drop it from the FTS index (trigger-maintained).
    repo.delete(o.id).await.unwrap();
    assert_eq!(count(&repo, &scope, "vanish").await, 0);

    // A fresh upsert is searchable again.
    let mut again = object("dir/returned.txt", 1);
    again.connection_id = conn_id;
    again.bucket_id = bucket_id;
    repo.upsert(&again).await.unwrap();
    assert_eq!(count(&repo, &scope, "return").await, 1);
}

#[tokio::test]
async fn name_filter_escapes_like_metacharacters() {
    let (db, scope) = catalog_fixture().await;
    let conn_id = db.connections().list().await.unwrap().remove(0).id;
    let bucket_id = db.workspaces().buckets(scope.workspace).await.unwrap()[0];
    let repo = db.remote_objects();
    for name in ["a%b.txt", "a_b.txt", "axb.txt", "plain.txt"] {
        let mut o = object(&format!("dir/{name}"), 1);
        o.connection_id = conn_id;
        o.bucket_id = bucket_id;
        repo.upsert(&o).await.unwrap();
    }

    for (needle, expected) in [
        ("a%b", 1), // literal % must not match "axb"
        ("a_b", 1), // literal _ must not match "axb"
        ("plain", 1),
        ("a", 4),
    ] {
        let n = repo
            .count(
                scope,
                &FileFilter {
                    name_contains: Some(needle.into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(n, expected, "needle {needle:?}");
    }
}

#[tokio::test]
async fn file_type_filter_uses_stored_classification() {
    let (db, scope) = catalog_fixture().await;
    let conn_id = db.connections().list().await.unwrap().remove(0).id;
    let bucket_id = db.workspaces().buckets(scope.workspace).await.unwrap()[0];
    let repo = db.remote_objects();
    let mut expected = std::collections::BTreeMap::new();
    for (path, kind) in [
        ("img/pic.bmp", "image"),   // missed by the pre-column SQL clause
        ("vid/clip.avi", "video"),  //
        ("aud/track.m4a", "audio"), //
        ("doc/report.pdf", "document"),
        ("arch/bundle.rar", "archive"),
        ("misc/noext", "other"),
    ] {
        let mut o = object(path, 1);
        o.connection_id = conn_id;
        o.bucket_id = bucket_id;
        repo.upsert(&o).await.unwrap();
        *expected.entry(kind.to_string()).or_insert(0u64) += 1;
    }

    use rstore_domain::FileType;
    for (kind, want) in [
        (FileType::Image, expected["image"]),
        (FileType::Video, expected["video"]),
        (FileType::Audio, expected["audio"]),
        (FileType::Document, expected["document"]),
        (FileType::Archive, expected["archive"]),
        (FileType::Other, expected["other"]),
    ] {
        let n = repo
            .count(
                scope,
                &FileFilter {
                    file_type: Some(kind),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(n, want, "{kind:?}");
    }
}
