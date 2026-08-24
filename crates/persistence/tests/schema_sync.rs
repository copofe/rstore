//! Schema-sync pins: the embedded schema and connection pragmas cannot drift silently.
use rstore_persistence::Database;

#[test]
fn database_schema_is_one_embedded_migration() {
    let migrator = sqlx::migrate!("./migrations");
    assert_eq!(migrator.migrations.len(), 1);
}

#[tokio::test]
async fn file_backed_database_enables_wal_and_foreign_keys() {
    let dir = std::env::temp_dir().join(format!("rstore-schema-sync-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = Database::open(&dir.join("sync.db")).await.unwrap();

    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        foreign_keys, 1,
        "foreign keys are load-bearing for every ON DELETE CASCADE"
    );

    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn deleting_a_bucket_cascades_its_catalog_rows() {
    let db = Database::open_in_memory().await.unwrap();
    let pool = db.pool();
    for sql in [
        "INSERT INTO storage_connections (id, name, endpoint, created_at, updated_at) \
         VALUES ('c1', 'nas', 'http://x', 0, 0)",
        "INSERT INTO buckets (id, connection_id, name, created_at, updated_at) \
         VALUES ('b1', 'c1', 'media', 0, 0)",
        "INSERT INTO workspaces (id, name, created_at, updated_at) \
         VALUES ('w1', 'main', 0, 0)",
        "INSERT INTO tags (id, workspace_id, name, normalized_name, created_at) \
         VALUES ('t1', 'w1', 'keep', 'keep', 0)",
        "INSERT INTO remote_objects (id, connection_id, bucket_id, object_path, confirmed_at) \
         VALUES ('o1', 'c1', 'b1', 'a.txt', 0)",
        "INSERT INTO remote_object_tags (workspace_id, remote_object_id, tag_id, applied_at) \
         VALUES ('w1', 'o1', 't1', 0)",
    ] {
        sqlx::query(sql).execute(pool).await.unwrap();
    }

    sqlx::query("DELETE FROM buckets WHERE id = 'b1'")
        .execute(pool)
        .await
        .unwrap();

    let objects: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM remote_objects")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(objects, 0, "the bucket's catalog rows must cascade");
    let associations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM remote_object_tags")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(associations, 0, "tag associations must follow their object");
}
