//! Shared behavioral contract for every catalog adapter behind the
//! `RemoteObjectRepository` / `CatalogQuery` seam.
//!
//! The same suite runs against the SQLite repositories and the in-memory
//! reference implementation. It pins the semantics the two adapters drifted
//! on before it existed: state filtering, workspace scoping, the `Type` sort
//! key, deterministic total order with keyset cursors stable under mid-walk
//! upserts, and `upsert` storing the row as given (confirmation semantics
//! belong to `confirmed_object`, not the repository).

use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use rstore_catalog::{
    CatalogQuery, FileFilter, FileSort, PageRequest, RemoteObjectRepository, Scope, SortField,
    SortOrder,
};
use rstore_domain::connection::{AddressingStyle, ConnectionConfig, Region};
use rstore_domain::id::{BucketId, TagId, WorkspaceId};
use rstore_domain::remote::{ObjectPath, RemoteObject};
use rstore_domain::{Bucket, ErrorCode, FileType, ObjectState, RemoteObjectId, TypedId, Workspace};
use time::OffsetDateTime;

/// One catalog adapter plus the topology setup the contract needs, so the
/// suite can run against any implementation of the seam.
#[async_trait]
pub trait CatalogFixture: Send + Sync {
    /// The write side (indexer, locator, transfers).
    fn objects(&self) -> Arc<dyn RemoteObjectRepository>;
    /// The read side (`files list` / `files search`).
    fn query(&self) -> Arc<dyn CatalogQuery>;
    /// Persist the rows a catalog entry references. Foreign-keyed adapters
    /// need them; idempotent, so it can be called per topology unit.
    async fn seed(&self, connection: &ConnectionConfig, bucket: &Bucket, workspace: &Workspace);
    /// Register query-scope membership — the SQL adapters' `workspace_buckets`
    /// join.
    async fn add_bucket_to_workspace(&self, workspace: WorkspaceId, bucket: BucketId);
    /// Record a tag association for the filter cases.
    async fn tag_object(&self, workspace: WorkspaceId, object: RemoteObjectId, tag: TagId);
}

/// The contract's expected sort value for one row and field — an independent
/// oracle, not a call into either adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SortKey {
    Num(i64),
    Text(String),
}

impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (SortKey::Num(a), SortKey::Num(b)) => a.cmp(b),
            (SortKey::Text(a), SortKey::Text(b)) => a.cmp(b),
            (SortKey::Num(_), SortKey::Text(_)) => Ordering::Less,
            (SortKey::Text(_), SortKey::Num(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn order_key(o: &RemoteObject, field: SortField) -> (SortKey, String, String) {
    let value = match field {
        SortField::Name => SortKey::Text(o.path.file_name().to_string()),
        SortField::Size => SortKey::Num(o.size as i64),
        SortField::Modified => SortKey::Num(o.last_modified.unix_timestamp()),
        // The domain classifier is the sort key (never the
        // provider-supplied content type).
        SortField::Type => SortKey::Text(o.type_hint().as_str().to_string()),
        SortField::Bucket => SortKey::Text(o.bucket_id.as_uuid().to_string()),
        SortField::Connection => SortKey::Text(o.connection_id.as_uuid().to_string()),
    };
    (
        value,
        o.path.as_str().to_string(),
        o.id.as_uuid().to_string(),
    )
}

fn t(secs: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(secs).expect("contract timestamp")
}

fn conn(name: &str) -> ConnectionConfig {
    ConnectionConfig {
        id: rstore_domain::ConnectionId::new_random(),
        name: name.into(),
        endpoint: "https://s3.example.com".into(),
        public_base_url: None,
        cdn_base_url: None,
        region: Region::Auto,
        addressing_style: AddressingStyle::Path,
        tls_verify: true,
        access_key: None,
        secret_key: None,
        created_at: t(0),
        updated_at: t(0),
        last_tested_at: None,
        last_test_status: None,
    }
}

fn bucket(connection: rstore_domain::ConnectionId, name: &str) -> Bucket {
    Bucket {
        id: BucketId::new_random(),
        connection_id: connection,
        name: name.into(),
        config: rstore_domain::bucket::BucketConfig::default(),
        created_at: t(0),
        updated_at: t(0),
    }
}

fn obj(
    connection: rstore_domain::ConnectionId,
    bucket: BucketId,
    path: &str,
    size: u64,
    modified: i64,
    state: ObjectState,
    content_type: Option<&str>,
) -> RemoteObject {
    RemoteObject {
        id: RemoteObjectId::new_random(),
        connection_id: connection,
        bucket_id: bucket,
        path: ObjectPath::new(path).expect("contract path"),
        size,
        etag: None,
        content_type: content_type.map(str::to_string),
        last_modified: t(modified),
        storage_class: None,
        version_id: None,
        // Deliberately distinct from `last_modified`: the contract asserts
        // the repository stores the row as given, not its own clock.
        confirmed_at: t(modified.saturating_mul(2)),
        state,
    }
}

fn ids(rows: &[RemoteObject]) -> Vec<String> {
    rows.iter().map(|o| o.id.as_uuid().to_string()).collect()
}

async fn count_filtered(query: &dyn CatalogQuery, scope: Scope, filter: &FileFilter) -> u64 {
    query.count(scope, filter).await.expect("contract count")
}

async fn walk(
    query: &dyn CatalogQuery,
    scope: Scope,
    filter: &FileFilter,
    sort: FileSort,
    limit: u32,
) -> Vec<RemoteObject> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let page = query
            .search(scope, filter, sort, &PageRequest { limit, after })
            .await
            .expect("contract search");
        assert!(
            page.items.len() <= limit as usize,
            "page exceeds the requested limit"
        );
        out.extend(page.items);
        after = page.next_cursor;
        if after.is_none() {
            return out;
        }
    }
}

/// A walk that upserts `mid_walk` after its first page, inside the run —
/// the cursor-stability case.
async fn walk_with_midwalk_upsert(
    query: &dyn CatalogQuery,
    objects: &dyn RemoteObjectRepository,
    scope: Scope,
    sort: FileSort,
    mid_walk: RemoteObject,
) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    let mut after = None;
    let mut pages = 0usize;
    loop {
        let page = query
            .search(
                scope,
                &FileFilter::default(),
                sort,
                &PageRequest { limit: 2, after },
            )
            .await
            .expect("contract search");
        seen.extend(ids(&page.items));
        after = page.next_cursor;
        pages += 1;
        if pages == 1 {
            objects.upsert(&mid_walk).await.expect("contract upsert");
        }
        if after.is_none() {
            return seen;
        }
    }
}

/// Exercise the substitutability contract shared by every catalog adapter.
pub async fn catalog_contract(fx: &dyn CatalogFixture) {
    // --- topology -----------------------------------------------------------
    let ws = Workspace {
        id: WorkspaceId::new_random(),
        name: "w".into(),
        description: None,
        created_at: t(0),
        updated_at: t(0),
    };
    let ws2 = Workspace {
        id: WorkspaceId::new_random(),
        name: "w2".into(),
        description: None,
        created_at: t(0),
        updated_at: t(0),
    };
    let conn_a = conn("alpha");
    let conn_b = conn("beta");
    let mem1 = bucket(conn_a.id, "mem1");
    let mem2 = bucket(conn_a.id, "mem2");
    let outsider = bucket(conn_a.id, "outsider");
    let writes = bucket(conn_a.id, "writes");
    for (c, b, w) in [
        (&conn_a, &mem1, &ws),
        (&conn_a, &mem2, &ws),
        (&conn_a, &outsider, &ws),
        (&conn_a, &writes, &ws),
        (&conn_a, &mem1, &ws2),
        (&conn_b, &mem1, &ws), // conn_b is seeded but owns no member bucket
    ] {
        fx.seed(c, b, w).await;
    }
    fx.add_bucket_to_workspace(ws.id, mem1.id).await;
    fx.add_bucket_to_workspace(ws.id, mem2.id).await;
    fx.add_bucket_to_workspace(ws2.id, mem1.id).await;

    // --- query rows ---------------------------------------------------------
    let o1 = obj(
        conn_a.id,
        mem1.id,
        "img/logo.png",
        100,
        1000,
        ObjectState::Confirmed,
        Some("image/png"),
    );
    let o2 = obj(
        conn_a.id,
        mem1.id,
        "img/photo.bmp",
        5000,
        2000,
        ObjectState::Confirmed,
        None,
    );
    let o3 = obj(
        conn_a.id,
        mem1.id,
        "docs/report.pdf",
        10,
        3000,
        ObjectState::Confirmed,
        None,
    );
    let o4 = obj(
        conn_a.id,
        mem2.id,
        "arch/old.zip",
        900,
        4000,
        ObjectState::Confirmed,
        None,
    );
    let o5 = obj(
        conn_a.id,
        mem2.id,
        "notes.txt",
        50,
        5000,
        ObjectState::Stale,
        None,
    );
    let o7 = obj(
        conn_a.id,
        mem1.id,
        "gone/deleted.txt",
        10,
        6000,
        ObjectState::Deleted,
        None,
    );
    // Same size and path in two member buckets: only the id tie-break
    // separates them in the total order.
    let o8 = obj(
        conn_a.id,
        mem1.id,
        "dup/tie.txt",
        77,
        8000,
        ObjectState::Confirmed,
        None,
    );
    let o9 = obj(
        conn_a.id,
        mem2.id,
        "dup/tie.txt",
        77,
        8000,
        ObjectState::Confirmed,
        None,
    );
    // Never inside the workspace scope.
    let o6 = obj(
        conn_a.id,
        outsider.id,
        "hidden/x.txt",
        70,
        7000,
        ObjectState::Confirmed,
        None,
    );
    for o in [&o1, &o2, &o3, &o4, &o5, &o7, &o8, &o9, &o6] {
        fx.objects().upsert(o).await.expect("contract upsert");
    }

    let scope = Scope { workspace: ws.id };
    let default = FileFilter::default();
    let query = fx.query();
    let objects = fx.objects();

    // --- upsert stores the row as given --------------------------------------
    let stored = objects
        .find_by_locator(conn_a.id, mem2.id, &ObjectPath::new("notes.txt").unwrap())
        .await
        .expect("contract find_by_locator")
        .expect("stale row present");
    assert_eq!(stored.state, ObjectState::Stale, "state stored as given");
    assert_eq!(
        stored.confirmed_at, o5.confirmed_at,
        "confirmed_at stored as given"
    );
    assert_eq!(stored.size, 50);
    let deleted = objects
        .get(o7.id)
        .await
        .expect("contract get")
        .expect("deleted row present");
    assert_eq!(deleted.state, ObjectState::Deleted);
    let typed = objects
        .get(o1.id)
        .await
        .expect("contract get")
        .expect("row present");
    assert_eq!(typed.content_type.as_deref(), Some("image/png"));
    assert_eq!(typed.last_modified, t(1000));

    // --- state filter and workspace scoping -----------------------------------
    assert_eq!(
        count_filtered(&*query, scope, &default).await,
        7,
        "not-deleted rows in member buckets only"
    );
    assert_eq!(
        count_filtered(
            &*query,
            scope,
            &FileFilter {
                state: Some(ObjectState::Confirmed),
                ..default.clone()
            }
        )
        .await,
        6
    );
    assert_eq!(
        count_filtered(
            &*query,
            scope,
            &FileFilter {
                state: Some(ObjectState::Stale),
                ..default.clone()
            }
        )
        .await,
        1
    );
    assert_eq!(
        count_filtered(
            &*query,
            scope,
            &FileFilter {
                state: Some(ObjectState::Deleted),
                ..default.clone()
            }
        )
        .await,
        1
    );
    let visible = walk(&*query, scope, &default, FileSort::default(), 50).await;
    assert!(
        visible.iter().all(|o| o.path.as_str() != "hidden/x.txt"),
        "non-member bucket rows never leak into the scope"
    );

    // --- filters ----------------------------------------------------------------
    for (case, filter, want) in [
        (
            "name",
            FileFilter {
                name_contains: Some("logo".into()),
                ..default.clone()
            },
            1u64,
        ),
        (
            "path",
            FileFilter {
                path_contains: Some("img/".into()),
                ..default.clone()
            },
            2,
        ),
        (
            "buckets",
            FileFilter {
                buckets: vec![mem2.id],
                ..default.clone()
            },
            3,
        ),
        (
            "connections-member",
            FileFilter {
                connections: vec![conn_a.id],
                ..default.clone()
            },
            7,
        ),
        (
            "connections-empty",
            FileFilter {
                connections: vec![conn_b.id],
                ..default.clone()
            },
            0,
        ),
        (
            "extension",
            FileFilter {
                extension: Some("pdf".into()),
                ..default.clone()
            },
            1,
        ),
        (
            "min-size",
            FileFilter {
                min_size: Some(100),
                ..default.clone()
            },
            3,
        ),
        (
            "max-size",
            FileFilter {
                max_size: Some(100),
                ..default.clone()
            },
            5,
        ),
        (
            "modified-after",
            FileFilter {
                modified_after: Some(t(2000)),
                ..default.clone()
            },
            6,
        ),
        (
            "modified-before",
            FileFilter {
                modified_before: Some(t(3000)),
                ..default.clone()
            },
            3,
        ),
    ] {
        assert_eq!(
            count_filtered(&*query, scope, &filter).await,
            want,
            "filter case {case}"
        );
    }
    for (kind, want) in [
        (FileType::Image, 2u64),
        // report.pdf plus the .txt rows (extension classification).
        (FileType::Document, 4),
        (FileType::Archive, 1),
    ] {
        assert_eq!(
            count_filtered(
                &*query,
                scope,
                &FileFilter {
                    file_type: Some(kind),
                    ..default.clone()
                }
            )
            .await,
            want,
            "file type {kind:?}"
        );
    }

    // --- tags --------------------------------------------------------------------
    let red = TagId::new_random();
    let green = TagId::new_random();
    fx.tag_object(ws.id, o1.id, red).await;
    fx.tag_object(ws2.id, o3.id, green).await;

    // The page seam assembles the read model in the adapter: total, source
    // names and workspace tag names travel with the object rows.
    let assembled = query
        .page(
            scope,
            &FileFilter {
                tags: vec![red],
                ..default.clone()
            },
            FileSort::default(),
            &PageRequest::default(),
        )
        .await
        .expect("assembled catalog page");
    assert_eq!(assembled.total, 1);
    assert_eq!(assembled.items.len(), 1);
    assert_eq!(assembled.items[0].tags.len(), 1);

    // `CatalogPage::total` is the count before the cursor, not the number of
    // rows remaining after it. The contract deliberately walks past the tail
    // so an adapter cannot manufacture zero for an empty cursor page.
    let sort = FileSort::default();
    let first = query
        .page(
            scope,
            &default,
            sort,
            &PageRequest {
                limit: 2,
                after: None,
            },
        )
        .await
        .expect("first assembled page");
    assert_eq!(first.total, 7);
    assert_eq!(first.items.len(), 2);
    let first_cursor = rstore_catalog::order::cursor_of(
        &first.items.last().expect("first page item").object,
        sort.field,
    );
    let second = query
        .page(
            scope,
            &default,
            sort,
            &PageRequest {
                limit: 2,
                after: Some(first_cursor),
            },
        )
        .await
        .expect("second assembled page");
    assert_eq!(second.total, 7);
    assert_eq!(second.items.len(), 2);
    let second_cursor = rstore_catalog::order::cursor_of(
        &second.items.last().expect("second page item").object,
        sort.field,
    );
    let third = query
        .page(
            scope,
            &default,
            sort,
            &PageRequest {
                limit: 2,
                after: Some(second_cursor),
            },
        )
        .await
        .expect("third assembled page");
    assert_eq!(third.total, 7);
    let third_cursor = rstore_catalog::order::cursor_of(
        &third.items.last().expect("third page item").object,
        sort.field,
    );
    let tail = query
        .page(
            scope,
            &default,
            sort,
            &PageRequest {
                limit: 2,
                after: Some(third_cursor),
            },
        )
        .await
        .expect("tail assembled page");
    assert_eq!(tail.total, 7);
    assert_eq!(tail.items.len(), 1);
    let tail_cursor = rstore_catalog::order::cursor_of(
        &tail.items.last().expect("tail page item").object,
        sort.field,
    );
    let empty = query
        .page(
            scope,
            &default,
            sort,
            &PageRequest {
                limit: 2,
                after: Some(tail_cursor),
            },
        )
        .await
        .expect("tail after assembled page");
    assert_eq!(empty.total, 7);
    assert!(empty.items.is_empty());

    for (case, filter, want) in [
        (
            "one tag",
            FileFilter {
                tags: vec![red],
                ..default.clone()
            },
            1u64,
        ),
        (
            "and",
            FileFilter {
                tags: vec![red, green],
                ..default.clone()
            },
            0,
        ),
        (
            "or",
            FileFilter {
                tags: vec![red, green],
                tag_mode_or: true,
                ..default.clone()
            },
            1,
        ),
        (
            "cross-workspace",
            FileFilter {
                tags: vec![green],
                ..default.clone()
            },
            0,
        ),
        (
            "untagged",
            FileFilter {
                untagged: true,
                ..default.clone()
            },
            6,
        ),
    ] {
        assert_eq!(
            count_filtered(&*query, scope, &filter).await,
            want,
            "tag case {case}"
        );
    }

    // --- deterministic total order, every field, both directions --------------
    for field in [
        SortField::Name,
        SortField::Size,
        SortField::Modified,
        SortField::Type,
        SortField::Bucket,
        SortField::Connection,
    ] {
        for order in [SortOrder::Asc, SortOrder::Desc] {
            let sort = FileSort { field, order };
            let mut expected: Vec<RemoteObject> = [&o1, &o2, &o3, &o4, &o5, &o8, &o9]
                .into_iter()
                .cloned()
                .collect();
            expected.sort_by_cached_key(|o| order_key(o, field));
            if order == SortOrder::Desc {
                expected.reverse();
            }
            let walked = walk(&*query, scope, &default, sort, 2).await;
            assert_eq!(
                ids(&walked),
                ids(&expected),
                "total order for {field:?} {order:?}"
            );
        }
    }
    // count agrees with the walked cardinality.
    for filter in [
        default.clone(),
        FileFilter {
            name_contains: Some("tie".into()),
            ..default.clone()
        },
        FileFilter {
            state: Some(ObjectState::Confirmed),
            ..default.clone()
        },
    ] {
        let walked = walk(&*query, scope, &filter, FileSort::default(), 3).await;
        assert_eq!(
            walked.len() as u64,
            count_filtered(&*query, scope, &filter).await,
            "count matches search"
        );
    }

    // --- keyset stability under mid-walk upserts ---------------------------------
    let name_desc = FileSort {
        field: SortField::Name,
        order: SortOrder::Desc,
    };
    let name_asc = FileSort {
        field: SortField::Name,
        order: SortOrder::Asc,
    };

    // A row landing in the already-walked region must not resurface, and must
    // not displace the rows still ahead of the cursor.
    let before = count_filtered(&*query, scope, &default).await;
    let alpha = obj(
        conn_a.id,
        mem1.id,
        "aaa/alpha.txt",
        5,
        9000,
        ObjectState::Confirmed,
        None,
    );
    let seen = walk_with_midwalk_upsert(&*query, &*objects, scope, name_asc, alpha.clone()).await;
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        seen.len(),
        "no row repeats across a mutated walk"
    );
    assert!(
        !seen.contains(&alpha.id.as_uuid().to_string()),
        "a row behind the cursor never resurfaces"
    );
    assert_eq!(seen.len() as u64, before, "every pre-walk row exactly once");

    // A row landing ahead of the cursor joins the remaining walk.
    let before = count_filtered(&*query, scope, &default).await;
    let omega = obj(
        conn_a.id,
        mem1.id,
        "zzz/omega.txt",
        6,
        9100,
        ObjectState::Confirmed,
        None,
    );
    let seen = walk_with_midwalk_upsert(&*query, &*objects, scope, name_desc, omega.clone()).await;
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        seen.len(),
        "no row repeats across a mutated walk"
    );
    assert!(
        seen.contains(&omega.id.as_uuid().to_string()),
        "a row ahead of the cursor joins the walk"
    );
    assert_eq!(seen.len() as u64, before + 1);

    // An unparsable cursor is invalid input, never silently misread.
    let err = query
        .search(
            scope,
            &default,
            FileSort::default(),
            &PageRequest {
                limit: 2,
                after: Some("not-a-cursor".into()),
            },
        )
        .await
        .expect_err("garbage cursor rejected");
    assert_eq!(err.code, ErrorCode::InvalidInput);

    // --- write face over the repository contract ------------------------------
    let w1 = obj(
        conn_a.id,
        writes.id,
        "write/a.txt",
        1,
        1000,
        ObjectState::Confirmed,
        None,
    );
    let w2 = obj(
        conn_a.id,
        writes.id,
        "write/b.txt",
        2,
        2000,
        ObjectState::Confirmed,
        None,
    );
    let w3 = obj(
        conn_a.id,
        writes.id,
        "write/c.txt",
        3,
        3000,
        ObjectState::Confirmed,
        None,
    );
    for o in [&w1, &w2, &w3] {
        objects.upsert(o).await.expect("contract upsert");
    }
    assert_eq!(
        objects
            .count_in_bucket(writes.id)
            .await
            .expect("count_in_bucket"),
        3
    );
    let found = objects
        .find_by_locator(
            conn_a.id,
            writes.id,
            &ObjectPath::new("write/a.txt").unwrap(),
        )
        .await
        .expect("contract find_by_locator");
    assert!(found.is_some());

    // The write rows carry confirmed_at = 2 × modified (the `obj` helper), so
    // the 2500 boundary catches only w1 — the comparison is against
    // confirmed_at, never last_modified.
    let flipped = objects
        .mark_stale_older_than(writes.id, t(2500))
        .await
        .expect("mark_stale_older_than");
    assert_eq!(flipped, 1, "only rows confirmed before the boundary flip");
    assert_eq!(
        objects.count_stale(writes.id).await.expect("count_stale"),
        1
    );
    assert_eq!(
        objects.get(w1.id).await.expect("get").expect("row").state,
        ObjectState::Stale
    );
    assert_eq!(
        objects.get(w3.id).await.expect("get").expect("row").state,
        ObjectState::Confirmed
    );

    objects
        .tombstone(
            conn_a.id,
            writes.id,
            &ObjectPath::new("write/a.txt").unwrap(),
            t(9999),
        )
        .await
        .expect("tombstone");
    assert!(
        objects
            .find_by_locator(
                conn_a.id,
                writes.id,
                &ObjectPath::new("write/a.txt").unwrap()
            )
            .await
            .expect("contract find_by_locator")
            .is_none(),
        "tombstoned row is gone"
    );
    assert_eq!(
        objects
            .count_in_bucket(writes.id)
            .await
            .expect("count_in_bucket"),
        2
    );

    let removed = objects
        .delete_all_in_bucket(writes.id)
        .await
        .expect("delete_all_in_bucket");
    assert_eq!(removed, 2);
    assert_eq!(
        objects
            .count_in_bucket(writes.id)
            .await
            .expect("count_in_bucket"),
        0
    );
    assert_eq!(
        objects.count_stale(writes.id).await.expect("count_stale"),
        0
    );

    objects.upsert(&w1).await.expect("reseed rebuild row");
    assert_eq!(
        objects
            .mark_all_stale_in_bucket(writes.id)
            .await
            .expect("mark_all_stale_in_bucket"),
        1
    );
    assert_eq!(
        objects
            .delete_stale_in_bucket(writes.id)
            .await
            .expect("delete_stale_in_bucket"),
        1
    );
    assert_eq!(objects.count_in_bucket(writes.id).await.unwrap(), 0);
}
