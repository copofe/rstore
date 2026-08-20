//! Catalog service integration tests: workspace → ignore → index → files →
//! tag, all through the Application API against FakeStorage.

use std::sync::Arc;

use rstore_catalog::{FileFilter, FileSort, PageRequest, SortField, SortOrder};
use rstore_domain::policy::ConflictPolicy;
use rstore_test_support::FakeStorageFactory;
use rstore_transfer::CancellationToken;

use rstore_application::test_util::{app_with, app_with_clock, app_with_secrets_only};
use rstore_application::{
    Application, CreateTagInput, CreateWorkspaceInput, UpdateWorkspaceInput, WorkspaceRef,
};

use rstore_application::tag::TagRef;

/// A fresh app: the `conn_nas` secret is pre-stored but nothing is
/// registered, so an imported bundle re-creates the registry from scratch.
async fn app() -> Application {
    app_with_secrets_only(&["nas"]).await
}

async fn seeded_app() -> Application {
    let (app, factory) = app_with(&["nas"], &[("nas", "media")]).await;
    seed_media(factory.as_ref());
    app
}

fn seed_media(factory: &FakeStorageFactory) {
    factory.storage().seed_bucket("media");
    factory
        .storage()
        .seed("media", "photos/keep/a.png", b"a", None);
    factory
        .storage()
        .seed("media", "photos/raw/b.nef", b"bb", None);
    factory
        .storage()
        .seed("media", "docs/report.pdf", b"ccc", None);
}

fn wsp_ref() -> WorkspaceRef {
    WorkspaceRef::Name("media-wsp".into())
}

#[tokio::test]
async fn workspace_crud_and_membership() {
    let app = seeded_app().await;
    let ws = app.api().catalog().workspaces();
    let created = ws
        .create(CreateWorkspaceInput {
            name: "media-wsp".into(),
            description: Some("everything".into()),
        })
        .await
        .unwrap();
    assert!(
        ws.create(CreateWorkspaceInput {
            name: "MEDIA-WSP".into(),
            description: Some("remove me".into()),
        })
        .await
        .is_err(),
        "name uniqueness is case-insensitive"
    );

    let bucket_id = ws
        .add_bucket(
            &wsp_ref(),
            &rstore_application::BucketRef::Name("media".into()),
        )
        .await
        .unwrap();
    assert!(
        ws.add_bucket(&wsp_ref(), &rstore_application::BucketRef::Id(bucket_id))
            .await
            .is_ok(),
        "duplicate membership is a no-op, not an error"
    );

    let detail = ws.show(&wsp_ref()).await.unwrap();
    assert_eq!(detail.workspace.id, created.id);
    assert_eq!(detail.buckets.len(), 1);

    let edited = ws
        .edit(
            &wsp_ref(),
            UpdateWorkspaceInput {
                name: Some("renamed".into()),
                description: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(edited.name, "renamed");

    let cleared = ws
        .edit(
            &WorkspaceRef::Name("renamed".into()),
            UpdateWorkspaceInput {
                name: None,
                description: Some(None),
            },
        )
        .await
        .unwrap();
    assert_eq!(cleared.description, None);

    ws.remove_bucket(
        &WorkspaceRef::Name("renamed".into()),
        &rstore_application::BucketRef::Name("media".into()),
    )
    .await
    .unwrap();
    assert_eq!(
        ws.show(&WorkspaceRef::Name("renamed".into()))
            .await
            .unwrap()
            .buckets
            .len(),
        0
    );
    ws.remove(&WorkspaceRef::Name("renamed".into()))
        .await
        .unwrap();
    assert!(ws.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn ignore_rules_normalise_conflict_and_list() {
    let app = seeded_app().await;
    let ws = app.api().catalog().workspaces();
    ws.create(CreateWorkspaceInput {
        name: "w".into(),
        description: None,
    })
    .await
    .unwrap();
    let bucket = rstore_application::BucketRef::Name("media".into());

    let rule = ws.ignore_add(&bucket, "/photos/raw/").await.unwrap();
    assert_eq!(rule.directory, "photos/raw/");
    assert!(
        ws.ignore_add(&bucket, "photos/raw/sub").await.is_err(),
        "nested rule conflicts"
    );
    assert!(
        ws.ignore_add(&bucket, "*.tmp").await.is_err(),
        "globs rejected"
    );

    let list = ws.ignore_list(&bucket).await.unwrap();
    assert_eq!(list.len(), 1);
    ws.ignore_remove(&bucket, "photos/raw").await.unwrap();
    assert!(ws.ignore_list(&bucket).await.unwrap().is_empty());
}

#[tokio::test]
async fn index_start_respects_ignore_and_supports_refresh_rebuild() {
    let app = seeded_app().await;
    let ws = app.api().catalog().workspaces();
    ws.create(CreateWorkspaceInput {
        name: "w".into(),
        description: None,
    })
    .await
    .unwrap();
    let bucket = rstore_application::BucketRef::Name("media".into());
    ws.ignore_add(&bucket, "photos/raw/").await.unwrap();

    let report = app
        .api()
        .catalog()
        .indexer()
        .start(&bucket, &CancellationToken::new())
        .await
        .unwrap();
    // 3 objects seen, but the ignored subtree's one object is not upserted.
    assert_eq!(report.objects_seen, 3);
    assert_eq!(report.objects_upserted, 2);
    assert_eq!(report.state, rstore_domain::IndexRunState::Succeeded);
    assert_eq!(report.catalog_revision, 1);

    let status = app.api().catalog().indexer().status().await.unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].object_count, 2);
    assert_eq!(status[0].ignore_rules, 1);

    // refresh: nothing disappeared remotely, nothing goes stale.
    let refresh = app
        .api()
        .catalog()
        .indexer()
        .refresh(&bucket, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(refresh.objects_stale, 0);

    // rebuild: keeps rows confirmed by the scan and only prunes stale rows
    // left unseen (still respecting the ignore).
    let rebuild = app
        .api()
        .catalog()
        .indexer()
        .rebuild(&bucket, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(rebuild.objects_pruned, 0);
    assert_eq!(rebuild.objects_upserted, 2);
}

#[tokio::test]
async fn files_list_search_show_with_tags() {
    let app = seeded_app().await;
    let ws = app.api().catalog().workspaces();
    ws.create(CreateWorkspaceInput {
        name: "w".into(),
        description: None,
    })
    .await
    .unwrap();
    let bucket = rstore_application::BucketRef::Name("media".into());
    ws.add_bucket(&WorkspaceRef::Name("w".into()), &bucket)
        .await
        .unwrap();
    app.api()
        .catalog()
        .indexer()
        .start(&bucket, &CancellationToken::new())
        .await
        .unwrap();

    // Tag one object (TAG-009) through its URI.
    app.api()
        .catalog()
        .tags()
        .create(CreateTagInput {
            workspace: WorkspaceRef::Name("w".into()),
            name: "Urgent".into(),
            color: None,
            description: None,
        })
        .await
        .unwrap();
    app.api()
        .catalog()
        .tags()
        .attach(
            &WorkspaceRef::Name("w".into()),
            "nas/media/docs/report.pdf",
            &["urgent".into()],
            false,
        )
        .await
        .unwrap();

    // Full mixed list (MIX-001): every object across the workspace buckets.
    let files = app.api().catalog().files();
    let page = files
        .list(
            &WorkspaceRef::Name("w".into()),
            &FileFilter::default(),
            FileSort::default(),
            &PageRequest {
                limit: 50,
                after: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.total, 3);
    assert!(page.next_cursor.is_none());

    // Name search (MIX-011).
    let filter = FileFilter {
        name_contains: Some("report".into()),
        ..Default::default()
    };
    let page = files
        .list(
            &WorkspaceRef::Name("w".into()),
            &filter,
            FileSort::default(),
            &PageRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].name, "report.pdf");
    assert_eq!(page.items[0].directory, "docs");
    assert_eq!(page.items[0].bucket, "media");
    assert_eq!(page.items[0].connection, "nas");
    assert_eq!(page.items[0].tags, vec!["Urgent".to_string()]);

    // Untagged view (MIX-021).
    let untagged = FileFilter {
        untagged: true,
        ..Default::default()
    };
    let page = files
        .list(
            &WorkspaceRef::Name("w".into()),
            &untagged,
            FileSort::default(),
            &PageRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.total, 2);

    // files show (IDX-015/016).
    let entry = files
        .show(&WorkspaceRef::Name("w".into()), "nas/media/docs/report.pdf")
        .await
        .unwrap();
    assert_eq!(entry.state, rstore_domain::ObjectState::Confirmed);
    assert_eq!(entry.tags, vec!["Urgent".to_string()]);

    // Keyset pagination (IDX-024): limit 2 leaves a cursor, no duplicates.
    let mut seen = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = files
            .list(
                &WorkspaceRef::Name("w".into()),
                &FileFilter::default(),
                FileSort {
                    field: SortField::Name,
                    order: SortOrder::Asc,
                },
                &PageRequest {
                    limit: 2,
                    after: after.clone(),
                },
            )
            .await
            .unwrap();
        seen.extend(page.items.iter().map(|i| i.uri.clone()));
        match page.next_cursor {
            Some(c) => after = Some(c),
            None => break,
        }
    }
    assert_eq!(
        seen.len(),
        3,
        "pagination neither drops nor duplicates rows"
    );
}

#[tokio::test]
async fn tag_lifecycle_and_isolation() {
    let app = seeded_app().await;
    let ws = app.api().catalog().workspaces();
    ws.create(CreateWorkspaceInput {
        name: "w".into(),
        description: None,
    })
    .await
    .unwrap();
    let bucket = rstore_application::BucketRef::Name("media".into());
    ws.add_bucket(&WorkspaceRef::Name("w".into()), &bucket)
        .await
        .unwrap();
    app.api()
        .catalog()
        .indexer()
        .start(&bucket, &CancellationToken::new())
        .await
        .unwrap();

    let tags = app.api().catalog().tags();
    let tag = tags
        .create(CreateTagInput {
            workspace: WorkspaceRef::Name("w".into()),
            name: "Keep".into(),
            color: Some("blue".into()),
            description: Some("remove me".into()),
        })
        .await
        .unwrap();
    assert!(
        tags.create(CreateTagInput {
            workspace: WorkspaceRef::Name("w".into()),
            name: "keep".into(),
            color: None,
            description: None,
        })
        .await
        .is_err(),
        "case-insensitive uniqueness (TAG-002)"
    );

    let uri = "nas/media/photos/keep/a.png";
    tags.attach(
        &WorkspaceRef::Name("w".into()),
        uri,
        &["keep".into()],
        false,
    )
    .await
    .unwrap();
    let outcomes = tags
        .attach_many(
            &WorkspaceRef::Name("w".into()),
            &[uri.into(), "nas/media/missing.txt".into()],
            &["keep".into()],
            false,
        )
        .await
        .unwrap();
    assert!(outcomes[0].succeeded());
    assert!(!outcomes[1].succeeded());
    let list = tags.list(&WorkspaceRef::Name("w".into())).await.unwrap();
    assert_eq!(list[0].object_count, 1);

    let edited = tags
        .edit(
            &WorkspaceRef::Name("w".into()),
            &TagRef::Name("keep".into()),
            rstore_application::UpdateTagInput {
                name: Some("Archive".into()),
                color: None,
                description: Some(String::new()),
            },
        )
        .await
        .unwrap();
    assert_eq!(edited.name, "Archive");
    assert_eq!(edited.description, None);

    // Attach by the renamed tag still resolves; detach-all works.
    tags.detach(&WorkspaceRef::Name("w".into()), uri, &[])
        .await
        .unwrap();
    assert_eq!(
        tags.tags_for(&WorkspaceRef::Name("w".into()), uri)
            .await
            .unwrap()
            .len(),
        0
    );

    tags.remove(&WorkspaceRef::Name("w".into()), &TagRef::Id(tag.id))
        .await
        .unwrap();
    assert!(tags
        .list(&WorkspaceRef::Name("w".into()))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn files_on_unknown_workspace_is_not_found() {
    let app = seeded_app().await;
    let _ = ConflictPolicy::Overwrite; // import exercised
    let result = app
        .api()
        .catalog()
        .files()
        .list(
            &WorkspaceRef::Name("none".into()),
            &FileFilter::default(),
            FileSort::default(),
            &PageRequest::default(),
        )
        .await;
    assert!(result.is_err(), "unknown workspace surfaces not_found");
}

#[tokio::test]
async fn tag_attach_rejects_object_outside_workspace() {
    // WSP-013: the object's bucket must belong to the workspace. `other-wsp`
    // exists but never adds the bucket, so attaching to its catalog object
    // must fail as NotFound — the object's existence is not leaked.
    let app = seeded_app().await;
    let ws = app.api().catalog().workspaces();
    ws.create(CreateWorkspaceInput {
        name: "w".into(),
        description: None,
    })
    .await
    .unwrap();
    ws.create(CreateWorkspaceInput {
        name: "other-wsp".into(),
        description: None,
    })
    .await
    .unwrap();
    let bucket = rstore_application::BucketRef::Name("media".into());
    ws.add_bucket(&WorkspaceRef::Name("w".into()), &bucket)
        .await
        .unwrap();
    app.api()
        .catalog()
        .indexer()
        .start(&bucket, &CancellationToken::new())
        .await
        .unwrap();

    let tags = app.api().catalog().tags();
    tags.create(CreateTagInput {
        workspace: WorkspaceRef::Name("other-wsp".into()),
        name: "keep".into(),
        color: None,
        description: None,
    })
    .await
    .unwrap();
    let uri = "nas/media/photos/keep/a.png";
    let err = tags
        .attach(
            &WorkspaceRef::Name("other-wsp".into()),
            uri,
            &["keep".into()],
            false,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, rstore_domain::ErrorCode::NotFound);
    let err = tags
        .tags_for(&WorkspaceRef::Name("other-wsp".into()), uri)
        .await
        .unwrap_err();
    assert_eq!(err.code, rstore_domain::ErrorCode::NotFound);

    // The same object stays taggable from the member workspace.
    tags.create(CreateTagInput {
        workspace: WorkspaceRef::Name("w".into()),
        name: "keep".into(),
        color: None,
        description: None,
    })
    .await
    .unwrap();
    tags.attach(
        &WorkspaceRef::Name("w".into()),
        uri,
        &["keep".into()],
        false,
    )
    .await
    .unwrap();
}

// --- catalog gc / index stats (IDX-020/021) ----------------------------------

#[tokio::test]
async fn catalog_gc_purges_only_expired_tombstones() {
    use rstore_test_support::TestClock;

    let t0 = time::OffsetDateTime::now_utc();
    let clock = Arc::new(TestClock::fixed(t0));
    let (app, factory) = app_with_clock(&["nas"], &[("nas", "media")], clock.clone()).await;
    factory
        .storage()
        .seed("media", "docs/report.pdf", b"ccc", None);
    factory
        .storage()
        .seed("media", "docs/fresh.pdf", b"dd", None);

    // One tombstone now, one two hours later.
    app.api()
        .transfer()
        .remote()
        .remove(
            &["nas/media/docs/report.pdf".into()],
            false,
            false,
            &rstore_transfer::CancellationToken::new(),
        )
        .await
        .unwrap();
    clock.advance_secs(2 * 3600);
    app.api()
        .transfer()
        .remote()
        .remove(
            &["nas/media/docs/fresh.pdf".into()],
            false,
            false,
            &rstore_transfer::CancellationToken::new(),
        )
        .await
        .unwrap();

    let indexer = app.api().catalog().indexer();
    // Dry run counts without deleting (1h retention: only the old one).
    let report = indexer.gc(Some(3600), true).await.unwrap();
    assert!(report.dry_run);
    assert_eq!(report.expired, 1);
    assert_eq!(report.retained, 1);

    let stats = indexer.stats().await.unwrap();
    assert_eq!(stats.tombstone_rows, 2);

    // The real run purges exactly the expired one.
    let report = indexer.gc(Some(3600), false).await.unwrap();
    assert_eq!(report.expired, 1);
    let stats = indexer.stats().await.unwrap();
    assert_eq!(stats.tombstone_rows, 1);
    assert!(stats.db_bytes > 0);
}

// --- tag merge (TAG-008) ------------------------------------------------------

#[tokio::test]
async fn tag_merge_moves_associations_and_removes_source() {
    let app = seeded_app().await;
    let wsman = app.api().catalog().workspaces();
    wsman
        .create(CreateWorkspaceInput {
            name: "w".into(),
            description: None,
        })
        .await
        .unwrap();
    let bucket = rstore_application::BucketRef::Name("media".into());
    wsman
        .add_bucket(&WorkspaceRef::Name("w".into()), &bucket)
        .await
        .unwrap();
    app.api()
        .catalog()
        .indexer()
        .start(&bucket, &CancellationToken::new())
        .await
        .unwrap();
    let tags = app.api().catalog().tags();
    let ws = WorkspaceRef::Name("w".into());
    for name in ["old", "new"] {
        tags.create(CreateTagInput {
            workspace: ws.clone(),
            name: name.into(),
            color: None,
            description: None,
        })
        .await
        .unwrap();
    }
    let a = "nas/media/photos/keep/a.png";
    let b = "nas/media/docs/report.pdf";
    tags.attach(&ws, a, &["old".into()], false).await.unwrap();
    tags.attach(&ws, b, &["new".into(), "old".into()], false)
        .await
        .unwrap();

    let report = tags
        .merge(
            &ws,
            &TagRef::Name("old".into()),
            &TagRef::Name("new".into()),
        )
        .await
        .unwrap();
    // `b` already carried `new`, so only `a`'s association counts as moved.
    assert_eq!(report.moved, 1);

    // The source is gone; both objects now carry only `new`.
    let list = tags.list(&ws).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].tag.name, "new");
    assert_eq!(list[0].object_count, 2);
    assert_eq!(
        tags.tags_for(&ws, a).await.unwrap(),
        vec!["new".to_string()]
    );

    // Merging a tag into itself is rejected.
    assert!(tags
        .merge(
            &ws,
            &TagRef::Name("new".into()),
            &TagRef::Name("new".into())
        )
        .await
        .is_err());
}

// --- config import/export (DATA-015/016) -------------------------------------

#[tokio::test]
async fn config_bundle_round_trips_without_secrets() {
    let seeded = seeded_app().await;
    let bundle = seeded.api().config().transfer().export().await.unwrap();
    assert!(bundle.connections.iter().any(|c| c.name == "nas"));
    assert!(bundle.buckets.iter().any(|b| b.name == "media"));
    // The serialised form never carries credential values (SEC-001).
    let json = serde_json::to_value(&bundle).unwrap().to_string();
    assert!(!json.contains("AK"));
    assert!(!json.contains("SK"));

    // A fresh dry-run must include buckets under connections that are only
    // planned, not yet persisted. This is the identity bug the import plan
    // prevents.
    let fresh = app().await;
    let preview = fresh
        .api()
        .config()
        .transfer()
        .import(&bundle, true)
        .await
        .unwrap();
    assert_eq!(preview.connections_added, 1);
    assert_eq!(preview.buckets_added, 1);
    assert!(fresh
        .api()
        .registry()
        .connections()
        .list()
        .await
        .unwrap()
        .is_empty());

    // Import into the same fresh app re-creates the registry.
    let report = fresh
        .api()
        .config()
        .transfer()
        .import(&bundle, false)
        .await
        .unwrap();
    assert_eq!(report.connections_added, 1);
    assert_eq!(report.buckets_added, 1);

    // Dry run on the now-populated app skips everything and adds nothing.
    let dry = fresh
        .api()
        .config()
        .transfer()
        .import(&bundle, true)
        .await
        .unwrap();
    assert!(dry.dry_run);
    assert_eq!(dry.connections_added, 0);
    assert_eq!(dry.connections_skipped, 1);
    assert_eq!(dry.buckets_skipped, 1);

    // A real re-import also skips: conflicts never overwrite (DATA-016).
    let again = fresh
        .api()
        .config()
        .transfer()
        .import(&bundle, false)
        .await
        .unwrap();
    assert_eq!(again.connections_skipped, 1);
    assert_eq!(again.buckets_skipped, 1);
}
