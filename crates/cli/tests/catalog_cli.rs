//! CLI acceptance: workspace → ignore → index → files → tag, through the
//! Application API against FakeStorage (CLI-030 / QA-013).

use std::sync::Arc;

use rstore_application::Application;
use rstore_cli::args::{
    Cli, Command, FilesCommand, IgnoreCommand, IndexCommand, RemoteCommand, TagCommand,
    WorkspaceBucketCommand, WorkspaceCommand,
};
use rstore_cli::output::envelope;
use rstore_domain::connection::{AddressingStyle, Region};
use rstore_persistence::Database;
use rstore_platform::resolve_app_dirs;
use rstore_test_support::{FakeStorageFactory, TestClock};
use rstore_transfer::CancellationToken;
use time::OffsetDateTime;

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

fn cli(command: Command) -> Cli {
    Cli {
        config: None,
        log_level: "warn".into(),
        json: true,
        quiet: false,
        command,
    }
}

async fn run(
    cli: &Cli,
    app: &Application,
    dirs: &rstore_platform::AppDirs,
    cancel: &CancellationToken,
) -> rstore_cli::HandlerResult {
    let api = app.api();
    rstore_cli::run_api(cli, &api, dirs, cancel).await
}

async fn catalog_app() -> (Application, Arc<FakeStorageFactory>) {
    let factory = Arc::new(FakeStorageFactory::new());
    factory.storage().seed_bucket("media");
    factory
        .storage()
        .seed("media", "photos/keep/a.png", b"a", None);
    factory
        .storage()
        .seed("media", "docs/report.pdf", b"ccc", None);
    let app = rstore_application_test_support::application_from_db(
        Database::open_in_memory().await.unwrap(),
    )
    .with_storage_factory(factory.clone())
    .with_clock(Arc::new(TestClock::fixed(now())));
    app.api()
        .registry()
        .connections()
        .add(rstore_application::AddConnectionInput {
            name: "nas".into(),
            endpoint: "http://localhost:9000".into(),
            public_base_url: None,
            cdn_base_url: None,
            region: Region::Auto,
            addressing_style: AddressingStyle::Path,
            tls_verify: true,
            credentials: Some(rstore_domain::Secret::new(
                rstore_application::CredentialInput {
                    access_key: "AK".into(),
                    secret_key: "SK".into(),
                },
            )),
        })
        .await
        .unwrap();
    app.api()
        .registry()
        .buckets()
        .add(
            rstore_application::AddBucketInput {
                connection: rstore_application::ConnectionRef::Name("nas".into()),
                name: "media".into(),
                config: Default::default(),
            },
            false,
        )
        .await
        .unwrap();
    (app, factory)
}

async fn run_ok(app: &Application, command: Command) -> rstore_cli::HandlerOutput {
    let dirs = resolve_app_dirs(None);
    run(&cli(command), app, &dirs, &CancellationToken::new())
        .await
        .unwrap()
}

#[tokio::test]
async fn catalog_flow_snapshot() {
    let (app, _factory) = catalog_app().await;
    let ts = now();

    // workspace create + bucket add
    let out = run_ok(
        &app,
        Command::Workspace {
            action: WorkspaceCommand::Create {
                name: "media-wsp".into(),
                description: Some("all media".into()),
            },
        },
    )
    .await;
    insta::assert_json_snapshot!(
        "catalog_workspace_create",
        envelope("workspace.create", &out, ts),
        { ".data.id" => "[id]" }
    );

    run_ok(
        &app,
        Command::Workspace {
            action: WorkspaceCommand::Bucket {
                action: WorkspaceBucketCommand::Add {
                    workspace: "media-wsp".into(),
                    bucket: "nas/media".into(),
                },
            },
        },
    )
    .await;

    // ignore rule
    run_ok(
        &app,
        Command::Bucket {
            action: rstore_cli::args::BucketCommand::Ignore {
                action: IgnoreCommand::Add {
                    bucket: "nas/media".into(),
                    directory: "photos/raw".into(),
                },
            },
        },
    )
    .await;

    // index start (2 of 3 objects upserted — ignored subtree pruned)
    let out = run_ok(
        &app,
        Command::Index {
            action: IndexCommand::Start {
                bucket: "nas/media".into(),
            },
        },
    )
    .await;
    let env = envelope("index.start", &out, ts);
    assert_eq!(env["data"]["objects_upserted"], 2);
    assert_eq!(env["data"]["state"], "succeeded");

    // tag + files
    run_ok(
        &app,
        Command::Tag {
            action: TagCommand::Create {
                workspace: "media-wsp".into(),
                name: "Important".into(),
                color: Some("red".into()),
                description: None,
            },
        },
    )
    .await;
    run_ok(
        &app,
        Command::Tag {
            action: TagCommand::Add {
                workspace: "media-wsp".into(),
                uris: vec!["nas/media/docs/report.pdf".into()],
                tags: vec!["important".into()],
                replace: false,
            },
        },
    )
    .await;
    let mixed = run_ok(
        &app,
        Command::Tag {
            action: TagCommand::Add {
                workspace: "media-wsp".into(),
                uris: vec![
                    "nas/media/docs/report.pdf".into(),
                    "nas/media/docs/missing.pdf".into(),
                ],
                tags: vec!["important".into()],
                replace: false,
            },
        },
    )
    .await;
    assert_eq!(mixed.exit_code, Some(13));

    let out = run_ok(
        &app,
        Command::Files {
            action: FilesCommand::List {
                workspace: "media-wsp".into(),
                path: None,
                buckets: vec![],
                sort: "name:asc".into(),
                limit: 50,
                after: None,
                untagged: false,
            },
        },
    )
    .await;
    let env = envelope("files.list", &out, ts);
    assert_eq!(env["data"]["total"], 2);
    assert_eq!(env["data"]["items"][0]["name"], "a.png");

    // files search narrows by name
    let out = run_ok(
        &app,
        Command::Files {
            action: FilesCommand::Search {
                workspace: "media-wsp".into(),
                pattern: "report".into(),
                sort: "name:asc".into(),
                limit: 50,
            },
        },
    )
    .await;
    let env = envelope("files.search", &out, ts);
    assert_eq!(env["data"]["total"], 1);
    assert_eq!(env["data"]["items"][0]["tags"][0], "Important");

    // files show carries tags and staleness
    let out = run_ok(
        &app,
        Command::Files {
            action: FilesCommand::Show {
                uri: "nas/media/docs/report.pdf".into(),
                workspace: "media-wsp".into(),
            },
        },
    )
    .await;
    let env = envelope("files.show", &out, ts);
    assert_eq!(env["data"]["state"], "confirmed");
    assert_eq!(env["data"]["tags"][0], "Important");
}

#[tokio::test]
async fn remote_cursor_hint_round_trips_opaque_provider_token() {
    let (app, factory) = catalog_app().await;
    factory
        .storage()
        .seed("media", "a/file name with 'quote'", b"a", None);
    factory.storage().seed("media", "a/zz.txt", b"z", None);

    let first = run_ok(
        &app,
        Command::Remote {
            action: RemoteCommand::Ls {
                uri: "nas/media/a/".into(),
                cursor: None,
                limit: 1,
                update_catalog: false,
            },
        },
    )
    .await;
    let hint = first
        .human
        .lines()
        .find(|line| line.contains("-- more: --cursor "))
        .expect("a truncated remote listing prints a cursor hint");
    let cursor = hint
        .split_whitespace()
        .last()
        .expect("cursor hint has a token")
        .to_string();
    assert!(cursor.starts_with("rstore-token-v1:"));
    assert_eq!(
        first.data.as_ref().unwrap()["next_cursor"].as_str(),
        Some(cursor.as_str())
    );

    let second = run_ok(
        &app,
        Command::Remote {
            action: RemoteCommand::Ls {
                uri: "nas/media/a/".into(),
                cursor: Some(cursor),
                limit: 1,
                update_catalog: false,
            },
        },
    )
    .await;
    assert!(second.human.contains("zz.txt"));
}

#[tokio::test]
async fn catalog_index_status_lists_buckets() {
    let (app, _) = catalog_app().await;
    run_ok(
        &app,
        Command::Workspace {
            action: WorkspaceCommand::Create {
                name: "w".into(),
                description: None,
            },
        },
    )
    .await;
    run_ok(
        &app,
        Command::Index {
            action: IndexCommand::Start {
                bucket: "nas/media".into(),
            },
        },
    )
    .await;
    let out = run_ok(
        &app,
        Command::Index {
            action: IndexCommand::Status,
        },
    )
    .await;
    let env = envelope("index.status", &out, now());
    assert_eq!(env["data"][0]["object_count"], 2);
    assert_eq!(env["data"][0]["last_run"]["state"], "succeeded");
}

#[tokio::test]
async fn catalog_destructive_commands_require_yes() {
    let (app, _) = catalog_app().await;
    run_ok(
        &app,
        Command::Workspace {
            action: WorkspaceCommand::Create {
                name: "w".into(),
                description: None,
            },
        },
    )
    .await;
    let dirs = resolve_app_dirs(None);
    let res = run(
        &cli(Command::Workspace {
            action: WorkspaceCommand::Remove {
                reference: "w".into(),
                yes: false,
            },
        }),
        &app,
        &dirs,
        &CancellationToken::new(),
    )
    .await;
    // SEC-006: non-interactive destructive commands need --yes.
    let err = res.unwrap_err();
    assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
}
