//! CLI snapshot + contract tests (CLI-030 / QA-013).

use std::sync::Arc;

use clap::CommandFactory;
use rstore_application::{AddConnectionInput, Application, ConnectionRef};
use rstore_cli::args::{Cli, Command, ConnectionAddArgs, ConnectionCommand, UrlArgs};
use rstore_cli::output::{envelope, exit_code_for};
use rstore_cli::{command_domains, HandlerResult};
use rstore_domain::connection::{AddressingStyle, Region};
use rstore_domain::ErrorCode;
use rstore_persistence::Database;
use rstore_platform::resolve_app_dirs;
use rstore_test_support::TestClock;
use time::OffsetDateTime;

/// Fixed timestamp so envelope output is reproducible (CORE-009).
fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

async fn run(
    cli: &Cli,
    app: &Application,
    dirs: &rstore_platform::AppDirs,
    cancel: &rstore_transfer::CancellationToken,
) -> HandlerResult {
    let api = app.api();
    rstore_cli::run_api(cli, &api, dirs, cancel).await
}

#[test]
fn help_snapshot() {
    let mut cmd = Cli::command();
    let help = cmd.render_help().to_string();
    insta::assert_snapshot!("cli_help", help);
}

#[test]
fn command_tree_lists_all_domains() {
    // CLI-001: the top-level command tree is complete. Domain names are read
    // from clap itself, not repeated in a test-only constant.
    let help = Cli::command().render_help().to_string();
    for domain in command_domains() {
        assert!(help.contains(&domain), "help missing command `{domain}`");
    }
}

#[tokio::test]
async fn url_without_uris_is_invalid_input() {
    let db = Database::open_in_memory().await.unwrap();
    let app = rstore_application_test_support::application_from_db(db);
    let dirs = resolve_app_dirs(None);
    let cli = Cli {
        config: None,
        log_level: "warn".into(),
        json: false,
        quiet: false,
        command: Command::Url(UrlArgs {
            uris: vec![],
            format: "path".into(),
            signed: false,
            ttl: None,
        }),
    };
    let res: HandlerResult = run(
        &cli,
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await;
    let err = res.unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert_eq!(exit_code_for(&err), 2);
}

fn add_cli(name: &str, addressing: &str) -> Cli {
    Cli {
        config: None,
        log_level: "warn".into(),
        json: true,
        quiet: false,
        command: Command::Connection {
            action: ConnectionCommand::Add(ConnectionAddArgs {
                name: name.into(),
                endpoint: "https://example.r2.cloudflarestorage.com".into(),
                region: "auto".into(),
                addressing_style: addressing.into(),
                tls_verify: true,
                secret_ref: Some("conn_prod_r2".into()),
                access_key: None,
                secret_key: None,
            }),
        },
    }
}

#[tokio::test]
async fn connection_add_produces_stable_envelope() {
    let db = Database::open_in_memory().await.unwrap();
    let app = rstore_application_test_support::application_from_db(db);
    let dirs = resolve_app_dirs(None);

    let out = run(
        &add_cli("prod-r2", "path"),
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await
    .unwrap();
    let env = envelope("connection.add", &out, now());
    let obj = env.as_object().unwrap();

    // CLI-004/005: envelope contract.
    assert!(!obj.contains_key("schema_version"));
    assert_eq!(obj["command"], "connection.add");
    assert_eq!(obj["ok"], true);
    assert_eq!(obj["data"]["name"], "prod-r2");
    assert_eq!(
        obj["data"]["endpoint"],
        "https://example.r2.cloudflarestorage.com"
    );
    assert!(obj["data"]["id"].as_str().unwrap().starts_with("conn_"));
    // CORE-009: timestamps are ISO 8601 strings, not component arrays.
    assert!(obj["data"]["created_at"].is_string());
    // The injected clock controls the envelope timestamp.
    assert_eq!(obj["meta"]["generated_at"], "2023-11-14T22:13:20Z");
    // The stored secret never leaks into JSON (SEC-001).
    assert_eq!(obj["data"]["secret_ref"], "conn_prod_r2"); // ref only, never the value
                                                           // No credentials were supplied: the envelope carries a warning (CLI-004).
    assert_eq!(obj["warnings"].as_array().unwrap().len(), 1);
    assert!(obj["warnings"][0]
        .as_str()
        .unwrap()
        .contains("no credentials provided"));
}

/// The whole envelope is deterministic, so it can be captured as a snapshot.
/// Only the random connection id is redacted.
#[tokio::test]
async fn connection_add_envelope_snapshot() {
    let ts = now();
    let db = Database::open_in_memory().await.unwrap();
    let app = rstore_application_test_support::application_from_db(db)
        .with_clock(Arc::new(TestClock::fixed(ts)));
    let dirs = resolve_app_dirs(None);

    let out = run(
        &add_cli("prod-r2", "path"),
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await
    .unwrap();
    let env = envelope("connection.add", &out, ts);
    insta::assert_json_snapshot!("envelope_connection_add", env, {
        ".data.id" => "[id]"
    });
}

#[tokio::test]
async fn application_contract_via_service_trait() {
    // QA-002: exercise the ConnectionService trait directly (no CLI).
    let db = Database::open_in_memory().await.unwrap();
    let app = rstore_application_test_support::application_from_db(db);
    let svc = app.api().registry().connections();

    let added = svc
        .add(AddConnectionInput {
            name: "nas".into(),
            endpoint: "https://nas.local:9000".into(),
            region: Region::Specific("us-east-1".into()),
            addressing_style: AddressingStyle::Path,
            tls_verify: true,
            secret_ref: None,
            credentials: None,
        })
        .await
        .unwrap();

    let got = svc.resolve(&ConnectionRef::Id(added.id)).await.unwrap();
    assert_eq!(got.endpoint, "https://nas.local:9000");

    let err = svc
        .resolve(&ConnectionRef::Name("missing".into()))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
}

/// Short-form upload targets resolve through the registered bucket before
/// the transfer runs: `media/docs/` means bucket `media`, prefix `docs/`.
#[tokio::test]
async fn upload_short_target_resolves_the_registered_bucket() {
    let db = Database::open_in_memory().await.unwrap();
    let app = rstore_application_test_support::application_from_db(db);
    app.api()
        .registry()
        .connections()
        .add(AddConnectionInput {
            name: "nas".into(),
            endpoint: "http://localhost:9000".into(),
            region: Region::Auto,
            addressing_style: AddressingStyle::Path,
            tls_verify: true,
            secret_ref: None,
            credentials: None,
        })
        .await
        .unwrap();
    app.api()
        .registry()
        .buckets()
        .add(
            rstore_application::AddBucketInput {
                connection: ConnectionRef::Name("nas".into()),
                name: "media".into(),
                config: Default::default(),
            },
            false,
        )
        .await
        .unwrap();
    let dirs = resolve_app_dirs(None);
    let source = std::env::temp_dir().join(format!(
        "rstore-cli-upload-short-target-{}.txt",
        std::process::id()
    ));
    std::fs::write(&source, b"fixture").unwrap();
    let cli = Cli {
        config: None,
        log_level: "warn".into(),
        json: false,
        quiet: false,
        command: Command::Upload(rstore_cli::args::TransferArgs {
            sources: vec![source.display().to_string()],
            target: "media/docs/".into(),
            recursive: false,
            conflict: Some("skip".into()),
            dry_run: false,
            yes: true,
            no_progress: true,
        }),
    };
    let res: HandlerResult = run(
        &cli,
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await;
    let _ = std::fs::remove_file(source);
    // Local planning passed, then the short target resolved into a registered
    // bucket and reached adapter construction. A parse or bucket-resolution
    // failure would have returned a different message.
    let err = res.unwrap_err();
    assert!(err.to_string().contains("no storage adapter"), "{err}");
}

/// The short form works wherever a remote reference is accepted: `url`
/// resolves `media/img/logo.png` through the registered bucket without any
/// storage adapter (public URLs need none).
#[tokio::test]
async fn url_accepts_short_references() {
    let db = Database::open_in_memory().await.unwrap();
    let app = rstore_application_test_support::application_from_db(db);
    app.api()
        .registry()
        .connections()
        .add(AddConnectionInput {
            name: "nas".into(),
            endpoint: "http://localhost:9000".into(),
            region: Region::Auto,
            addressing_style: AddressingStyle::Path,
            tls_verify: true,
            secret_ref: None,
            credentials: None,
        })
        .await
        .unwrap();
    app.api()
        .registry()
        .buckets()
        .add(
            rstore_application::AddBucketInput {
                connection: ConnectionRef::Name("nas".into()),
                name: "media".into(),
                config: Default::default(),
            },
            false,
        )
        .await
        .unwrap();
    let dirs = resolve_app_dirs(None);
    let cli = Cli {
        config: None,
        log_level: "warn".into(),
        json: false,
        quiet: false,
        command: Command::Url(UrlArgs {
            uris: vec!["media/img/logo.png".into()],
            format: "s3".into(),
            signed: false,
            ttl: None,
        }),
    };
    let out = run(
        &cli,
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(
        out.human.contains("s3://media/img/logo.png"),
        "{}",
        out.human
    );
}

#[tokio::test]
async fn url_mixed_batch_failure_exits_thirteen() {
    let app = rstore_application_test_support::application_from_db(
        Database::open_in_memory().await.unwrap(),
    );
    app.api()
        .registry()
        .connections()
        .add(AddConnectionInput {
            name: "nas".into(),
            endpoint: "http://localhost:9000".into(),
            region: Region::Auto,
            addressing_style: AddressingStyle::Path,
            tls_verify: true,
            secret_ref: None,
            credentials: None,
        })
        .await
        .unwrap();
    for bucket in ["public", "private"] {
        app.api()
            .registry()
            .buckets()
            .add(
                rstore_application::AddBucketInput {
                    connection: ConnectionRef::Name("nas".into()),
                    name: bucket.into(),
                    config: Default::default(),
                },
                false,
            )
            .await
            .unwrap();
    }
    app.api()
        .access()
        .links()
        .set_profile(
            &rstore_application::BucketRef::Name("private".into()),
            rstore_application::LinkProfileInput {
                allow_public_url: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let out = run(
        &Cli {
            config: None,
            log_level: "warn".into(),
            json: false,
            quiet: false,
            command: Command::Url(UrlArgs {
                uris: vec!["public/a.txt".into(), "private/b.txt".into()],
                format: "url".into(),
                signed: false,
                ttl: None,
            }),
        },
        &app,
        &resolve_app_dirs(None),
        &rstore_transfer::CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(out.exit_code, Some(13));
}

/// `rm` with a short reference reports the bucket from the registry — the
/// expansion ran, and a missing bucket is a not-found, not a parse error.
#[tokio::test]
async fn rm_accepts_short_references() {
    let app = rstore_application_test_support::application_from_db(
        Database::open_in_memory().await.unwrap(),
    );
    let dirs = resolve_app_dirs(None);
    let cli = Cli {
        config: None,
        log_level: "warn".into(),
        json: false,
        quiet: false,
        command: Command::Rm {
            uris: vec!["ghost/tmp.bin".into()],
            recursive: false,
            dry_run: true,
            yes: true,
        },
    };
    let res: HandlerResult = run(
        &cli,
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await;
    let err = res.unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert!(err.to_string().contains("bucket `ghost`"), "{err}");
}
