//! Snapshot tests for the batch renderers: upload/download, batch download
//! and cp/mv all render
//! through `render_batch`, and these pin their human grammar — per-item
//! lines, summary, dry-run banner — plus the exit policy. Random job ids
//! and temp-dir paths are filtered so the snapshots stay stable.

use std::sync::Arc;

use rstore_application::{AddBucketInput, AddConnectionInput, Application, ConnectionRef};
use rstore_cli::args::{Cli, Command, CopyArgs, TransferArgs};
use rstore_domain::connection::{AddressingStyle, Region};
use rstore_persistence::Database;
use rstore_platform::resolve_app_dirs;
use rstore_test_support::FakeStorageFactory;

fn transfer_cli(command: Command) -> Cli {
    Cli {
        config: None,
        log_level: "warn".into(),
        json: false,
        quiet: false,
        command,
    }
}

async fn batch_app() -> (Application, Arc<FakeStorageFactory>) {
    let db = Database::open_in_memory().await.unwrap();
    let secrets = Arc::new(rstore_secrets::InMemorySecretStore::new());
    rstore_secrets::put_credentials(secrets.as_ref(), "conn_nas", "AK", "SK")
        .await
        .unwrap();
    let factory = Arc::new(FakeStorageFactory::new());
    let app = rstore_application_test_support::application_from_db(db)
        .with_secrets(secrets)
        .with_storage_factory(factory.clone());
    app.api()
        .registry()
        .connections()
        .add(AddConnectionInput {
            name: "nas".into(),
            endpoint: "http://localhost:9000".into(),
            region: Region::Auto,
            addressing_style: AddressingStyle::Path,
            tls_verify: true,
            secret_ref: Some("conn_nas".into()),
            credentials: None,
        })
        .await
        .unwrap();
    app.api()
        .registry()
        .buckets()
        .add(
            AddBucketInput {
                connection: ConnectionRef::Name("nas".into()),
                name: "media".into(),
                config: Default::default(),
            },
            false,
        )
        .await
        .unwrap();
    let storage = factory.storage();
    storage.seed("media", "b.txt", b"existing", None); // upload collision
    storage.seed("media", "src.txt", b"src", None); // cp source
    storage.seed("media", "dup.txt", b"dup", None); // cp source (collides on target)
    storage.seed("media", "out/dup.txt", b"taken", None); // cp collision target
    storage.seed("media", "a.txt", b"a", None); // download fresh
    storage.seed("media", "local.txt", b"local", None); // download collision
    (app, factory)
}

async fn run(
    cli: &Cli,
    app: &Application,
    dirs: &rstore_platform::AppDirs,
    cancel: &rstore_transfer::CancellationToken,
) -> rstore_cli::HandlerResult {
    let api = app.api();
    rstore_cli::run_api(cli, &api, dirs, cancel).await
}

/// Redact random ids (`job_<uuid>`, `conn_<uuid>`) and the temp-dir prefix
/// so snapshots stay stable across runs.
fn snapshot_human(name: &str, tmp: &str, human: String) {
    let mut redacted = if tmp.is_empty() {
        human
    } else {
        human.replace(tmp, "<tmp>")
    };
    // Every `<prefix>_<hex-and-dash>` id becomes `<prefix>_<id>` (the
    // batch-download summary's `job(s)` never matches — no underscore).
    for prefix in ["job_", "conn_"] {
        let mut out = String::new();
        let mut rest = redacted.as_str();
        while let Some(pos) = rest.find(prefix) {
            out.push_str(&rest[..pos]);
            let tail = &rest[pos + prefix.len()..];
            let end = tail
                .find(|c: char| !c.is_ascii_hexdigit() && c != '-')
                .unwrap_or(tail.len());
            out.push_str(&format!("{prefix}<id>"));
            rest = &tail[end..];
        }
        out.push_str(rest);
        redacted = out;
    }
    insta::assert_snapshot!(name, redacted);
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rstore-batchsnap-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn upload_mixed_batch_renders_the_shared_grammar() {
    let (app, _) = batch_app().await;
    let dirs = resolve_app_dirs(None);
    let dir = temp_dir("up");
    std::fs::write(dir.join("new.txt"), b"new").unwrap();
    std::fs::write(dir.join("b.txt"), b"changed").unwrap();

    let cli = transfer_cli(Command::Upload(TransferArgs {
        sources: vec![
            dir.join("new.txt").to_string_lossy().into_owned(),
            dir.join("b.txt").to_string_lossy().into_owned(),
        ],
        target: "nas/media".into(),
        recursive: false,
        conflict: Some("skip".into()),
        dry_run: false,
        yes: false,
        no_progress: true,
    }));
    let out = run(
        &cli,
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(out.exit_code, None, "ok+skip is not a partial failure");
    snapshot_human("upload_mixed_batch", dir.to_str().unwrap(), out.human);
}

#[tokio::test]
async fn upload_dry_run_shows_the_banner() {
    let (app, _) = batch_app().await;
    let dirs = resolve_app_dirs(None);
    let dir = temp_dir("dry");
    std::fs::write(dir.join("new.txt"), b"new").unwrap();
    std::fs::write(dir.join("b.txt"), b"changed").unwrap();

    let cli = transfer_cli(Command::Upload(TransferArgs {
        sources: vec![
            dir.join("new.txt").to_string_lossy().into_owned(),
            dir.join("b.txt").to_string_lossy().into_owned(),
        ],
        target: "nas/media".into(),
        recursive: false,
        conflict: Some("skip".into()),
        dry_run: true,
        yes: false,
        no_progress: true,
    }));
    let out = run(
        &cli,
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(out.human.starts_with("(dry run — nothing transferred)\n"));
    snapshot_human("upload_dry_run", dir.to_str().unwrap(), out.human);
}

#[tokio::test]
async fn cp_mixed_batch_renders_the_shared_grammar() {
    let (app, _) = batch_app().await;
    let dirs = resolve_app_dirs(None);

    let cli = transfer_cli(Command::Cp(CopyArgs {
        sources: vec!["nas/media/src.txt".into(), "nas/media/dup.txt".into()],
        target: "nas/media/out/".into(),
        dry_run: false,
        conflict: Some("skip".into()),
        yes: false,
        recursive: false,
        keep_tags: false,
    }));
    let out = run(
        &cli,
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        out.exit_code, None,
        "ok+skip is not a partial failure:\n{}",
        out.human
    );
    snapshot_human("cp_mixed_batch", "", out.human);
}

#[tokio::test]
async fn batch_download_renders_the_flattened_children() {
    let (app, _) = batch_app().await;
    let dirs = resolve_app_dirs(None);
    let dir = temp_dir("dl");
    std::fs::write(dir.join("local.txt"), b"already here").unwrap();

    let cli = transfer_cli(Command::Download(TransferArgs {
        sources: vec!["nas/media/a.txt".into(), "nas/media/local.txt".into()],
        target: dir.to_string_lossy().into_owned(),
        recursive: false,
        conflict: Some("skip".into()),
        dry_run: false,
        yes: false,
        no_progress: true,
    }));
    let out = run(
        &cli,
        &app,
        &dirs,
        &rstore_transfer::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(out.exit_code, None, "ok+skip is not a partial failure");
    snapshot_human("batch_download_mixed", dir.to_str().unwrap(), out.human);
}
