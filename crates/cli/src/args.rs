//! Command-line argument definitions (CLI-001).
//!
//! The command tree is the CLI presentation contract. Every command is
//! dispatched through the Bootstrap-created Application API.

use std::path::PathBuf;

use clap::{Args, CommandFactory, Parser, Subcommand};

/// `rstore` — Remote Storage Workbench CLI.
#[derive(Parser, Debug)]
#[command(
    name = "rstore",
    version,
    about = "Local-first workbench for S3-compatible remote storage",
    long_about = "rstore connects to your own S3-compatible storage, indexes buckets into \
                  workspaces, and exposes a stable CLI plus (later) a GPUI desktop client.\n\
                  Documentation: see `rstore <command> --help`."
)]
pub struct Cli {
    /// Override the data/config root directory.
    #[arg(long, value_name = "PATH", global = true)]
    pub config: Option<PathBuf>,

    /// Log level for stderr diagnostics.
    #[arg(long, value_name = "LEVEL", global = true, default_value = "warn")]
    pub log_level: String,

    /// Emit machine-readable JSON with a stable envelope.
    #[arg(long, global = true)]
    pub json: bool,

    /// Suppress progress and successful human-readable output; errors remain on stderr.
    #[arg(long, short = 'q', global = true)]
    pub quiet: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Manage storage connections.
    Connection {
        #[command(subcommand)]
        action: ConnectionCommand,
    },
    /// Manage buckets.
    Bucket {
        #[command(subcommand)]
        action: BucketCommand,
    },
    /// Manage workspaces — named groups of buckets for listing and tagging.
    Workspace {
        #[command(subcommand)]
        action: WorkspaceCommand,
    },
    /// Live remote listing/stat.
    Remote {
        #[command(subcommand)]
        action: RemoteCommand,
    },
    /// Index & catalog control.
    Index {
        #[command(subcommand)]
        action: IndexCommand,
    },
    /// List and search files across workspace buckets.
    Files {
        #[command(subcommand)]
        action: FilesCommand,
    },
    /// Workspace tags.
    Tag {
        #[command(subcommand)]
        action: TagCommand,
    },
    /// Upload files to a remote bucket.
    Upload(TransferArgs),
    /// Download remote objects.
    Download(TransferArgs),
    /// Copy objects.
    Cp(CopyArgs),
    /// Move/rename objects.
    Mv(CopyArgs),
    /// Delete remote objects.
    Rm {
        /// One or more remote references `<bucket>/[prefix/]` (add the
        /// connection segment when ambiguous), or `-` to read one per line
        /// from stdin.
        uris: Vec<String>,
        /// Delete every object under a prefix URI; the
        /// confirmation count is precomputed from the listing.
        #[arg(long)]
        recursive: bool,
        /// Preview without deleting anything.
        #[arg(long)]
        dry_run: bool,
        /// Skip the interactive confirmation (required non-interactively).
        #[arg(long)]
        yes: bool,
    },
    /// Inspect/control transfer jobs.
    Task {
        #[command(subcommand)]
        action: TaskCommand,
    },
    /// Generate object links.
    Url(UrlArgs),
    /// Catalog maintenance.
    Catalog {
        #[command(subcommand)]
        action: CatalogCommand,
    },
    /// Local configuration.
    Config {
        #[command(subcommand)]
        action: ConfigCommand,
    },
    /// Environment & health diagnostics.
    Doctor,
    /// Print consolidated version information.
    Version,
}

impl Command {
    /// Dotted path used as the envelope `command` field. The single
    /// source of command identity — the binary and the library both read it.
    pub fn path(&self) -> &'static str {
        match self {
            Command::Connection { action } => match action {
                ConnectionCommand::Add { .. } => "connection.add",
                ConnectionCommand::List { .. } => "connection.list",
                ConnectionCommand::Show { .. } => "connection.show",
                ConnectionCommand::Test { .. } => "connection.test",
                ConnectionCommand::Edit { .. } => "connection.edit",
                ConnectionCommand::Remove { .. } => "connection.remove",
            },
            Command::Bucket { action } => match action {
                BucketCommand::Discover { .. } => "bucket.discover",
                BucketCommand::Link { action } => match action {
                    LinkProfileCommand::Show { .. } => "bucket.link.show",
                    LinkProfileCommand::Set { .. } => "bucket.link.set",
                },
                BucketCommand::Add { .. } => "bucket.add",
                BucketCommand::List => "bucket.list",
                BucketCommand::Show { .. } => "bucket.show",
                BucketCommand::Edit { .. } => "bucket.edit",
                BucketCommand::Remove { .. } => "bucket.remove",
                BucketCommand::Ignore { action } => match action {
                    IgnoreCommand::Add { .. } => "bucket.ignore.add",
                    IgnoreCommand::List { .. } => "bucket.ignore.list",
                    IgnoreCommand::Remove { .. } => "bucket.ignore.remove",
                },
            },
            Command::Config { action } => match action {
                ConfigCommand::Show => "config.show",
                ConfigCommand::Export { .. } => "config.export",
                ConfigCommand::Import { .. } => "config.import",
            },
            Command::Doctor => "doctor",
            Command::Version => "version",
            Command::Workspace { action } => match action {
                WorkspaceCommand::Create { .. } => "workspace.create",
                WorkspaceCommand::List => "workspace.list",
                WorkspaceCommand::Show { .. } => "workspace.show",
                WorkspaceCommand::Edit { .. } => "workspace.edit",
                WorkspaceCommand::Remove { .. } => "workspace.remove",
                WorkspaceCommand::Bucket { action } => match action {
                    WorkspaceBucketCommand::Add { .. } => "workspace.bucket.add",
                    WorkspaceBucketCommand::Remove { .. } => "workspace.bucket.remove",
                    WorkspaceBucketCommand::List { .. } => "workspace.bucket.list",
                },
            },
            Command::Remote { action } => match action {
                RemoteCommand::Ls { .. } => "remote.ls",
                RemoteCommand::Stat { .. } => "remote.stat",
            },
            Command::Index { action } => match action {
                IndexCommand::Start { .. } => "index.start",
                IndexCommand::Status => "index.status",
                IndexCommand::Stats => "index.stats",
                IndexCommand::Pause { .. } => "index.pause",
                IndexCommand::Resume { .. } => "index.resume",
                IndexCommand::Cancel { .. } => "index.cancel",
                IndexCommand::Refresh { .. } => "index.refresh",
                IndexCommand::Rebuild { .. } => "index.rebuild",
            },
            Command::Files { action } => match action {
                FilesCommand::List { .. } => "files.list",
                FilesCommand::Search { .. } => "files.search",
                FilesCommand::Show { .. } => "files.show",
            },
            Command::Tag { action } => match action {
                TagCommand::Create { .. } => "tag.create",
                TagCommand::List { .. } => "tag.list",
                TagCommand::Edit { .. } => "tag.edit",
                TagCommand::Remove { .. } => "tag.remove",
                TagCommand::Merge { .. } => "tag.merge",
                TagCommand::Add { .. } => "tag.add",
                TagCommand::Detach { .. } => "tag.detach",
            },
            Command::Upload { .. } => "upload",
            Command::Download { .. } => "download",
            Command::Cp { .. } => "cp",
            Command::Mv { .. } => "mv",
            Command::Rm { .. } => "rm",
            Command::Task { action } => match action {
                TaskCommand::List => "task.list",
                TaskCommand::Show { .. } => "task.show",
                TaskCommand::Pause { .. } => "task.pause",
                TaskCommand::Resume { .. } => "task.resume",
                TaskCommand::Retry { .. } => "task.retry",
                TaskCommand::Cancel { .. } => "task.cancel",
            },
            Command::Url { .. } => "url",
            Command::Catalog { action } => match action {
                CatalogCommand::Gc { .. } => "catalog.gc",
            },
        }
    }
}

/// Top-level command domains derived from clap's command tree. Command names
/// therefore live in the enum/path table, never in a third manual list.
pub fn command_domains() -> Vec<String> {
    Cli::command()
        .get_subcommands()
        .map(|command| command.get_name().to_string())
        .collect()
}

#[derive(Subcommand, Debug)]
pub enum ConnectionCommand {
    /// Register a new storage connection.
    Add(ConnectionAddArgs),
    /// List connections; `--filter` narrows by name/endpoint.
    List {
        /// Case-insensitive substring over name and endpoint.
        #[arg(long, short = 'f')]
        filter: Option<String>,
    },
    /// Show details for a connection (by name or `conn_…` id). Never prints secrets.
    Show {
        reference: String,
        /// Include the probed capability report.
        #[arg(long)]
        capabilities: bool,
    },
    /// Probe a connection: credentials, endpoint/auth, ListBuckets.
    Test {
        reference: String,
        /// Also write and clean up a temporary object.
        #[arg(long)]
        write_test: bool,
    },
    /// Edit a connection's mutable configuration.
    Edit(ConnectionEditArgs),
    /// Remove a connection (local config only — never touches remote data).
    Remove {
        reference: String,
        /// Skip the interactive confirmation (required in non-interactive mode).
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Args, Debug)]
pub struct ConnectionEditArgs {
    /// Connection to edit (name or `conn_…` id).
    pub reference: String,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Region (`auto` or explicit like `us-east-1`).
    #[arg(long)]
    pub region: Option<String>,
    /// Addressing style: auto, path, virtual-host.
    #[arg(long)]
    pub addressing_style: Option<String>,
    /// TLS certificate verification (tri-state: absent = unchanged).
    #[arg(long)]
    pub tls_verify: Option<bool>,
    /// Reference key under which credentials are stored.
    #[arg(long)]
    pub secret_ref: Option<String>,
    /// Replace stored credentials.
    #[arg(long)]
    pub access_key: Option<String>,
    #[arg(long)]
    pub secret_key: Option<String>,
}

/// Bucket management.
#[derive(Subcommand, Debug)]
pub enum BucketCommand {
    /// List buckets visible on the remote.
    Discover { connection: String },
    /// Show or edit the bucket's link profile (public/CDN bases,
    /// presign permissions, default TTL).
    Link {
        #[command(subcommand)]
        action: LinkProfileCommand,
    },
    /// Register a bucket manually — works without ListBuckets permission.
    Add(BucketAddArgs),
    /// List locally registered buckets.
    List,
    /// Show a registered bucket's settings.
    Show { reference: String },
    /// Edit a bucket's local settings.
    Edit(BucketEditArgs),
    /// Remove the local bucket registration (never deletes remote data).
    Remove {
        reference: String,
        /// Skip the interactive confirmation (required in non-interactive mode).
        #[arg(long)]
        yes: bool,
    },
    /// Manage ignored directories for a bucket.
    Ignore {
        #[command(subcommand)]
        action: IgnoreCommand,
    },
}

#[derive(Args, Debug)]
pub struct BucketAddArgs {
    /// Connection the bucket belongs to (name or `conn_…` id).
    pub connection: String,
    /// The real remote bucket name.
    pub name: String,
    #[arg(long)]
    pub display_name: Option<String>,
    /// Refuse all writes to this bucket (uploads and copies into it are blocked).
    #[arg(long)]
    pub read_only: bool,
    /// Allow deletions (default on).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub allow_delete: bool,
    /// Include this bucket in the index (default on).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub index_enabled: bool,
    /// Default conflict policy: ask, skip, overwrite, rename.
    #[arg(long, default_value = "ask")]
    pub conflict: String,
    /// Skip the readability probe at add time.
    #[arg(long)]
    pub skip_verify: bool,
}

#[derive(Args, Debug)]
pub struct BucketEditArgs {
    /// Bucket to edit (`bkt_…` id, `<connection>/<name>`, or bare name).
    pub reference: String,
    /// Display name override; an empty value clears it.
    #[arg(long)]
    pub display_name: Option<String>,
    /// Mark read-only; pass =false to lift.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub read_only: Option<bool>,
    /// Allow deletions; pass =false to forbid.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub allow_delete: Option<bool>,
    /// Include in the index; pass =false to exclude.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub index_enabled: Option<bool>,
    /// Default conflict policy: ask, skip, overwrite, rename.
    #[arg(long)]
    pub conflict: Option<String>,
    /// Per-bucket transfer concurrency override; 0 clears it.
    #[arg(long)]
    pub concurrency: Option<u32>,
    /// Link-profile flags shared with `bucket link set`; only the given
    /// flags change.
    #[command(flatten)]
    pub link: LinkProfileFlags,
}

/// The link-profile flag set `bucket edit` and `bucket link set` share —
/// one spelling of the five editable profile knobs (LKT-flags).
#[derive(Args, Debug, Default)]
pub struct LinkProfileFlags {
    /// Base for public URLs (e.g. a CDN or the provider's public-object
    /// route); an empty value clears it.
    #[arg(long)]
    pub public_base: Option<String>,
    /// CDN override, preferred over the public base when set.
    #[arg(long)]
    pub cdn_base: Option<String>,
    /// Allow plain public URLs for this bucket.
    #[arg(long)]
    pub allow_public_url: Option<bool>,
    /// Allow presigned URLs for this bucket.
    #[arg(long)]
    pub allow_presigned: Option<bool>,
    /// Default presign TTL (30s/15m/1h/7d or plain seconds).
    #[arg(long)]
    pub default_ttl: Option<String>,
}

impl LinkProfileFlags {
    /// Whether any profile flag was passed.
    pub fn any(&self) -> bool {
        self.public_base.is_some()
            || self.cdn_base.is_some()
            || self.allow_public_url.is_some()
            || self.allow_presigned.is_some()
            || self.default_ttl.is_some()
    }
}

#[derive(Args, Debug)]
pub struct ConnectionAddArgs {
    /// Unique, human-friendly connection name.
    pub name: String,
    /// S3 API endpoint URL.
    #[arg(long)]
    pub endpoint: String,
    /// Region (`auto` or an explicit region like `us-east-1`).
    #[arg(long, default_value = "auto")]
    pub region: String,
    /// Addressing style: auto, path, virtual-host.
    #[arg(long, default_value = "auto")]
    pub addressing_style: String,
    /// Disable TLS certificate verification (not recommended).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub tls_verify: bool,
    /// Reference key under which credentials are stored (default: derived from the name).
    #[arg(long)]
    pub secret_ref: Option<String>,
    /// Access key. Prompted with hidden input on a TTY when omitted;
    /// required explicitly with `--json`.
    #[arg(long)]
    pub access_key: Option<String>,
    /// Secret key. Prompted with hidden input on a TTY when omitted;
    /// required explicitly with `--json`.
    #[arg(long)]
    pub secret_key: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum RemoteCommand {
    /// List a remote directory page.
    Ls {
        /// `<connection>/<bucket>/[prefix/]` (or bare `<bucket>/…`) —
        /// trailing `/` lists.
        uri: String,
        /// Continuation token from the previous page.
        #[arg(long)]
        cursor: Option<String>,
        /// Page size.
        #[arg(long, default_value_t = 50)]
        limit: u32,
        /// Add the listed objects to the local catalog.
        #[arg(long)]
        update_catalog: bool,
    },
    /// Show one object's live metadata.
    Stat { uri: String },
}

/// Shared transfer flags (UP-/DOWN-).
#[derive(Args, Debug)]
pub struct TransferArgs {
    /// Sources: local paths for upload; remote references
    /// `<bucket>/[prefix/]` for download (add the connection segment when
    /// ambiguous).
    #[arg(required = true)]
    pub sources: Vec<String>,
    /// Destination for upload: `<bucket>/[prefix/]` (add the connection
    /// segment when ambiguous). Local path for download.
    pub target: String,
    /// Recurse directories.
    #[arg(long, short = 'r')]
    pub recursive: bool,
    /// Conflict policy: ask, skip, overwrite, rename. Omitted → the target
    /// bucket's stored default (uploads; downloads have no bucket target) →
    /// interactive prompt.
    #[arg(long)]
    pub conflict: Option<String>,
    /// Preview without transferring.
    #[arg(long)]
    pub dry_run: bool,
    /// Accept overwrites without prompting (non-interactive `ask` resolves to
    /// an error unless this is set).
    #[arg(long)]
    pub yes: bool,
    /// Suppress the stderr progress line.
    #[arg(long)]
    pub no_progress: bool,
}

/// Task inspection and control.
#[derive(Subcommand, Debug)]
pub enum TaskCommand {
    /// List jobs, newest first.
    List,
    /// Show one job with per-item progress.
    Show {
        /// `job_…` id or 1-based ordinal (newest first).
        reference: String,
        /// Include failed-item error details.
        #[arg(long)]
        errors: bool,
    },
    /// Pause a running job.
    Pause { reference: String },
    /// Resume a paused/interrupted job.
    Resume { reference: String },
    /// Re-run a failed/partial job.
    Retry { reference: String },
    /// Cancel a job in any non-terminal state.
    Cancel { reference: String },
}

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Show resolved application directories and configuration.
    Show,
    /// Export the registry (connections + buckets, never secrets) as JSON.
    /// Prints to stdout or writes `--out`.
    Export {
        /// Write to this path instead of stdout.
        #[arg(long)]
        out: Option<String>,
    },
    /// Import a previously exported bundle. Existing names are
    /// skipped, secrets are never touched; `--dry-run` adds nothing.
    Import {
        /// Path to a bundle produced by `config export`.
        path: String,
        /// Preview without adding anything.
        #[arg(long)]
        dry_run: bool,
    },
}

/// Workspace management.
#[derive(Subcommand, Debug)]
pub enum WorkspaceCommand {
    /// Create a workspace.
    Create {
        /// Unique workspace name.
        name: String,
        #[arg(long)]
        description: Option<String>,
    },
    /// List workspaces with bucket counts.
    List,
    /// Show one workspace with buckets and stats.
    Show {
        /// Workspace name or `wsp_…` id.
        reference: String,
    },
    /// Edit name/description.
    Edit {
        reference: String,
        #[arg(long)]
        name: Option<String>,
        /// New description; empty string clears it.
        #[arg(long)]
        description: Option<String>,
    },
    /// Remove a workspace — buckets and remote data are untouched.
    Remove {
        reference: String,
        /// Skip the interactive confirmation (required non-interactively).
        #[arg(long)]
        yes: bool,
    },
    /// Manage bucket membership.
    Bucket {
        #[command(subcommand)]
        action: WorkspaceBucketCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum WorkspaceBucketCommand {
    /// Add a bucket to a workspace.
    Add {
        workspace: String,
        /// Bucket reference: `bkt_…` id, `<connection>/<name>`, or bare name.
        bucket: String,
    },
    /// Remove a bucket from a workspace.
    Remove { workspace: String, bucket: String },
    /// List the buckets of a workspace.
    List { workspace: String },
}

/// Index & catalog control.
#[derive(Subcommand, Debug)]
pub enum IndexCommand {
    /// Scan a bucket into the local catalog; resumes from a checkpoint.
    Start {
        /// Bucket reference: `bkt_…` id, `<connection>/<name>`, or bare name.
        bucket: String,
    },
    /// Per-bucket index status.
    Status,
    /// Catalog size overview.
    Stats,
    /// Pause the active scan.
    Pause { bucket: String },
    /// Resume a paused scan from its checkpoint.
    Resume { bucket: String },
    /// Cancel the active scan; completed data is kept.
    Cancel { bucket: String },
    /// Full reconciliation: mark remotely-missing rows stale.
    Refresh { bucket: String },
    /// Drop the bucket's catalog rows and rescan; tags survive.
    Rebuild {
        bucket: String,
        /// Skip the interactive confirmation (required non-interactively).
        #[arg(long)]
        yes: bool,
    },
}

/// List and search files across workspace buckets.
#[derive(Subcommand, Debug)]
pub enum FilesCommand {
    /// List files across a workspace's buckets.
    List {
        workspace: String,
        #[arg(long)]
        path: Option<String>,
        /// Filter by bucket reference (repeatable).
        #[arg(long = "bucket")]
        buckets: Vec<String>,
        /// `name:size:modified:type` + `asc|desc`, e.g. `size:desc`.
        #[arg(long, default_value = "modified:desc")]
        sort: String,
        #[arg(long, default_value_t = 50)]
        limit: u32,
        /// Cursor from the previous page.
        #[arg(long)]
        after: Option<String>,
        /// Only files without tags.
        #[arg(long)]
        untagged: bool,
    },
    /// Search file names.
    Search {
        workspace: String,
        /// Partial name to match.
        pattern: String,
        #[arg(long, default_value = "modified:desc")]
        sort: String,
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
    /// Show one indexed object with tags and staleness.
    Show {
        /// A remote reference `<connection>/<bucket>/<key>` (or bare
        /// `<bucket>/…`).
        uri: String,
        #[arg(long)]
        workspace: String,
    },
}

/// Workspace tags.
#[derive(Subcommand, Debug)]
pub enum TagCommand {
    /// Create a tag in a workspace.
    Create {
        workspace: String,
        name: String,
        #[arg(long)]
        color: Option<String>,
        #[arg(long)]
        description: Option<String>,
    },
    /// List tags with live object counts.
    List { workspace: String },
    /// Edit a tag.
    Edit {
        workspace: String,
        /// Tag name or `tag_…` id.
        tag: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        color: Option<String>,
        #[arg(long)]
        description: Option<String>,
    },
    /// Remove a tag.
    Remove {
        workspace: String,
        tag: String,
        #[arg(long)]
        yes: bool,
    },
    /// Merge one tag into another: associations move (deduplicated) and the
    /// source tag is removed.
    Merge {
        workspace: String,
        /// Tag to fold away (name or `tag_…` id).
        source: String,
        /// Tag that keeps the associations.
        target: String,
    },
    /// Attach tags to an object.
    Add {
        workspace: String,
        /// One or more remote references `<bucket>/[prefix/]` (add the
        /// connection segment when ambiguous), or `-` to read them from
        /// stdin.
        uris: Vec<String>,
        /// Tag names to attach (repeatable).
        #[arg(long = "tag", required = true)]
        tags: Vec<String>,
        /// Replace the object's whole tag set.
        #[arg(long)]
        replace: bool,
    },
    /// Detach tags from objects; no --tag = all; `-` reads
    /// URIs from stdin.
    Detach {
        workspace: String,
        /// One or more remote references, or `-` for stdin.
        uris: Vec<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
    },
}

/// Link profile management for a bucket.
#[derive(Subcommand, Debug)]
pub enum LinkProfileCommand {
    /// Show the bucket's link profile.
    Show {
        /// Bucket reference: `bkt_…` id, `<connection>/<name>`, or bare name.
        bucket: String,
    },
    /// Edit the link profile; only the given flags change.
    Set {
        /// Bucket reference: `bkt_…` id, `<connection>/<name>`, or bare name.
        bucket: String,
        #[command(flatten)]
        link: LinkProfileFlags,
    },
}

/// Bucket ignore rules.
#[derive(Subcommand, Debug)]
pub enum IgnoreCommand {
    /// Ignore a bucket-relative directory.
    Add {
        /// Bucket reference.
        bucket: String,
        /// Directory path relative to the bucket root.
        directory: String,
    },
    /// List ignore rules.
    List { bucket: String },
    /// Remove an ignore rule.
    Remove { bucket: String, directory: String },
}

/// Arguments for `cp` / `mv`.
#[derive(Args, Debug)]
pub struct CopyArgs {
    /// Source remote references `<bucket>/…` (add the connection segment
    /// when ambiguous); at least one.
    #[arg(required = true)]
    pub sources: Vec<String>,
    /// Target remote reference; a trailing `/` (or bare bucket) keeps the
    /// source file name.
    pub target: String,
    /// Preview without sending any write request.
    #[arg(long)]
    pub dry_run: bool,
    /// Conflict policy: ask, skip, overwrite, rename. Omitted → the target
    /// bucket's stored default → interactive prompt.
    #[arg(long)]
    pub conflict: Option<String>,
    /// Resolve `ask` as overwrite non-interactively.
    #[arg(long)]
    pub yes: bool,
    /// Copy/move every object under a source prefix.
    #[arg(long)]
    pub recursive: bool,
    /// Carry tag associations from source to target.
    #[arg(long)]
    pub keep_tags: bool,
}

/// Arguments for `url`.
#[derive(Args, Debug)]
pub struct UrlArgs {
    /// Remote references `<bucket>/…` (add the connection segment when
    /// ambiguous). Results are per-item: one bad reference does not sink
    /// the batch.
    pub uris: Vec<String>,
    /// Output format: path, s3, url, markdown, html, json.
    #[arg(long, default_value = "url")]
    pub format: String,
    /// Produce a presigned URL instead of a public one.
    #[arg(long)]
    pub signed: bool,
    /// TTL for --signed: 30s, 15m, 1h, 1d, 7d or plain seconds.
    #[arg(long)]
    pub ttl: Option<String>,
}

/// Subcommands of `catalog`.
#[derive(Subcommand, Debug)]
pub enum CatalogCommand {
    /// Purge records of deleted objects past their retention period.
    Gc {
        /// Count without deleting.
        #[arg(long)]
        dry_run: bool,
        /// Override the retention setting: plain seconds or 30m/1h/1d/2w.
        #[arg(long)]
        retention: Option<String>,
    },
}
