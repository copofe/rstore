//! Command-line argument definitions: the full clap command tree of the CLI.
//! Every command is dispatched through the `Runtime`-created application API;
//! [`parse_command_line`] derives the envelope's `command` identity from the
//! same tree, so the grammar has exactly one spelling.
//!
//! Remote arguments share one reference grammar, `[<connection>/]<bucket>/[key]`
//! (the connection segment only when a bucket name is ambiguous). Catalog
//! commands act on the workspace from `-w`, else the current one.

use std::path::PathBuf;

use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};

/// `rstore` — Remote Storage Workbench CLI.
#[derive(Parser, Debug)]
#[command(
    name = "rstore",
    version,
    about = "Local-first workbench for S3-compatible remote storage",
    long_about = "rstore connects to your own S3-compatible storage, indexes buckets into \
                  workspaces, and exposes a stable CLI plus (later) a GPUI desktop client.\n\
                  Remote references read `[<connection>/]<bucket>/[key]`.\n\
                  Documentation: see `rstore <command> --help`."
)]
pub struct Cli {
    /// Keep the database and logs under this directory instead of the
    /// platform default (same as `RSTORE_HOME`).
    #[arg(long, value_name = "PATH", global = true)]
    pub home: Option<PathBuf>,

    /// Log level for stderr diagnostics.
    #[arg(long, value_name = "LEVEL", global = true, default_value = "warn")]
    pub log_level: String,

    /// Emit machine-readable JSON with a stable envelope.
    #[arg(long, global = true)]
    pub json: bool,

    /// Suppress progress and successful human-readable output; errors remain on stderr.
    #[arg(long, short = 'q', global = true)]
    pub quiet: bool,

    /// Workspace for catalog commands; defaults to the current workspace.
    #[arg(
        long,
        short = 'w',
        global = true,
        env = "RSTORE_WORKSPACE",
        value_name = "WS"
    )]
    pub workspace: Option<String>,

    /// Confirm destructive actions without prompting (required when no terminal can confirm).
    #[arg(long, short = 'y', global = true)]
    pub yes: bool,

    #[command(subcommand)]
    pub command: Command,
}

/// The top-level command grammar; variant order matches help output.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// List a remote directory, or the registered buckets when no reference is given.
    Ls(LsArgs),
    /// Show one object's metadata (live; `--offline` reads the catalog).
    Stat {
        /// Remote reference `[<connection>/]<bucket>/<key>`.
        reference: String,
        /// Read the local catalog instead (adds tags and staleness, no network).
        #[arg(long)]
        offline: bool,
    },
    /// Copy files and objects: upload, download, or remote-to-remote.
    Cp(CopyArgs),
    /// Move objects between remote locations.
    Mv(CopyArgs),
    /// Delete remote objects.
    Rm(RmArgs),
    /// Generate links for remote objects.
    Url(UrlArgs),
    /// Search the workspace catalog.
    Find(FindArgs),
    /// Tag objects in the workspace.
    Tag {
        #[command(subcommand)]
        action: TagCommand,
    },
    /// Manage storage connections.
    #[command(visible_alias = "conn")]
    Connection {
        #[command(subcommand)]
        action: ConnectionCommand,
    },
    /// Manage registered buckets.
    Bucket {
        #[command(subcommand)]
        action: BucketCommand,
    },
    /// Manage workspaces — named groups of buckets that scope `find` and `tag`.
    #[command(visible_alias = "ws")]
    Workspace {
        #[command(subcommand)]
        action: WorkspaceCommand,
    },
    /// Build and maintain the local catalog.
    Index {
        #[command(subcommand)]
        action: IndexCommand,
    },
    /// Inspect and cancel transfer jobs.
    Task {
        #[command(subcommand)]
        action: TaskCommand,
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

/// Parses a command line into the typed grammar plus its dotted command
/// identity, derived from the clap tree itself — there is no second,
/// hand-typed spelling of the grammar left to drift.
///
/// Exits the process on parse errors, exactly like [`Cli::parse`] would.
pub fn parse_command_line<I, T>(argv: I) -> (Cli, String)
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let matches = Cli::command()
        .try_get_matches_from(argv)
        .unwrap_or_else(|error| error.exit());

    // The matched subcommand chain is the command identity: `rstore bucket
    // list` becomes `bucket.list` (aliases resolve to the canonical name).
    let mut parts: Vec<&str> = Vec::new();
    let mut cursor = &matches;
    while let Some((name, sub)) = cursor.subcommand() {
        parts.push(name);
        cursor = sub;
    }
    let identity = parts.join(".");

    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    (cli, identity)
}

/// Top-level command domains derived from clap's command tree, so command
/// names live in the enum/path table and never in a second manual list.
pub fn command_domains() -> Vec<String> {
    Cli::command()
        .get_subcommands()
        .map(|command| command.get_name().to_string())
        .collect()
}

/// Arguments for `ls`.
#[derive(Args, Debug)]
pub struct LsArgs {
    /// `[<connection>/]<bucket>/[prefix/]`; omit to list the registered buckets.
    pub reference: Option<String>,
    /// Page size.
    #[arg(long, default_value_t = 50)]
    pub limit: u32,
    /// Continuation token from the prior page.
    #[arg(long)]
    pub cursor: Option<String>,
    /// Record the listed objects in the local catalog.
    #[arg(long)]
    pub catalog: bool,
}

/// Arguments for `cp` / `mv`.
#[derive(Args, Debug)]
pub struct CopyArgs {
    /// Sources: local paths or remote references; `-` reads one per line
    /// from stdin. Paths starting with `./`, `../`, `~/` or `/` are always
    /// local.
    #[arg(required = true)]
    pub sources: Vec<String>,
    /// Target: a local path or a remote reference; a trailing `/` keeps the
    /// source names.
    pub target: String,
    /// Recurse into directories and prefixes.
    #[arg(long, short = 'r')]
    pub recursive: bool,
    /// Preview without transferring or writing anything.
    #[arg(long, short = 'n')]
    pub dry_run: bool,
    /// When a target exists: ask, skip, overwrite, rename. Omitted → the
    /// target bucket's default → interactive prompt.
    #[arg(long, value_name = "POLICY", conflicts_with = "force")]
    pub conflict: Option<String>,
    /// Overwrite existing targets (same as `--conflict overwrite`).
    #[arg(long, short = 'f')]
    pub force: bool,
    /// Carry tag associations to the copies (remote-to-remote only).
    #[arg(long)]
    pub keep_tags: bool,
    /// Suppress the stderr progress line.
    #[arg(long)]
    pub no_progress: bool,
}

/// Arguments for `rm`.
#[derive(Args, Debug)]
pub struct RmArgs {
    /// Remote references, or `-` to read one per line from stdin.
    #[arg(required = true)]
    pub references: Vec<String>,
    /// Delete every object under a prefix; the confirmation count is
    /// precomputed from the listing.
    #[arg(long, short = 'r')]
    pub recursive: bool,
    /// Preview without deleting anything.
    #[arg(long, short = 'n')]
    pub dry_run: bool,
}

/// Arguments for `url`.
#[derive(Args, Debug)]
pub struct UrlArgs {
    /// Remote references, or `-` to read one per line from stdin. Results are
    /// per-item: one bad reference does not sink the batch.
    #[arg(required = true)]
    pub references: Vec<String>,
    /// Output format: path, s3, url, markdown, html, json.
    #[arg(long, default_value = "url")]
    pub format: String,
    /// Produce a presigned URL instead of a public one.
    #[arg(long)]
    pub signed: bool,
    /// Presigned URL lifetime: 30s, 15m, 1h, 1d, 7d or plain seconds
    /// (implies `--signed`).
    #[arg(long)]
    pub ttl: Option<String>,
}

/// Arguments for `find`.
#[derive(Args, Debug)]
pub struct FindArgs {
    /// Part of the file name to match; omit to list everything.
    pub pattern: Option<String>,
    /// Only these buckets (repeatable).
    #[arg(long = "bucket", short = 'b', value_name = "BUCKET")]
    pub buckets: Vec<String>,
    /// Only paths containing this text.
    #[arg(long)]
    pub path: Option<String>,
    /// Only files carrying these tags (repeatable or comma-separated; all
    /// must match unless `--any-tag`).
    #[arg(long = "tag", short = 't', value_name = "TAG", value_delimiter = ',')]
    pub tags: Vec<String>,
    /// Match files carrying any of the `--tag` tags.
    #[arg(long, requires = "tags")]
    pub any_tag: bool,
    /// Only files without tags.
    #[arg(long, conflicts_with = "tags")]
    pub untagged: bool,
    /// Only this file type: image, video, audio, document, archive, other.
    #[arg(long = "type", value_name = "TYPE")]
    pub file_type: Option<String>,
    /// Only this extension, e.g. `png`.
    #[arg(long = "ext", value_name = "EXT")]
    pub extension: Option<String>,
    /// `name|size|modified|type|bucket|connection` + `:asc|:desc`.
    #[arg(long, default_value = "modified:desc")]
    pub sort: String,
    /// Page size.
    #[arg(long, default_value_t = 50)]
    pub limit: u32,
    /// Cursor from the previous page.
    #[arg(long)]
    pub after: Option<String>,
}

/// Workspace tags: object associations plus tag definitions.
#[derive(Subcommand, Debug)]
pub enum TagCommand {
    /// Attach tags to objects; tags that do not exist yet are created.
    Add {
        /// Remote references, or `-` to read one per line from stdin.
        #[arg(required = true)]
        references: Vec<String>,
        /// Tags to attach (repeatable or comma-separated).
        #[arg(
            long = "tag",
            short = 't',
            value_name = "TAG",
            required = true,
            value_delimiter = ','
        )]
        tags: Vec<String>,
        /// Replace each object's whole tag set.
        #[arg(long)]
        replace: bool,
    },
    /// Remove tags from objects; without `--tag`, every tag is removed.
    Remove {
        /// Remote references, or `-` to read one per line from stdin.
        #[arg(required = true)]
        references: Vec<String>,
        /// Tags to remove (repeatable or comma-separated).
        #[arg(long = "tag", short = 't', value_name = "TAG", value_delimiter = ',')]
        tags: Vec<String>,
    },
    /// List tags with object counts.
    List,
    /// Rename, recolor, or describe a tag.
    Edit {
        /// Tag name or `tag_…` id.
        tag: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        color: Option<String>,
        /// New description; an empty value clears it.
        #[arg(long)]
        description: Option<String>,
    },
    /// Delete a tag and all its associations.
    Delete {
        /// Tag name or `tag_…` id.
        tag: String,
    },
    /// Fold one tag into another: associations move (deduplicated) and the
    /// source tag is deleted.
    Merge {
        /// Tag to fold away (name or `tag_…` id).
        source: String,
        /// Tag that keeps the associations.
        target: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConnectionCommand {
    /// Register a connection, test it, and register its buckets; on a
    /// terminal, asks for whatever is omitted.
    Add(ConnectionAddArgs),
    /// List connections, optionally filtered by a name/endpoint substring.
    List {
        /// Case-insensitive substring over name and endpoint.
        filter: Option<String>,
    },
    /// Show a connection, including masked credentials.
    Show {
        /// Connection name or `conn_…` id.
        connection: String,
    },
    /// Probe a connection: credentials, endpoint/auth, ListBuckets.
    Test {
        /// Connection name or `conn_…` id.
        connection: String,
        /// Also put and delete a temporary object in each registered bucket
        /// that allows writes and deletes; read-only buckets are never touched.
        #[arg(long)]
        write: bool,
    },
    /// Edit a connection's configuration; only the flags passed change.
    Edit(ConnectionEditArgs),
    /// Remove a connection (local registration only — never touches remote data).
    Remove {
        /// Connection name or `conn_…` id.
        connection: String,
    },
}

#[derive(Args, Debug)]
pub struct ConnectionAddArgs {
    /// Unique, human-friendly connection name. Asked on a terminal when
    /// omitted.
    pub name: Option<String>,
    /// S3 API endpoint URL. Asked on a terminal when omitted.
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Provider preset that fills defaults: aws, r2, minio, supabase, generic.
    #[arg(long)]
    pub provider: Option<String>,
    /// Region (`auto` or an explicit region like `us-east-1`); `auto` when
    /// omitted, asked on a terminal for providers that need it.
    #[arg(long)]
    pub region: Option<String>,
    /// Addressing style: auto, path, virtual-host (overrides `--provider`).
    #[arg(long)]
    pub addressing_style: Option<String>,
    /// Public root serving every bucket under `/<bucket>` (Supabase:
    /// `…/storage/v1/object/public`); an empty value leaves it unset.
    #[arg(long)]
    pub public_base: Option<String>,
    /// CDN root serving every bucket under `/<bucket>`, preferred over the
    /// public base; an empty value leaves it unset.
    #[arg(long)]
    pub cdn_base: Option<String>,
    /// TLS certificate verification (`--tls-verify=false` disables it; not recommended).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub tls_verify: bool,
    /// Access key. Prompted with hidden input on a TTY when omitted.
    #[arg(long)]
    pub access_key: Option<String>,
    /// Secret key. Prompted with hidden input on a TTY when omitted.
    #[arg(long)]
    pub secret_key: Option<String>,
    /// Buckets to register: `all` visible buckets, or a comma-separated list.
    /// Omitted on a terminal → choose from the discovered buckets.
    #[arg(long, value_name = "all|NAME,…", value_delimiter = ',')]
    pub buckets: Vec<String>,
    /// Skip the connection test (and the interactive bucket choice).
    #[arg(long)]
    pub no_test: bool,
}

/// Arguments for `connection edit`; only the flags passed change the record.
#[derive(Args, Debug)]
pub struct ConnectionEditArgs {
    /// Connection to edit (name or `conn_…` id).
    pub connection: String,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Provider preset whose defaults to apply: aws, r2, minio, supabase, generic.
    #[arg(long)]
    pub provider: Option<String>,
    /// Region (`auto` or explicit like `us-east-1`).
    #[arg(long)]
    pub region: Option<String>,
    /// Addressing style: auto, path, virtual-host (overrides `--provider`).
    #[arg(long)]
    pub addressing_style: Option<String>,
    /// Public root serving every bucket under `/<bucket>`; an empty value
    /// clears it.
    #[arg(long)]
    pub public_base: Option<String>,
    /// CDN root serving every bucket under `/<bucket>`, preferred over the
    /// public base; an empty value clears it.
    #[arg(long)]
    pub cdn_base: Option<String>,
    /// TLS certificate verification (tri-state: absent = unchanged).
    #[arg(long)]
    pub tls_verify: Option<bool>,
    /// Replace the credentials stored on the connection.
    #[arg(long)]
    pub access_key: Option<String>,
    #[arg(long)]
    pub secret_key: Option<String>,
}

/// Bucket registration and local settings.
#[derive(Subcommand, Debug)]
pub enum BucketCommand {
    /// List registered buckets; `--remote` lists what the credentials can see.
    List {
        /// Only this connection (name or `conn_…` id).
        connection: Option<String>,
        /// List the buckets visible on the remote, marking registered ones.
        #[arg(long)]
        remote: bool,
    },
    /// Register buckets and add them to the workspace.
    Add(BucketAddArgs),
    /// Show a bucket's settings, link profile, and ignore rules.
    Show {
        /// `[<connection>/]<bucket>` or a `bkt_…` id.
        bucket: String,
    },
    /// Edit a bucket's local settings; only the flags passed change.
    Edit(BucketEditArgs),
    /// Remove bucket registrations (never deletes remote data).
    Remove {
        /// `[<connection>/]<bucket>` or `bkt_…` ids.
        #[arg(required = true)]
        buckets: Vec<String>,
    },
}

/// Arguments for `bucket add`.
#[derive(Args, Debug)]
pub struct BucketAddArgs {
    /// `[<connection>/]<bucket>`; the connection may be omitted while only
    /// one is registered. Omitted on a terminal → choose from the buckets
    /// the connection can see.
    pub buckets: Vec<String>,
    /// Refuse all writes to these buckets.
    #[arg(long)]
    pub read_only: bool,
    /// Forbid deletions.
    #[arg(long)]
    pub no_delete: bool,
    /// Default conflict policy: ask, skip, overwrite, rename.
    #[arg(long, default_value = "ask")]
    pub conflict: String,
    /// Skip the readability probe.
    #[arg(long)]
    pub skip_verify: bool,
}

/// Arguments for `bucket edit`; only the flags passed change the record.
#[derive(Args, Debug)]
pub struct BucketEditArgs {
    /// `[<connection>/]<bucket>` or a `bkt_…` id.
    pub bucket: String,
    /// Display name override; an empty value clears it.
    #[arg(long)]
    pub display_name: Option<String>,
    /// Refuse writes; `=false` lifts it.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub read_only: Option<bool>,
    /// Allow deletions; `=false` forbids them.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub allow_delete: Option<bool>,
    /// Include in indexing; `=false` excludes it.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub index: Option<bool>,
    /// Default conflict policy: ask, skip, overwrite, rename.
    #[arg(long)]
    pub conflict: Option<String>,
    /// Per-bucket transfer concurrency override; 0 clears it.
    #[arg(long)]
    pub concurrency: Option<u32>,
    #[command(flatten)]
    pub link: LinkProfileFlags,
    /// Skip this bucket-relative directory when indexing (repeatable).
    #[arg(long, value_name = "DIR")]
    pub ignore: Vec<String>,
    /// Stop skipping this directory (repeatable).
    #[arg(long, value_name = "DIR")]
    pub unignore: Vec<String>,
}

/// The bucket link-profile flags of `bucket edit`.
#[derive(Args, Debug, Default)]
pub struct LinkProfileFlags {
    /// URL serving this bucket's root (custom domain, r2.dev address, …);
    /// overrides the connection's. An empty value clears it.
    #[arg(long)]
    pub public_base: Option<String>,
    /// CDN URL serving this bucket's root, preferred over the public base;
    /// an empty value clears it.
    #[arg(long)]
    pub cdn_base: Option<String>,
    /// Allow plain public URLs; `=false` blocks them.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub public_url: Option<bool>,
    /// Allow presigned URLs; `=false` blocks them.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub presigned: Option<bool>,
    /// Default presign TTL (30s/15m/1h/7d or plain seconds).
    #[arg(long)]
    pub default_ttl: Option<String>,
}

impl LinkProfileFlags {
    /// Whether any profile flag was passed.
    pub fn any(&self) -> bool {
        self.public_base.is_some()
            || self.cdn_base.is_some()
            || self.public_url.is_some()
            || self.presigned.is_some()
            || self.default_ttl.is_some()
    }
}

/// Workspace management. Commands without a workspace argument act on the
/// workspace from `-w`, else the current one.
#[derive(Subcommand, Debug)]
pub enum WorkspaceCommand {
    /// List workspaces; `*` marks the current one.
    List,
    /// Make a workspace the current one.
    Use {
        /// Workspace name or `wsp_…` id.
        workspace: String,
    },
    /// Create a workspace; it becomes current when none is selected.
    Create {
        /// Unique workspace name. Omitted on a terminal → asked, along with
        /// the description and buckets.
        name: Option<String>,
        #[arg(long)]
        description: Option<String>,
    },
    /// Show the workspace with its buckets.
    Show,
    /// Rename or describe the workspace.
    Edit {
        #[arg(long)]
        name: Option<String>,
        /// New description; an empty value clears it.
        #[arg(long)]
        description: Option<String>,
    },
    /// Delete a workspace — buckets and remote data are untouched.
    Delete {
        /// Workspace name or `wsp_…` id.
        workspace: String,
    },
    /// Add buckets to the workspace.
    Add {
        /// `[<connection>/]<bucket>` or `bkt_…` ids. Omitted on a terminal →
        /// choose from the registered buckets.
        buckets: Vec<String>,
    },
    /// Remove buckets from the workspace.
    Remove {
        /// `[<connection>/]<bucket>` or `bkt_…` ids.
        #[arg(required = true)]
        buckets: Vec<String>,
    },
}

/// Index & catalog control.
#[derive(Subcommand, Debug)]
pub enum IndexCommand {
    /// Scan buckets into the catalog, resuming from saved checkpoints.
    /// Naming a bucket turns its index on.
    Run {
        /// Buckets to index; omit for the indexed buckets of the workspace.
        buckets: Vec<String>,
        /// Scan every indexed bucket.
        #[arg(long, conflicts_with = "buckets")]
        all: bool,
        /// Rescan from the root and mark rows gone remotely as stale.
        #[arg(long, conflicts_with = "rebuild")]
        refresh: bool,
        /// Drop the catalog rows and rescan (tags survive).
        #[arg(long)]
        rebuild: bool,
    },
    /// Catalog totals and per-bucket index state.
    Status,
    /// Discard a bucket's paused scan and its checkpoint; indexed rows stay.
    Cancel {
        /// `[<connection>/]<bucket>` or a `bkt_…` id.
        bucket: String,
    },
    /// Remove untagged catalog rows the last refresh no longer saw.
    Gc {
        /// Count without deleting.
        #[arg(long, short = 'n')]
        dry_run: bool,
    },
}

/// Task inspection and control.
#[derive(Subcommand, Debug)]
pub enum TaskCommand {
    /// List jobs, newest first.
    List,
    /// Show one job with per-item progress.
    Show {
        /// `job_…` id or 1-based ordinal (newest first); defaults to the newest.
        job: Option<String>,
        /// Include failed-item error details.
        #[arg(long)]
        errors: bool,
    },
    /// Cancel a job that has not finished; re-running its command resumes
    /// instead.
    Cancel {
        /// `job_…` id or 1-based ordinal.
        job: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Show resolved application directories and configuration.
    Show,
    /// Export the registry (connections + buckets, never credentials) as JSON.
    /// Prints to stdout or writes `--out`.
    Export {
        /// Write to this path instead of stdout.
        #[arg(long)]
        out: Option<String>,
    },
    /// Import a previously exported bundle. Existing names are
    /// skipped, stored credentials are never changed.
    Import {
        /// Path to a bundle produced by `config export`.
        path: String,
        /// Preview without adding anything.
        #[arg(long, short = 'n')]
        dry_run: bool,
    },
}
