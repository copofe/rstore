# rstore CLI Guide and Reference

rstore is a local-first workbench for S3-compatible storage: connect your own
endpoints (MinIO, Cloudflare R2, Supabase Storage, and others), index buckets
into a local catalog, and run everyday object work — copy in every direction,
move, delete, search, tagging, link generation.

The command tree is defined in `crates/cli/src/args.rs`, which is the source
of truth; `rstore <command> --help` always has the most authoritative flag
list. The first half of this guide is a tutorial in usage order; the second
half is the command reference.

---

## Tutorial

### 0. Conventions

- **One reference grammar.** Every remote argument reads
  `[<connection>/]<bucket>/[key]`, e.g. `media/img/logo.png`; the bucket root
  is `media/`. Add the connection segment only when a bucket name exists on
  several connections (`nas/media/img/`). Bucket arguments use the same
  grammar without a key (`media`, `nas/media`, or a `bkt_…` id).
- **The current workspace.** Catalog commands (`find`, `tag`, `stat
  --offline`, `index run`, and the `workspace` subcommands without a name)
  act on the workspace from `-w/--workspace` (or `RSTORE_WORKSPACE`), else
  the current workspace, else the only one. Registering the first bucket
  creates a `default` workspace and selects it.
- **Verbs mean one thing.** `add`/`remove` change a registration or an
  association and never touch remote data; `create`/`delete` manage local
  entities (workspaces, tags); only `rm` deletes remote objects.
- **Output discipline**: results go to stdout; progress, warnings, and logs
  go to stderr. Add `--json` for the machine-readable envelope.
- **Confirmation**: destructive actions (`rm`, `bucket remove`, `connection
  remove`, `workspace delete`, `tag delete`, `tag merge`, `index run
  --rebuild`, `index gc`) prompt on a terminal; scripts pass the global
  `-y/--yes`. `-y` never means "overwrite" — that is `-f`.
- **Exit codes**: `0` success; `13` a batch in which any item failed; `130`
  interrupted by Ctrl+C; `6` endpoint unreachable (DNS, refused or dropped
  connection, timeout); `14` local disk I/O failure; `11` capability
  unsupported (e.g. presigning). Every other failure maps to the code in the
  JSON envelope's `errors[].code`.
- **Background verification**: commands that reach the remote anyway (`ls`
  with a reference, `stat`, `cp`, `mv`, `rm`, `index run`, `conn add/test`,
  `bucket add`, `bucket list --remote`) first spend at most two seconds
  re-checking earlier uploads, copies, and deletes; anything left waits for
  the next such command. Local-only commands never touch the network.

### 1. Connecting storage

```sh
# Guided setup: on a terminal, everything omitted is asked — name, provider,
# region (AWS, Supabase), endpoint, keys (hidden), then which of the visible
# buckets to register.
rstore conn add

# Register, test, and pick buckets in one step.
rstore conn add nas --endpoint http://localhost:9000 --provider minio

# Non-interactive: pass everything, including which buckets to register.
rstore conn add r2 --endpoint https://<account>.r2.cloudflarestorage.com \
  --provider r2 --access-key AK --secret-key SK --buckets all
rstore conn add supa --endpoint https://<ref>.storage.supabase.co/storage/v1/s3 \
  --provider supabase --access-key AK --secret-key SK --buckets media,avatars

# Everything else.
rstore conn list prod                      # substring filter on name/endpoint
rstore conn show nas                       # secrets shown masked
rstore conn test nas --write               # put+delete in each writable registered bucket
rstore conn edit nas --region us-east-1
rstore conn remove nas -y                  # removes only the registration
```

**Provider presets** fill the addressing style: `minio` and `supabase` use
path-style requests, `aws`, `r2`, and `generic` let the SDK decide.
`--addressing-style auto|path|virtual-host` overrides the preset. If the
connection test passes but bucket operations return 404, check this first.

**The test** lists buckets with the stored keys. `endpoint/auth` passes only
when the endpoint answered and accepted the credentials (a refused listing
still counts as authenticated); an unreachable endpoint or rejected keys fail
it. `--write` checks the keys can write: it puts and deletes a temporary
object under `.rstore-write-test/` in each registered bucket that allows
writes and deletes. Read-only, delete-forbidden, and unregistered buckets are
never written to — with none left, the check fails without writing anything.
An operation a provider cannot perform (presigning, server-side copy) fails
on its own with `capability_unsupported` (exit 11).

**After the test**, `conn add` registers buckets: the `--buckets` list, every
visible bucket for `--buckets all`, or your choice on a terminal. A failing
test or a missing ListBuckets permission degrades to a warning — the
connection stays registered; add buckets by name later. `--no-test` skips
the test and the interactive choice.

**Credential storage**

- Keys are written into the connection row of the local database (`rstore.db`).
- Terminals prompt with hidden input when both key flags are omitted;
  non-interactive runs and `--json` never prompt.
- `conn show` masks everything but the first and last characters.
  Plaintext credentials never enter exports, JSON output, or logs.

### 2. Registering buckets

Only registered buckets can be operated on — protection against pointing at
the wrong bucket. Registered buckets join the current workspace.

```sh
rstore bucket list --remote                # what the credentials can see
rstore bucket list --remote nas            # one connection only

rstore bucket add                          # terminal: choose buckets, give each a public URL
rstore bucket add media photos             # connection optional while only one exists
rstore bucket add nas/archive --read-only  # refuse writes
rstore bucket add tmp --no-delete --conflict skip

rstore bucket list                         # registered buckets
rstore bucket show media                   # settings, link profile, ignore rules
rstore bucket edit media --read-only=false --concurrency 8   # 0 clears the override
rstore bucket edit media --ignore .trash --ignore cache/     # skip when indexing
rstore bucket edit media --unignore cache/
rstore bucket remove tmp archive -y        # removes only the registrations
```

### 3. Working against the live remote

```sh
rstore ls                                  # registered buckets, as browse roots
rstore ls media/                           # paginated listing
rstore ls media/img/ --limit 100
rstore ls media/ --cursor <token>          # next page
rstore ls media/ --catalog                 # also record the rows in the catalog

rstore stat media/img/logo.png             # live metadata
```

### 4. Copy, move, delete

`cp` covers uploads, downloads, and remote copies. The direction comes from
the arguments:

- `./…`, `../…`, `~/…` and absolute paths are always local;
- otherwise an argument is remote when its first segment names a registered
  bucket or connection, and local when it names neither;
- if the first segment is registered **and** exists as a local path, the
  command stops and asks you to write `./…` (or qualify the remote with its
  connection) instead of guessing.

```sh
rstore cp ./report.pdf media/docs/          # upload; trailing / keeps the name
rstore cp ./src media/backup/ -r            # recursive, keeps structure
rstore cp a.txt b.txt media/                # several sources
rstore cp media/img/logo.png ./out.png      # download
rstore cp media/img/ ./img-backup -r        # recursive mirror
rstore cp media/a.txt backup/a.txt          # remote copy (server-side on one connection)
rstore cp nas/media/ r2/media/ -r --keep-tags   # cross connection: bounded-memory relay
printf '%s\n' media/a.txt media/b.txt | rstore cp - backup/

rstore mv media/old.bin media/archive/      # remote → remote only
rstore mv media/old/ media/arc/ -r --keep-tags

rstore rm media/tmp/junk.bin
rstore rm media/tmp/ -r -y                  # confirmation shows the exact count
rstore rm media/tmp/ -r -n                  # dry run
```

**Conflicts** (`cp`/`mv`): `-f` overwrites; `--conflict skip|overwrite|
rename|ask` picks a policy. Omitted, uploads and remote copies use the target
bucket's default; anything still `ask` prompts on a terminal and fails
non-interactively. `-n/--dry-run` previews per item; `--no-progress`
silences the stderr progress line.

Large files switch to multipart automatically and resume from checkpoints.
Ctrl+C is safe at any point: progress persists as an interrupted job, and
re-running the same command resumes.

`mv` safety order: copy → verify target size → delete source. A failed
verification leaves the source untouched. `rm` drops the object's catalog
row and tags, and is itself an interruptible job.

### 5. Indexing the catalog

Indexing scans buckets into the SQLite catalog so `find` can search them by
name and attributes. It is opt-in per bucket and off by default: naming a
bucket in `index run` switches its index on (`bucket edit B --index=false`
switches it off and drops its untagged rows). Tags need no index.

```sh
rstore index run media photos              # index these (switches them on)
rstore index run                           # refresh the workspace's indexed buckets
rstore index run --all                     # every indexed bucket
rstore index run media --refresh           # rescan; rows gone remotely turn stale
rstore index run media --rebuild -y        # drop untagged rows and rescan

rstore index status                        # catalog totals + per-bucket state
rstore index cancel media                  # drop a paused scan's checkpoint
rstore index gc -n                         # count untagged stale rows
rstore index gc -y                         # remove them (tagged rows stay)
```

Ctrl+C pauses the running scan at a checkpoint and skips the remaining
buckets; `index run` resumes from there. Without a workspace you can still
feed the catalog through `ls --catalog`.

### 6. Workspaces

A workspace is a named view over a set of member buckets; `find` and `tag`
are scoped to it, so the same tag name in two projects never collides.

```sh
rstore ws list                             # `*` marks the current workspace
rstore ws create                           # terminal: asks name, description, buckets
rstore ws create proj-a --description "Project A assets"
rstore ws use proj-a                       # make it current
rstore ws add                              # terminal: choose from the registered buckets
rstore ws add media nas/backup             # membership of the current workspace
rstore ws remove nas/backup
rstore ws show                             # buckets and object count
rstore ws show -w default                  # any other workspace, once
rstore ws edit --name proj-alpha
rstore ws delete proj-alpha -y             # buckets and remote data untouched
```

### 7. Search and tags

```sh
rstore find                                # everything, newest first
rstore find logo                           # name contains "logo"
rstore find -b media --path docs/ --sort size:desc --limit 100
rstore find --type image --ext png
rstore find -t brand,final                 # carries every tag
rstore find -t brand -t draft --any-tag    # carries either
rstore find --untagged
rstore find --after <cursor>               # next page

rstore stat media/img/logo.png --offline   # catalog entry with tags, no network

rstore tag add media/img/logo.png -t brand,final   # unknown tags are created on first use
rstore tag add - -t draft < uris.txt               # batch
rstore tag add <ref> -t a,b --replace              # replace the whole set
rstore tag remove <ref> -t draft                   # omit -t to clear all
rstore tag list                                    # with object counts
rstore tag edit brand --color ff0000 --description "Brand assets"
rstore tag merge old-name new-name -y              # source tag is deleted
rstore tag delete draft -y
```

Any object can be tagged, indexed or not: `tag add` confirms the object
exists (a live `stat` when the catalog does not know it yet). Tag queries
therefore cover the whole workspace, while a name or attribute search covers
indexed buckets only — `find` lists the buckets it could not search under
`not indexed:` (`unindexed` in JSON).

A tag created by `tag add` is reported as a warning, so a mistyped name is
noticed. Creation waits until an object resolves: a batch that tags nothing
creates nothing.

### 8. Link generation

```sh
rstore url media/img/logo.png                    # public URL; presigned when no public base
rstore url media/img/logo.png --format markdown
rstore url <ref> --format s3                     # s3://bucket/key
rstore url <ref> --format path                   # bucket-relative path
rstore url <ref> --format html --json            # batch, machine-readable
rstore url - --format json < uris.txt            # stdin batch (# = comment)

rstore url media/report.pdf --ttl 1h             # presigned (--ttl implies --signed)
rstore url media/report.pdf --signed             # bucket default TTL
```

Each reference succeeds or fails independently (human output stays one
pipeable link per line; per-item errors go to stderr). Any failure exits 13.

**Link configuration**: a bucket's public/CDN base is the URL serving that
bucket's root (a custom domain, an r2.dev address, Supabase's
`…/object/public/<bucket>`); a connection's base serves every bucket under
`/<bucket>`, and a bucket base overrides it. Permissions and the default TTL
belong to the bucket. Guided `bucket add` (and the bucket choice in
`conn add`) asks each bucket's public URL, with the provider's shape as an
example; an empty answer keeps a private bucket's links presigned. `bucket
show` displays the effective root and marks one inherited from the
connection:

```sh
rstore conn edit supa \
  --public-base https://<ref>.supabase.co/storage/v1/object/public \
  --cdn-base https://assets.example.com
rstore bucket edit media \
  --public-base https://public.example.com/media \
  --cdn-base https://cdn.example.com/media \
  --presigned --public-url=false --default-ttl 15m
```

Effective public base precedence: bucket CDN override → connection CDN
default → bucket public override → connection public default. The S3 endpoint
is never used as a public base — R2, Supabase, and private buckets refuse
unsigned reads there — so with no base configured, `url` hands out a
presigned link (bucket default TTL) and a warning says how to set a permanent
one. rstore never guesses provider-specific URL shapes. An empty
`--public-base`/`--cdn-base` clears the override. Presigning requires bucket
and adapter support and reports `capability_unsupported` otherwise.

### 9. Jobs

Every `cp`/`mv`/`rm` is a persisted job:

```sh
rstore task list                 # newest first
rstore task show                 # the newest job; items update while it runs
rstore task show 3 --errors      # by ordinal or job_… id, with failure details
rstore task cancel 3             # cancel an unfinished job, wherever it runs
```

Several rstore processes may share one data directory. A running job holds a
lease its process renews; `task cancel` on a job another process runs asks
that process to stop at its next checkpoint (within about ten seconds) and the
job ends `cancelled`. A job whose process died is marked `interrupted` the
next time rstore starts. To resume an interrupted transfer, re-issue the
original command — transfers continue from their checkpoints.

### 10. Diagnostics, configuration, migration

```sh
rstore doctor          # adapter, connections (+untested), stale rows, interrupts
rstore config show     # database and log locations

# Machine migration/backup of the registry — never contains credentials.
rstore config export > bundle.json
rstore config export --out bundle.json
rstore config import bundle.json -n           # preview
rstore config import bundle.json              # duplicates are skipped, never overwritten
```

After migrating, re-enter credentials with
`conn edit NAME --access-key … --secret-key …` — excluding secrets from
bundles is deliberate.

### 11. Scripting cheat sheet

```sh
rstore ... --json | jq .data            # structured payload lives in data
some-cmd | rstore rm - -y               # stdin batch references (# starts a comment)
some-cmd | rstore url - --format json
rstore -w proj-a find --json            # pin the workspace instead of relying on `ws use`
RSTORE_WORKSPACE=proj-a rstore tag list
rstore ... --quiet                      # results only
set -e; rstore cp ./dist/ media/site/ -r -f   # any item failure exits 13
```

Scripts should pass `-w` (or set `RSTORE_WORKSPACE`) and write local paths
with `./`, so neither the current workspace nor a same-named local directory
can change what a command does.

---

## Command reference

Global options on every command: `--home <PATH>` (data root, env `RSTORE_HOME`),
`--log-level <LEVEL>` (default `warn`), `--json`, `-q/--quiet`,
`-w/--workspace <WS>` (env `RSTORE_WORKSPACE`), `-y/--yes`.

| Command | Purpose | Key flags |
|---|---|---|
| `ls [REF]` | Remote listing; registered buckets without `REF` | `--limit` `--cursor` `--catalog` |
| `stat REF` | Object metadata | `--offline` reads the catalog (tags, staleness) |
| `cp SRC… DST` | Upload, download, or remote copy, routed by the arguments | `-r` `-n` `-f` `--conflict` `--keep-tags` `--no-progress`; `-` reads stdin |
| `mv SRC… DST` | Move between remote locations | as `cp`; verifies the target before deleting the source |
| `rm REF…` | Delete remote objects | `-r` `-n`; `-` reads stdin |
| `url REF…` | Links | `--format path/s3/url/markdown/html/json` `--signed` `--ttl` |
| `find [PATTERN]` | Catalog search in the workspace | `-b` (repeatable) `--path` `-t` `--any-tag` `--untagged` `--type` `--ext` `--sort <field>:<dir>` `--limit` `--after` |
| `tag add REF… -t TAG…` | Attach tags, creating missing ones | `--replace`; `-` reads stdin |
| `tag remove REF…` | Detach tags | `-t` (omit to clear all) |
| `tag list` / `tag edit TAG` | Tag definitions | edit: `--name` `--color` `--description` |
| `tag delete TAG` | Delete a tag | destructive |
| `tag merge SRC DST` | Merge tags | source deleted, associations deduplicated; destructive |
| `connection add [NAME]` (`conn`) | Register, test, register buckets; asks for what is omitted on a terminal | `--endpoint` `--provider` `--region` `--addressing-style` `--public-base` `--cdn-base` `--tls-verify` `--access-key` `--secret-key` `--buckets all\|A,B` `--no-test` |
| `connection list [FILTER]` | List connections | substring over name/endpoint |
| `connection show CONN` | Details, masked keys | |
| `connection test CONN` | Three-step test | `--write` probes each registered bucket that allows writes and deletes; failure exits 13 |
| `connection edit CONN` | Edit fields | as `add` (plus re-entering keys) |
| `connection remove CONN` | Remove registration | destructive; deletes stored credentials too |
| `bucket list [CONN]` | Registered buckets | `--remote` lists what the credentials can see |
| `bucket add [BUCKET…]` | Register and join the workspace; none → choose on a terminal | `--read-only` `--no-delete` `--conflict` `--skip-verify` |
| `bucket show BUCKET` | Settings, link profile, ignore rules | |
| `bucket edit BUCKET` | Adjust settings | `--display-name` `--read-only[=B]` `--allow-delete[=B]` `--index[=B]` `--conflict` `--concurrency` `--public-base` `--cdn-base` `--public-url[=B]` `--presigned[=B]` `--default-ttl` `--ignore DIR` `--unignore DIR` |
| `bucket remove BUCKET…` | Remove registrations | destructive |
| `workspace list` (`ws`) | Workspaces, `*` = current | |
| `workspace use WS` | Select the current workspace | |
| `workspace create [NAME]` | Create; current when none is selected; none → guided on a terminal | `--description` |
| `workspace show` / `edit` | The target workspace | edit: `--name` `--description` |
| `workspace delete WS` | Delete a workspace | destructive |
| `workspace add\|remove BUCKET…` | Membership | `add` without buckets chooses on a terminal |
| `index run [BUCKET…]` | Scan into the catalog; naming a bucket switches its index on | `--all` `--refresh` `--rebuild` (destructive) |
| `index status` | Totals and per-bucket state | |
| `index cancel BUCKET` | Drop a paused scan's checkpoint | |
| `index gc` | Remove untagged stale rows | `-n`; destructive |
| `task list` / `task show [JOB]` / `task cancel JOB` | Jobs; cancel reaches other processes | show: `--errors`; ordinals accepted |
| `config show\|export\|import` | Local configuration | export `--out`; import `-n` |
| `doctor` | Health report | |
| `version` | Version | |

### Where data lives

| Contents | Location |
|---|---|
| Connections, buckets, workspaces, tags, jobs, and the catalog cache | `rstore.db` (SQLite, WAL) |
| Credentials | The connection table inside `rstore.db` |
| Current workspace | The `workspace.current` setting inside `rstore.db` |
| Logs | The log directory, rolling daily |

Default locations — macOS: `~/Library/Application Support/rstore/rstore.db`,
logs in `~/Library/Logs/rstore`; Linux: `~/.local/share/rstore/rstore.db`,
logs in `~/.local/state/rstore`; Windows: `%LOCALAPPDATA%\rstore\rstore.db`,
logs in `%LOCALAPPDATA%\rstore\log`. `--home <PATH>` or `RSTORE_HOME` moves
both under one directory (`<PATH>/rstore.db`, `<PATH>/log/`); `rstore config
show` prints the resolved paths.

### Troubleshooting

- **Connection test passes, bucket operations 404** — verify the endpoint is
  complete; gateways like Supabase need path-style requests
  (`--provider supabase` or `--addressing-style path`).
- **`bucket add` warns "not readable" but registers** — the probe is
  non-blocking; run `ls <bucket>/` for the real error or pass `--skip-verify`.
- **"N workspaces exist and none is selected"** — run `rstore ws use
  <name>` once, or pass `-w <name>`.
- **"`…` is ambiguous"** — a local file or directory shares its name with a
  registered bucket; write `./name` for the local path.
- **Non-interactive confirm errors** — destructive operations need `-y`; an
  unresolved `ask` conflict policy needs `-f` or `--conflict`.
- **Self-signed certificates / `--tls-verify=false`** — the setting is
  stored but not yet applied to a custom verifier; system defaults still
  apply.
- **After Ctrl+C** — the job persists as interrupted; re-run the original
  command to resume, `task list` to inspect.
- **Changing machines** — `config export/import` moves the registry;
  re-enter credentials afterwards because bundles exclude them.
