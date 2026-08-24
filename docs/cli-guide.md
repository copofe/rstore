# rstore CLI Guide and Reference

rstore is a local-first workbench for S3-compatible storage: connect your own
endpoints (MinIO, Cloudflare R2, Supabase Storage, and others), index buckets
into a local catalog, and run everyday object work — upload, download, copy,
move, search, tagging, link generation.

The command tree is defined in `crates/cli/src/args.rs`, which is the source
of truth; `rstore <command> --help` always has the most authoritative flag
list. The first half of this guide is a tutorial in usage order; the second
half is the command reference.

---

## Tutorial

### 0. Conventions

- **Short references** (recommended): every command that accepts a remote
  location takes `<bucket>/[prefix/]<key>`, e.g. `media/img/logo.png`; the
  bucket root is `media/` or bare `media`. When the bucket name is ambiguous
  across connections, write `<connection>/<bucket>/…` (e.g.
  `nas/media/img/`).
- **Reference forms**: connections accept a name or a `conn_…` id; buckets
  accept a `bkt_…` id, `<connection>/<name>`, or a bare unique name; tasks
  accept a `job_…` id or the ordinal shown by `task list` (`1` = newest).
- **Output discipline**: results go to stdout; progress and logs go to
  stderr. Add `--json` for the machine-readable envelope.
- **Exit codes**: `0` success; `13` a batch in which any item failed;
  `130` interrupted by Ctrl+C; `14` local disk I/O failure; `11`
  capability unsupported (e.g. presigning). Every other failure maps to the
  code in the JSON envelope's `errors[].code`.
- **Destructive operations** (`rm`, `bucket remove`, `connection remove`,
  `catalog gc`, `tag merge`, `index rebuild`, …) confirm interactively;
  scripts and pipelines must pass `--yes`.

### 1. Connecting storage

```sh
# Register a connection (credentials are stored with the connection config,
# or entered interactively when omitted).
rstore connection add bucket_name \
  --endpoint http://localhost:9000 \
  --public-base https://public.example.com \
  --cdn-base https://cdn.example.com \
  --region auto \
  --access-key AK... --secret-key SK...

# Three-step check: stored credentials, authentication, ListBuckets.
rstore connection test bucket_name

# Common variants.
rstore connection list --filter prod          # fuzzy match on name/endpoint
rstore connection show bucket_name            # secrets shown masked
rstore connection show bucket_name --capabilities  # probe list/multipart/copy/presign
rstore connection edit bucket_name --region us-east-1
rstore connection remove bucket_name --yes    # removes only the registration
```

**Endpoint details**

- A trailing `/` on path-style endpoints (e.g. Supabase's
  `https://<ref>.storage.supabase.co/storage/v1/s3`) is normalized, but write
  the full URL anyway.
- `--addressing-style` controls where the bucket name sits in request URLs:
  - `auto` — let the SDK/service decide;
  - `path` — `https://endpoint/bucket/key`; gateways like Supabase usually
    require this;
  - `virtual-host` — `https://bucket.endpoint/key`; requires DNS and a TLS
    certificate covering the bucket subdomain.
  If connection tests pass but bucket operations return 404, check this
  first.
- Managed clouds are fine on the default `auto`.

**Credential storage**

- `--access-key/--secret-key` are written directly into the connection row of
  the local SQLite catalog.
- Interactive terminals prompt with hidden input when both flags are omitted;
  non-interactive runs and `--json` never prompt.
- `connection show` masks everything but the first and last characters.
  Plaintext credentials never enter exports, JSON output, or logs.
- Registering without credentials is allowed; tests and remote operations
  then report the missing credentials explicitly.

### 2. Registering buckets

Only explicitly registered buckets can be operated on — protection against
pointing at the wrong bucket:

```sh
# Discovery lists what the credentials can see.
rstore bucket discover bucket_name

# Registration probes readability; a failed probe warns but does not block.
rstore bucket add bucket_name media

# Local policy at registration time.
rstore bucket add bucket_name archive --read-only            # refuse writes
rstore bucket add bucket_name trash --allow-delete=false     # forbid deletes
rstore bucket add bucket_name tmp --index-enabled=false      # skip indexing
rstore bucket add bucket_name cold --conflict skip           # default policy

# Inspect and adjust.
rstore bucket list
rstore bucket show bucket_name/media
rstore bucket edit bucket_name/media --read-only=false
rstore bucket edit bucket_name/media --concurrency 8   # 0 clears the override
rstore bucket remove bucket_name/media --yes           # removes only the registration
```

**Ignore rules** skip directories during indexing:

```sh
rstore bucket ignore add bucket_name/media .trash
rstore bucket ignore list bucket_name/media
rstore bucket ignore remove bucket_name/media .trash
```

### 3. Working against the live remote

```sh
rstore remote ls media/                          # paginated listing
rstore remote ls media/img/ --limit 100
rstore remote ls media/ --cursor <token>         # next page
rstore remote ls media/ --update-catalog         # feed rows into the catalog

rstore remote stat media/img/logo.png
```

### 4. Upload and download

```sh
# Upload: target is <bucket>/[prefix/]; the file name lands under the prefix.
rstore upload ./report.pdf media/docs/
rstore upload ./report.pdf media/                # bucket root
rstore upload ./src media/backup/ -r             # recursive, keeps structure
rstore upload a.txt b.txt media/                 # multiple sources
rstore upload - media/ < files.txt               # batch: one local path per line

# Connection-qualified form when bucket names are ambiguous.
rstore upload ./report.pdf bucket_name/media/docs/

# Download: same source rules, local directory or file target.
rstore download media/img/logo.png ./out.png
rstore download media/img/ ./img-backup -r       # recursive mirror
rstore download - ./downloads < refs.txt

# Conflict policies (upload/download/cp/mv).
--conflict skip        # keep the existing object
--conflict overwrite   # replace it
--conflict rename      # derive name-<random8>.ext
--conflict ask         # interactive resolution
# Omitted: uploads/cp/mv use the target bucket's stored default; anything
# unresolved (including downloads writing to disk) behaves as ask.
--yes                  # resolves ask as overwrite non-interactively

--dry-run              # per-item preview, nothing transferred
--no-progress          # silence the stderr progress reporting
```

Large files switch to multipart automatically and resume from checkpoints.
Ctrl+C is safe at any point: progress persists as an interrupted job, and
re-running the same command resumes.

### 5. Copy, move, delete

```sh
# Same connection: server-side copy. Cross connection: bounded-memory relay.
rstore cp media/a.txt backup/a.txt
rstore cp media/a.txt backup/dir/       # directory-style target keeps the name
rstore mv media/old.bin media/archive/

rstore cp media/docs/ backup/docs/ -r
rstore mv media/old/ media/arc/ -r --keep-tags   # associations travel along

printf '%s\n' media/a.txt media/b.txt | rstore cp - backup/

# Delete: confirmation shows the exact precomputed count; removals persist
# tombstones into the catalog, and rm itself is an interruptible job.
rstore rm media/tmp/junk.bin
rstore rm media/tmp/ -r --yes
rstore rm media/tmp/ -r --dry-run
```

`mv` safety order: copy → verify target size → delete source. A failed
verification leaves the source untouched.

### 6. Indexing and the local catalog

Indexing scans buckets into the SQLite catalog; it feeds `files` and `tag`:

```sh
rstore index start bucket_name/media     # note: a bucket reference
rstore index status                      # per-bucket progress/checkpoints
rstore index pause bucket_name/media     # mark paused (Ctrl+C stops a live scan)
rstore index resume bucket_name/media    # continue from the checkpoint
rstore index cancel bucket_name/media    # stop; completed data stays

rstore index refresh bucket_name/media   # reconcile: remote-gone rows go stale
rstore index rebuild bucket_name/media --yes  # rescan (tag definitions survive)
rstore index stats                       # catalog size overview
rstore catalog gc --dry-run              # preview tombstone purge (30-day default)
```

Indexing is bucket-scoped; workspaces only scope retrieval (next section).
Without a workspace you can still feed the catalog via
`remote ls --update-catalog`, but `files`/`tags` need one.

### 7. Workspaces: scoping retrieval

A workspace is a named view over a set of member buckets; `files` and `tag`
are workspace-scoped:

```sh
rstore workspace create proj-a --description "Project A assets"
rstore workspace bucket add proj-a bucket_name/media
rstore workspace bucket add proj-a bucket_name/backup
rstore workspace bucket list proj-a

rstore workspace list
rstore workspace show proj-a
rstore workspace remove proj-a --yes    # buckets and remote data untouched
```

### 8. Search and tags

```sh
# Mixed listing across member buckets: SQL-level filters + cursor paging.
rstore files list proj-a
rstore files list proj-a --bucket bucket_name/media --path docs/
rstore files list proj-a --sort size:desc --limit 100
rstore files list proj-a --after <cursor>
rstore files list proj-a --untagged

rstore files search proj-a logo          # trigram name search

rstore files show media/img/logo.png --workspace proj-a

rstore tag create proj-a important --color ff0000
rstore tag add proj-a media/img/logo.png --tag important
rstore tag add proj-a - --tag draft < uris.txt        # batch
rstore tag add proj-a <uri> --tag a --tag b --replace # replace the whole set
rstore tag detach proj-a <uri> --tag draft            # omit --tag to clear all
rstore tag list proj-a                                # includes object counts
rstore tag merge proj-a old-name new-name --yes       # source tag disappears
rstore tag remove proj-a draft --yes
```

Tags are workspace-scoped: the same tag name in two projects never collides.

### 9. Link generation

```sh
rstore url media/img/logo.png                    # public URL from bucket config
rstore url media/img/logo.png --format markdown
rstore url <uri> --format s3                     # s3://bucket/key
rstore url <uri> --format path                   # bucket-relative path
rstore url <uri> --format html --json            # batch, machine-readable
rstore url - --format json < uris.txt            # stdin batch (# = comment)

# Presigned URLs (time-limited, up to 7 days).
rstore url media/report.pdf --signed --ttl 1h
rstore url media/report.pdf --signed             # bucket default TTL
```

Each URI succeeds or fails independently — one bad reference does not affect
the rest (human output stays one pipeable link per line; per-item errors go
to stderr). Any failure exits 13.

**Link configuration**: connections store public/CDN base defaults and each
bucket may override them; permissions and the default TTL belong to the
bucket. `bucket show` displays the effective value and marks values inherited
from the connection:

```sh
rstore connection edit supabase \
  --public-base https://<ref>.supabase.co/storage/v1/object/public \
  --cdn-base https://assets.example.com
rstore bucket link show bucket_name/media
rstore bucket link set bucket_name/media \
  --public-base https://public.example.com/media \
  --cdn-base https://cdn.example.com/media \
  --allow-presigned --default-ttl 15m
# The same five flags also work after `bucket edit`.
```

Effective public base precedence: bucket CDN override → connection CDN
default → bucket public override → connection public default → connection
endpoint. rstore never guesses provider-specific URL shapes; Supabase, for
example, needs its public base configured explicitly. Passing an empty string
to `--public-base`/`--cdn-base` clears the override. Presigning requires
bucket and adapter support and reports `capability_unsupported` otherwise.

### 10. Task management

Every upload/download/cp/mv/rm is a persisted job:

```sh
rstore task list                 # newest first; repair counts when present
rstore task show 1               # newest job, or a job_… id
rstore task show 1 --errors      # include failure details
rstore task retry 1              # re-arm a failed/partial job
rstore task cancel 3
rstore task pause 2 / rstore task resume 2
```

To actually resume work, re-issue the original command — transfers continue
from checkpoints. Pausing a live transfer inside another rstore process is
not supported yet (the process lock serializes runtimes).

### 11. Diagnostics, configuration, migration

```sh
rstore doctor          # adapter, connections (+untested), outbox/repairs, interrupts
rstore config show     # data/config/cache/log directories and db path

# Machine migration/backup of the registry — never contains credentials.
rstore config export > bundle.json
rstore config export --out bundle.json
rstore config import bundle.json --dry-run    # preview
rstore config import bundle.json              # duplicates are skipped, never overwritten
```

After migrating, re-enter credentials with
`connection edit NAME --access-key … --secret-key …` — excluding secrets from
bundles is deliberate.

### 12. Scripting cheat sheet

```sh
rstore ... --json | jq .data            # structured payload lives in data
some-cmd | rstore rm -                  # stdin batch URIs (# starts a comment)
some-cmd | rstore url - --format json
rstore ... --quiet                      # results only
rstore ... 2>/dev/null                  # stdout results, drop diagnostics
set -e; rstore upload ...               # any item failure exits 13
```

---

## Command reference

Global options on every command: `--config <PATH>` (data root), `--log-level
<LEVEL>` (default `warn`), `--json`, `-q/--quiet`.

| Command | Purpose | Key flags |
|---|---|---|
| `connection add <NAME>` | Register a connection | `--endpoint` `--public-base` `--cdn-base` `--region` `--addressing-style {auto,path,virtual-host}` `--access-key` `--secret-key` `--tls-verify` |
| `connection list` | List connections | `--filter <substring>` (case-insensitive name/endpoint) |
| `connection show <REF>` | Connection details | masked keys; `--capabilities` runs live probes |
| `connection test <REF>` | Three-step test | `--write-test` adds a write probe |
| `connection edit <REF>` | Edit mutable fields | `--name` `--endpoint` `--public-base` `--cdn-base` `--region` `--addressing-style` (plus re-entering keys) |
| `connection remove <REF>` | Remove registration | `--yes`; deletes stored credentials too |
| `bucket discover <CONN>` | List visible remote buckets | |
| `bucket add <CONN> <NAME>` | Register a bucket | `--display-name` `--read-only` `--allow-delete` `--index-enabled` `--conflict` `--skip-verify` |
| `bucket list` | Registered buckets | |
| `bucket show <REF>` / `bucket edit <REF>` | View/adjust settings | edit accepts the registration flags plus `--concurrency` and the five link flags |
| `bucket remove <REF>` | Remove registration | `--yes` |
| `bucket link show/set <REF>` | Bucket link profile | `--public-base` `--cdn-base` `--allow-public-url` `--allow-presigned` `--default-ttl`; only given flags change |
| `bucket ignore add/list/remove <REF> <DIR>` | Index ignore directories | |
| `workspace create <NAME>` | Create a workspace | `--description` |
| `workspace list/show/edit/remove` | Manage | remove requires `--yes` |
| `workspace bucket add/remove/list <WS>` | Membership | |
| `remote ls <URI>` | Live listing | short references; `--cursor` `--limit` `--update-catalog` |
| `remote stat <URI>` | Object metadata | |
| `index start <BUCKET>` | Scan into catalog | checkpointed; takes a **bucket reference** |
| `index status/stats` | State and sizes | |
| `index pause/resume/cancel <BUCKET>` | Scan control | |
| `index refresh <BUCKET>` | Reconcile stale rows | |
| `index rebuild <BUCKET>` | Full rescan | destructive confirmation; non-interactive requires `--yes` |
| `files list <WS>` | Mixed listing | `--path` `--bucket` (repeatable) `--sort <field>:<dir>` `--limit` `--after` `--untagged` |
| `files search <WS> <PATTERN>` | Name search | `--sort` `--limit` |
| `files show <URI>` | Object detail | `--workspace` |
| `tag create <WS> <NAME>` | Create a tag | `--color` `--description` |
| `tag list/edit/remove` | Manage | remove requires `--yes` |
| `tag merge <WS> <SRC> <DST>` | Merge tags | source removed, associations deduplicated; destructive confirmation, non-interactive requires `--yes` |
| `tag add <WS> <URIS…/- >` | Attach tags | `--tag` (repeatable) `--replace`; `-` reads stdin |
| `tag detach <WS> <URIS…/- >` | Detach tags | omitting `--tag` clears all |
| `upload <SOURCES…> <TARGET>` | Upload | `-r` `--conflict` `--dry-run` `--yes` `--no-progress`; `-` reads stdin |
| `download <SOURCES…> <TARGET>` | Download | multi-URI batches become per-URI subtasks |
| `cp <SOURCES…/-> <TARGET>` | Copy | `-r` `--conflict` `--dry-run` `--yes` `--keep-tags` |
| `mv <SOURCES…/-> <TARGET>` | Move | as cp; verifies target before deleting source |
| `rm <URIS…/->` | Delete | `-r` `--dry-run` `--yes` |
| `task list/show/pause/resume/retry/cancel` | Job management | show supports `--errors`; ordinals accepted |
| `url <URIS…/->` | Links | per-item independence; `--format path/s3/url/markdown/html/json` `--signed` `--ttl` |
| `catalog gc` | Purge tombstones | `--dry-run` `--retention 30m/1h/1d/2w/seconds` (max 365d); actual purge is destructive confirmation, non-interactive requires `--yes` |
| `config show/export/import` | Local configuration | export `--out`; import `--dry-run` skips duplicates |
| `doctor` | Health report | |
| `version` | Version | |

### Where data lives

| Contents | Location |
|---|---|
| Catalog (connections, buckets, workspaces, tags, index, jobs) | `catalog.db` (SQLite, WAL) — see `rstore config show` |
| Credentials | The connection table inside the catalog database |
| Logs | `<log dir>/` (daily rolling) |

### Troubleshooting

- **Connection test passes, bucket operations 404** — verify the endpoint is
  complete; gateways like Supabase need `--addressing-style path`.
- **`bucket add` warns "not readable" but registers** — the probe is
  non-blocking; run `remote ls` for the real error or pass `--skip-verify`.
- **Non-interactive delete/confirm errors** — destructive operations need
  `--yes` (including `catalog gc`, `tag merge`, `index rebuild`); an `ask`
  conflict policy needs `--yes` (resolved as overwrite) or a different
  `--conflict`.
- **Self-signed certificates / `--tls-verify=false`** — the setting is
  stored but not yet applied to a custom verifier; system defaults still
  apply.
- **After Ctrl+C** — the job persists as interrupted; re-run the original
  command to resume, `task list` to inspect.
- **Changing machines** — `config export/import` moves the registry;
  re-enter credentials afterwards because bundles exclude them.
