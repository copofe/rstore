-- rstore database schema.
-- Timestamps are INTEGER unix seconds (UTC).
--
-- The catalog is a cache: it holds every object of an indexed bucket plus any
-- tagged object, and the provider stays the source of truth.

-- Registry context ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS storage_connections (
    id                TEXT PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,
    endpoint          TEXT NOT NULL,
    public_base_url   TEXT,
    cdn_base_url      TEXT,
    region            TEXT NOT NULL DEFAULT 'auto',
    addressing_style  TEXT NOT NULL DEFAULT 'auto',
    tls_verify        INTEGER NOT NULL DEFAULT 1,
    access_key        TEXT,
    secret_key        TEXT,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    last_tested_at    INTEGER,
    last_test_status  TEXT
);

CREATE TABLE IF NOT EXISTS buckets (
    id                TEXT PRIMARY KEY,
    connection_id     TEXT NOT NULL REFERENCES storage_connections(id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    display_name      TEXT,
    read_only         INTEGER NOT NULL DEFAULT 0,
    allow_delete      INTEGER NOT NULL DEFAULT 1,
    index_enabled     INTEGER NOT NULL DEFAULT 0,
    default_conflict  TEXT NOT NULL DEFAULT 'ask',
    concurrency       INTEGER,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    UNIQUE(connection_id, name)
);
CREATE INDEX IF NOT EXISTS idx_buckets_connection ON buckets(connection_id);

CREATE TABLE IF NOT EXISTS link_profiles (
    bucket_id            TEXT PRIMARY KEY REFERENCES buckets(id) ON DELETE CASCADE,
    public_base_url      TEXT,
    cdn_base_url         TEXT,
    allow_public_url     INTEGER NOT NULL DEFAULT 0,
    allow_presigned      INTEGER NOT NULL DEFAULT 0,
    default_ttl_seconds  INTEGER NOT NULL DEFAULT 3600
);

CREATE TABLE IF NOT EXISTS app_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Catalog context -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspaces (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_workspaces_name_ci ON workspaces(lower(name));

CREATE TABLE IF NOT EXISTS workspace_buckets (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    bucket_id    TEXT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    added_at     INTEGER NOT NULL,
    PRIMARY KEY(workspace_id, bucket_id)
);

CREATE TABLE IF NOT EXISTS bucket_ignore_rules (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id  TEXT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    directory  TEXT NOT NULL,
    enabled    INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    UNIQUE(bucket_id, directory)
);

CREATE TABLE IF NOT EXISTS remote_objects (
    id             TEXT PRIMARY KEY,
    connection_id  TEXT NOT NULL REFERENCES storage_connections(id) ON DELETE CASCADE,
    bucket_id      TEXT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    object_path    TEXT NOT NULL,
    size           INTEGER NOT NULL DEFAULT 0,
    etag           TEXT,
    content_type   TEXT,
    last_modified  INTEGER,
    storage_class  TEXT,
    version_id     TEXT,
    confirmed_at   INTEGER NOT NULL,
    state          TEXT NOT NULL DEFAULT 'confirmed',
    file_type      TEXT NOT NULL DEFAULT 'other',
    UNIQUE(connection_id, bucket_id, object_path)
);
CREATE INDEX IF NOT EXISTS idx_remote_objects_bucket ON remote_objects(bucket_id);
CREATE INDEX IF NOT EXISTS idx_remote_objects_path ON remote_objects(object_path);

CREATE TABLE IF NOT EXISTS index_runs (
    id           TEXT PRIMARY KEY,
    bucket_id    TEXT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    state        TEXT NOT NULL,
    started_at   INTEGER NOT NULL,
    finished_at  INTEGER,
    objects_seen INTEGER NOT NULL DEFAULT 0,
    error_count  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS index_checkpoints (
    bucket_id          TEXT PRIMARY KEY REFERENCES buckets(id) ON DELETE CASCADE,
    continuation_token TEXT,
    updated_at         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tags (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    color           TEXT NOT NULL DEFAULT 'gray',
    description     TEXT,
    created_at      INTEGER NOT NULL,
    UNIQUE(workspace_id, normalized_name)
);
CREATE INDEX IF NOT EXISTS idx_tags_workspace ON tags(workspace_id);

-- Tags key on (bucket, path), so an object can be tagged without being
-- indexed and a tag outlives its cached row.
CREATE TABLE IF NOT EXISTS object_tags (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    bucket_id    TEXT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    object_path  TEXT NOT NULL,
    tag_id       TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    applied_at   INTEGER NOT NULL,
    PRIMARY KEY (workspace_id, bucket_id, object_path, tag_id)
);
CREATE INDEX IF NOT EXISTS idx_object_tags_object ON object_tags(bucket_id, object_path);
CREATE INDEX IF NOT EXISTS idx_object_tags_tag ON object_tags(tag_id);

-- FTS is part of the database schema.
CREATE VIRTUAL TABLE IF NOT EXISTS remote_objects_fts USING fts5(path, tokenize='trigram');

CREATE TRIGGER IF NOT EXISTS remote_objects_fts_ai AFTER INSERT ON remote_objects BEGIN
    INSERT INTO remote_objects_fts(rowid, path) VALUES (new.rowid, new.object_path);
END;

CREATE TRIGGER IF NOT EXISTS remote_objects_fts_au AFTER UPDATE OF object_path ON remote_objects BEGIN
    DELETE FROM remote_objects_fts WHERE rowid = old.rowid;
    INSERT INTO remote_objects_fts(rowid, path) VALUES (new.rowid, new.object_path);
END;

CREATE TRIGGER IF NOT EXISTS remote_objects_fts_ad AFTER DELETE ON remote_objects BEGIN
    DELETE FROM remote_objects_fts WHERE rowid = old.rowid;
END;

-- Transfer context ----------------------------------------------------------
-- A running job belongs to the process renewing its lease; a lapsed lease
-- marks it orphaned. Cancellation from another process is a request the
-- owner polls on its heartbeat.
CREATE TABLE IF NOT EXISTS transfer_jobs (
    id               TEXT PRIMARY KEY,
    kind             TEXT NOT NULL,
    state            TEXT NOT NULL,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    error            TEXT,
    lease_owner      TEXT,
    lease_expires_at INTEGER,
    cancel_requested INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS transfer_job_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL REFERENCES transfer_jobs(id) ON DELETE CASCADE,
    label       TEXT NOT NULL,
    state       TEXT NOT NULL,
    bytes_total INTEGER NOT NULL DEFAULT 0,
    bytes_done  INTEGER NOT NULL DEFAULT 0,
    error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_items_job ON transfer_job_items(job_id);

CREATE TABLE IF NOT EXISTS multipart_uploads (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id              TEXT REFERENCES transfer_jobs(id) ON DELETE CASCADE,
    connection_id       TEXT NOT NULL,
    bucket              TEXT NOT NULL,
    key                 TEXT NOT NULL,
    upload_id           TEXT NOT NULL,
    created_at          INTEGER NOT NULL,
    source_size         INTEGER NOT NULL DEFAULT 0,
    source_modified_ns  INTEGER,
    UNIQUE(connection_id, bucket, key)
);

CREATE TABLE IF NOT EXISTS multipart_parts (
    multipart_upload_id INTEGER NOT NULL REFERENCES multipart_uploads(id) ON DELETE CASCADE,
    part_number         INTEGER NOT NULL,
    etag                TEXT,
    size                INTEGER NOT NULL,
    uploaded_at         INTEGER NOT NULL,
    PRIMARY KEY(multipart_upload_id, part_number)
);
