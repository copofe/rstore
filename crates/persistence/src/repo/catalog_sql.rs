//! The internal SQL builder behind `CatalogQuery` (MIX-001..024).
//!
//! One place owns everything dynamic about the mixed file list: filter
//! predicates, LIKE escaping, sort expressions (including the `name` rtrim
//! trick), the keyset cursor format and its typed comparison, and binding
//! values sqlx cannot express against `format!`-built SQL. Repository methods
//! declare *what* they want (`select`/`count` + `ordered_after` +
//! `fetch`); the *how* stays here, so the future FTS index (IDX-023) swaps
//! this module out without touching callers or the `CatalogQuery` seam.

use rstore_catalog::{FileFilter, FileSort, SortField, SortOrder};
use rstore_domain::{Result, WorkspaceId};

use crate::codec::uuid_to_text;

/// The SQLite name of a sortable field plus its ORDER BY expression (MIX-023).
/// `name` sorts by the path's file-name component (the SQLite rtrim trick).
const NAME_EXPR: &str =
    "replace(object_path, rtrim(object_path, replace(object_path, '/', '')), '')";

/// Column list for the joined query, `ro.`-qualified (search/count join
/// `workspace_buckets`, so bare names would be ambiguous).
pub(super) fn columns_qualified(columns: &str) -> String {
    columns
        .split(", ")
        .map(|c| format!("ro.{c}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn sort_expr(field: SortField) -> &'static str {
    use SortField as F;
    match field {
        F::Name => NAME_EXPR,
        F::Size => "ro.size",
        F::Modified => "ro.last_modified",
        // The domain classifier column (MIX-015) — same source of truth the
        // `file_type` filter reads; `content_type` is provider-supplied and
        // often NULL.
        F::Type => "ro.file_type",
        // `ro.`-qualified: the workspace join also carries a bucket_id, and
        // an ambiguous name is a runtime error, not a plan-time one.
        F::Bucket => "ro.bucket_id",
        F::Connection => "ro.connection_id",
    }
}

/// A bind value for dynamically assembled SQL. sqlx cannot bind into
/// `format!`-built text, so the builder collects typed values and applies
/// them positionally at fetch time.
enum Bind {
    Text(String),
    Integer(i64),
}

/// A `CatalogQuery` statement under construction: SQL text plus positional
/// bind values, extended declaratively by the repository.
pub(super) struct CatalogSql {
    sql: String,
    params: Vec<Bind>,
}

impl CatalogSql {
    /// `SELECT {columns} …` scoped to a workspace, filters applied.
    pub(super) fn select(
        columns: &str,
        scope_workspace: &WorkspaceId,
        filter: &FileFilter,
    ) -> Result<Self> {
        let mut sql = String::from(
            "SELECT {COLUMNS} FROM remote_objects ro \
             JOIN workspace_buckets wb ON wb.bucket_id = ro.bucket_id \
             AND wb.workspace_id = ? ",
        );
        let mut params = vec![Bind::Text(uuid_to_text(scope_workspace))];
        push_filters(&mut sql, &mut params, scope_workspace, filter)?;
        let mut this = Self { sql, params };
        this.sql = this.sql.replace("{COLUMNS}", &columns_qualified(columns));
        Ok(this)
    }

    /// `SELECT COUNT(*) …` with the same scoping and filters.
    pub(super) fn count(scope_workspace: &WorkspaceId, filter: &FileFilter) -> Result<Self> {
        let mut sql = String::from(
            "SELECT COUNT(*) AS n FROM remote_objects ro \
             JOIN workspace_buckets wb ON wb.bucket_id = ro.bucket_id \
             AND wb.workspace_id = ? ",
        );
        let mut params = vec![Bind::Text(uuid_to_text(scope_workspace))];
        push_filters(&mut sql, &mut params, scope_workspace, filter)?;
        Ok(Self { sql, params })
    }

    /// `SELECT` for the fully assembled files page. The page is still scoped
    /// and filtered by the same predicates as [`Self::select`], but the
    /// adapter also joins source identities and aggregates workspace tag
    /// names. The caller obtains the total from [`Self::count`] before adding
    /// the cursor predicate, so a cursor can never change its meaning.
    pub(super) fn page(
        columns: &str,
        scope_workspace: &WorkspaceId,
        filter: &FileFilter,
    ) -> Result<Self> {
        let mut sql = String::from(
            "SELECT {COLUMNS}, b.name AS bucket_name, c.name AS connection_name, \
             COALESCE((SELECT json_group_array(tag_name) FROM ( \
                 SELECT t.name AS tag_name \
                 FROM remote_object_tags rot \
                 JOIN tags t ON t.id = rot.tag_id \
                 WHERE rot.workspace_id = wb.workspace_id \
                   AND rot.remote_object_id = ro.id \
                 ORDER BY t.normalized_name \
             )), '[]') AS tags_json \
             FROM remote_objects ro \
             JOIN workspace_buckets wb ON wb.bucket_id = ro.bucket_id \
             AND wb.workspace_id = ? \
             JOIN buckets b ON b.id = ro.bucket_id \
             JOIN storage_connections c ON c.id = ro.connection_id ",
        );
        let mut params = vec![Bind::Text(uuid_to_text(scope_workspace))];
        push_filters(&mut sql, &mut params, scope_workspace, filter)?;
        let mut this = Self { sql, params };
        this.sql = this.sql.replace("{COLUMNS}", &columns_qualified(columns));
        Ok(this)
    }

    /// Keyset pagination: keep rows strictly after the cursor's
    /// `(sort value, path, id)`. The cursor's encoding is private to this
    /// module; callers only ever pass the opaque string through.
    pub(super) fn ordered_after(mut self, sort: FileSort, cursor: &str) -> Result<Self> {
        // The one cursor vocabulary: decode yields the typed sort value the
        // keyset comparison binds by (numeric fields as INTEGER, like the
        // ordered column).
        let (value, path, id) = rstore_catalog::order::decode(cursor, sort.field)?;
        let typed = match value {
            rstore_catalog::order::SortKey::Num(n) => Bind::Integer(n),
            rstore_catalog::order::SortKey::Text(t) => Bind::Text(t),
        };
        let expr = sort_expr(sort.field);
        // Walk direction: a DESC page continues *down* the total order, so
        // the keyset comparison flips with the sort direction.
        let cmp = if sort.order == SortOrder::Asc {
            ">"
        } else {
            "<"
        };
        self.sql.push_str(&format!(
            " AND ({expr}, ro.object_path, ro.id) {cmp} (?, ?, ?) "
        ));
        self.params.push(typed);
        self.params.push(Bind::Text(path));
        self.params.push(Bind::Text(id));
        Ok(self)
    }

    /// Terminal ordering + `LIMIT n+1` (the extra row detects `has_more`).
    pub(super) fn ordered(mut self, sort: FileSort, limit: u32) -> Self {
        self.order_and_limit(sort, limit + 1);
        self
    }

    fn order_and_limit(&mut self, sort: FileSort, limit: u32) {
        let dir = if sort.order == SortOrder::Asc {
            "ASC"
        } else {
            "DESC"
        };
        // path+id are the stable secondary sort (MIX-023) in the SAME
        // direction so the cursor comparison stays consistent. Direction
        // applies to every key column — SQL binds `ASC`/`DESC` to one column
        // at a time, so it must be repeated.
        self.sql.push_str(&format!(
            " ORDER BY {expr} {dir}, ro.object_path {dir}, ro.id {dir} LIMIT {limit}",
            expr = sort_expr(sort.field),
            dir = dir,
            limit = limit
        ));
    }

    /// Run the assembled statement and return all rows.
    pub(super) async fn fetch<'e, E>(self, executor: E) -> Result<Vec<sqlx::sqlite::SqliteRow>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query = sqlx::query(&self.sql);
        for p in &self.params {
            query = match p {
                Bind::Text(t) => query.bind(t.as_str()),
                Bind::Integer(i) => query.bind(*i),
            };
        }
        query
            .fetch_all(executor)
            .await
            .map_err(crate::error::map_sqlx)
    }
}

/// Append filter predicates (MIX-011..022). `state` defaults to "not deleted".
fn push_filters(
    sql: &mut String,
    params: &mut Vec<Bind>,
    workspace: &WorkspaceId,
    filter: &FileFilter,
) -> Result<()> {
    match filter.state {
        Some(state) => sql.push_str(&format!(" AND ro.state = '{}' ", state.as_str())),
        None => sql.push_str(" AND ro.state != 'deleted' "),
    }
    if let Some(name) = &filter.name_contains {
        // IDX-023: token search through the FTS index instead of a LIKE
        // scan. Each whitespace-separated term becomes a quoted prefix
        // match, ANDed together — user input is never parsed as FTS syntax.
        if name.chars().count() >= 3 {
            // IDX-023: the trigram index narrows candidates to paths
            // containing the substring; the LIKE clause keeps the exact
            // MIX-011 semantics (a name containing the substring implies
            // its path does too, so the prefilter never drops a match).
            // The query is a quoted FTS string — input is never parsed as
            // FTS syntax.
            let quoted = format!("\"{}\"", name.replace('"', "\"\""));
            sql.push_str(&format!(
                " AND ro.rowid IN (SELECT rowid FROM remote_objects_fts \
                   WHERE remote_objects_fts MATCH ?) \
                   AND {NAME_EXPR} LIKE ? ESCAPE '\\' "
            ));
            params.push(Bind::Text(quoted));
            params.push(Bind::Text(format!("%{}%", escape_like(name))));
        } else {
            // Trigram needs three characters; shorter needles LIKE-scan.
            sql.push_str(&format!(" AND {NAME_EXPR} LIKE ? ESCAPE '\\' "));
            params.push(Bind::Text(format!("%{}%", escape_like(name))));
        }
    }
    if let Some(path) = &filter.path_contains {
        sql.push_str(" AND ro.object_path LIKE ? ESCAPE '\\' ");
        params.push(Bind::Text(format!("%{}%", escape_like(path))));
    }
    if !filter.buckets.is_empty() {
        sql.push_str(&format!(
            " AND ro.bucket_id IN ({}) ",
            placeholders(filter.buckets.len())
        ));
        for b in &filter.buckets {
            params.push(Bind::Text(uuid_to_text(b)));
        }
    }
    if !filter.connections.is_empty() {
        sql.push_str(&format!(
            " AND ro.connection_id IN ({}) ",
            placeholders(filter.connections.len())
        ));
        for c in &filter.connections {
            params.push(Bind::Text(uuid_to_text(c)));
        }
    }
    if let Some(ext) = &filter.extension {
        let ext = ext.trim_start_matches('.');
        sql.push_str(&format!(" AND {NAME_EXPR} LIKE ? ESCAPE '\\' "));
        params.push(Bind::Text(format!("%.{}", escape_like(ext))));
    }
    if let Some(min) = filter.min_size {
        sql.push_str(" AND ro.size >= ? ");
        params.push(Bind::Integer(min as i64));
    }
    if let Some(max) = filter.max_size {
        sql.push_str(" AND ro.size <= ? ");
        params.push(Bind::Integer(max as i64));
    }
    if let Some(after) = filter.modified_after {
        sql.push_str(" AND ro.last_modified >= ? ");
        params.push(Bind::Integer(after.unix_timestamp()));
    }
    if let Some(before) = filter.modified_before {
        sql.push_str(" AND ro.last_modified <= ? ");
        params.push(Bind::Integer(before.unix_timestamp()));
    }
    if let Some(kind) = filter.file_type {
        sql.push_str(&format!(" AND ro.file_type = '{}' ", kind.as_str()));
    }
    if filter.untagged {
        sql.push_str(
            " AND NOT EXISTS (SELECT 1 FROM remote_object_tags t \
              WHERE t.remote_object_id = ro.id AND t.workspace_id = ?) ",
        );
        params.push(Bind::Text(uuid_to_text(workspace)));
    } else if !filter.tags.is_empty() {
        if filter.tag_mode_or {
            sql.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM remote_object_tags t \
                   WHERE t.remote_object_id = ro.id AND t.workspace_id = ? AND t.tag_id IN ({})) ",
                placeholders(filter.tags.len())
            ));
            params.push(Bind::Text(uuid_to_text(workspace)));
            for t in &filter.tags {
                params.push(Bind::Text(uuid_to_text(t)));
            }
        } else {
            for t in &filter.tags {
                sql.push_str(
                    " AND EXISTS (SELECT 1 FROM remote_object_tags t \
                      WHERE t.remote_object_id = ro.id AND t.workspace_id = ? AND t.tag_id = ?) ",
                );
                params.push(Bind::Text(uuid_to_text(workspace)));
                params.push(Bind::Text(uuid_to_text(t)));
            }
        }
    }
    Ok(())
}

fn placeholders(n: usize) -> String {
    vec!["?"; n].join(", ")
}

/// Escape LIKE metacharacters in user input (partial-match search, MIX-011).
fn escape_like(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
