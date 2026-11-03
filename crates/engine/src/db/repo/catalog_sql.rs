//! Internal SQL builder behind the catalog search.
//!
//! One place owns everything dynamic about the mixed file list: filter
//! predicates, LIKE escaping, sort expressions (including the file-name rtrim
//! trick), the keyset cursor comparison, and binding into dynamically
//! assembled SQL. Repository methods declare what they want; the how stays
//! here.
use crate::catalog::{FileFilter, FileSort, SortField, SortOrder};
use rstore_domain::{Result, WorkspaceId};

use crate::db::codec::uuid_to_text;

const NAME_EXPR: &str =
    "replace(object_path, rtrim(object_path, replace(object_path, '/', '')), '')";

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
        F::Type => "ro.file_type",
        F::Bucket => "ro.bucket_id",
        F::Connection => "ro.connection_id",
    }
}

enum Bind {
    Text(String),
    Integer(i64),
}

pub(super) struct CatalogSql {
    sql: String,
    params: Vec<Bind>,
}

impl CatalogSql {
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

    pub(super) fn page(
        columns: &str,
        scope_workspace: &WorkspaceId,
        filter: &FileFilter,
    ) -> Result<Self> {
        let mut sql = String::from(
            "SELECT {COLUMNS}, b.name AS bucket_name, c.name AS connection_name, \
             COALESCE((SELECT json_group_array(tag_name) FROM ( \
                 SELECT t.name AS tag_name \
                 FROM object_tags ot \
                 JOIN tags t ON t.id = ot.tag_id \
                 WHERE ot.workspace_id = wb.workspace_id \
                   AND ot.bucket_id = ro.bucket_id AND ot.object_path = ro.object_path \
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

    pub(super) fn ordered_after(mut self, sort: FileSort, cursor: &str) -> Result<Self> {
        let (value, path, id) = crate::catalog::order::decode(cursor, sort.field)?;
        let typed = match value {
            crate::catalog::order::SortKey::Num(n) => Bind::Integer(n),
            crate::catalog::order::SortKey::Text(t) => Bind::Text(t),
        };
        let expr = sort_expr(sort.field);
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
        self.sql.push_str(&format!(
            " ORDER BY {expr} {dir}, ro.object_path {dir}, ro.id {dir} LIMIT {limit}",
            expr = sort_expr(sort.field),
            dir = dir,
            limit = limit
        ));
    }

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
            .map_err(crate::db::error::map_sqlx)
    }
}

fn push_filters(
    sql: &mut String,
    params: &mut Vec<Bind>,
    workspace: &WorkspaceId,
    filter: &FileFilter,
) -> Result<()> {
    if let Some(state) = filter.state {
        sql.push_str(" AND ro.state = ? ");
        params.push(Bind::Text(state.as_str().to_owned()));
    }
    if let Some(name) = &filter.name_contains {
        if name.chars().count() >= 3 {
            let quoted = format!("\"{}\"", name.replace('"', "\"\""));
            sql.push_str(&format!(
                " AND ro.rowid IN (SELECT rowid FROM remote_objects_fts \
                   WHERE remote_objects_fts MATCH ?) \
                   AND {NAME_EXPR} LIKE ? ESCAPE '\\' "
            ));
            params.push(Bind::Text(quoted));
            params.push(Bind::Text(format!("%{}%", escape_like(name))));
        } else {
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
        sql.push_str(" AND ro.file_type = ? ");
        params.push(Bind::Text(kind.as_str().to_owned()));
    }
    if filter.untagged {
        sql.push_str(
            " AND NOT EXISTS (SELECT 1 FROM object_tags t \
              WHERE t.bucket_id = ro.bucket_id AND t.object_path = ro.object_path \
              AND t.workspace_id = ?) ",
        );
        params.push(Bind::Text(uuid_to_text(workspace)));
    } else if !filter.tags.is_empty() {
        if filter.tag_mode_or {
            sql.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM object_tags t \
                   WHERE t.bucket_id = ro.bucket_id AND t.object_path = ro.object_path \
                   AND t.workspace_id = ? AND t.tag_id IN ({})) ",
                placeholders(filter.tags.len())
            ));
            params.push(Bind::Text(uuid_to_text(workspace)));
            for t in &filter.tags {
                params.push(Bind::Text(uuid_to_text(t)));
            }
        } else {
            for t in &filter.tags {
                sql.push_str(
                    " AND EXISTS (SELECT 1 FROM object_tags t \
                      WHERE t.bucket_id = ro.bucket_id AND t.object_path = ro.object_path \
                      AND t.workspace_id = ? AND t.tag_id = ?) ",
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

fn escape_like(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
