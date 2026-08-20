//! Row ↔ domain-struct codec shared by every repository (DATA-).
//!
//! [`RowReader`] owns the loud-failure policy (see `CONTEXT.md`): a field that
//! fails to decode is a bug in stored state, not user input, so it surfaces as
//! `Error::database` naming the column and the raw value — never a silent
//! default. The write-side helpers keep the reverse conversions in the same
//! place, so each repository states only its SQL and field bindings.

use rstore_domain::{Error, Result, TypedId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use time::OffsetDateTime;
use uuid::Uuid;

/// Typed read access to one SQLite row; every getter fails loud.
pub struct RowReader {
    row: SqliteRow,
}

impl RowReader {
    pub fn new(row: SqliteRow) -> Self {
        Self { row }
    }

    /// TEXT column.
    pub fn text(&self, column: &str) -> Result<String> {
        self.row
            .try_get::<String, _>(column)
            .map_err(|e| unreadable(column, e))
    }

    /// Nullable TEXT column.
    pub fn opt_text(&self, column: &str) -> Result<Option<String>> {
        self.row
            .try_get::<Option<String>, _>(column)
            .map_err(|e| unreadable(column, e))
    }

    /// TEXT column holding a UUID.
    pub fn uuid(&self, column: &str) -> Result<Uuid> {
        let raw = self.text(column)?;
        Uuid::parse_str(&raw).map_err(|e| {
            Error::database(format!("invalid uuid in column `{column}`: `{raw}`")).with_source(e)
        })
    }

    /// INTEGER column storing a `0`/`1` boolean.
    pub fn bool(&self, column: &str) -> Result<bool> {
        let raw = self.integer(column)?;
        match raw {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(Error::database(format!(
                "invalid boolean in column `{column}`: `{other}`"
            ))),
        }
    }

    /// INTEGER column holding unix seconds.
    pub fn timestamp(&self, column: &str) -> Result<OffsetDateTime> {
        let raw = self.integer(column)?;
        OffsetDateTime::from_unix_timestamp(raw).map_err(|e| {
            Error::database(format!("invalid timestamp in column `{column}`: `{raw}`"))
                .with_source(e)
        })
    }

    /// Nullable INTEGER column holding unix seconds.
    pub fn opt_timestamp(&self, column: &str) -> Result<Option<OffsetDateTime>> {
        let raw = self.opt_integer(column)?;
        raw.map(|secs| {
            OffsetDateTime::from_unix_timestamp(secs).map_err(|e| {
                Error::database(format!("invalid timestamp in column `{column}`: `{secs}`"))
                    .with_source(e)
            })
        })
        .transpose()
    }

    /// Nullable INTEGER column.
    pub fn opt_integer(&self, column: &str) -> Result<Option<i64>> {
        self.row
            .try_get::<Option<i64>, _>(column)
            .map_err(|e| unreadable(column, e))
    }

    /// TEXT column parsed through a domain token parser (closed vocabularies
    /// such as `AddressingStyle`). The parse error is rewrapped as a database
    /// error naming the column and raw value; the original stays as `source`.
    pub fn token<T>(&self, column: &str, parse: fn(&str) -> Result<T>) -> Result<T> {
        let raw = self.text(column)?;
        parse(&raw).map_err(|e| {
            Error::database(format!("invalid token in column `{column}`: `{raw}`")).with_source(e)
        })
    }

    /// INTEGER column (non-nullable).
    pub fn integer(&self, column: &str) -> Result<i64> {
        self.row
            .try_get::<i64, _>(column)
            .map_err(|e| unreadable(column, e))
    }
}

fn unreadable(column: &str, e: sqlx::Error) -> Error {
    Error::database(format!("cannot read column `{column}`")).with_source(e)
}

/// Bind form of a typed ID: the UUID rendered as TEXT.
pub fn uuid_to_text(id: &impl TypedId) -> String {
    id.as_uuid().to_string()
}

/// Bind form of a timestamp: unix seconds.
pub fn unix_secs(ts: OffsetDateTime) -> i64 {
    ts.unix_timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_to_text_uses_the_bare_uuid() {
        let id = rstore_domain::ConnectionId::new();
        assert_eq!(uuid_to_text(&id), id.as_uuid().to_string());
    }

    #[test]
    fn unix_secs_matches_offset_datetime() {
        let now = OffsetDateTime::now_utc();
        assert_eq!(unix_secs(now), now.unix_timestamp());
    }
}

/// One bindable value, in the vocabulary repositories write with. Keeping
/// the shapes typed (rather than passing `impl Encode` around) lets the
/// [`Insert`]/[`Update`] builders collect column names and values as pairs
/// and derive the placeholder list — the SQL column list, placeholder count
/// and bind order can no longer disagree.
#[derive(Debug, Clone)]
pub(crate) enum Bind {
    Text(String),
    NullText,
    Integer(i64),
    NullInteger,
    Bool(bool),
}

impl Bind {
    fn add_to(self, arguments: &mut sqlx::sqlite::SqliteArguments<'_>) {
        use sqlx::Arguments as _;
        match self {
            Bind::Text(v) => {
                arguments.add(v).expect("sqlite encodes text");
            }
            Bind::NullText => {
                arguments
                    .add(Option::<String>::None)
                    .expect("sqlite encodes null text");
            }
            Bind::Integer(v) => {
                arguments.add(v).expect("sqlite encodes integers");
            }
            Bind::NullInteger => {
                arguments
                    .add(Option::<i64>::None)
                    .expect("sqlite encodes null integers");
            }
            Bind::Bool(v) => {
                arguments.add(v).expect("sqlite encodes booleans");
            }
        }
    }
}

/// Ergonomic conversions so `.column(name, value)` reads like the row.
pub(crate) trait IntoBind {
    fn into_bind(self) -> Bind;
}

impl IntoBind for String {
    fn into_bind(self) -> Bind {
        Bind::Text(self)
    }
}

impl IntoBind for &String {
    fn into_bind(self) -> Bind {
        Bind::Text(self.clone())
    }
}

impl IntoBind for &str {
    fn into_bind(self) -> Bind {
        Bind::Text(self.to_string())
    }
}

impl IntoBind for Option<String> {
    fn into_bind(self) -> Bind {
        self.map(Bind::Text).unwrap_or(Bind::NullText)
    }
}

impl IntoBind for Option<&String> {
    fn into_bind(self) -> Bind {
        self.cloned().map(Bind::Text).unwrap_or(Bind::NullText)
    }
}

impl IntoBind for &Option<String> {
    fn into_bind(self) -> Bind {
        self.clone().map(Bind::Text).unwrap_or(Bind::NullText)
    }
}

impl IntoBind for Option<&str> {
    fn into_bind(self) -> Bind {
        self.map(str::to_string)
            .map(Bind::Text)
            .unwrap_or(Bind::NullText)
    }
}

impl IntoBind for i64 {
    fn into_bind(self) -> Bind {
        Bind::Integer(self)
    }
}

impl IntoBind for Option<i64> {
    fn into_bind(self) -> Bind {
        self.map(Bind::Integer).unwrap_or(Bind::NullInteger)
    }
}

impl IntoBind for bool {
    fn into_bind(self) -> Bind {
        Bind::Bool(self)
    }
}

impl IntoBind for Bind {
    fn into_bind(self) -> Bind {
        self
    }
}

/// An INSERT under construction: column names and bind values join as one
/// ordered list, so the generated SQL's column list, placeholder count and
/// bind order derive from the same source. Raw suffixes (`ON CONFLICT …`)
/// stay the caller's SQL.
pub(crate) struct Insert {
    table: &'static str,
    columns: Vec<&'static str>,
    values: Vec<Bind>,
    tail: String,
}

impl Insert {
    pub(crate) fn into(table: &'static str) -> Self {
        Self {
            table,
            columns: Vec::new(),
            values: Vec::new(),
            tail: String::new(),
        }
    }

    pub(crate) fn column(mut self, name: &'static str, value: impl IntoBind) -> Self {
        self.columns.push(name);
        self.values.push(value.into_bind());
        self
    }

    /// Append raw SQL after the VALUES list (`ON CONFLICT … DO UPDATE …`).
    pub(crate) fn suffix(mut self, tail: &str) -> Self {
        self.tail.push(' ');
        self.tail.push_str(tail);
        self
    }

    fn finish(self) -> (String, Vec<Bind>) {
        let placeholders = vec!["?"; self.columns.len()].join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}){}",
            self.table,
            self.columns.join(", "),
            placeholders,
            self.tail
        );
        (sql, self.values)
    }

    pub(crate) async fn execute(
        self,
        executor: impl sqlx::Executor<'_, Database = sqlx::Sqlite>,
    ) -> Result<sqlx::sqlite::SqliteQueryResult> {
        let (sql, values) = self.finish();
        let mut arguments = sqlx::sqlite::SqliteArguments::default();
        for value in values {
            value.add_to(&mut arguments);
        }
        sqlx::query_with(&sql, arguments)
            .execute(executor)
            .await
            .map_err(crate::error::map_sqlx)
    }
}

/// An UPDATE under construction: SET pairs (or raw fragments with their own
/// bind, for expressions like `COALESCE(?, state)`) plus a WHERE clause with
/// its trailing binds — position never leaves the pair it belongs to.
pub(crate) struct Update {
    table: &'static str,
    assignments: Vec<String>,
    values: Vec<Bind>,
    predicate: Option<String>,
}

impl Update {
    pub(crate) fn table(table: &'static str) -> Self {
        Self {
            table,
            assignments: Vec::new(),
            values: Vec::new(),
            predicate: None,
        }
    }

    pub(crate) fn set(mut self, column: &'static str, value: impl IntoBind) -> Self {
        self.assignments.push(format!("{column} = ?"));
        self.values.push(value.into_bind());
        self
    }

    /// A raw assignment expression with exactly one `?` of its own
    /// (`state = COALESCE(?, state)`) — the fragment and its bind stay a
    /// pair.
    pub(crate) fn set_sql(mut self, fragment: &str, value: impl IntoBind) -> Self {
        self.assignments.push(fragment.to_string());
        self.values.push(value.into_bind());
        self
    }

    /// The WHERE clause (without the keyword) and its trailing binds.
    pub(crate) fn filter(
        mut self,
        predicate: &str,
        values: impl IntoIterator<Item = Bind>,
    ) -> Self {
        self.predicate = Some(predicate.to_string());
        self.values.extend(values);
        self
    }

    fn finish(self) -> (String, Vec<Bind>) {
        let mut sql = format!("UPDATE {} SET {}", self.table, self.assignments.join(", "));
        if let Some(predicate) = &self.predicate {
            sql.push_str(&format!(" WHERE {predicate}"));
        }
        (sql, self.values)
    }

    pub(crate) async fn execute(
        self,
        executor: impl sqlx::Executor<'_, Database = sqlx::Sqlite>,
    ) -> Result<sqlx::sqlite::SqliteQueryResult> {
        let (sql, values) = self.finish();
        let mut arguments = sqlx::sqlite::SqliteArguments::default();
        for value in values {
            value.add_to(&mut arguments);
        }
        sqlx::query_with(&sql, arguments)
            .execute(executor)
            .await
            .map_err(crate::error::map_sqlx)
    }
}

/// One scalar COUNT query — the `SELECT COUNT(*) AS n …` shape every
/// repository and `Database` shares. Binds ride the same [`Bind`]
/// vocabulary as the write builders, and the read rides [`RowReader`]'s
/// loud-failure policy.
pub(crate) async fn count_scalar(
    executor: impl sqlx::Executor<'_, Database = sqlx::Sqlite>,
    sql: &str,
    binds: Vec<Bind>,
) -> Result<u64> {
    let mut arguments = sqlx::sqlite::SqliteArguments::default();
    for value in binds {
        value.add_to(&mut arguments);
    }
    let row = sqlx::query_with(sql, arguments)
        .fetch_one(executor)
        .await
        .map_err(crate::error::map_sqlx)?;
    RowReader::new(row).integer("n").map(|n| n as u64)
}

#[cfg(test)]
mod builder_tests {
    use super::*;

    fn insert_sql(insert: Insert) -> String {
        insert.finish().0
    }

    fn update_sql(update: Update) -> String {
        update.finish().0
    }

    #[test]
    fn insert_derives_columns_placeholders_and_order_together() {
        let sql = insert_sql(
            Insert::into("buckets")
                .column("id", "bkt_1")
                .column("read_only", true)
                .column("concurrency", Some(4_i64))
                .column("display_name", Option::<String>::None)
                .suffix("ON CONFLICT(id) DO NOTHING"),
        );
        assert_eq!(
            sql,
            "INSERT INTO buckets (id, read_only, concurrency, display_name) \
             VALUES (?, ?, ?, ?) ON CONFLICT(id) DO NOTHING"
        );
    }

    #[test]
    fn update_pairs_assignments_with_binds_and_appends_filters() {
        let sql = update_sql(
            Update::table("buckets")
                .set("name", "media")
                .set_sql("state = COALESCE(?, state)", "confirmed")
                .filter("id = ?", vec![Bind::Text("bkt_1".into())]),
        );
        assert_eq!(
            sql,
            "UPDATE buckets SET name = ?, state = COALESCE(?, state) WHERE id = ?"
        );
    }
}
