//! Repository for `ConnectionConfig` rows.
//!
//! Stores complete local connection configuration including credentials;
//! credential values are never serialized or logged. Enum ↔ column encodings
//! live on the domain enums; this module only binds and reads those tokens
//! through the shared row reader, which fails loud on undecodable fields.
use rstore_domain::connection::{AddressingStyle, ConnectionConfig, ConnectionTestStatus, Region};
use rstore_domain::id::ConnectionId;
use rstore_domain::{Error, Result, Secret};
use sqlx::sqlite::SqlitePool;
use time::OffsetDateTime;

use crate::db::codec::{unix_secs, uuid_to_text, Bind, Insert, RowReader, Update};
use crate::db::error::map_sqlx;
use crate::db::tx::WriteGate;

/// Every column of `storage_connections` in canonical order; one list serves every SELECT/INSERT here,
/// so adding a field is a bind + decode change, not an SQL-in-four-places change.
const COLUMNS: &str =
    "id, name, endpoint, public_base_url, cdn_base_url, region, addressing_style, \
                       tls_verify, access_key, secret_key, created_at, updated_at, \
                       last_tested_at, last_test_status";

#[derive(Clone)]
/// CRUD access to `storage_connections`.
pub struct ConnectionRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
}

impl ConnectionRepository {
    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool) -> Self {
        Self { gate, read_pool }
    }
}

impl ConnectionRepository {
    /// Insert a new connection. The caller is responsible for name uniqueness; a duplicate surfaces as
    /// a `Conflict` error.
    pub async fn insert(&self, config: &ConnectionConfig) -> Result<()> {
        Insert::into("storage_connections")
            .column("id", uuid_to_text(&config.id))
            .column("name", &config.name)
            .column("endpoint", &config.endpoint)
            .column("public_base_url", &config.public_base_url)
            .column("cdn_base_url", &config.cdn_base_url)
            .column("region", config.region.as_token())
            .column("addressing_style", config.addressing_style.as_str())
            .column("tls_verify", config.tls_verify)
            .column(
                "access_key",
                config.access_key.as_ref().map(|value| value.expose()),
            )
            .column(
                "secret_key",
                config.secret_key.as_ref().map(|value| value.expose()),
            )
            .column("created_at", unix_secs(config.created_at))
            .column("updated_at", unix_secs(config.updated_at))
            .column("last_tested_at", config.last_tested_at.map(unix_secs))
            .column(
                "last_test_status",
                config.last_test_status.map(|s| s.as_str()),
            )
            .execute(self.gate.pool())
            .await?;
        Ok(())
    }

    /// All connections, ordered by name.
    pub async fn list(&self) -> Result<Vec<ConnectionConfig>> {
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM storage_connections ORDER BY name ASC"
        ))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(row_to_config).collect()
    }

    pub async fn get(&self, id: ConnectionId) -> Result<Option<ConnectionConfig>> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM storage_connections WHERE id = ?"
        ))
        .bind(uuid_to_text(&id))
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(row_to_config).transpose()
    }

    pub async fn get_by_name(&self, name: &str) -> Result<Option<ConnectionConfig>> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM storage_connections WHERE name = ?"
        ))
        .bind(name)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(row_to_config).transpose()
    }

    /// Update an existing connection's mutable fields; a missing row is `NotFound`.
    pub async fn update(&self, config: &ConnectionConfig) -> Result<()> {
        let affected = Update::table("storage_connections")
            .set("name", &config.name)
            .set("endpoint", &config.endpoint)
            .set("public_base_url", &config.public_base_url)
            .set("cdn_base_url", &config.cdn_base_url)
            .set("region", config.region.as_token())
            .set("addressing_style", config.addressing_style.as_str())
            .set("tls_verify", config.tls_verify)
            .set(
                "access_key",
                config.access_key.as_ref().map(|value| value.expose()),
            )
            .set(
                "secret_key",
                config.secret_key.as_ref().map(|value| value.expose()),
            )
            .set("updated_at", unix_secs(config.updated_at))
            .set("last_tested_at", config.last_tested_at.map(unix_secs))
            .set(
                "last_test_status",
                config.last_test_status.map(|s| s.as_str()),
            )
            .filter("id = ?", [Bind::Text(uuid_to_text(&config.id))])
            .execute(self.gate.pool())
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(Error::not_found("connection not found"));
        }
        Ok(())
    }

    /// Persist the outcome of a connection test.
    pub async fn record_test(
        &self,
        id: ConnectionId,
        status: ConnectionTestStatus,
        at: OffsetDateTime,
    ) -> Result<()> {
        let affected = Update::table("storage_connections")
            .set("last_tested_at", unix_secs(at))
            .set("last_test_status", status.as_str())
            .set("updated_at", unix_secs(at))
            .filter("id = ?", [Bind::Text(uuid_to_text(&id))])
            .execute(self.gate.pool())
            .await?
            .rows_affected();
        if affected == 0 {
            return Err(Error::not_found("connection not found"));
        }
        Ok(())
    }

    /// Delete a connection; returns `false` when it did not exist.
    pub async fn delete(&self, id: ConnectionId) -> Result<bool> {
        let affected = sqlx::query("DELETE FROM storage_connections WHERE id = ?")
            .bind(uuid_to_text(&id))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?
            .rows_affected();
        Ok(affected > 0)
    }
}

/// Decode one row. `Region` is an open vocabulary (`parse_token` never fails); `AddressingStyle` is
/// closed and fails loud.
fn row_to_config(row: sqlx::sqlite::SqliteRow) -> Result<ConnectionConfig> {
    let row = RowReader::new(row);
    Ok(ConnectionConfig {
        id: ConnectionId::from_uuid(row.uuid("id")?),
        name: row.text("name")?,
        endpoint: row.text("endpoint")?,
        public_base_url: row.opt_text("public_base_url")?,
        cdn_base_url: row.opt_text("cdn_base_url")?,
        region: Region::parse_token(&row.text("region")?),
        addressing_style: row.token("addressing_style", AddressingStyle::parse)?,
        tls_verify: row.bool("tls_verify")?,
        access_key: row.opt_text("access_key")?.map(Secret::new),
        secret_key: row.opt_text("secret_key")?.map(Secret::new),
        created_at: row.timestamp("created_at")?,
        updated_at: row.timestamp("updated_at")?,
        last_tested_at: row.opt_timestamp("last_tested_at")?,
        last_test_status: row
            .opt_text("last_test_status")?
            .map(|token| {
                ConnectionTestStatus::parse(&token).ok_or_else(|| {
                    Error::database(format!("invalid connection test status `{token}`"))
                })
            })
            .transpose()?,
    })
}
