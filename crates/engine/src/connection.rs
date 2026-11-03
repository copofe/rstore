//! Connection lifecycle service.
//!
//! Owns CRUD over [`ConnectionConfig`] and credential storage. Diagnostics — the connection
//! test and capability probe — live in [`crate::diagnostics::ConnectionDiagnostics`]; both
//! services share the same repositories but neither knows about the other.

use std::sync::Arc;

use rstore_domain::connection::{AddressingStyle, ConnectionConfig, Region};
use rstore_domain::id::ConnectionId;
use rstore_domain::{Clock, Error, Result, Secret};

/// Reference a connection by stable id or by name.
#[derive(Debug, Clone)]
pub enum ConnectionRef {
    Id(ConnectionId),
    Name(String),
}

impl ConnectionRef {
    /// Parse a user-supplied string: a `conn_…` id, otherwise a name.
    pub fn parse(s: &str) -> Self {
        use std::str::FromStr;
        match ConnectionId::from_str(s) {
            Ok(id) => ConnectionRef::Id(id),
            Err(_) => ConnectionRef::Name(s.to_string()),
        }
    }

    /// Human-readable form for messages.
    pub fn display(&self) -> String {
        match self {
            ConnectionRef::Id(id) => id.to_string(),
            ConnectionRef::Name(n) => n.clone(),
        }
    }
}

/// Resolve a connection reference against a store.
///
/// The single lookup and the single not-found message, shared by [`ConnectionService`] and the
/// gateway so every caller reports identical failures.
pub(crate) async fn resolve_connection(
    store: &crate::db::ConnectionRepository,
    reference: &ConnectionRef,
) -> Result<ConnectionConfig> {
    let found = match reference {
        ConnectionRef::Id(id) => store.get(*id).await?,
        ConnectionRef::Name(name) => store.get_by_name(name).await?,
    };
    found.ok_or_else(|| Error::not_found(format!("connection `{}` not found", reference.display())))
}

/// A known S3-compatible service whose connection defaults differ from the generic ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderPreset {
    Aws,
    R2,
    Minio,
    Supabase,
    Generic,
}

impl ProviderPreset {
    /// Parse a preset name: `aws`, `r2`, `minio`, `supabase`, or `generic`.
    ///
    /// # Errors
    /// `invalid_input` for any other name.
    pub fn parse(token: &str) -> Result<Self> {
        match token {
            "aws" => Ok(Self::Aws),
            "r2" => Ok(Self::R2),
            "minio" => Ok(Self::Minio),
            "supabase" => Ok(Self::Supabase),
            "generic" => Ok(Self::Generic),
            other => Err(Error::invalid_input(format!(
                "unknown provider `{other}` (aws|r2|minio|supabase|generic)"
            ))),
        }
    }

    /// The addressing style the provider needs when none is given explicitly: MinIO and
    /// Supabase gateways only serve path-style requests.
    pub fn addressing_style(self) -> AddressingStyle {
        match self {
            Self::Minio | Self::Supabase => AddressingStyle::Path,
            Self::Aws | Self::R2 | Self::Generic => AddressingStyle::Auto,
        }
    }
}

/// A credential pair supplied by the user.
///
/// Wrapped in `Secret<…>` at the CLI boundary so it never reaches logs or `Debug` output.
#[derive(Clone)]
pub struct CredentialInput {
    pub access_key: String,
    pub secret_key: String,
}

type StoredCredentials = (Option<Secret<String>>, Option<Secret<String>>);

/// Input for creating a connection. Credentials are stored directly with the connection when
/// supplied.
#[derive(Debug, Clone)]
pub struct AddConnectionInput {
    pub name: String,
    pub endpoint: String,
    /// Public-object base URL; `None` uses the endpoint unchanged.
    pub public_base_url: Option<String>,
    /// Default accelerated/proxy delivery base for this connection.
    pub cdn_base_url: Option<String>,
    pub region: Region,
    pub addressing_style: AddressingStyle,
    pub tls_verify: bool,
    /// Credentials to store for this connection, if the user supplied them.
    pub credentials: Option<Secret<CredentialInput>>,
}

/// Input for editing a connection's mutable fields.
///
/// Clearing convention: a field of type `Option<Option<T>>` is left untouched when the outer
/// value is `None`; `Some(None)` explicitly clears the stored value; `Some(Some(v))` replaces
/// it. Base-URL values are normalized through
/// [`ConnectionConfig::normalize_base_url`] — that method is the single home for the
/// trim-trailing-slashes / empty-to-`None` rule, applied here and by every other writer.
#[derive(Debug, Clone, Default)]
pub struct UpdateConnectionInput {
    pub name: Option<String>,
    pub endpoint: Option<String>,
    /// Replace the public-object base URL; the inner `None` clears the override (an empty
    /// string normalizes to a clear as well).
    pub public_base_url: Option<Option<String>>,
    /// Replace the accelerated/proxy delivery base (same clearing rule).
    pub cdn_base_url: Option<Option<String>>,
    pub region: Option<Region>,
    pub addressing_style: Option<AddressingStyle>,
    pub tls_verify: Option<bool>,
    /// Replace stored credentials when present.
    pub credentials: Option<Secret<CredentialInput>>,
}

/// Connection lifecycle operations.
pub struct ConnectionService {
    repo: Arc<crate::db::ConnectionRepository>,
    clock: Arc<dyn Clock>,
}

impl ConnectionService {
    pub fn new(repo: Arc<crate::db::ConnectionRepository>, clock: Arc<dyn Clock>) -> Self {
        Self { repo, clock }
    }

    /// Split a credential pair into the stored form, trimming whitespace and rejecting
    /// blank halves.
    fn split_credentials(
        credentials: Option<Secret<CredentialInput>>,
    ) -> Result<StoredCredentials> {
        let Some(credentials) = credentials else {
            return Ok((None, None));
        };
        let pair = credentials.into_inner();
        if pair.access_key.trim().is_empty() || pair.secret_key.trim().is_empty() {
            return Err(Error::invalid_input(
                "access key and secret key must be non-empty",
            ));
        }
        Ok((
            Some(Secret::new(pair.access_key.trim().to_string())),
            Some(Secret::new(pair.secret_key.trim().to_string())),
        ))
    }
}

impl ConnectionService {
    pub async fn add(&self, input: AddConnectionInput) -> Result<ConnectionConfig> {
        if !ConnectionConfig::is_valid_name(&input.name) {
            return Err(Error::invalid_input(format!(
                "invalid connection name `{}`",
                input.name
            )));
        }
        if input.endpoint.trim().is_empty() {
            return Err(Error::invalid_input("endpoint must not be empty"));
        }
        if self.repo.get_by_name(&input.name).await?.is_some() {
            return Err(Error::conflict(format!(
                "a connection named `{}` already exists",
                input.name
            )));
        }

        let (access_key, secret_key) = Self::split_credentials(input.credentials)?;
        let now = self.clock.now();
        let config = ConnectionConfig {
            id: ConnectionId::new_random(),
            name: input.name,
            endpoint: input.endpoint,
            public_base_url: ConnectionConfig::normalize_base_url(input.public_base_url),
            cdn_base_url: ConnectionConfig::normalize_base_url(input.cdn_base_url),
            region: input.region,
            addressing_style: input.addressing_style,
            tls_verify: input.tls_verify,
            access_key,
            secret_key,
            created_at: now,
            updated_at: now,
            last_tested_at: None,
            last_test_status: None,
        };
        self.repo.insert(&config).await?;
        Ok(config)
    }

    pub async fn list(&self) -> Result<Vec<ConnectionConfig>> {
        self.repo.list().await
    }

    pub async fn resolve(&self, reference: &ConnectionRef) -> Result<ConnectionConfig> {
        crate::connection::resolve_connection(self.repo.as_ref(), reference).await
    }

    pub async fn update(
        &self,
        id: ConnectionId,
        input: UpdateConnectionInput,
    ) -> Result<ConnectionConfig> {
        let before = self.resolve(&ConnectionRef::Id(id)).await?;
        let mut config = before.clone();
        if let Some(name) = &input.name {
            if !ConnectionConfig::is_valid_name(name) {
                return Err(Error::invalid_input(format!(
                    "invalid connection name `{name}`"
                )));
            }
            if let Some(existing) = self.repo.get_by_name(name).await? {
                if existing.id != config.id {
                    return Err(Error::conflict(format!(
                        "a connection named `{name}` already exists"
                    )));
                }
            }
            config.name = name.clone();
        }
        if let Some(endpoint) = input.endpoint {
            if endpoint.trim().is_empty() {
                return Err(Error::invalid_input("endpoint must not be empty"));
            }
            config.endpoint = endpoint;
        }
        if let Some(base) = input.public_base_url {
            config.public_base_url = ConnectionConfig::normalize_base_url(base);
        }
        if let Some(base) = input.cdn_base_url {
            config.cdn_base_url = ConnectionConfig::normalize_base_url(base);
        }
        if let Some(region) = input.region {
            config.region = region;
        }
        if let Some(style) = input.addressing_style {
            config.addressing_style = style;
        }
        if let Some(tls) = input.tls_verify {
            config.tls_verify = tls;
        }
        if input.credentials.is_some() {
            let (access_key, secret_key) = Self::split_credentials(input.credentials)?;
            config.access_key = access_key;
            config.secret_key = secret_key;
        }
        config.updated_at = self.clock.now();
        self.repo.update(&config).await?;
        Ok(config)
    }

    pub async fn remove(&self, reference: &ConnectionRef) -> Result<()> {
        let config = self.resolve(reference).await?;
        let removed = self.repo.delete(config.id).await?;
        if !removed {
            return Err(Error::not_found(format!(
                "connection `{}` was already absent",
                config.name
            )));
        }
        Ok(())
    }

    /// Narrow [`Self::list`] by a case-insensitive substring over name and endpoint.
    ///
    /// One implementation of the filter semantics, so the CLI and the desktop agree on what
    /// the `connection list` filter matches.
    pub async fn list_matching(&self, needle: &str) -> Result<Vec<ConnectionConfig>> {
        let needle = needle.to_lowercase();
        Ok(self
            .list()
            .await?
            .into_iter()
            .filter(|c| {
                c.name.to_lowercase().contains(&needle)
                    || c.endpoint.to_lowercase().contains(&needle)
            })
            .collect())
    }
}
