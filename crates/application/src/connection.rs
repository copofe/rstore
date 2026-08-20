//! Connection lifecycle service (CON-001..013).
//!
//! Owns CRUD over [`ConnectionConfig`] and credential storage. Diagnostics —
//! the connection test and capability probe — live in
//! [`crate::diagnostics::ConnectionDiagnostics`]; both services share the same
//! repositories but neither knows about the other.

use std::sync::Arc;

use async_trait::async_trait;
use rstore_application_ports::ConnectionStore;
use rstore_domain::connection::{AddressingStyle, ConnectionConfig, Region};
use rstore_domain::id::ConnectionId;
use rstore_domain::{Clock, CredentialRef, Error, Result};
use rstore_secrets::Secret;
use rstore_secrets::SecretStore;

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

/// A credential pair supplied by the user (CON-005). Wrapped in
/// `Secret<…>` at the CLI boundary so it never reaches logs or Debug output
/// (SEC-001).
#[derive(Clone)]
pub struct CredentialInput {
    pub access_key: String,
    pub secret_key: String,
}

/// Input for creating a connection. `credentials` are stored in the
/// SecretStore under the resolved `secret_ref` (DATA-013/014).
#[derive(Debug, Clone)]
pub struct AddConnectionInput {
    pub name: String,
    pub endpoint: String,
    pub region: Region,
    pub addressing_style: AddressingStyle,
    pub tls_verify: bool,
    /// Reference under which credentials are stored; defaults to
    /// `conn_<normalised name>`.
    pub secret_ref: Option<String>,
    /// Credentials to store for this connection, if the user supplied them.
    pub credentials: Option<Secret<CredentialInput>>,
}

/// Input for editing a connection's mutable fields.
#[derive(Debug, Clone, Default)]
pub struct UpdateConnectionInput {
    pub name: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<Region>,
    pub addressing_style: Option<AddressingStyle>,
    pub tls_verify: Option<bool>,
    pub secret_ref: Option<String>,
    /// Replace stored credentials when present.
    pub credentials: Option<Secret<CredentialInput>>,
}

/// Connection lifecycle operations.
#[async_trait]
pub trait ConnectionService: Send + Sync {
    async fn add(&self, input: AddConnectionInput) -> Result<ConnectionConfig>;
    async fn list(&self) -> Result<Vec<ConnectionConfig>>;
    /// CON-015: narrow by a case-insensitive substring over name/endpoint.
    /// One implementation of the filter semantics, so the CLI and the future
    /// desktop agree on what `--filter` matches.
    async fn list_matching(&self, needle: &str) -> Result<Vec<ConnectionConfig>> {
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
    async fn resolve(&self, reference: &ConnectionRef) -> Result<ConnectionConfig>;
    async fn update(
        &self,
        id: ConnectionId,
        input: UpdateConnectionInput,
    ) -> Result<ConnectionConfig>;
    async fn remove(&self, reference: &ConnectionRef) -> Result<()>;
}

/// Default implementation backed by the SQLite catalog.
pub struct DefaultConnectionService {
    repo: Arc<dyn ConnectionStore>,
    clock: Arc<dyn Clock>,
    secrets: Arc<dyn SecretStore>,
}

impl DefaultConnectionService {
    pub fn new(
        repo: Arc<dyn ConnectionStore>,
        clock: Arc<dyn Clock>,
        secrets: Arc<dyn SecretStore>,
    ) -> Self {
        Self {
            repo,
            clock,
            secrets,
        }
    }

    /// Store `credentials` under `secret_ref` (DATA-013/014).
    async fn store_credentials(
        &self,
        secret_ref: &str,
        credentials: &Secret<CredentialInput>,
    ) -> Result<()> {
        let pair = credentials.expose();
        rstore_secrets::put_credentials(
            self.secrets.as_ref(),
            secret_ref,
            &pair.access_key,
            &pair.secret_key,
        )
        .await
    }
}

#[async_trait]
impl ConnectionService for DefaultConnectionService {
    async fn add(&self, input: AddConnectionInput) -> Result<ConnectionConfig> {
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

        // Always record a secret reference: an explicit one wins, otherwise
        // derive it from the name so env-var credentials (DATA-013) resolve
        // even when nothing was stored interactively.
        let secret_ref = input
            .secret_ref
            .clone()
            .or_else(|| Some(rstore_secrets::default_secret_ref(&input.name)))
            .map(CredentialRef::new)
            .transpose()?;
        if let (Some(ref_name), Some(creds)) = (&secret_ref, &input.credentials) {
            self.store_credentials(ref_name.as_str(), creds).await?;
        }
        let now = self.clock.now();
        let config = ConnectionConfig {
            id: ConnectionId::new(),
            name: input.name,
            endpoint: input.endpoint,
            region: input.region,
            addressing_style: input.addressing_style,
            tls_verify: input.tls_verify,
            secret_ref,
            created_at: now,
            updated_at: now,
            last_tested_at: None,
            last_test_status: None,
        };
        self.repo.insert(&config).await?;
        Ok(config)
    }

    async fn list(&self) -> Result<Vec<ConnectionConfig>> {
        self.repo.list().await
    }

    async fn resolve(&self, reference: &ConnectionRef) -> Result<ConnectionConfig> {
        let found = match reference {
            ConnectionRef::Id(id) => self.repo.get(*id).await?,
            ConnectionRef::Name(name) => self.repo.get_by_name(name).await?,
        };
        found.ok_or_else(|| {
            Error::not_found(format!(
                "connection `{}` not found",
                match reference {
                    ConnectionRef::Id(id) => id.to_string(),
                    ConnectionRef::Name(n) => n.clone(),
                }
            ))
        })
    }

    async fn update(
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
        if let Some(region) = input.region {
            config.region = region;
        }
        if let Some(style) = input.addressing_style {
            config.addressing_style = style;
        }
        if let Some(tls) = input.tls_verify {
            config.tls_verify = tls;
        }
        if let Some(secret_ref) = input.secret_ref {
            config.secret_ref = Some(CredentialRef::new(secret_ref)?);
        }
        if let Some(creds) = &input.credentials {
            let secret_ref = match config.secret_ref.clone() {
                Some(secret_ref) => secret_ref,
                None => {
                    let derived =
                        CredentialRef::new(rstore_secrets::default_secret_ref(&config.name))?;
                    config.secret_ref = Some(derived.clone());
                    derived
                }
            };
            self.store_credentials(secret_ref.as_str(), creds).await?;
        }
        config.updated_at = self.clock.now();
        self.repo.update(&config).await?;
        Ok(config)
    }

    async fn remove(&self, reference: &ConnectionRef) -> Result<()> {
        let config = self.resolve(reference).await?;
        let removed = self.repo.delete(config.id).await?;
        if !removed {
            return Err(Error::not_found(format!(
                "connection `{}` was already absent",
                config.name
            )));
        }
        // Credentials are dropped with the connection (CON-013).
        if let Some(secret_ref) = &config.secret_ref {
            rstore_secrets::delete_credentials(self.secrets.as_ref(), secret_ref.as_str()).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_domain::{ErrorCode, SystemClock};
    use rstore_persistence::Database;
    use rstore_secrets::{get_credentials, InMemorySecretStore, NullSecretStore};

    async fn setup() -> DefaultConnectionService {
        let db = Database::open_in_memory().await.unwrap();
        DefaultConnectionService::new(
            Arc::new(db.connections()),
            Arc::new(SystemClock),
            Arc::new(NullSecretStore),
        )
    }

    fn input(name: &str) -> AddConnectionInput {
        AddConnectionInput {
            name: name.into(),
            endpoint: "https://example.r2.cloudflarestorage.com".into(),
            region: Region::Auto,
            addressing_style: AddressingStyle::Path,
            tls_verify: true,
            secret_ref: None,
            credentials: None,
        }
    }

    #[tokio::test]
    async fn add_list_resolve_remove() {
        let svc = setup().await;
        let mut add = input("prod-r2");
        add.secret_ref = Some("conn_prod_r2".into());
        let added = svc.add(add).await.unwrap();

        assert_eq!(svc.list().await.unwrap().len(), 1);

        let by_name = svc
            .resolve(&ConnectionRef::Name("prod-r2".into()))
            .await
            .unwrap();
        assert_eq!(by_name.id, added.id);

        let by_id = svc
            .resolve(&ConnectionRef::parse(&added.id.to_string()))
            .await
            .unwrap();
        assert_eq!(by_id.id, added.id);

        // Duplicate name -> conflict.
        let dup = svc.add(input("prod-r2")).await;
        assert_eq!(dup.unwrap_err().code, ErrorCode::Conflict);

        svc.remove(&ConnectionRef::Id(added.id)).await.unwrap();
        assert_eq!(svc.list().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn add_stores_credentials_and_remove_deletes_them() {
        let db = Database::open_in_memory().await.unwrap();
        let secrets = Arc::new(InMemorySecretStore::new());
        let svc = DefaultConnectionService::new(
            Arc::new(db.connections()),
            Arc::new(SystemClock),
            secrets.clone(),
        );

        let mut add = input("nas");
        add.credentials = Some(Secret::new(CredentialInput {
            access_key: "AK".into(),
            secret_key: "SK".into(),
        }));
        let added = svc.add(add).await.unwrap();

        // The derived default reference is recorded and backed by the store.
        assert_eq!(added.secret_ref.as_deref(), Some("conn_nas"));
        let pair = get_credentials(secrets.as_ref(), "conn_nas")
            .await
            .unwrap()
            .expect("stored");
        assert_eq!(pair.access_key.expose(), "AK");

        svc.remove(&ConnectionRef::Id(added.id)).await.unwrap();
        assert!(get_credentials(secrets.as_ref(), "conn_nas")
            .await
            .unwrap()
            .is_none());
    }
}
