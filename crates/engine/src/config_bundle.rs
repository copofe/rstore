//! Config import/export.
//!
//! A ConfigBundle is the portable JSON form of the registry: every connection and bucket, with
//! credential values omitted by serde. Importing re-adds entries through the ordinary services
//! with conflict checks (existing name → skipped) and an optional dry run.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rstore_domain::connection::ConnectionConfig;
use rstore_domain::Result;
use rstore_domain::{ConnectionId, Error};
use serde::{Deserialize, Serialize};

use crate::bucket::{AddBucketInput, BucketService};
use crate::connection::ConnectionService;

/// One bucket in a bundle, anchored to its connection by name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketExport {
    pub connection: String,
    pub name: String,
    #[serde(flatten)]
    pub config: rstore_domain::BucketConfig,
}

/// The portable registry snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigBundle {
    pub connections: Vec<ConnectionConfig>,
    pub buckets: Vec<BucketExport>,
}

/// Outcome of an import: per-section counts, never an error — conflicts are skips, and a dry
/// run adds nothing.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ImportReport {
    pub dry_run: bool,
    pub connections_added: usize,
    pub connections_skipped: usize,
    pub buckets_added: usize,
    pub buckets_skipped: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportDecision {
    Add,
    Skip,
}

#[derive(Debug, Clone)]
struct PlannedConnection {
    config: ConnectionConfig,
    decision: ImportDecision,
    existing_id: Option<ConnectionId>,
}

#[derive(Debug, Clone)]
struct PlannedBucket {
    export: BucketExport,
    decision: ImportDecision,
}

/// The registry snapshot's decisions for one import.
///
/// Dry-run and execution both consume this value so they cannot disagree about logical
/// identities.
#[derive(Debug, Clone)]
struct ImportPlan {
    connections: Vec<PlannedConnection>,
    buckets: Vec<PlannedBucket>,
}

impl ImportPlan {
    fn report(&self, dry_run: bool) -> ImportReport {
        ImportReport {
            dry_run,
            connections_added: self
                .connections
                .iter()
                .filter(|item| item.decision == ImportDecision::Add)
                .count(),
            connections_skipped: self
                .connections
                .iter()
                .filter(|item| item.decision == ImportDecision::Skip)
                .count(),
            buckets_added: self
                .buckets
                .iter()
                .filter(|item| item.decision == ImportDecision::Add)
                .count(),
            buckets_skipped: self
                .buckets
                .iter()
                .filter(|item| item.decision == ImportDecision::Skip)
                .count(),
        }
    }
}

/// Registry portability service.
pub(crate) struct ConfigTransferService {
    connections: Arc<ConnectionService>,
    buckets: Arc<BucketService>,
}

impl ConfigTransferService {
    pub(crate) fn new(connections: Arc<ConnectionService>, buckets: Arc<BucketService>) -> Self {
        Self {
            connections,
            buckets,
        }
    }

    /// Serialise the registry without credential values; the exported bundle never contains a
    /// secret.
    pub(crate) async fn export(&self) -> Result<ConfigBundle> {
        let list = self
            .connections
            .list()
            .await?
            .into_iter()
            .map(|mut config| {
                config.access_key = None;
                config.secret_key = None;
                config
            })
            .collect::<Vec<_>>();
        let mut buckets = Vec::new();
        let existing = self.buckets.list().await?;
        for config in &list {
            for bucket in existing.iter().filter(|b| b.connection_id == config.id) {
                buckets.push(BucketExport {
                    connection: config.name.clone(),
                    name: bucket.name.clone(),
                    config: bucket.config.clone(),
                });
            }
        }
        Ok(ConfigBundle {
            connections: list,
            buckets,
        })
    }

    /// Build a logical import plan from one registry snapshot.
    ///
    /// A newly added connection gets a planned identity immediately, so buckets can refer to
    /// it before execution has created its database ID.
    async fn plan(&self, bundle: &ConfigBundle) -> Result<ImportPlan> {
        let existing_connections = self.connections.list().await?;
        let existing_buckets = self.buckets.list().await?;

        let mut targets = HashMap::<String, Option<ConnectionId>>::new();
        for config in &existing_connections {
            targets.insert(config.name.clone(), Some(config.id));
        }

        let mut connections = Vec::with_capacity(bundle.connections.len());
        for config in &bundle.connections {
            let (decision, existing_id) = if let Some(target) = targets.get(&config.name) {
                (ImportDecision::Skip, *target)
            } else {
                // `None` is a deliberate logical target: execution will fill in the generated
                // ID after the connection is inserted.
                targets.insert(config.name.clone(), None);
                (ImportDecision::Add, None)
            };
            connections.push(PlannedConnection {
                config: config.clone(),
                decision,
                existing_id,
            });
        }

        let connection_names = existing_connections
            .iter()
            .map(|config| (config.id, config.name.as_str()))
            .collect::<HashMap<_, _>>();
        let mut seen_buckets = HashSet::<String>::new();
        for bucket in &existing_buckets {
            if let Some(connection_name) = connection_names.get(&bucket.connection_id) {
                seen_buckets.insert(bucket_key(connection_name, &bucket.name));
            }
        }

        let mut planned_buckets = Vec::with_capacity(bundle.buckets.len());
        for bucket in &bundle.buckets {
            let key = bucket_key(&bucket.connection, &bucket.name);
            let decision = if targets.contains_key(&bucket.connection) && seen_buckets.insert(key) {
                ImportDecision::Add
            } else {
                ImportDecision::Skip
            };
            planned_buckets.push(PlannedBucket {
                export: bucket.clone(),
                decision,
            });
        }

        Ok(ImportPlan {
            connections,
            buckets: planned_buckets,
        })
    }

    /// Execute or preview a previously derived plan.
    ///
    /// Existing names are skipped and never overwritten.
    pub(crate) async fn import(
        &self,
        bundle: &ConfigBundle,
        dry_run: bool,
    ) -> Result<ImportReport> {
        let plan = self.plan(bundle).await?;
        let report = plan.report(dry_run);
        if !dry_run {
            self.execute(&plan).await?;
        }
        Ok(report)
    }

    async fn execute(&self, plan: &ImportPlan) -> Result<()> {
        let mut name_to_id = HashMap::<String, ConnectionId>::new();
        for connection in &plan.connections {
            match connection.decision {
                ImportDecision::Skip => {
                    if let Some(id) = connection.existing_id {
                        name_to_id.insert(connection.config.name.clone(), id);
                    }
                }
                ImportDecision::Add => {
                    let added = self
                        .connections
                        .add(connection_input(&connection.config))
                        .await?;
                    name_to_id.insert(connection.config.name.clone(), added.id);
                }
            }
        }
        for bucket in &plan.buckets {
            if bucket.decision == ImportDecision::Skip {
                continue;
            }
            let connection_id = name_to_id
                .get(&bucket.export.connection)
                .copied()
                .ok_or_else(|| {
                    Error::database("import plan lost a connection identity for a bucket")
                })?;
            self.buckets
                .add(
                    AddBucketInput {
                        connection: crate::connection::ConnectionRef::Id(connection_id),
                        name: bucket.export.name.clone(),
                        config: bucket.export.config.clone(),
                    },
                    // Imported buckets skip the readability probe.
                    false,
                )
                .await?;
        }
        Ok(())
    }
}

fn connection_input(config: &ConnectionConfig) -> crate::connection::AddConnectionInput {
    crate::connection::AddConnectionInput {
        name: config.name.clone(),
        endpoint: config.endpoint.clone(),
        public_base_url: config.public_base_url.clone(),
        cdn_base_url: config.cdn_base_url.clone(),
        region: config.region.clone(),
        addressing_style: config.addressing_style,
        tls_verify: config.tls_verify,
        // Credentials are intentionally not portable.
        credentials: None,
    }
}

fn bucket_key(connection: &str, bucket: &str) -> String {
    format!("{connection}\0{bucket}")
}
