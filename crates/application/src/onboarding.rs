//! Bucket onboarding: register buckets on a connection and enroll each into
//! the target workspace — the one sequence `bucket add`, `connection add`,
//! and a desktop setup flow share, so no entry point re-implements it.

use std::collections::HashMap;
use std::sync::Arc;

use rstore_domain::bucket::BucketConfig;
use rstore_domain::{Bucket, Result, Workspace};

use crate::batch_outcome::{BatchOutcome, ItemOutcome};
use crate::bucket::{AddBucketInput, BucketRef, BucketService, ReadabilityCheck};
use crate::connection::{ConnectionRef, ConnectionService};
use crate::reference::NewBucket;
use crate::workspace::{WorkspaceRef, WorkspaceService};

/// One registered bucket: the row, the readability probe when one ran, and
/// the workspace it joined (`None` when several exist and none is current).
#[derive(Debug, Clone, serde::Serialize)]
pub struct Registration {
    pub bucket: Bucket,
    pub readability: Option<ReadabilityCheck>,
    pub workspace: Option<Workspace>,
}

/// Bucket registration as one use case over the registry and workspace
/// services.
pub struct Onboarding {
    connections: Arc<dyn ConnectionService>,
    buckets: Arc<dyn BucketService>,
    workspaces: Arc<dyn WorkspaceService>,
}

impl Onboarding {
    pub(crate) fn new(
        connections: Arc<dyn ConnectionService>,
        buckets: Arc<dyn BucketService>,
        workspaces: Arc<dyn WorkspaceService>,
    ) -> Self {
        Self {
            connections,
            buckets,
            workspaces,
        }
    }

    /// Register each bucket (probing readability when `verify`) and enroll it
    /// into `workspace`, else the current or only one — creating `default`
    /// when none exists yet. Items are labelled `<connection>/<bucket>` and
    /// fail individually.
    ///
    /// # Errors
    /// An explicit `workspace` that does not exist fails the call before
    /// anything is registered.
    pub async fn register(
        &self,
        buckets: &[NewBucket],
        config: &BucketConfig,
        verify: bool,
        workspace: Option<&WorkspaceRef>,
    ) -> Result<BatchOutcome<Registration>> {
        if let Some(reference) = workspace {
            self.workspaces.resolve(reference).await?;
        }
        let mut names = HashMap::new();
        let mut items = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let label = format!(
                "{}/{}",
                self.connection_label(&bucket.connection, &mut names).await,
                bucket.name
            );
            items.push(
                match self.register_one(bucket, config, verify, workspace).await {
                    Ok(registration) => ItemOutcome::succeeded(label, registration),
                    Err(error) => ItemOutcome::failed(label, &error),
                },
            );
        }
        Ok(BatchOutcome::new(items))
    }

    /// The buckets visible on `connection` that are not registered yet.
    ///
    /// # Errors
    /// Whatever listing the connection's buckets fails with — typically a
    /// missing ListBuckets permission, in which case buckets are registered
    /// by name instead.
    pub async fn unregistered(&self, connection: &ConnectionRef) -> Result<Vec<String>> {
        Ok(self
            .buckets
            .discover(connection)
            .await?
            .into_iter()
            .filter(|bucket| !bucket.already_registered)
            .map(|bucket| bucket.name)
            .collect())
    }

    async fn register_one(
        &self,
        bucket: &NewBucket,
        config: &BucketConfig,
        verify: bool,
        workspace: Option<&WorkspaceRef>,
    ) -> Result<Registration> {
        let (bucket, readability) = self
            .buckets
            .add(
                AddBucketInput {
                    connection: bucket.connection.clone(),
                    name: bucket.name.clone(),
                    config: config.clone(),
                },
                verify,
            )
            .await?;
        let workspace = self
            .workspaces
            .enroll(&BucketRef::Id(bucket.id), workspace)
            .await?;
        Ok(Registration {
            bucket,
            readability,
            workspace,
        })
    }

    /// The connection's name for item labels, resolved once per reference;
    /// an unresolvable reference labels as typed (its item then fails).
    async fn connection_label(
        &self,
        connection: &ConnectionRef,
        cache: &mut HashMap<String, String>,
    ) -> String {
        let key = connection.display();
        if let Some(name) = cache.get(&key) {
            return name.clone();
        }
        let name = self
            .connections
            .resolve(connection)
            .await
            .map(|config| config.name)
            .unwrap_or_else(|_| key.clone());
        cache.insert(key, name.clone());
        name
    }
}
