//! Application Core for the context APIs.
//!
//! This crate owns use-case orchestration and depends on domain contracts,
//! application ports and provider-facing interfaces only. SQLite, SQLx, the
//! AWS SDK and GPUI enter through the Bootstrap composition root, never through
//! this public layer.

pub mod api;
pub mod app;
pub mod batch_plan;
pub mod bucket;
pub(crate) mod catalog_follow;
pub mod commands;
pub mod config_bundle;
pub mod connection;
pub mod copy;
pub mod diagnostics;
pub(crate) mod expand;
pub mod files;
pub mod gateway;
pub(crate) mod guards;
pub mod health;
mod index_run;
pub mod indexer;
pub mod job;
pub mod job_run;
pub mod link;
pub mod locator;
pub(crate) mod mutation;
pub mod notifications;
pub mod outbox;
pub mod ports;
pub mod remote;
pub mod tag;
#[cfg(feature = "test-util")]
pub mod test_util;
pub mod transfer;
mod transfer_plan;
mod transfer_report;
pub mod workspace;

pub use api::{
    AccessApi, ApplicationApi, CatalogApi, ConfigApi, DiagnosticsApi, JobApi, RegistryApi,
    TransferApi,
};
pub use app::Application;
pub use bucket::{
    AddBucketInput, BucketRef, BucketService, DiscoveredBucket, ReadabilityCheck, UpdateBucketInput,
};
pub use commands::{CopyObjectsCommand, DownloadObjectsCommand, UploadObjectsCommand};
pub use config_bundle::{BucketExport, ConfigBundle, ImportReport};
pub use connection::{
    AddConnectionInput, ConnectionRef, ConnectionService, CredentialInput, UpdateConnectionInput,
};
pub use copy::{validate_copy_target, CopyOptions, CopyPair};
pub use diagnostics::{CapabilityReport, CheckResult, ConnectionDiagnostics, ConnectionTestReport};
pub use files::{FileEntry, FilesPage, FilesService};
pub use gateway::RemoteGateway;
pub use health::{HealthReport, HealthService};
pub use indexer::{BucketIndexStatus, IndexReport, IndexService};
pub use job::{JobRef, JobService};
pub use link::{
    parse_ttl, resolve_link_base, GeneratedLink, LinkFormat, LinkOutcome, LinkProfileInput,
    LinkRequest,
};
pub use notifications::{JobNotification, ProgressEvent};
pub use outbox::{DispatchReport, OutboxDispatcher, OutboxHandler};
pub use ports::{
    ApplicationDependencies, CatalogDependencies, RegistryDependencies, RuntimeDependencies,
    TransferDependencies,
};
pub use remote::{
    expand_reference, expand_references, LsEntry, LsResult, RemoteService, RemoteUri, RemoveItem,
    RemoveOutcome, RemoveReport,
};
pub use rstore_catalog::{FileFilter, FileSort, PageRequest, SortField, SortOrder};
pub use tag::{CreateTagInput, MergeReport, TagBatchOutcome, TagRef, TagService, UpdateTagInput};
pub use transfer::{
    classify_download, download_plans, BatchDownloadReport, DownloadMode, DownloadPlan, Outcome,
    RunOptions, TransferReport, TransferReportItem, UploadPlan,
};
pub use workspace::{
    CreateWorkspaceInput, UpdateWorkspaceInput, WorkspaceDetail, WorkspaceRef, WorkspaceService,
};
