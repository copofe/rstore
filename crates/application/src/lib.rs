//! Application Core for the context APIs.
//!
//! This crate owns use-case orchestration and depends on domain contracts,
//! application ports and provider-facing interfaces only. SQLite, SQLx, the
//! AWS SDK and GPUI enter through the Bootstrap composition root, never through
//! this public layer.

pub mod api;
pub mod app;
pub mod batch_outcome;
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
pub(crate) mod guarded_storage;
pub mod health;
mod index_run;
pub mod indexer;
pub mod job;
pub mod job_run;
pub mod link;
pub mod locator;
pub(crate) mod mutation;
pub mod outbox;
pub mod ports;
pub mod reference;
pub mod remote;
pub mod tag;
pub mod transfer;
mod transfer_plan;
mod transfer_report;
pub mod verify;
pub mod workspace;

pub use api::{
    AccessApi, ApplicationApi, CatalogApi, ConfigApi, DiagnosticsApi, JobApi, RegistryApi,
    TransferApi,
};
pub use app::Application;
pub use batch_outcome::{
    BatchOutcome, BlockReason, Disposition, Failure, ItemOutcome, Severity, SkipReason,
};
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
    parse_ttl, resolve_link_base, GeneratedLink, LinkFormat, LinkProfileInput, LinkRequest,
};
pub use outbox::{DispatchReport, OutboxDispatcher, OutboxHandler};
pub use ports::{
    ApplicationDependencies, CatalogDependencies, RegistryDependencies, RuntimeDependencies,
    TransferDependencies,
};
pub use reference::{expand_reference, expand_references, RemoteUri};
pub use remote::{LsEntry, LsResult, RemoteService, RemoveReport};
pub use rstore_catalog::{FileFilter, FileSort, PageRequest, SortField, SortOrder};
pub use tag::{CreateTagInput, MergeReport, TagRef, TagService, UpdateTagInput};
pub use transfer::{
    classify_download, download_plans, BatchDownloadReport, DownloadMode, DownloadPlan, RunOptions,
    TransferReport, TransferReportItem, UploadPlan,
};
pub use verify::RemoteVerifyHandler;
pub use workspace::{
    CreateWorkspaceInput, UpdateWorkspaceInput, WorkspaceDetail, WorkspaceRef, WorkspaceService,
};

#[cfg(feature = "test-util")]
pub mod test_util;
