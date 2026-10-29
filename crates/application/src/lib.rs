//! Application Core for the context APIs.
//!
//! This crate owns use-case orchestration and depends on domain contracts,
//! application ports and provider-facing interfaces only. SQLite, SQLx, the
//! AWS SDK and GPUI enter through the Bootstrap composition root, never through
//! this public layer.

mod api;
mod app;
mod batch_outcome;
mod batch_plan;
mod bucket;
mod catalog_follow;
mod commands;
mod config_bundle;
mod connection;
mod copy;
mod diagnostics;
mod expand;
mod files;
mod gateway;
mod guarded_storage;
mod health;
mod index_run;
mod indexer;
mod job;
mod job_run;
mod link;
mod locator;
mod mutation;
mod onboarding;
mod outbox;
mod reference;
mod remote;
mod settlement;
mod tag;
mod transfer;
mod transfer_plan;
mod transfer_report;
mod verify;
mod workspace;

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
pub use commands::{
    BatchDownloadCommand, CopyObjectsCommand, DownloadObjectsCommand, RunControl,
    UploadObjectsCommand,
};
// The run-driving types callers construct; re-exported so entry points never
// depend on the transfer engine crate.
pub use config_bundle::{BucketExport, ConfigBundle, ImportReport};
pub use connection::{
    AddConnectionInput, ConnectionRef, ConnectionService, CredentialInput, ProviderPreset,
    UpdateConnectionInput,
};
pub use copy::{validate_copy_target, CopyOptions, CopyPair};
pub use diagnostics::{CapabilityReport, CheckResult, ConnectionTestReport};
pub use files::{FileEntry, FilesPage, FilesService};
pub use health::HealthReport;
pub use indexer::{
    BucketIndexStatus, GcReport, IndexMode, IndexReport, IndexScope, IndexService, IndexStats,
};
pub use job::{JobRef, JobService};
pub use link::{
    parse_ttl, resolve_link_base, GeneratedLink, LinkFormat, LinkProfileInput, LinkRequest,
};
pub use onboarding::{Onboarding, Registration};
pub use outbox::DispatchReport;
pub use reference::{NewBucket, References, RemoteUri, TransferRoute};
pub use remote::{LsEntry, LsResult, RemoteService, RemovalPlan, RemoveReport};
pub use rstore_application_ports::{
    ApplicationDependencies, CatalogDependencies, RegistryDependencies, RuntimeDependencies,
    TransferDependencies,
};
pub use rstore_catalog::{FileFilter, FileSort, PageRequest, SortField, SortOrder};
pub use rstore_transfer::{
    CancellationToken, NullProgressSink, Progress, ProgressSink, TransferConfig,
};
pub use tag::{
    AttachReport, CreateTagInput, MergeReport, TagRef, TagService, TagWithCount, UpdateTagInput,
};
pub use transfer::{
    classify_download, download_plans, BatchDownloadReport, DownloadMode, DownloadPlan, RunOptions,
    TransferReport, TransferReportItem, UploadPlan,
};
pub use workspace::{
    CreateWorkspaceInput, UpdateWorkspaceInput, WorkspaceDetail, WorkspaceRef, WorkspaceService,
    WorkspaceWithCount,
};
