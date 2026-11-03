//! The rstore engine: use cases, the SQLite store, the transfer executor, and
//! job bookkeeping behind one context-oriented API.
//!
//! The store (`db`), the streaming transfer machinery (`transfer`), and the
//! catalog query vocabulary (`catalog`) are private modules: entry points see
//! only the API groups, commands, views, and reports re-exported here. GPUI
//! never enters this crate; the provider SDK stays behind `rstore-storage`.

mod api;
mod app;
mod batch_outcome;
mod batch_plan;
mod bucket;
mod catalog;
mod commands;
mod config_bundle;
mod connection;
mod copy;
mod db;
mod diagnostics;
pub mod dirs;
mod events;
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
mod onboarding;
mod reference;
mod remote;
mod runtime;
mod tag;
mod transfer;
mod transfer_plan;
mod transfer_report;
mod transfer_service;
mod workspace;
mod write_through;

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
pub use catalog::{FileFilter, FileSort, PageRequest, SortField, SortOrder};
pub use config_bundle::{BucketExport, ConfigBundle, ImportReport};
pub use connection::{
    AddConnectionInput, ConnectionRef, ConnectionService, CredentialInput, ProviderPreset,
    UpdateConnectionInput,
};
pub use copy::{validate_copy_target, CopyOptions, CopyPair};
pub use diagnostics::{CapabilityReport, CheckResult, ConnectionTestReport};
pub use dirs::{resolve_app_dirs, AppDirs};
pub use events::{EngineEvent, EventBus};
pub use files::{FileEntry, FilesPage, FilesService};
pub use health::HealthReport;
pub use indexer::{
    BucketIndexStatus, GcReport, IndexMode, IndexReport, IndexScope, IndexService, IndexStats,
};
pub use job::{CancelReport, JobRef, JobService};
pub use link::{
    parse_ttl, resolve_link_base, GeneratedLink, LinkFormat, LinkProfileInput, LinkRequest,
};
pub use onboarding::{Onboarding, Registration};
pub use reference::{NewBucket, References, RemoteUri, TransferRoute};
pub use remote::{LsEntry, LsResult, RemoteService, RemovalPlan, RemoveReport};
pub use runtime::{Runtime, RuntimeConfig};
pub use tag::{
    AttachReport, CreateTagInput, MergeReport, TagRef, TagService, TagWithCount, UpdateTagInput,
};
pub use transfer::{CancellationToken, NullProgressSink, Progress, ProgressSink, TransferConfig};
pub use transfer_service::{
    classify_download, download_plans, BatchDownloadReport, DownloadMode, DownloadPlan, RunOptions,
    TransferReport, TransferReportItem, UploadPlan,
};
pub use workspace::{
    CreateWorkspaceInput, UpdateWorkspaceInput, WorkspaceDetail, WorkspaceRef, WorkspaceService,
    WorkspaceWithCount,
};
