//! Application-layer command records for bulk object operations.
//!
//! Each command bundles the execution plan with its run options so handlers
//! receive a single self-contained request; [`RunControl`] carries what every
//! run is driven by, in one shape for every transfer method.

use crate::transfer::{CancellationToken, ProgressSink};

use crate::copy::CopyOptions;
use crate::transfer_service::{DownloadPlan, RunOptions, UploadPlan};

/// How the caller drives a run: where progress goes and how it is cancelled.
#[derive(Clone, Copy)]
pub struct RunControl<'a> {
    pub progress: &'a dyn ProgressSink,
    pub cancel: &'a CancellationToken,
}

/// Uploads a planned set of objects to a remote storage target.
#[derive(Debug, Clone)]
pub struct UploadObjectsCommand {
    pub plan: UploadPlan,
    pub options: RunOptions,
}

/// Downloads a planned set of remote objects to local storage.
#[derive(Debug, Clone)]
pub struct DownloadObjectsCommand {
    pub plan: DownloadPlan,
    pub options: RunOptions,
}

/// Downloads several remote sources into one local directory, one child job
/// per source.
#[derive(Debug, Clone)]
pub struct BatchDownloadCommand {
    pub plans: Vec<DownloadPlan>,
    pub options: RunOptions,
}

/// Copies a batch of source/target object pairs within or across connections.
#[derive(Debug, Clone)]
pub struct CopyObjectsCommand {
    pub pairs: Vec<crate::copy::CopyPair>,
    pub options: CopyOptions,
}
