//! Application-layer command records for bulk object operations.
//!
//! Each command bundles the execution plan with its run options so handlers
//! receive a single self-contained request.

use crate::copy::CopyOptions;
use crate::transfer::{DownloadPlan, RunOptions, UploadPlan};

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

/// Copies a batch of source/target object pairs within or across connections.
#[derive(Debug, Clone)]
pub struct CopyObjectsCommand {
    pub pairs: Vec<crate::copy::CopyPair>,
    pub options: CopyOptions,
}
