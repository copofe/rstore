//! Typed command/query DTOs for the stable application API.

use crate::copy::CopyOptions;
use crate::transfer::{DownloadPlan, RunOptions, UploadPlan};

#[derive(Debug, Clone)]
pub struct UploadObjectsCommand {
    pub plan: UploadPlan,
    pub options: RunOptions,
}

#[derive(Debug, Clone)]
pub struct DownloadObjectsCommand {
    pub plan: DownloadPlan,
    pub options: RunOptions,
}

#[derive(Debug, Clone)]
pub struct CopyObjectsCommand {
    pub pairs: Vec<crate::copy::CopyPair>,
    pub options: CopyOptions,
}
