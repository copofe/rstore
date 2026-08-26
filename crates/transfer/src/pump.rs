//! The one bounded chunk walk: constant-memory loops over remote ranges.
//!
//! Every download chunk and cross-adapter relay part walks `[0, total)` in
//! exclusive ranges of at most `chunk` bytes, checks cancellation per chunk,
//! and reports cumulative byte progress. [`ChunkPlan`] owns the range math
//! (including the multipart skip decisions); [`pump`] owns the loop.

use async_trait::async_trait;
use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::Result;
use rstore_storage_api::{ReadRange, Storage};

use crate::progress::Progress;

/// Successive exclusive ranges of at most `chunk` bytes covering a `[start,
/// total)` window. `start` is where the walk resumes — a staged file's
/// already-stored length — so both the ranges and the progress base model
/// resume in one place.
#[derive(Debug, Clone, Copy)]
pub struct ChunkPlan {
    start: u64,
    total: u64,
    chunk: u64,
}

impl ChunkPlan {
    /// A chunk size of zero is a broken internal config, not user input —
    /// fail loudly rather than loop forever.
    pub fn new(total: u64, chunk: u64) -> Self {
        Self::resumed(total, chunk, 0)
    }

    /// A walk that continues from `start` bytes already stored.
    pub fn resumed(total: u64, chunk: u64, start: u64) -> Self {
        assert!(chunk > 0, "chunk size must be positive");
        Self {
            start: start.min(total),
            total,
            chunk,
        }
    }

    /// The plan's ranges, first to last (the last may be short).
    pub fn ranges(&self) -> impl Iterator<Item = ReadRange> + '_ {
        let total = self.total;
        let chunk = self.chunk;
        let mut start = self.start;
        std::iter::from_fn(move || {
            if start >= total {
                return None;
            }
            let end = (start + chunk).min(total);
            let range = ReadRange::exclusive(start, end);
            start = end;
            Some(range)
        })
    }

    /// The multipart resume view of the same range math: the plan's parts
    /// whose 1-based numbers are absent from `done`. Yields
    /// `(part_number, range)`, so a caller never mixes offset arithmetic
    /// with skip decisions again — the inline shape that once truncated a
    /// resumed upload by exactly the resumed bytes.
    pub fn pending<'a>(&'a self, done: &'a [u32]) -> impl Iterator<Item = (u32, ReadRange)> + 'a {
        self.ranges()
            .enumerate()
            .filter(move |(index, _)| !done.contains(&(*index as u32 + 1)))
            .map(|(index, range)| (index as u32 + 1, range))
    }

    /// Where the walk begins (bytes already stored before it).
    pub fn start(&self) -> u64 {
        self.start
    }
}

/// A ranged-read producer: one (storage, bucket, key) serving chunk bytes.
pub struct RangedSource<'a> {
    storage: &'a dyn Storage,
    bucket: &'a str,
    key: &'a ObjectPath,
}

impl<'a> RangedSource<'a> {
    pub fn new(storage: &'a dyn Storage, bucket: &'a str, key: &'a ObjectPath) -> Self {
        Self {
            storage,
            bucket,
            key,
        }
    }

    async fn read(&self, range: ReadRange) -> Result<Bytes> {
        self.storage.read(self.bucket, self.key, Some(range)).await
    }
}

/// Consumer of one chunk's bytes. `index` is the 0-based plan position (its
/// 1-based value is the multipart part number for part-shaped consumers).
#[async_trait]
pub trait ChunkSink {
    async fn accept(&mut self, index: usize, body: Bytes) -> Result<()>;
}

/// Drive one plan to completion: per chunk — check cancellation, read, check
/// cancellation again (a cancel landing during the read settles before the
/// write), consume, then report cumulative progress from the plan's start
/// (the bytes that were already safely stored before the walk).
pub async fn pump(
    source: &RangedSource<'_>,
    plan: ChunkPlan,
    ctx: &crate::run::RunContext<'_>,
    sink: &mut dyn ChunkSink,
) -> Result<()> {
    let (cancel, progress) = (ctx.cancel(), ctx.progress());
    let mut done = plan.start();
    for (index, range) in plan.ranges().enumerate() {
        cancel.check()?;
        let len = range.end.map(|end| end - range.start).unwrap_or(0);
        let body = source.read(range).await?;
        cancel.check()?;
        sink.accept(index, body).await?;
        done += len;
        progress.report(Progress::Bytes {
            done: done.min(plan.total),
            total: plan.total,
        });
    }
    Ok(())
}
