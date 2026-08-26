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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_covers_total_in_bounded_exclusive_ranges() {
        let ranges: Vec<ReadRange> = ChunkPlan::new(10, 4).ranges().collect();
        let expected = [
            ReadRange::exclusive(0, 4),
            ReadRange::exclusive(4, 8),
            ReadRange::exclusive(8, 10),
        ];
        assert_eq!(ranges, expected);
    }

    #[test]
    fn plan_of_empty_total_is_empty() {
        assert_eq!(ChunkPlan::new(0, 4).ranges().count(), 0);
    }

    #[test]
    fn plan_where_chunk_divides_total_has_no_short_tail() {
        let ranges: Vec<ReadRange> = ChunkPlan::new(8, 4).ranges().collect();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[1], ReadRange::exclusive(4, 8));
    }

    #[test]
    fn resumed_plan_walks_only_unstored_bytes() {
        let ranges: Vec<ReadRange> = ChunkPlan::resumed(24, 4, 8).ranges().collect();
        assert_eq!(ranges.first(), Some(&ReadRange::exclusive(8, 12)));
        assert_eq!(ranges.last(), Some(&ReadRange::exclusive(20, 24)));
        // A start past the total is clamped, never an empty-range walk.
        assert_eq!(ChunkPlan::resumed(24, 4, 30).ranges().count(), 0);
    }

    #[test]
    fn pending_skips_done_parts_and_keeps_numbers_aligned() {
        let plan = ChunkPlan::new(24, 4);
        let pending: Vec<(u32, ReadRange)> = plan.pending(&[1, 2]).collect();
        assert_eq!(
            pending,
            vec![
                (3, ReadRange::exclusive(8, 12)),
                (4, ReadRange::exclusive(12, 16)),
                (5, ReadRange::exclusive(16, 20)),
                (6, ReadRange::exclusive(20, 24)),
            ]
        );
        // A done set that names no live part changes nothing.
        assert_eq!(plan.pending(&[99]).count(), 6);
        assert!(plan.pending(&[1, 2, 3, 4, 5, 6]).count() == 0);
    }

    #[test]
    #[should_panic(expected = "chunk size must be positive")]
    fn zero_chunk_fails_loudly() {
        let _ = ChunkPlan::new(10, 0);
    }
}
