//! One vocabulary for per-item batch results and their aggregate severity.
//!
//! Every batch-shaped operation classifies each item with the same
//! [`Disposition`] — succeeded, skipped with a typed reason, blocked by a
//! guard or an unresolved policy, or failed — and aggregates the run into a
//! [`Severity`]. Severity is the single home of the question "did this batch
//! succeed": renderers map dispositions to display, and entry layers map
//! severity onto their own exit semantics, without re-deriving policy from
//! raw counts.
//!
//! [`Failure`] is a cloneable, serializable projection of a domain error —
//! reports outlive the error values that produced them.

use rstore_domain::{Error, ErrorCode};
use serde::ser::{Serialize, SerializeStruct, Serializer};

/// Why an item was intentionally not executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// Dry run: the operation was fully resolved but deliberately not
    /// executed.
    DryRun,
    /// The conflict policy declined the planned target (e.g. it exists and
    /// the policy says skip).
    Conflict,
}

/// Why a guard or policy refused the item before it could run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockReason {
    /// The bucket forbids mutations.
    ReadOnly,
    /// The bucket forbids deletions.
    DeleteForbidden,
    /// The conflict policy could not resolve a target (unresolved `ask`, or
    /// rename found no free name).
    Unresolved,
}

/// Cloneable projection of one item's error: what a report can carry after
/// the live [`Error`] is gone.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Failure {
    pub code: ErrorCode,
    pub message: String,
}

impl Failure {
    /// Projects a live domain error into its reportable form.
    pub fn from_error(e: &Error) -> Self {
        Self {
            code: e.code,
            message: e.to_string(),
        }
    }
}

/// Terminal classification of one batch item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    /// The item completed as requested.
    Succeeded,
    /// The item was intentionally not executed.
    Skipped(SkipReason),
    /// A guard or an unresolvable policy refused the item.
    Blocked(BlockReason),
    /// The item failed.
    Failed(Failure),
}

impl Disposition {
    /// Stable machine tag for serialization and rendering (`"succeeded"`,
    /// `"skipped"`, `"blocked"`, `"failed"`).
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Skipped(_) => "skipped",
            Self::Blocked(_) => "blocked",
            Self::Failed(_) => "failed",
        }
    }

    /// Whether the item completed as requested.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

/// One item's result: its display target, the [`Disposition`], optional free
/// detail text (guard message, conflict detail, dry-run preview), and the
/// operation payload on success.
#[derive(Debug, Clone)]
pub struct ItemOutcome<T> {
    pub target: String,
    pub disposition: Disposition,
    pub detail: Option<String>,
    pub value: Option<T>,
}

impl<T> ItemOutcome<T> {
    /// A succeeded item carrying an optional payload.
    pub fn succeeded(target: impl Into<String>, value: impl Into<Option<T>>) -> Self {
        Self {
            target: target.into(),
            disposition: Disposition::Succeeded,
            detail: None,
            value: value.into(),
        }
    }

    /// A skipped item with its reason and optional display detail.
    pub fn skipped(target: impl Into<String>, reason: SkipReason, detail: Option<String>) -> Self {
        Self {
            target: target.into(),
            disposition: Disposition::Skipped(reason),
            detail,
            value: None,
        }
    }

    /// A blocked item with its reason and optional display detail.
    pub fn blocked(target: impl Into<String>, reason: BlockReason, detail: Option<String>) -> Self {
        Self {
            target: target.into(),
            disposition: Disposition::Blocked(reason),
            detail,
            value: None,
        }
    }

    /// A failed item projected from a live domain error.
    pub fn failed(target: impl Into<String>, e: &Error) -> Self {
        Self {
            target: target.into(),
            disposition: Disposition::Failed(Failure::from_error(e)),
            detail: None,
            value: None,
        }
    }
}

impl<T: Serialize> Serialize for ItemOutcome<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // `value` is omitted when absent so `()`-shaped reports keep the same
        // flat JSON shape as before the vocabulary existed.
        let fields = if self.value.is_some() { 4 } else { 3 };
        let mut out = serializer.serialize_struct("ItemOutcome", fields)?;
        out.serialize_field("target", &self.target)?;
        out.serialize_field("disposition", self.disposition.tag())?;
        out.serialize_field("detail", &self.detail)?;
        if let Some(value) = &self.value {
            out.serialize_field("value", value)?;
        }
        out.end()
    }
}

/// Aggregated per-item results for one batch operation.
#[derive(Debug, Clone)]
pub struct BatchOutcome<T> {
    items: Vec<ItemOutcome<T>>,
}

impl<T> BatchOutcome<T> {
    /// A batch from its per-item outcomes.
    pub fn new(items: Vec<ItemOutcome<T>>) -> Self {
        Self { items }
    }

    /// The per-item outcomes, in submission order.
    pub fn items(&self) -> &[ItemOutcome<T>] {
        &self.items
    }

    /// Consumes the batch, returning the per-item outcomes.
    pub fn into_items(self) -> Vec<ItemOutcome<T>> {
        self.items
    }

    /// Number of items.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the batch had no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Items that completed as requested.
    pub fn succeeded(&self) -> usize {
        self.count(|d| d.is_success())
    }

    /// Items that failed.
    pub fn failed(&self) -> usize {
        self.count(|d| matches!(d, Disposition::Failed(_)))
    }

    /// Items skipped or blocked.
    pub fn not_run(&self) -> usize {
        self.count(|d| matches!(d, Disposition::Skipped(_) | Disposition::Blocked(_)))
    }

    fn count(&self, f: impl Fn(&Disposition) -> bool) -> usize {
        self.items.iter().filter(|i| f(&i.disposition)).count()
    }

    /// The aggregate verdict. `Failed` dominates, then `Partial` (something
    /// did not run), else `AllOk`. An empty batch is `AllOk`.
    pub fn severity(&self) -> Severity {
        let mut partial = false;
        for item in &self.items {
            match &item.disposition {
                Disposition::Failed(_) => return Severity::Failed,
                Disposition::Skipped(_) | Disposition::Blocked(_) => partial = true,
                Disposition::Succeeded => {}
            }
        }
        if partial {
            Severity::Partial
        } else {
            Severity::AllOk
        }
    }
}

impl<T: Serialize> Serialize for BatchOutcome<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.items.serialize(serializer)
    }
}

impl<T> FromIterator<ItemOutcome<T>> for BatchOutcome<T> {
    fn from_iter<I: IntoIterator<Item = ItemOutcome<T>>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

/// The aggregate verdict for a batch. Ordered from best to worst so runs of
/// batches combine with `max`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Every item succeeded (vacuously for an empty batch).
    AllOk,
    /// Nothing failed, but at least one item was skipped or blocked.
    Partial,
    /// At least one item failed.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(target: &str) -> ItemOutcome<()> {
        ItemOutcome::succeeded(target, None)
    }

    #[test]
    fn severity_all_ok_when_everything_succeeds() {
        let batch: BatchOutcome<()> = [ok("a"), ok("b")].into_iter().collect();
        assert_eq!(batch.severity(), Severity::AllOk);
        assert_eq!(batch.succeeded(), 2);
    }

    #[test]
    fn empty_batch_is_all_ok() {
        let batch: BatchOutcome<()> = BatchOutcome::new(Vec::new());
        assert_eq!(batch.severity(), Severity::AllOk);
        assert!(batch.is_empty());
    }

    #[test]
    fn skipped_without_failure_is_partial() {
        let batch: BatchOutcome<()> = [
            ok("a"),
            ItemOutcome::skipped("b", SkipReason::DryRun, Some("would copy".into())),
        ]
        .into_iter()
        .collect();
        assert_eq!(batch.severity(), Severity::Partial);
        assert_eq!(batch.not_run(), 1);
    }

    #[test]
    fn failure_dominates_partial() {
        let e = Error::unknown("boom");
        let batch: BatchOutcome<()> = [
            ok("a"),
            ItemOutcome::blocked("b", BlockReason::ReadOnly, None),
            ItemOutcome::failed("c", &e),
        ]
        .into_iter()
        .collect();
        assert_eq!(batch.severity(), Severity::Failed);
        assert_eq!(batch.failed(), 1);
        assert_eq!(batch.succeeded(), 1);
        assert_eq!(batch.not_run(), 1);
    }

    #[test]
    fn failure_message_projects_display_text() {
        let e = Error::unknown("boom");
        let item: ItemOutcome<()> = ItemOutcome::failed("a", &e);
        match item.disposition {
            Disposition::Failed(f) => assert_eq!(f.message, e.to_string()),
            other => panic!("expected failed, got {other:?}"),
        }
    }
}
