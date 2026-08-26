//! One vocabulary for per-item batch results and their aggregate severity.
//!
//! The classification words themselves ([`Disposition`], [`SkipReason`],
//! [`BlockReason`], [`Severity`]) live in `rstore_domain::outcome` so every
//! layer shares them; this module adds the reportable shapes on top: the
//! cloneable [`Failure`] projection of a domain error, and the generic
//! per-item [`ItemOutcome`] plus [`BatchOutcome`] aggregates used by
//! non-transfer batches (links, tags, removal).
//!
//! [`Failure`] is a cloneable, serializable projection of a domain error —
//! reports outlive the error values that produced them.

pub use rstore_domain::outcome::{
    severity_of, BlockReason, Disposition, Failure, Severity, SkipReason,
};

use serde::ser::{Serialize, SerializeStruct, Serializer};

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
    pub fn failed(target: impl Into<String>, e: &rstore_domain::Error) -> Self {
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
        self.count(|d| d.is_not_run())
    }

    fn count(&self, f: impl Fn(&Disposition) -> bool) -> usize {
        self.items.iter().filter(|i| f(&i.disposition)).count()
    }

    /// The aggregate verdict — the single rule, shared with every other
    /// batch shape via [`severity_of`].
    pub fn severity(&self) -> Severity {
        severity_of(self.items.iter().map(|i| &i.disposition))
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
