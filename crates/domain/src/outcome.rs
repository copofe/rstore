//! The classification vocabulary for batch results: per-item
//! [`Disposition`], its typed reasons, and the aggregate [`Severity`].
//!
//! Pure business words — no I/O, no error values — so every layer that
//! classifies items (transfer engine, run loop, non-transfer batches such
//! as links and tags, renderers) shares one vocabulary and one aggregation
//! rule instead of re-deriving "did this batch succeed" per module.

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
    /// The bucket forbids deletions.
    DeleteForbidden,
    /// The conflict policy could not resolve a target (unresolved `ask`, or
    /// rename found no free name).
    Unresolved,
}

/// Cloneable projection of one item's error: what a report can carry
/// after the live [`crate::Error`] is gone.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Failure {
    pub code: crate::ErrorCode,
    pub message: String,
}

impl Failure {
    /// Projects a live domain error into its reportable form.
    pub fn from_error(e: &crate::Error) -> Self {
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

    /// Whether the item neither succeeded nor failed — the shapes a
    /// `Partial` aggregate is made of.
    pub fn is_not_run(&self) -> bool {
        matches!(self, Self::Skipped(_) | Self::Blocked(_))
    }
}

/// The aggregate verdict over a batch. Ordered from best to worst so runs
/// of batches combine with `max`.
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

/// The one aggregation rule: `Failed` dominates, then `Partial` (something
/// did not run), else `AllOk`. An empty batch is `AllOk`.
pub fn severity_of<'a>(dispositions: impl IntoIterator<Item = &'a Disposition>) -> Severity {
    let mut partial = false;
    for disposition in dispositions {
        match disposition {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn failed() -> Disposition {
        Disposition::Failed(Failure::from_error(&crate::Error::not_found("x")))
    }

    #[test]
    fn severity_rule_is_total() {
        // Empty is AllOk; skip/block make Partial; failure dominates.
        assert_eq!(severity_of([]), Severity::AllOk);
        assert_eq!(severity_of([&Disposition::Succeeded]), Severity::AllOk);
        assert_eq!(
            severity_of([
                &Disposition::Succeeded,
                &Disposition::Skipped(SkipReason::Conflict),
            ]),
            Severity::Partial
        );
        assert_eq!(
            severity_of([
                &Disposition::Succeeded,
                &Disposition::Blocked(BlockReason::Unresolved),
            ]),
            Severity::Partial
        );
        assert_eq!(
            severity_of([&Disposition::Skipped(SkipReason::DryRun), &failed(),]),
            Severity::Failed
        );
    }

    #[test]
    fn severities_combine_with_max() {
        // Ordered best..worst so batch-of-batches combines with max.
        assert!(Severity::AllOk < Severity::Partial);
        assert!(Severity::Partial < Severity::Failed);
        assert_eq!(Severity::Partial.max(Severity::Failed), Severity::Failed);
    }

    #[test]
    fn tags_are_stable_machine_words() {
        assert_eq!(Disposition::Succeeded.tag(), "succeeded");
        assert_eq!(Disposition::Skipped(SkipReason::DryRun).tag(), "skipped");
        assert_eq!(
            Disposition::Blocked(BlockReason::Unresolved).tag(),
            "blocked"
        );
        assert_eq!(failed().tag(), "failed");
    }
}
