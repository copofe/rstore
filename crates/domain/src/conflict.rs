//! The one decision table for [`ConflictPolicy`] (UP-010..013 / DOWN-005/006 /
//! MOVE-013). Upload, download and copy/move all answer "the write target
//! already exists — now what?" through [`resolve_conflict`].
//!
//! Unresolved `Ask` is a per-item [`ConflictDecision::Blocked`], never a
//! silent overwrite: the CLI edge resolves `Ask` before services run, so a
//! service seeing it means nobody resolved it. The rename path derives
//! `name-<random>.ext`, so a full bucket cannot exhaust sequential names.

use std::future::Future;
use std::pin::Pin;

use crate::policy::ConflictPolicy;

/// What to do with the write target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictDecision {
    /// Write to this name (unchanged, or renamed by the policy).
    Proceed(String),
    /// The target exists and the policy leaves it alone (carries the target).
    Skip(String),
    /// The policy is unresolved (`ask`) or rename found no free name
    /// (carries the reason).
    Blocked(String),
}

/// What a resolved conflict means for the item carrying it: the one
/// interpretation of [`ConflictDecision`] shared by the engine's
/// upload/download paths and copy. `Write` carries the final target (equal
/// to the request, or renamed by the policy); the not-done arms carry the
/// report-facing detail verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// Write this target.
    Write(String),
    /// The policy left the existing target alone (`target exists: …`).
    Skipped(String),
    /// The policy could not resolve (unresolved `ask`, rename exhausted).
    Blocked(String),
}

impl ConflictDecision {
    /// Interpret one decision — the single translation every caller shares,
    /// so the skip wording and the outcome taxonomy cannot fork per site.
    pub fn interpret(self) -> Resolved {
        match self {
            ConflictDecision::Proceed(target) => Resolved::Write(target),
            ConflictDecision::Skip(target) => Resolved::Skipped(format!("target exists: {target}")),
            ConflictDecision::Blocked(reason) => Resolved::Blocked(reason),
        }
    }
}

/// Existence probe: does `name` collide? Probe errors read as "no conflict"
/// at this layer — the write itself surfaces real failures. Owned names keep
/// the async probe independent of the caller's borrowed target.
pub type ConflictProbe = dyn Fn(String) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync;

/// How many random redraws `rename` attempts before giving up (`Blocked`).
pub const RENAME_ATTEMPTS: u32 = 5;

/// Resolve one write target against the policy. The initial probe decides
/// whether a conflict exists at all; only then does the policy apply.
pub async fn resolve_conflict(
    target: &str,
    policy: ConflictPolicy,
    exists: &ConflictProbe,
    mut random_hex: impl FnMut() -> String,
) -> ConflictDecision {
    if !exists(target.to_string()).await {
        return ConflictDecision::Proceed(target.to_string());
    }
    match policy {
        ConflictPolicy::Overwrite => ConflictDecision::Proceed(target.to_string()),
        ConflictPolicy::Skip => ConflictDecision::Skip(target.to_string()),
        ConflictPolicy::Ask => ConflictDecision::Blocked(format!(
            "target exists and the conflict policy is unresolved (ask): {target}"
        )),
        ConflictPolicy::Rename => {
            for _ in 0..RENAME_ATTEMPTS {
                let candidate = suffixed_name(target, &random_hex());
                if !exists(candidate.clone()).await {
                    return ConflictDecision::Proceed(candidate);
                }
            }
            ConflictDecision::Blocked(format!(
                "rename found no free name for {target} after {RENAME_ATTEMPTS} attempts"
            ))
        }
    }
}

/// Production random suffix: 8 hex chars from a fresh v4 UUID. Tests inject
/// their own source through the `random_hex` parameter.
pub fn random_suffix8() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..8].to_string()
}

/// Insert `suffix` before the extension of the final path segment:
/// `dir/photo.jpg` + `a1b2c3d4` → `dir/photo-a1b2c3d4.jpg`. Dotfiles and
/// extension-less names append at the end (`.hidden` → `.hidden-a1b2c3d4`).
pub fn suffixed_name(name: &str, suffix: &str) -> String {
    let segment_start = name.rfind('/').map_or(0, |s| s + 1);
    let dot = name[segment_start..]
        .rfind('.')
        .map(|i| i + segment_start)
        .filter(|i| *i > segment_start);
    match dot {
        Some(i) => format!("{}-{}{}", &name[..i], suffix, &name[i..]),
        None => format!("{name}-{suffix}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    /// A scripted probe: exactly the listed names "collide".
    struct FixedProbe(Arc<HashSet<String>>);

    impl FixedProbe {
        fn new(occupied: &[&str]) -> Self {
            Self(Arc::new(occupied.iter().map(|s| s.to_string()).collect()))
        }

        fn probe(&self) -> impl Fn(String) -> Pin<Box<dyn Future<Output = bool> + Send>> {
            let occupied = self.0.clone();
            move |name: String| {
                let occupied = occupied.clone();
                Box::pin(async move { occupied.contains(&name) })
            }
        }
    }

    async fn resolve(target: &str, policy: ConflictPolicy, occupied: &[&str]) -> ConflictDecision {
        let fixed = FixedProbe::new(occupied);
        let probe = fixed.probe();
        resolve_conflict(target, policy, &probe, || "deadbeef".to_string()).await
    }

    #[tokio::test]
    async fn no_conflict_proceeds_regardless_of_policy() {
        for policy in [
            ConflictPolicy::Ask,
            ConflictPolicy::Skip,
            ConflictPolicy::Overwrite,
            ConflictPolicy::Rename,
        ] {
            assert_eq!(
                resolve("a.jpg", policy, &["other.jpg"]).await,
                ConflictDecision::Proceed("a.jpg".to_string())
            );
        }
    }

    #[tokio::test]
    async fn existing_target_follows_the_policy() {
        assert_eq!(
            resolve("a.jpg", ConflictPolicy::Overwrite, &["a.jpg"]).await,
            ConflictDecision::Proceed("a.jpg".to_string())
        );
        assert_eq!(
            resolve("a.jpg", ConflictPolicy::Skip, &["a.jpg"]).await,
            ConflictDecision::Skip("a.jpg".to_string())
        );
        let decision = resolve("a.jpg", ConflictPolicy::Ask, &["a.jpg"]).await;
        assert!(matches!(decision, ConflictDecision::Blocked(ref r) if r.contains("ask")));
    }

    #[tokio::test]
    async fn rename_takes_first_free_random_suffix() {
        // First redraw collides, second is free.
        let fixed = FixedProbe::new(&["a.jpg", "a-deadbeef.jpg"]);
        let probe = fixed.probe();
        let mut suffixes = vec!["deadbeef".to_string(), "cafebabe".to_string()].into_iter();
        let decision = resolve_conflict("a.jpg", ConflictPolicy::Rename, &probe, move || {
            suffixes.next().unwrap()
        })
        .await;
        assert_eq!(
            decision,
            ConflictDecision::Proceed("a-cafebabe.jpg".to_string())
        );
    }

    #[tokio::test]
    async fn rename_blocks_after_exhausted_redraws() {
        let calls = Arc::new(Mutex::new(0u32));
        let counter = calls.clone();
        let p: Box<ConflictProbe> = Box::new(move |name: String| {
            let counter = counter.clone();
            Box::pin(async move {
                *counter.lock().unwrap() += 1;
                name.ends_with(".jpg") // every candidate "collides"
            })
        });
        let decision = resolve_conflict("a.jpg", ConflictPolicy::Rename, p.as_ref(), || {
            "x1y2z3w4".to_string()
        })
        .await;
        assert!(matches!(decision, ConflictDecision::Blocked(ref r) if r.contains("no free name")));
        // initial probe + RENAME_ATTEMPTS redraws
        assert_eq!(*calls.lock().unwrap(), 1 + RENAME_ATTEMPTS);
    }

    #[test]
    fn suffix_placement() {
        assert_eq!(suffixed_name("photo.jpg", "aa"), "photo-aa.jpg");
        assert_eq!(suffixed_name("dir/sub/a.b.c", "aa"), "dir/sub/a.b-aa.c");
        assert_eq!(suffixed_name("dir/.hidden", "aa"), "dir/.hidden-aa");
        assert_eq!(suffixed_name("Makefile", "aa"), "Makefile-aa");
        assert_eq!(suffixed_name("archive.tar.gz", "aa"), "archive.tar-aa.gz");
    }

    #[test]
    fn random_suffix_is_eight_hex_chars() {
        let s = random_suffix8();
        assert_eq!(s.len(), 8);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn interpret_is_the_one_translation() {
        assert_eq!(
            ConflictDecision::Proceed("a-b1c2d3e4.jpg".into()).interpret(),
            Resolved::Write("a-b1c2d3e4.jpg".into())
        );
        assert_eq!(
            ConflictDecision::Skip("a.jpg".into()).interpret(),
            Resolved::Skipped("target exists: a.jpg".into())
        );
        assert_eq!(
            ConflictDecision::Blocked("ask".into()).interpret(),
            Resolved::Blocked("ask".into())
        );
    }
}
