//! Remote key/prefix expansion: turn a reference key into the concrete
//! object list a batch operation will act on, plus the shared paginated
//! listing driver.

use async_trait::async_trait;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, Result};
use rstore_storage_api::model::{ListOptions, ListPage};
use rstore_storage_api::Storage;

/// Normalize a directory-style key into a listing prefix: strip a trailing
/// slash, then re-add exactly one (empty key stays empty — bucket root).
pub(crate) fn listing_prefix(key: &str) -> String {
    let trimmed = key.trim_end_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}/")
    }
}

/// Whether a key addresses a directory/prefix (empty or trailing slash)
/// rather than a single object.
pub(crate) fn is_directory_ref(key: &str) -> bool {
    key.is_empty() || key.ends_with('/')
}

/// One expanded object: its full path plus the key relative to the
/// requested prefix.
#[derive(Debug)]
pub(crate) struct Expanded {
    pub path: ObjectPath,
    pub relative: String,
}

/// List every object under a directory-style prefix, non-recursively in
/// the flat-listing sense (no delimiter, so the whole subtree is walked).
///
/// Errors when `key` is an exact object key rather than a directory
/// reference, when a listed key falls outside the requested prefix
/// (adapter contract violation), or on path validation failure.
pub(crate) async fn expand_prefix(
    storage: &dyn Storage,
    bucket: &str,
    key: &str,
) -> Result<Vec<Expanded>> {
    if !is_directory_ref(key) {
        return Err(Error::invalid_input(format!(
            "`{key}` is an exact object key; directory references must end with `/`"
        )));
    }
    let prefix = listing_prefix(key);
    let mut sink = CollectKeys::default();
    scan_pages(storage, bucket, Some(&prefix), None, 1000, &mut sink).await?;
    sink.keys
        .into_iter()
        .map(|key| {
            let relative = key
                .strip_prefix(&prefix)
                .ok_or_else(|| {
                    Error::unknown("storage listing returned a key outside its requested prefix")
                })?
                .to_string();
            Ok(Expanded {
                path: ObjectPath::new(key)?,
                relative,
            })
        })
        .collect()
}

/// Reject a directory reference passed without `--recursive`.
///
/// Exact object keys pass regardless of `recursive`.
pub(crate) fn require_recursive(reference: &str, key: &str, recursive: bool) -> Result<()> {
    if is_directory_ref(key) && !recursive {
        return Err(Error::invalid_input(format!(
            "directory reference `{reference}` requires `--recursive`"
        )));
    }
    Ok(())
}

/// Expand a source key into the entries an operation will act on: the full
/// subtree for a recursive directory reference, otherwise the single
/// object named by the key.
pub(crate) async fn source_entries(
    storage: &dyn Storage,
    bucket: &str,
    key: &str,
    recursive: bool,
) -> Result<Vec<Expanded>> {
    if recursive && is_directory_ref(key) {
        expand_prefix(storage, bucket, key).await
    } else {
        let path = ObjectPath::new(key)?;
        Ok(vec![Expanded {
            relative: path.file_name().to_string(),
            path,
        }])
    }
}

/// Consumer of one listing page during a paginated scan.
///
/// Returns `true` to continue scanning, `false` to stop early.
#[async_trait]
pub(crate) trait PageSink: Send {
    async fn page(&mut self, page: &ListPage) -> Result<bool>;
}

/// Walk a bucket listing page by page, feeding each page to `sink` until
/// the sink stops the scan or the listing is exhausted.
///
/// `after` seeds the continuation token for resumed scans. Every storage
/// error is annotated with the bucket name for diagnostics.
pub(crate) async fn scan_pages(
    storage: &dyn Storage,
    bucket: &str,
    prefix: Option<&str>,
    after: Option<String>,
    max_keys: u32,
    sink: &mut dyn PageSink,
) -> Result<()> {
    let mut token = after;
    loop {
        let page = storage
            .list(
                bucket,
                &ListOptions {
                    prefix: prefix.map(str::to_string),
                    delimiter: None,
                    continuation_token: token,
                    max_keys: Some(max_keys),
                },
            )
            .await
            .map_err(|e| e.with_context("bucket", bucket.to_string()))?;
        if !sink.page(&page).await? {
            return Ok(());
        }
        match page.continuation_token {
            Some(next) => token = Some(next),
            None => return Ok(()),
        }
    }
}

#[derive(Default)]
struct CollectKeys {
    keys: Vec<String>,
}

#[async_trait]
impl PageSink for CollectKeys {
    async fn page(&mut self, page: &ListPage) -> Result<bool> {
        self.keys.extend(
            page.objects
                .iter()
                .map(|object| object.metadata.key.clone()),
        );
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_test_support::FakeStorage;

    fn seeded() -> FakeStorage {
        let storage = FakeStorage::new();
        storage.seed_bucket("media");
        storage.seed("media", "docs/a.txt", b"a", None);
        storage.seed("media", "docs/sub/b.txt", b"b", None);
        storage.seed("media", "docs-old/c.txt", b"c", None);
        storage.seed("media", "root.txt", b"r", None);
        storage
    }

    #[test]
    fn prefix_normalisation() {
        assert_eq!(listing_prefix(""), "");
        assert_eq!(listing_prefix("/"), "");
        assert_eq!(listing_prefix("docs"), "docs/");
        assert_eq!(listing_prefix("docs/"), "docs/");
        assert_eq!(listing_prefix("docs///"), "docs/");
    }

    #[test]
    fn directory_refs() {
        assert!(is_directory_ref(""));
        assert!(is_directory_ref("/"));
        assert!(is_directory_ref("docs/"));
        assert!(!is_directory_ref("docs"));
        assert!(!is_directory_ref("docs/a.txt"));
    }

    #[tokio::test]
    async fn slashed_prefix_never_matches_sibling_keys() {
        // A directory prefix must not match a sibling key with the same text.
        let expanded = expand_prefix(&seeded(), "media", "docs/").await.unwrap();
        let relatives: Vec<&str> = expanded.iter().map(|e| e.relative.as_str()).collect();
        assert_eq!(relatives, vec!["a.txt", "sub/b.txt"]);
    }

    #[tokio::test]
    async fn bare_bucket_lists_everything_at_the_root() {
        let expanded = expand_prefix(&seeded(), "media", "").await.unwrap();
        assert_eq!(expanded.len(), 4);
        assert!(expanded.iter().all(|e| !e.relative.starts_with('-')));
    }

    #[tokio::test]
    async fn unslashed_key_is_not_a_directory_reference() {
        let err = expand_prefix(&seeded(), "media", "docs")
            .await
            .expect_err("an unslashed key must stay exact");
        assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
    }

    #[test]
    fn directory_reference_requires_recursive() {
        let err = require_recursive("nas/media/docs/", "docs/", false).unwrap_err();
        assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
        assert!(err.message.contains("nas/media/docs/"));
        assert!(
            require_recursive("nas/media/docs/", "docs/", true).is_ok(),
            "recursive unlocks the directory reference"
        );
        assert!(
            require_recursive("nas/media/a.txt", "a.txt", false).is_ok(),
            "an exact object never needs recursive"
        );
    }

    #[tokio::test]
    async fn source_entries_pairing_rule() {
        // Directory + recursive: prefix walk with relativised names.
        let expanded = source_entries(&seeded(), "media", "docs/", true)
            .await
            .unwrap();
        let relatives: Vec<&str> = expanded.iter().map(|e| e.relative.as_str()).collect();
        assert_eq!(relatives, vec!["a.txt", "sub/b.txt"]);

        // Bare key with or without recursive: one exact object whose
        // relative name is its file name — `docs` never matches `docs-old`.
        for recursive in [false, true] {
            let expanded = source_entries(&seeded(), "media", "docs", recursive)
                .await
                .unwrap();
            assert_eq!(expanded.len(), 1);
            assert_eq!(expanded[0].path.as_str(), "docs");
            assert_eq!(expanded[0].relative, "docs");
        }
    }

    struct StopAfterFirstPage {
        keys: Vec<String>,
        token: Option<String>,
    }

    #[async_trait]
    impl PageSink for StopAfterFirstPage {
        async fn page(&mut self, page: &ListPage) -> Result<bool> {
            self.keys
                .extend(page.objects.iter().map(|o| o.metadata.key.clone()));
            self.token = page.continuation_token.clone();
            Ok(false)
        }
    }

    #[tokio::test]
    async fn scan_pages_stops_early_and_resumes_from_the_token() {
        let storage = seeded();
        let mut first = StopAfterFirstPage {
            keys: Vec::new(),
            token: None,
        };
        scan_pages(&storage, "media", None, None, 2, &mut first)
            .await
            .unwrap();
        assert!(
            first.keys.len() <= 2,
            "an early stop leaves later pages unlisted"
        );
        let token = first.token.expect("a truncated first page carries a token");

        let mut rest = CollectKeys::default();
        scan_pages(&storage, "media", None, Some(token), 2, &mut rest)
            .await
            .unwrap();

        let mut walked = first.keys;
        walked.extend(rest.keys);
        walked.sort();
        walked.dedup();
        assert_eq!(
            walked.len(),
            4,
            "stop + resume covers every row exactly once: {walked:?}"
        );
    }
}
