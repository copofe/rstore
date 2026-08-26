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
