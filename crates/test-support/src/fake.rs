//! An in-memory [`Storage`] for tests.
//!
//! Supports prefix/delimiter listing, ranged reads, the standard CRUD set and
//! multipart uploads, so repository and application contract tests run with
//! zero network. Fault injection ([`FakeStorage::fail_next`] one-shot,
//! [`FakeStorage::fail_until_cleared`] outage) drives adapter error paths;
//! the multipart simulation enforces the provider's legality rules so
//! engine bugs surface offline instead of only against a real bucket.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use rstore_domain::remote::ObjectPath;
use rstore_domain::{Error, ErrorCode, ErrorContext, Result};
use rstore_storage_api::model::{
    BucketSummary, DeleteOutcome, ListOptions, ListPage, ObjectEntry, ObjectMetadata, PutOptions,
    ReadRange, UploadedPart,
};
use rstore_storage_api::{MultipartSession, Storage};
use time::OffsetDateTime;

type Store = HashMap<(String, String), (ObjectMetadata, Bytes)>;
/// Part staging for in-flight multipart uploads
/// (bucket, key, upload_id, part_number).
type PartStore = HashMap<(String, String, String, u32), Vec<u8>>;
type UploadStore = HashSet<(String, String, String)>;

/// S3's real minimum for every non-final part of a multipart upload. The
/// fake enforces it at `complete_multipart` — the same point the provider
/// does — so undersized part plans fail offline instead of only on AWS.
const MIN_NON_FINAL_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Which [`Storage`] method a [`FakeStorage::fail_next`] fault targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMethod {
    ListBuckets,
    List,
    Stat,
    Read,
    Put,
    Delete,
    CreateMultipart,
    UploadPart,
    CompleteMultipart,
    AbortMultipart,
    Presign,
}

struct ErrorTemplate {
    code: ErrorCode,
    message: String,
    retryable: Option<bool>,
    context: Vec<ErrorContext>,
}

impl ErrorTemplate {
    fn from_error(error: Error) -> Self {
        Self {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
            context: error.context,
        }
    }

    fn build(&self) -> Error {
        let mut error = Error::new(self.code, self.message.clone());
        error.retryable = self.retryable;
        error.context = self.context.clone();
        error
    }
}

/// Shared mutable state: the fake's object store, upload registry, part
/// staging and fault queues — held by the fake itself and by every
/// [`FakeMultipartSession`] it hands out. Opaque: every field and method is
/// crate-private.
pub struct FakeInner {
    objects: Mutex<Store>,
    /// Buckets known to `list_buckets`; `put`/`seed` register buckets implicitly.
    buckets: Mutex<Vec<String>>,
    uploads: Mutex<UploadStore>,
    parts: Mutex<PartStore>,
    /// Queued one-shot faults, consumed front-first per method.
    faults: Mutex<Vec<(StorageMethod, Error)>>,
    /// Persistent faults, removed only through [`FakeStorage::clear_fault`].
    persistent_faults: Mutex<Vec<(StorageMethod, ErrorTemplate)>>,
    /// Optional one-shot hook invoked after a successful read.
    read_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

/// In-memory [`Storage`] with fault injection and multipart simulation.
///
/// Clones share one store: every handle sees the same objects, uploads,
/// parts and fault queues. The `fail_*` knobs queue errors per
/// [`StorageMethod`]; the `seed*` methods install fixture state without
/// going through the trait methods those faults would target.
pub struct FakeStorage {
    inner: Arc<FakeInner>,
}

impl std::ops::Deref for FakeStorage {
    type Target = FakeInner;

    fn deref(&self) -> &FakeInner {
        &self.inner
    }
}

impl FakeInner {
    fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            buckets: Mutex::new(Vec::new()),
            uploads: Mutex::new(HashSet::new()),
            parts: Mutex::new(HashMap::new()),
            faults: Mutex::new(Vec::new()),
            persistent_faults: Mutex::new(Vec::new()),
            read_hook: Mutex::new(None),
        }
    }

    /// Consume the next queued fault for `method`, if any.
    fn fault(&self, method: StorageMethod) -> Result<()> {
        if let Some((_, error)) = self
            .persistent_faults
            .lock()
            .expect("fake store poisoned")
            .iter()
            .find(|(fault_method, _)| *fault_method == method)
        {
            return Err(error.build());
        }
        let mut faults = self.faults.lock().expect("fake store poisoned");
        if let Some(pos) = faults.iter().position(|(m, _)| *m == method) {
            let (_, error) = faults.remove(pos);
            return Err(error);
        }
        Ok(())
    }

    fn register_bucket(&self, name: &str) {
        let mut buckets = self.buckets.lock().expect("fake store poisoned");
        if !buckets.iter().any(|b| b == name) {
            buckets.push(name.to_string());
        }
    }

    fn take_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<Vec<u8>> {
        self.parts
            .lock()
            .expect("fake store poisoned")
            .remove(&(
                bucket.to_string(),
                key.to_string(),
                upload_id.to_string(),
                part_number,
            ))
            .ok_or_else(|| Error::not_found("multipart part not found"))
    }

    fn part_buffer(&self, bucket: &str, key: &str, upload_id: &str, part_number: u32, body: &[u8]) {
        self.parts.lock().expect("fake store poisoned").insert(
            (
                bucket.to_string(),
                key.to_string(),
                upload_id.to_string(),
                part_number,
            ),
            body.to_vec(),
        );
    }
}

impl FakeStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(FakeInner::new()),
        }
    }

    /// Fail the next call of `method` with `error` — the fault-injection knob
    /// for adapter error paths (retry classification, abort-on-failure,
    /// surface-the-original-error). One-shot: the call after the failed one
    /// succeeds, so a test scripts a failure sequence without teardown.
    pub fn fail_next(&self, method: StorageMethod, error: rstore_domain::Error) {
        self.faults
            .lock()
            .expect("fake store poisoned")
            .push((method, error));
    }

    /// Keep failing calls of `method` until [`Self::clear_fault`] is called.
    /// This models an outage that persists across retries or pages.
    pub fn fail_until_cleared(&self, method: StorageMethod, error: rstore_domain::Error) {
        self.persistent_faults
            .lock()
            .expect("fake store poisoned")
            .push((method, ErrorTemplate::from_error(error)));
    }

    /// Restore normal behavior for a method with a persistent fault.
    pub fn clear_fault(&self, method: StorageMethod) {
        self.persistent_faults
            .lock()
            .expect("fake store poisoned")
            .retain(|(fault_method, _)| *fault_method != method);
    }

    /// Run a one-shot callback after the next successful read.
    pub fn after_read(&self, hook: impl Fn() + Send + Sync + 'static) {
        *self.read_hook.lock().expect("fake store poisoned") = Some(Box::new(hook));
    }

    fn run_read_hook(&self) {
        let hook = self.read_hook.lock().expect("fake store poisoned").take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Register a bucket (affects `list_buckets` only — listing an unseeded
    /// bucket still returns an empty page).
    pub fn seed_bucket(&self, name: &str) {
        self.inner.register_bucket(name);
    }

    /// Seed a part as if an interrupted run had already uploaded it to the
    /// provider — multipart-resume tests use this to model server-side state.
    pub fn seed_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: &[u8],
    ) {
        self.uploads.lock().expect("fake store poisoned").insert((
            bucket.to_string(),
            key.to_string(),
            upload_id.to_string(),
        ));
        self.parts.lock().expect("fake store poisoned").insert(
            (
                bucket.to_string(),
                key.to_string(),
                upload_id.to_string(),
                part_number,
            ),
            body.to_vec(),
        );
    }

    /// Whether a provider-side multipart upload is still active.
    pub fn has_upload(&self, bucket: &str, key: &str, upload_id: &str) -> bool {
        self.uploads
            .lock()
            .expect("fake store poisoned")
            .contains(&(bucket.to_string(), key.to_string(), upload_id.to_string()))
    }

    /// Number of active provider-side multipart uploads.
    pub fn active_upload_count(&self) -> usize {
        self.uploads.lock().expect("fake store poisoned").len()
    }

    /// Directly seed an object (bypassing `put`) for fixture setup.
    pub fn seed(&self, bucket: &str, key: &str, data: &[u8], content_type: Option<&str>) {
        let body = Bytes::copy_from_slice(data);
        let size = body.len() as u64;
        let meta = ObjectMetadata::after_put(
            key,
            size,
            Some(format!("{size:x}")), // non-cryptographic, not MD5
            &PutOptions {
                content_type: content_type.map(str::to_string),
                ..PutOptions::default()
            },
            OffsetDateTime::now_utc(),
        );
        self.objects
            .lock()
            .expect("fake store poisoned")
            .insert((bucket.to_string(), key.to_string()), (meta, body));
        self.seed_bucket(bucket);
    }
}

impl Default for FakeStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for FakeStorage {
    fn name(&self) -> &str {
        "fake"
    }

    /// A deterministic fake signature so link plumbing (TTL defaults,
    /// expiry propagation) is testable without crypto. The expiry rides the
    /// same clock basis as the real adapter (call time plus TTL), pinned by
    /// `storage_contract`.
    async fn presign(
        &self,
        req: &rstore_storage_api::model::PresignRequest,
    ) -> rstore_domain::Result<rstore_storage_api::model::PresignedUrl> {
        self.fault(StorageMethod::Presign)?;
        Ok(rstore_storage_api::model::PresignedUrl::expiring(
            format!("http://fake/{}/{}?sig=fake", req.bucket, req.key),
            req.method,
            req.ttl_seconds,
            time::OffsetDateTime::now_utc(),
        ))
    }

    async fn list_buckets(&self) -> Result<Vec<BucketSummary>> {
        self.fault(StorageMethod::ListBuckets)?;
        let buckets = self.buckets.lock().expect("fake store poisoned");
        Ok(buckets
            .iter()
            .map(|name| BucketSummary {
                name: name.clone(),
                created_at: None,
            })
            .collect())
    }

    async fn list(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage> {
        self.fault(StorageMethod::List)?;
        let store = self.objects.lock().expect("fake store poisoned");
        let prefix = opts.prefix.as_deref().unwrap_or("");
        let delimiter = opts.delimiter.as_deref().unwrap_or("");

        let mut entries: Vec<(String, Option<ObjectEntry>)> = Vec::new();
        let mut common_prefixes = HashSet::new();

        for ((b, key), (meta, _)) in store.iter() {
            if b != bucket || !key.starts_with(prefix) {
                continue;
            }
            let remainder = &key[prefix.len()..];
            if !delimiter.is_empty() {
                if let Some(idx) = remainder.find(delimiter) {
                    let cp = format!("{prefix}{}", &remainder[..=idx]);
                    if common_prefixes.insert(cp.clone()) {
                        entries.push((cp, None));
                    }
                    continue;
                }
            }
            entries.push((
                key.clone(),
                Some(ObjectEntry {
                    metadata: meta.clone(),
                }),
            ));
        }

        entries.sort_by(|a, b| a.0.cmp(&b.0));
        // Keyset continuation: the token is the last key (or folded prefix)
        // served, so a mid-walk write cannot shift what the next page sees —
        // the same stability dimension `catalog_contract` pins for catalog
        // cursors, and the basis the indexer's resumable scan relies on.
        let start = match opts.continuation_token.as_deref() {
            Some(token) => {
                let last = token
                    .strip_prefix("fake:")
                    .ok_or_else(|| Error::invalid_input("invalid fake continuation token"))?;
                entries.partition_point(|(key, _)| key.as_str() <= last)
            }
            None => 0,
        };
        let limit = opts.max_keys.unwrap_or(1000).max(1) as usize;
        let end = (start + limit).min(entries.len());
        let is_truncated = end < entries.len();
        let continuation_token = is_truncated.then(|| format!("fake:{}", entries[end - 1].0));
        let mut objects = Vec::new();
        let mut prefixes = Vec::new();
        for (key, object) in entries.into_iter().skip(start).take(end - start) {
            match object {
                Some(object) => objects.push(object),
                None => prefixes.push(key),
            }
        }

        Ok(ListPage {
            objects,
            common_prefixes: prefixes,
            continuation_token,
            is_truncated,
        })
    }

    async fn stat(&self, bucket: &str, key: &ObjectPath) -> Result<ObjectMetadata> {
        self.fault(StorageMethod::Stat)?;
        self.objects
            .lock()
            .expect("fake store poisoned")
            .get(&(bucket.to_string(), key.as_str().to_string()))
            .map(|(m, _)| m.clone())
            .ok_or_else(|| Error::not_found(format!("object not found: {bucket}/{}", key.as_str())))
    }

    async fn read(
        &self,
        bucket: &str,
        key: &ObjectPath,
        range: Option<ReadRange>,
    ) -> Result<Bytes> {
        self.fault(StorageMethod::Read)?;
        let store = self.objects.lock().expect("fake store poisoned");
        let bytes = store
            .get(&(bucket.to_string(), key.as_str().to_string()))
            .map(|(_, b)| b.clone())
            .ok_or_else(|| {
                Error::not_found(format!("object not found: {bucket}/{}", key.as_str()))
            })?;
        let result = match range {
            None => Ok(bytes),
            Some(r) => {
                let start = r.start as usize;
                let end = r.end.map(|e| e as usize).unwrap_or(bytes.len());
                if start > bytes.len() || end > bytes.len() || start > end {
                    return Err(Error::new(
                        ErrorCode::InvalidInput,
                        format!("invalid range {start}..{end} for len {}", bytes.len()),
                    ));
                }
                Ok(bytes.slice(start..end))
            }
        };
        if result.is_ok() {
            self.run_read_hook();
        }
        result
    }

    /// In-memory copy: read the stored body and re-put it through the
    /// normal path so metadata rules, bucket registration and fault
    /// injection (`Read`, then `Put`) stay identical to a real round-trip.
    async fn copy(
        &self,
        req: &rstore_storage_api::model::CopyRequest,
        source: &ObjectMetadata,
    ) -> Result<ObjectMetadata> {
        let body = self.read(&req.source_bucket, &req.source_key, None).await?;
        self.put(
            &req.target_bucket,
            &req.target_key,
            body,
            &PutOptions {
                content_type: source.content_type.clone(),
                ..PutOptions::default()
            },
        )
        .await
    }

    async fn put(
        &self,
        bucket: &str,
        key: &ObjectPath,
        body: Bytes,
        opts: &PutOptions,
    ) -> Result<ObjectMetadata> {
        self.fault(StorageMethod::Put)?;
        let size = body.len() as u64;
        let meta = ObjectMetadata::after_put(
            key.as_str(),
            size,
            Some(format!("{size:x}")),
            opts,
            OffsetDateTime::now_utc(),
        );
        self.objects.lock().expect("fake store poisoned").insert(
            (bucket.to_string(), key.as_str().to_string()),
            (meta.clone(), body),
        );
        self.register_bucket(bucket);
        Ok(meta)
    }

    /// Delete is idempotent like the provider's: a missing key is still a
    /// successful delete (`deleted: true`) — the contract pins
    /// "absent afterwards", not "a row went".
    async fn delete(&self, bucket: &str, key: &ObjectPath) -> Result<DeleteOutcome> {
        self.fault(StorageMethod::Delete)?;
        self.objects
            .lock()
            .expect("fake store poisoned")
            .remove(&(bucket.to_string(), key.as_str().to_string()));
        Ok(DeleteOutcome {
            deleted: true,
            version_id: None,
        })
    }

    async fn create_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        _opts: &PutOptions,
    ) -> Result<Box<dyn MultipartSession>> {
        self.fault(StorageMethod::CreateMultipart)?;
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let upload_id = format!("fake-upload-{}", NEXT_ID.fetch_add(1, Ordering::SeqCst));
        self.uploads.lock().expect("fake store poisoned").insert((
            bucket.to_string(),
            key.as_str().to_string(),
            upload_id.clone(),
        ));
        Ok(Box::new(FakeMultipartSession {
            store: self.inner.clone(),
            bucket: bucket.to_string(),
            key: key.as_str().to_string(),
            upload_id,
        }))
    }

    async fn adopt_multipart(
        &self,
        bucket: &str,
        key: &ObjectPath,
        upload_id: &str,
    ) -> Result<Box<dyn MultipartSession>> {
        // Binding an id is no provider call; the session validates the id
        // on first use exactly like the provider's NoSuchUpload.
        Ok(Box::new(FakeMultipartSession {
            store: self.inner.clone(),
            bucket: bucket.to_string(),
            key: key.as_str().to_string(),
            upload_id: upload_id.to_owned(),
        }))
    }
}

/// One in-flight fake multipart upload over the shared state. Implements the
/// same legality rules as the provider so engine/relay bugs fail offline:
/// part-number range, upload existence, the 5 MiB non-final floor and
/// etag echo at completion, ascending order. `copy_part` keeps the session
/// port's default (`CapabilityUnsupported`) — the fake has no server-side
/// copy, which the storage contract pins as the fallback capability.
struct FakeMultipartSession {
    store: Arc<FakeInner>,
    bucket: String,
    key: String,
    upload_id: String,
}

#[async_trait]
impl MultipartSession for FakeMultipartSession {
    fn upload_id(&self) -> &str {
        &self.upload_id
    }

    async fn upload_part(&self, part_number: u32, body: Bytes) -> Result<UploadedPart> {
        self.store.fault(StorageMethod::UploadPart)?;
        // S3 rejects part numbers outside 1..=10_000 outright.
        if !(1..=10_000).contains(&part_number) {
            return Err(Error::invalid_input(format!(
                "part number {part_number} is outside the S3 range 1..=10000"
            )));
        }
        if !self
            .store
            .uploads
            .lock()
            .expect("fake store poisoned")
            .contains(&(
                self.bucket.clone(),
                self.key.clone(),
                self.upload_id.clone(),
            ))
        {
            return Err(Error::not_found("multipart upload not found"));
        }
        let size = body.len() as u64;
        // Accumulate part bytes so completion can assemble the object.
        self.store
            .part_buffer(&self.bucket, &self.key, &self.upload_id, part_number, &body);
        Ok(UploadedPart {
            part_number,
            etag: FakeStorage::etag_for(part_number),
            size,
        })
    }

    async fn complete(&self, parts: &[UploadedPart]) -> Result<ObjectMetadata> {
        self.store.fault(StorageMethod::CompleteMultipart)?;
        // Validate before consuming the upload: an illegal part list leaves
        // the multipart upload alive for a corrected retry, exactly like the
        // provider's InvalidPartList response.
        FakeStorage::validate_completion(parts)?;
        let upload = (
            self.bucket.clone(),
            self.key.clone(),
            self.upload_id.clone(),
        );
        if !self
            .store
            .uploads
            .lock()
            .expect("fake store poisoned")
            .remove(&upload)
        {
            return Err(Error::not_found("multipart upload not found"));
        }
        let mut body = Vec::new();
        for part in parts {
            body.extend_from_slice(&self.store.take_part(
                &self.bucket,
                &self.key,
                &self.upload_id,
                part.part_number,
            )?);
        }
        let body = Bytes::from(body);
        let size = body.len() as u64;
        let meta = ObjectMetadata::from_parts(
            &self.key,
            Some(format!("multipart-{size:x}")),
            parts,
            OffsetDateTime::now_utc(),
        );
        self.store
            .objects
            .lock()
            .expect("fake store poisoned")
            .insert(
                (self.bucket.clone(), self.key.clone()),
                (meta.clone(), body),
            );
        // Completed uploads leave no staged parts behind, exactly like the
        // provider after a successful CompleteMultipartUpload.
        self.store
            .parts
            .lock()
            .expect("fake store poisoned")
            .retain(|(b, k, id, _), _| {
                !(b == &self.bucket && k == &self.key && id == &self.upload_id)
            });
        self.store.register_bucket(&self.bucket);
        Ok(meta)
    }

    async fn abort(&self) -> Result<()> {
        self.store.fault(StorageMethod::AbortMultipart)?;
        let removed = self
            .store
            .uploads
            .lock()
            .expect("fake store poisoned")
            .remove(&(
                self.bucket.clone(),
                self.key.clone(),
                self.upload_id.clone(),
            ));
        if !removed {
            // Abort is strict: an unknown upload id surfaces, exactly like
            // the provider's NoSuchUpload.
            return Err(Error::not_found("multipart upload not found"));
        }
        self.store
            .parts
            .lock()
            .expect("fake store poisoned")
            .retain(|(b, k, id, _), _| {
                !(b == &self.bucket && k == &self.key && id == &self.upload_id)
            });
        Ok(())
    }
}

impl FakeStorage {
    /// The deterministic ETag an uploaded part carries. Completion must
    /// echo it back — a caller that fabricates etags is not exercising the
    /// provider contract.
    fn etag_for(part_number: u32) -> String {
        format!("etag-{part_number}")
    }

    /// S3's CompleteMultipartUpload legality rules, enforced so engine/relay
    /// bugs fail fast offline instead of surfacing only against AWS/MinIO:
    ///
    /// - at least one part, part numbers strictly ascending without gaps in
    ///   ordering and no duplicates;
    /// - every non-final part is at least 5 MiB;
    /// - each entry echoes the exact ETag its UploadPart returned.
    fn validate_completion(parts: &[UploadedPart]) -> Result<()> {
        if parts.is_empty() {
            return Err(Error::invalid_input(
                "multipart completion requires at least one part",
            ));
        }
        for (index, part) in parts.iter().enumerate() {
            if !(1..=10_000).contains(&part.part_number) {
                return Err(Error::invalid_input(format!(
                    "part number {} is outside the S3 range 1..=10000",
                    part.part_number
                )));
            }
            if let Some(previous) = parts.get(index.wrapping_sub(1)) {
                if previous.part_number >= part.part_number {
                    return Err(Error::invalid_input(
                        "multipart parts must be ascending and unique",
                    ));
                }
            }
            if index + 1 < parts.len() && part.size < MIN_NON_FINAL_PART_SIZE {
                return Err(Error::invalid_input(format!(
                    "non-final part {} is {} bytes; S3 requires at least 5 MiB",
                    part.part_number, part.size
                )));
            }
            if part.etag != Self::etag_for(part.part_number) {
                return Err(Error::invalid_input(format!(
                    "part {} was submitted with an etag that was never issued",
                    part.part_number
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_domain::Error;

    #[tokio::test]
    async fn satisfies_storage_contract() {
        let s = FakeStorage::new();
        s.seed_bucket("b");
        crate::storage_contract(&s, "b").await;
        crate::multipart_legality_negatives(&s, "b").await;
    }

    #[tokio::test]
    async fn fail_next_is_one_shot_and_returns_the_injected_error() {
        let s = FakeStorage::new();
        let key = ObjectPath::new("boom.txt").unwrap();
        s.fail_next(StorageMethod::Put, Error::network("injected put failure"));
        let err = s
            .put("b", &key, Bytes::from_static(b"x"), &PutOptions::default())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("injected put failure"),
            "the injected error surfaces verbatim, got {err}"
        );
        // One-shot: the fault is consumed and the next call succeeds.
        s.put("b", &key, Bytes::from_static(b"x"), &PutOptions::default())
            .await
            .expect("fault consumed, next put succeeds");
    }

    #[tokio::test]
    async fn fail_until_cleared_survives_and_then_recovers() {
        let s = FakeStorage::new();
        let key = ObjectPath::new("persistent.txt").unwrap();
        s.fail_until_cleared(StorageMethod::Put, Error::network("injected outage"));

        for _ in 0..2 {
            let err = s
                .put("b", &key, Bytes::from_static(b"x"), &PutOptions::default())
                .await
                .unwrap_err();
            assert!(err.to_string().contains("injected outage"));
        }

        s.clear_fault(StorageMethod::Put);
        s.put("b", &key, Bytes::from_static(b"x"), &PutOptions::default())
            .await
            .expect("clearing the fault restores normal behavior");
    }

    #[tokio::test]
    async fn failed_part_upload_leaves_the_upload_active_until_aborted() {
        let s = FakeStorage::new();
        let key = ObjectPath::new("multipart.bin").unwrap();
        let upload = s
            .create_multipart("b", &key, &PutOptions::default())
            .await
            .unwrap();
        s.fail_next(
            StorageMethod::UploadPart,
            Error::network("injected part failure"),
        );
        upload
            .upload_part(1, Bytes::from_static(b"xxx"))
            .await
            .unwrap_err();
        // The provider upload survives the failed part — an abort decision is
        // the caller's, and the abort path stays exercisable afterwards.
        assert!(s.has_upload("b", "multipart.bin", upload.upload_id()));
        s.seed_part("b", "multipart.bin", upload.upload_id(), 1, b"xxx");
        upload.abort().await.unwrap();
        assert_eq!(s.active_upload_count(), 0);
    }

    /// A completed multipart upload leaves no staged parts behind — the
    /// provider forgets them after a successful CompleteMultipartUpload.
    #[tokio::test]
    async fn complete_multipart_clears_staged_parts() {
        let s = FakeStorage::new();
        let key = ObjectPath::new("multipart.bin").unwrap();
        let upload = s
            .create_multipart("b", &key, &PutOptions::default())
            .await
            .unwrap();
        let part = upload
            .upload_part(1, Bytes::from_static(b"final"))
            .await
            .unwrap();
        let meta = upload.complete(&[part]).await.unwrap();
        assert_eq!(meta.size, 5);
        assert_eq!(s.active_upload_count(), 0);
    }
}
