//! Repository for per-bucket link settings (LINK-003..007).

use async_trait::async_trait;
use rstore_application_ports::LinkProfileStore;
use rstore_domain::id::BucketId;
use rstore_domain::LinkProfile;
use rstore_domain::Result;
use sqlx::sqlite::SqlitePool;

use crate::codec::{Insert, RowReader};
use crate::error::map_sqlx;
use crate::tx::WriteGate;

/// CRUD over `link_profiles`; absent rows mean [`LinkProfile::default`].
#[derive(Clone)]
pub struct LinkRepository {
    gate: WriteGate,
    read_pool: SqlitePool,
}

impl LinkRepository {
    pub(crate) fn new(gate: WriteGate, read_pool: SqlitePool) -> Self {
        Self { gate, read_pool }
    }
}

#[async_trait]
impl LinkProfileStore for LinkRepository {
    /// The bucket's profile, or the default when none is stored.
    async fn get(&self, bucket: BucketId) -> Result<LinkProfile> {
        let row = sqlx::query(
            "SELECT public_base_url, cdn_base_url, allow_public_url, allow_presigned, \
             default_ttl_seconds FROM link_profiles WHERE bucket_id = ?",
        )
        .bind(crate::codec::uuid_to_text(&bucket))
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx)?;
        match row {
            None => Ok(LinkProfile::default()),
            Some(row) => {
                let row = RowReader::new(row);
                Ok(LinkProfile {
                    public_base_url: row.opt_text("public_base_url")?,
                    cdn_base_url: row.opt_text("cdn_base_url")?,
                    allow_public_url: row.bool("allow_public_url")?,
                    allow_presigned: row.bool("allow_presigned")?,
                    default_ttl_seconds: row.integer("default_ttl_seconds")?.max(0) as u64,
                })
            }
        }
    }

    /// Insert or replace the bucket's profile (LINK-003).
    async fn upsert(&self, bucket: BucketId, profile: &LinkProfile) -> Result<()> {
        let _write = self.gate.permit().await?;
        Insert::into("link_profiles")
            .column("bucket_id", crate::codec::uuid_to_text(&bucket))
            .column("public_base_url", &profile.public_base_url)
            .column("cdn_base_url", &profile.cdn_base_url)
            .column("allow_public_url", profile.allow_public_url)
            .column("allow_presigned", profile.allow_presigned)
            .column("default_ttl_seconds", profile.default_ttl_seconds as i64)
            .suffix(
                "ON CONFLICT(bucket_id) DO UPDATE SET \
                 public_base_url = excluded.public_base_url, \
                 cdn_base_url = excluded.cdn_base_url, \
                 allow_public_url = excluded.allow_public_url, \
                 allow_presigned = excluded.allow_presigned, \
                 default_ttl_seconds = excluded.default_ttl_seconds",
            )
            .execute(self.gate.pool())
            .await?;
        Ok(())
    }

    /// Forget the bucket's profile (back to defaults).
    async fn clear(&self, bucket: BucketId) -> Result<()> {
        let _write = self.gate.permit().await?;
        sqlx::query("DELETE FROM link_profiles WHERE bucket_id = ?")
            .bind(crate::codec::uuid_to_text(&bucket))
            .execute(self.gate.pool())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }
}
