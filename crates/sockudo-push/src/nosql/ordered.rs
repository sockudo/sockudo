//! Ordered advisory indexes for legacy document families. Canonical rows remain authoritative.
use base64::Engine as _;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::{cursor_position, from_json_str, limit_plus_one, to_json_string};
use crate::domain::{ChannelSubscription, PushCursor, PushCursorKind};
use crate::storage::{Page, PushStorageError, PushStorageResult};

pub(super) const FAMILY_SUBSCRIPTION_ORDERED: &str = "subscription-ordered-v1";
pub(super) const FAMILY_PUBLISH_LOG_ORDERED: &str = "publish-log-ordered-v1";
pub(super) const FAMILY_OPERATOR_ORDERED: &str = "operator-ordered-v1";
const FAMILY_ORDERED_MIGRATION: &str = "ordered-migration";
const FAMILIES: [(&str, &str); 3] = [
    (FAMILY_SUBSCRIPTION, FAMILY_SUBSCRIPTION_ORDERED),
    (FAMILY_PUBLISH_LOG, FAMILY_PUBLISH_LOG_ORDERED),
    (FAMILY_OPERATOR_INVALIDATION, FAMILY_OPERATOR_ORDERED),
];

/// Persisted internal checkpoint for automatic bounded index backfill.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct OrderedIndexMigrationCursor {
    version: u8,
    app_id: String,
    family: usize,
    position: Option<(String, String)>,
}

impl<B: DocumentBackend> DocumentPushStore<B> {
    /// Build at most `limit` references. Current writers update their index before canonical data.
    pub(crate) async fn migrate_ordered_indexes_page(
        &self,
        app_id: &str,
        cursor: Option<OrderedIndexMigrationCursor>,
        limit: usize,
    ) -> PushStorageResult<Option<OrderedIndexMigrationCursor>> {
        let mut cursor = match cursor {
            Some(cursor)
                if cursor.version == 1
                    && cursor.app_id == app_id
                    && cursor.family < FAMILIES.len() =>
            {
                cursor
            }
            Some(_) => {
                return Err(PushStorageError::Backend(
                    "invalid ordered index migration checkpoint".to_owned(),
                ));
            }
            None => {
                self.disable_ordered_indexes(app_id).await?;
                OrderedIndexMigrationCursor {
                    version: 1,
                    app_id: app_id.to_owned(),
                    family: 0,
                    position: None,
                }
            }
        };
        let mut remaining = limit.clamp(1, 1_000);
        while remaining > 0 && cursor.family < FAMILIES.len() {
            let (canonical, index) = FAMILIES[cursor.family];
            let rows = self
                .backend
                .scan_app_page(canonical, app_id, cursor.position.as_ref(), remaining)
                .await?;
            let count = rows.len();
            for row in rows {
                let position = if canonical == FAMILY_SUBSCRIPTION {
                    let subscription: ChannelSubscription = from_json_str(&row.data)?;
                    format!("{}:{}", subscription.channel, subscription.device_id)
                } else {
                    row.sk.clone()
                };
                self.write_ordered_reference(index, app_id, &position, &row.pk, &row.sk)
                    .await?;
                cursor.position = Some((row.pk, row.sk));
            }
            if count < remaining {
                cursor.family += 1;
                cursor.position = None;
            }
            remaining -= count;
        }
        if cursor.family == FAMILIES.len() {
            self.backend
                .put(
                    FAMILY_ORDERED_MIGRATION,
                    app_id,
                    "ordered-v1",
                    DEFAULT_SK,
                    "1".to_owned(),
                )
                .await?;
            Ok(None)
        } else {
            Ok(Some(cursor))
        }
    }

    /// Invalidate incomplete metadata before starting a resumable rebuild.
    pub(crate) async fn disable_ordered_indexes(&self, app_id: &str) -> PushStorageResult<()> {
        self.backend
            .delete(FAMILY_ORDERED_MIGRATION, app_id, "ordered-v1", DEFAULT_SK)
            .await?;
        Ok(())
    }

    pub(super) async fn write_ordered_reference(
        &self,
        index: &'static str,
        app_id: &str,
        position: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<()> {
        let reference = to_json_string(&(pk, sk))?;
        let key = ordered_reference_key(pk, sk)?;
        self.backend
            .put(index, app_id, position, &key, reference)
            .await
    }

    pub(super) async fn delete_ordered_reference(
        &self,
        index: &'static str,
        app_id: &str,
        position: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<()> {
        let key = ordered_reference_key(pk, sk)?;
        self.backend.delete(index, app_id, position, &key).await?;
        Ok(())
    }

    pub(super) async fn ordered_page<T: DeserializeOwned>(
        &self,
        canonical: &'static str,
        index: &'static str,
        app_id: &str,
        kind: PushCursorKind,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<T>> {
        if self
            .backend
            .get_consistent(FAMILY_ORDERED_MIGRATION, app_id, "ordered-v1", DEFAULT_SK)
            .await?
            .as_deref()
            != Some("1")
        {
            let checkpoint = self
                .backend
                .get_consistent(
                    FAMILY_ORDERED_MIGRATION,
                    app_id,
                    "checkpoint-v1",
                    DEFAULT_SK,
                )
                .await?;
            let cursor = checkpoint
                .as_deref()
                .map(from_json_str::<OrderedIndexMigrationCursor>)
                .transpose()?;
            if let Some(next) = self
                .migrate_ordered_indexes_page(app_id, cursor, 256)
                .await?
            {
                let encoded = to_json_string(&next)?;
                match checkpoint {
                    Some(previous) => {
                        self.backend
                            .compare_and_swap(
                                FAMILY_ORDERED_MIGRATION,
                                app_id,
                                "checkpoint-v1",
                                DEFAULT_SK,
                                &previous,
                                encoded,
                            )
                            .await?;
                    }
                    None => {
                        self.backend
                            .put_if_absent(
                                FAMILY_ORDERED_MIGRATION,
                                app_id,
                                "checkpoint-v1",
                                DEFAULT_SK,
                                encoded,
                            )
                            .await?;
                    }
                }
                return Err(PushStorageError::Backend(
                    "ordered push index backfill is in progress; retry the page".to_owned(),
                ));
            }
            if let Some(previous) = checkpoint {
                self.backend
                    .compare_and_delete(
                        FAMILY_ORDERED_MIGRATION,
                        app_id,
                        "checkpoint-v1",
                        DEFAULT_SK,
                        &previous,
                    )
                    .await?;
            }
        }
        let start = cursor_position(cursor, app_id)?;
        let nested = start
            .as_deref()
            .and_then(|position| position.strip_prefix("ordered-v1:"))
            .map(|encoded| {
                let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(encoded)
                    .map_err(|_| crate::domain::PushDomainError::CursorDecode)?;
                sonic_rs::from_slice::<(String, String)>(&bytes)
                    .map_err(|_| crate::domain::PushDomainError::CursorDecode)
            })
            .transpose()?;
        let mut rows = if let Some(position) = &nested {
            self.backend
                .scan_app_page(index, app_id, Some(position), limit_plus_one(limit))
                .await?
        } else {
            self.backend
                .scan_app_page_by_pk(index, app_id, start.as_deref(), limit_plus_one(limit))
                .await?
        };
        let more = rows.len() > limit.max(1);
        rows.truncate(limit.max(1));
        let next_cursor = if more {
            rows.last().map(|row| PushCursor {
                app_id: app_id.to_owned(),
                kind,
                position: format!(
                    "ordered-v1:{}",
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                        sonic_rs::to_vec(&(&row.pk, &row.sk)).expect("string pair serializes")
                    )
                ),
                issued_at_ms: 0,
            })
        } else {
            None
        };
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let (pk, sk): (String, String) = from_json_str(&row.data)?;
            if let Some(data) = self.backend.get(canonical, app_id, &pk, &sk).await? {
                items.push(from_json_str(&data)?);
            } else {
                self.backend
                    .compare_and_delete(index, app_id, &row.pk, &row.sk, &row.data)
                    .await?;
            }
        }
        // Advance by scanned index references even when a crashed deletion left stale entries.
        Ok(Page { items, next_cursor })
    }
}

fn ordered_reference_key(pk: &str, sk: &str) -> PushStorageResult<String> {
    // The storage envelope uses object serialization whose field order is not canonical.
    // Identity uses an unambiguous array with deterministic field order instead.
    let encoded = sonic_rs::to_string(&(pk, sk)).map_err(|_| {
        PushStorageError::Backend("ordered reference serialization failed".to_owned())
    })?;
    Ok(crate::domain::stable_hash(encoded.as_bytes()))
}
