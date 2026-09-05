#![allow(unused_imports)]
use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::*;
use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    ProviderCredential, PublishLogEvent, PublishStatus, PushCursor, PushCursorKind, ShardJob,
};
use crate::storage::{
    DeviceFeedbackApplied, DeviceFeedbackEffect, DeviceFeedbackRequest, PushStorageError,
    apply_device_feedback_effect,
};
use crate::storage::{
    DeviceRegistrationChange, DeviceRegistrationOutcome, IdempotencyRecord,
    OperatorInvalidationEvent, Page, PushCredentialStore, PushDeliveryEventStore, PushDeviceStore,
    PushFanoutShardStore, PushIdempotencyStore, PushOperatorEventStore, PushPublishLogStore,
    PushPublishStatusStore, PushScheduleStore, PushSchedulerLockStore, PushStorageResult,
    PushSubscriptionStore, PushTemplateStore, ScheduledPushJob, SchedulerLock,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FeedbackDevice {
    #[serde(flatten)]
    device: DeviceDetails,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    feedback_receipts: BTreeMap<String, u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RetiredFeedbackDevice {
    feedback_retired_device: DeviceDetails,
    feedback_receipts: BTreeMap<String, u64>,
}

fn decode_feedback_device(raw: &str) -> PushStorageResult<(FeedbackDevice, bool)> {
    if raw.contains("\"feedbackRetiredDevice\"")
        && let Ok(retired) = from_json_str::<RetiredFeedbackDevice>(raw)
    {
        return Ok((
            FeedbackDevice {
                device: retired.feedback_retired_device,
                feedback_receipts: retired.feedback_receipts,
            },
            true,
        ));
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Receipts {
        #[serde(default)]
        feedback_receipts: BTreeMap<String, u64>,
    }
    let device = from_json_str::<DeviceDetails>(raw)?;
    let receipts = from_json_str::<Receipts>(raw)?;
    Ok((
        FeedbackDevice {
            device,
            feedback_receipts: receipts.feedback_receipts,
        },
        false,
    ))
}

#[async_trait]
impl<B> PushDeviceStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn complete_device_feedback_receipt(
        &self,
        app_id: &str,
        device_id: &str,
        receipt_id: &str,
    ) -> PushStorageResult<()> {
        for _ in 0..16 {
            let Some(raw) = self
                .backend
                .get_consistent(FAMILY_DEVICE, app_id, device_id, DEFAULT_SK)
                .await?
            else {
                return Ok(());
            };
            let (mut stored, retired) = decode_feedback_device(&raw)?;
            if !stored.feedback_receipts.contains_key(receipt_id) {
                return Ok(());
            }
            if self
                .get_idempotency_record(app_id, &format!("delivery-result:{receipt_id}"))
                .await?
                .is_none()
            {
                return Err(PushStorageError::Backend(
                    "cannot release an incomplete device feedback receipt".into(),
                ));
            }
            stored.feedback_receipts.remove(receipt_id);
            let applied = if retired && stored.feedback_receipts.is_empty() {
                self.backend
                    .compare_and_delete(FAMILY_DEVICE, app_id, device_id, DEFAULT_SK, &raw)
                    .await?
            } else {
                let next = if retired {
                    to_json_string(&RetiredFeedbackDevice {
                        feedback_retired_device: stored.device,
                        feedback_receipts: stored.feedback_receipts,
                    })?
                } else {
                    to_json_string(&stored)?
                };
                self.backend
                    .compare_and_swap(FAMILY_DEVICE, app_id, device_id, DEFAULT_SK, &raw, next)
                    .await?
            };
            if applied {
                return Ok(());
            }
        }
        Err(PushStorageError::Backend(
            "device feedback receipt cleanup CAS retries exhausted".into(),
        ))
    }

    async fn apply_device_feedback_once(
        &self,
        request: DeviceFeedbackRequest,
    ) -> PushStorageResult<DeviceFeedbackApplied> {
        for _ in 0..16 {
            let Some(raw) = self
                .backend
                .get_consistent(
                    FAMILY_DEVICE,
                    &request.app_id,
                    &request.device_id,
                    DEFAULT_SK,
                )
                .await?
            else {
                return Ok(DeviceFeedbackApplied::default());
            };
            let (mut stored, retired) = decode_feedback_device(&raw)?;
            if self
                .get_idempotency_record(
                    &request.app_id,
                    &format!("delivery-result:{}", request.receipt_id),
                )
                .await?
                .is_some()
            {
                return Ok(DeviceFeedbackApplied::default());
            }
            if stored.feedback_receipts.contains_key(&request.receipt_id) {
                // A retry also repairs advisory work after a committed device CAS.
                if retired {
                    self.delete_device_indexes(&stored.device).await?;
                    self.delete_subscriptions_by_device(&request.app_id, &request.device_id)
                        .await?;
                } else {
                    self.put_device_indexes(&stored.device).await?;
                }
                return Ok(DeviceFeedbackApplied::default());
            }
            if retired {
                return Ok(DeviceFeedbackApplied::default());
            }
            // Only completed receipts may leave the bounded canonical fence.
            for id in stored.feedback_receipts.keys().cloned().collect::<Vec<_>>() {
                if self
                    .get_idempotency_record(&request.app_id, &format!("delivery-result:{id}"))
                    .await?
                    .is_some()
                {
                    stored.feedback_receipts.remove(&id);
                }
            }
            if stored.feedback_receipts.len() >= 64 {
                return Err(PushStorageError::Backend(
                    "device feedback receipt capacity reached".into(),
                ));
            }
            let previous = stored.device.clone();
            apply_device_feedback_effect(&mut stored.device, &request);
            stored
                .feedback_receipts
                .insert(request.receipt_id.clone(), request.expires_at_ms);
            let deleted = matches!(request.effect, DeviceFeedbackEffect::Delete);
            let next = if deleted {
                to_json_string(&RetiredFeedbackDevice {
                    feedback_retired_device: stored.device.clone(),
                    feedback_receipts: stored.feedback_receipts.clone(),
                })?
            } else {
                to_json_string(&stored)?
            };
            if !self
                .backend
                .compare_and_swap(
                    FAMILY_DEVICE,
                    &request.app_id,
                    &request.device_id,
                    DEFAULT_SK,
                    &raw,
                    next,
                )
                .await?
            {
                continue;
            }
            if deleted {
                self.delete_device_indexes(&previous).await?;
                self.delete_subscriptions_by_device(&request.app_id, &request.device_id)
                    .await?;
            } else {
                self.put_device_indexes(&stored.device).await?;
                if previous.last_active_at_ms != stored.device.last_active_at_ms {
                    self.backend
                        .delete(
                            FAMILY_DEVICE_BY_DAY,
                            &previous.app_id,
                            &day_bucket_for_ms(previous.last_active_at_ms),
                            &format!("{:020}:{}", previous.last_active_at_ms, previous.id),
                        )
                        .await?;
                }
            }
            return Ok(DeviceFeedbackApplied {
                applied: true,
                previous: Some(previous.push.state),
                next: (!deleted).then_some(stored.device.push.state),
            });
        }
        Err(PushStorageError::Backend(
            "device feedback CAS retries exhausted".into(),
        ))
    }

    async fn upsert_device(
        &self,
        device: DeviceDetails,
    ) -> PushStorageResult<DeviceRegistrationOutcome> {
        device.validate()?;
        let token_hash = device.push.recipient.token_hash();
        if self
            .backend
            .put_if_absent(
                FAMILY_DEVICE,
                &device.app_id,
                &device.id,
                DEFAULT_SK,
                to_json_string(&device)?,
            )
            .await?
        {
            self.put_device_indexes(&device).await?;
            return Ok(DeviceRegistrationOutcome {
                change: DeviceRegistrationChange::Inserted,
                token_hash,
            });
        }

        for _ in 0..16 {
            let Some(raw) = self
                .backend
                .get_consistent(FAMILY_DEVICE, &device.app_id, &device.id, DEFAULT_SK)
                .await?
            else {
                if self
                    .backend
                    .put_if_absent(
                        FAMILY_DEVICE,
                        &device.app_id,
                        &device.id,
                        DEFAULT_SK,
                        to_json_string(&device)?,
                    )
                    .await?
                {
                    self.put_device_indexes(&device).await?;
                    return Ok(DeviceRegistrationOutcome {
                        change: DeviceRegistrationChange::Inserted,
                        token_hash,
                    });
                }
                continue;
            };
            let (existing, retired) = decode_feedback_device(&raw)?;
            let change = registration_change((!retired).then_some(&existing.device), &device);
            let next = FeedbackDevice {
                device: device.clone(),
                feedback_receipts: existing.feedback_receipts,
            };
            if !self
                .backend
                .compare_and_swap(
                    FAMILY_DEVICE,
                    &device.app_id,
                    &device.id,
                    DEFAULT_SK,
                    &raw,
                    to_json_string(&next)?,
                )
                .await?
            {
                continue;
            }
            if !retired && existing.device != device {
                self.delete_device_indexes(&existing.device).await?;
            }
            self.put_device_indexes(&device).await?;
            return Ok(DeviceRegistrationOutcome { change, token_hash });
        }
        Err(PushStorageError::Backend(
            "device registration CAS retries exhausted".into(),
        ))
    }

    async fn get_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<Option<DeviceDetails>> {
        self.backend
            .get_consistent(FAMILY_DEVICE, app_id, device_id, DEFAULT_SK)
            .await?
            .map(|raw| {
                decode_feedback_device(&raw)
                    .map(|(stored, retired)| (!retired).then_some(stored.device))
            })
            .transpose()
            .map(Option::flatten)
    }

    async fn delete_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        if let Some(device) = self.get_device(app_id, device_id).await? {
            self.delete_device_indexes(&device).await?;
        }
        self.delete_subscriptions_by_device(app_id, device_id)
            .await?;
        self.delete_json(FAMILY_DEVICE, app_id, device_id, DEFAULT_SK)
            .await
    }

    async fn list_devices(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        let start = cursor_position(cursor, app_id)?;
        let documents = self
            .backend
            .scan_app_page_by_pk(
                FAMILY_DEVICE,
                app_id,
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?;
        let has_more = documents.len() > limit.max(1);
        let mut last = None;
        let mut items = Vec::new();
        for document in documents.into_iter().take(limit.max(1)) {
            last = Some(document.pk);
            let (stored, retired) = decode_feedback_device(&document.data)?;
            if !retired {
                items.push(stored.device);
            }
        }
        Ok(Page {
            items,
            next_cursor: has_more.then(|| PushCursor {
                app_id: app_id.into(),
                kind: PushCursorKind::Device,
                position: last.unwrap_or_default(),
                issued_at_ms: 0,
            }),
        })
    }

    async fn list_devices_by_client(
        &self,
        app_id: &str,
        client_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        let start = cursor_position(cursor, app_id)?;
        let index_rows = self
            .scan_pk_page_json::<String>(
                FAMILY_DEVICE_BY_CLIENT,
                app_id,
                client_id,
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?;
        let has_more = index_rows.len() > limit.max(1);
        let mut rows = Vec::with_capacity(limit.max(1).min(index_rows.len()));
        let mut last = None;
        for (_, position, device_id) in index_rows.into_iter().take(limit.max(1)) {
            last = Some(position);
            if let Some(device) = self.get_device(app_id, &device_id).await?
                && device.client_id.as_deref() == Some(client_id)
            {
                rows.push(device);
            }
        }
        // Advance by the index position even if canonical reads reject stale rows.
        Ok(Page {
            items: rows,
            next_cursor: has_more.then(|| PushCursor {
                app_id: app_id.to_owned(),
                kind: PushCursorKind::Device,
                position: last.unwrap_or_default(),
                issued_at_ms: 0,
            }),
        })
    }

    async fn delete_devices_by_client(
        &self,
        app_id: &str,
        client_id: &str,
    ) -> PushStorageResult<u64> {
        let mut cursor = None;
        let mut deleted = 0_u64;
        loop {
            let page = self
                .list_devices_by_client(app_id, client_id, 256, cursor)
                .await?;
            for device in page.items {
                if self.delete_device(app_id, &device.id).await? == DeleteDeviceOutcome::Deleted {
                    deleted += 1;
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(deleted)
    }

    async fn list_stale_devices(
        &self,
        app_id: &str,
        day_bucket: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        let start = cursor_position(cursor, app_id)?;
        let index_rows = self
            .scan_pk_page_json::<String>(
                FAMILY_DEVICE_BY_DAY,
                app_id,
                day_bucket,
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?;
        let has_more = index_rows.len() > limit.max(1);
        let mut rows = Vec::new();
        let mut last = None;
        for (_, position, device_id) in index_rows.into_iter().take(limit.max(1)) {
            last = Some(position);
            if let Some(device) = self.get_device(app_id, &device_id).await?
                && day_bucket_for_ms(device.last_active_at_ms) == day_bucket
            {
                rows.push(device);
            }
        }
        Ok(Page {
            items: rows,
            next_cursor: has_more.then(|| PushCursor {
                app_id: app_id.to_owned(),
                kind: PushCursorKind::Device,
                position: last.unwrap_or_default(),
                issued_at_ms: 0,
            }),
        })
    }
}

impl<B> DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_device_indexes(&self, device: &DeviceDetails) -> PushStorageResult<()> {
        if let Some(client_id) = &device.client_id {
            self.put_json(
                FAMILY_DEVICE_BY_CLIENT,
                &device.app_id,
                client_id,
                &device.id,
                &device.id,
            )
            .await?;
        }
        self.put_json(
            FAMILY_DEVICE_BY_DAY,
            &device.app_id,
            &day_bucket_for_ms(device.last_active_at_ms),
            &format!("{:020}:{}", device.last_active_at_ms, device.id),
            &device.id,
        )
        .await
    }

    async fn delete_device_indexes(&self, device: &DeviceDetails) -> PushStorageResult<()> {
        if let Some(client_id) = &device.client_id {
            self.backend
                .delete(
                    FAMILY_DEVICE_BY_CLIENT,
                    &device.app_id,
                    client_id,
                    &device.id,
                )
                .await?;
        }
        self.backend
            .delete(
                FAMILY_DEVICE_BY_DAY,
                &device.app_id,
                &day_bucket_for_ms(device.last_active_at_ms),
                &format!("{:020}:{}", device.last_active_at_ms, device.id),
            )
            .await?;
        Ok(())
    }
}
