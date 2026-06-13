#![allow(unused_imports)]
use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::*;
use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    ProviderCredential, PublishLogEvent, PublishStatus, PushCursor, PushCursorKind, ShardJob,
};
use crate::storage::{
    DeviceRegistrationChange, DeviceRegistrationOutcome, IdempotencyRecord,
    OperatorInvalidationEvent, Page, PushCredentialStore, PushDeliveryEventStore, PushDeviceStore,
    PushFanoutShardStore, PushIdempotencyStore, PushOperatorEventStore, PushPublishLogStore,
    PushPublishStatusStore, PushScheduleStore, PushSchedulerLockStore, PushStorageResult,
    PushSubscriptionStore, PushTemplateStore, ScheduledPushJob, SchedulerLock,
};
use async_trait::async_trait;

#[async_trait]
impl<B> PushDeviceStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
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

        let existing = self.get_device(&device.app_id, &device.id).await?;
        let change = registration_change(existing.as_ref(), &device);
        if matches!(change, DeviceRegistrationChange::Unchanged) {
            self.put_device_indexes(&device).await?;
            return Ok(DeviceRegistrationOutcome { change, token_hash });
        }
        if let Some(existing) = existing.as_ref() {
            self.delete_device_indexes(existing).await?;
        }
        self.put_json(
            FAMILY_DEVICE,
            &device.app_id,
            &device.id,
            DEFAULT_SK,
            &device,
        )
        .await?;
        self.put_device_indexes(&device).await?;
        Ok(DeviceRegistrationOutcome { change, token_hash })
    }

    async fn get_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<Option<DeviceDetails>> {
        self.get_json(FAMILY_DEVICE, app_id, device_id, DEFAULT_SK)
            .await
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
        let rows = self
            .scan_app_page_by_pk_json::<DeviceDetails>(
                FAMILY_DEVICE,
                app_id,
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?
            .into_iter()
            .map(|(device_id, _, device)| (device_id, device))
            .collect();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::Device,
            rows,
            limit,
            start,
        ))
    }

    async fn delete_devices_by_client(
        &self,
        app_id: &str,
        client_id: &str,
    ) -> PushStorageResult<u64> {
        let devices = self
            .scan_pk_json::<String>(FAMILY_DEVICE_BY_CLIENT, app_id, client_id)
            .await?;
        let mut deleted = 0_u64;
        for (_, device_id, _) in devices {
            if self.delete_device(app_id, &device_id).await? == DeleteDeviceOutcome::Deleted {
                deleted += 1;
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
            .scan_pk_json::<String>(FAMILY_DEVICE_BY_DAY, app_id, day_bucket)
            .await?
            .into_iter();
        let mut rows = Vec::new();
        for (_, position, device_id) in index_rows {
            if let Some(device) = self
                .get_json::<DeviceDetails>(FAMILY_DEVICE, app_id, &device_id, DEFAULT_SK)
                .await?
            {
                rows.push((position, device));
            }
        }
        Ok(page_from_rows(
            app_id,
            PushCursorKind::Device,
            rows,
            limit,
            start,
        ))
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
