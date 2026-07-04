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
impl<B> PushIdempotencyStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_idempotency_record_if_absent(
        &self,
        record: IdempotencyRecord,
    ) -> PushStorageResult<bool> {
        if let Some(existing) = self
            .get_json::<IdempotencyRecord>(
                FAMILY_IDEMPOTENCY,
                &record.app_id,
                &record.key,
                DEFAULT_SK,
            )
            .await?
            && idempotency_record_expired(&existing, crate::pipeline::now_ms())
        {
            self.backend
                .delete(FAMILY_IDEMPOTENCY, &record.app_id, &record.key, DEFAULT_SK)
                .await?;
        }
        self.backend
            .put_if_absent(
                FAMILY_IDEMPOTENCY,
                &record.app_id,
                &record.key,
                DEFAULT_SK,
                to_json_string(&record)?,
            )
            .await
    }

    async fn get_idempotency_record(
        &self,
        app_id: &str,
        key: &str,
    ) -> PushStorageResult<Option<IdempotencyRecord>> {
        let Some(record) = self
            .get_json::<IdempotencyRecord>(FAMILY_IDEMPOTENCY, app_id, key, DEFAULT_SK)
            .await?
        else {
            return Ok(None);
        };
        if idempotency_record_expired(&record, crate::pipeline::now_ms()) {
            self.backend
                .delete(FAMILY_IDEMPOTENCY, app_id, key, DEFAULT_SK)
                .await?;
            return Ok(None);
        }
        Ok(Some(record))
    }
}

fn idempotency_record_expired(record: &IdempotencyRecord, now_ms: u64) -> bool {
    record.expires_at_ms >= 1_000_000_000_000 && record.expires_at_ms <= now_ms
}

#[async_trait]
impl<B> PushSchedulerLockStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn acquire_scheduler_lock(
        &self,
        lock: SchedulerLock,
        now_ms: u64,
    ) -> PushStorageResult<bool> {
        if let Some(existing) = self
            .get_json::<SchedulerLock>(
                FAMILY_SCHEDULER_LOCK,
                &lock.app_id,
                &lock.publish_id,
                DEFAULT_SK,
            )
            .await?
            && existing.owner_id != lock.owner_id
            && existing.expires_at_ms > now_ms
        {
            return Ok(false);
        }
        self.put_json(
            FAMILY_SCHEDULER_LOCK,
            &lock.app_id,
            &lock.publish_id,
            DEFAULT_SK,
            &lock,
        )
        .await?;
        Ok(true)
    }

    async fn release_scheduler_lock(
        &self,
        app_id: &str,
        publish_id: &str,
        owner_id: &str,
    ) -> PushStorageResult<()> {
        if let Some(existing) = self
            .get_json::<SchedulerLock>(FAMILY_SCHEDULER_LOCK, app_id, publish_id, DEFAULT_SK)
            .await?
            && existing.owner_id == owner_id
        {
            self.backend
                .delete(FAMILY_SCHEDULER_LOCK, app_id, publish_id, DEFAULT_SK)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<B> PushOperatorEventStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn append_operator_invalidation(
        &self,
        event: OperatorInvalidationEvent,
    ) -> PushStorageResult<()> {
        self.put_json(
            FAMILY_OPERATOR_INVALIDATION,
            &event.app_id,
            &event.event_id,
            &format!("{:020}:{}", event.occurred_at_ms, event.event_id),
            &event,
        )
        .await
    }

    async fn list_operator_invalidations(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<OperatorInvalidationEvent>> {
        let start = cursor_position(cursor, app_id)?;
        let mut rows = self
            .scan_json::<OperatorInvalidationEvent>(FAMILY_OPERATOR_INVALIDATION, app_id)
            .await?
            .into_iter()
            .map(|(_, position, event)| (position, event))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(page_from_rows(
            app_id,
            PushCursorKind::OperatorInvalidation,
            rows,
            limit,
            start,
        ))
    }
}
