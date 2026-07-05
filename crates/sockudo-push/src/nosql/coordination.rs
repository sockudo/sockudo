#![allow(unused_imports)]
use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::*;
use super::publishing::document_cleanup_publish_statuses;
use crate::cleanup::{PushCleanupCounters, PushCleanupReport, PushCleanupRequest};
use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    ProviderCredential, PublishLogEvent, PublishStatus, PushCursor, PushCursorKind, ShardJob,
};
use crate::storage::{
    DeviceRegistrationChange, DeviceRegistrationOutcome, IdempotencyRecord,
    OperatorInvalidationEvent, Page, PushCleanupStore, PushCredentialStore, PushDeliveryEventStore,
    PushDeviceStore, PushFanoutShardStore, PushIdempotencyStore, PushOperatorEventStore,
    PushPublishLogStore, PushPublishStatusStore, PushScheduleStore, PushSchedulerLockStore,
    PushStorageResult, PushSubscriptionStore, PushTemplateStore, ScheduledPushJob, SchedulerLock,
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
        self.remember_cleanup_app(&record.app_id).await?;
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
        self.remember_cleanup_app(&lock.app_id).await?;
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
        self.remember_cleanup_app(&event.app_id).await?;
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

#[async_trait]
impl<B> PushCleanupStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn cleanup_expired_push_data(
        &self,
        request: PushCleanupRequest,
    ) -> PushStorageResult<PushCleanupReport> {
        let apps = self
            .scan_pk_json::<String>(FAMILY_CLEANUP_APP, GLOBAL_APP_ID, "apps")
            .await?
            .into_iter()
            .map(|(_, _, app_id)| app_id)
            .collect::<Vec<_>>();
        let mut report = PushCleanupReport::default();
        let mut remaining = request.policy.max_deleted_per_tick;

        for app_id in apps {
            if remaining == 0 {
                break;
            }
            if let Some(cutoff_ms) = request.publish_status_cutoff_ms() {
                let counters = document_cleanup_publish_statuses(
                    self,
                    &app_id,
                    cutoff_ms,
                    request.limit_for(remaining),
                )
                .await?;
                remaining = remaining.saturating_sub(counters.deleted as usize);
                merge(&mut report.publish_statuses, counters);
            }

            if remaining == 0 {
                break;
            }
            if let Some(cutoff_ms) = request.delivery_event_cutoff_ms() {
                let counters =
                    cleanup_delivery_events(self, &app_id, cutoff_ms, request.limit_for(remaining))
                        .await?;
                remaining = remaining.saturating_sub(counters.deleted as usize);
                merge(&mut report.delivery_events, counters);
            }

            if remaining == 0 {
                break;
            }
            let counters =
                cleanup_idempotency(self, &app_id, request.now_ms, request.limit_for(remaining))
                    .await?;
            remaining = remaining.saturating_sub(counters.deleted as usize);
            merge(&mut report.idempotency_records, counters);

            if remaining == 0 {
                break;
            }
            let counters = cleanup_scheduler_locks(
                self,
                &app_id,
                request.now_ms,
                request.limit_for(remaining),
            )
            .await?;
            remaining = remaining.saturating_sub(counters.deleted as usize);
            merge(&mut report.scheduler_locks, counters);

            if remaining == 0 {
                break;
            }
            if let Some(cutoff_ms) = request.operator_event_cutoff_ms() {
                let counters = cleanup_operator_invalidations(
                    self,
                    &app_id,
                    cutoff_ms,
                    request.limit_for(remaining),
                )
                .await?;
                remaining = remaining.saturating_sub(counters.deleted as usize);
                merge(&mut report.operator_invalidations, counters);
            }
        }

        Ok(report)
    }
}

async fn cleanup_delivery_events<B>(
    store: &DocumentPushStore<B>,
    app_id: &str,
    cutoff_ms: u64,
    limit: usize,
) -> PushStorageResult<PushCleanupCounters>
where
    B: DocumentBackend,
{
    let rows = store
        .scan_pk_page_json::<DeliveryEvent>(FAMILY_DELIVERY_EVENT_TIME, app_id, "time", None, limit)
        .await?;
    let mut counters = PushCleanupCounters::default();
    for (_, time_position, event) in rows {
        counters.scanned = counters.scanned.saturating_add(1);
        if event.occurred_at_ms >= cutoff_ms {
            continue;
        }
        if store
            .backend
            .delete(
                FAMILY_DELIVERY_EVENT,
                app_id,
                &event.publish_id,
                &delivery_event_position(&event),
            )
            .await?
        {
            counters.deleted = counters.deleted.saturating_add(1);
        }
        store
            .backend
            .delete(FAMILY_DELIVERY_EVENT_TIME, app_id, "time", &time_position)
            .await?;
    }
    Ok(counters)
}

async fn cleanup_idempotency<B>(
    store: &DocumentPushStore<B>,
    app_id: &str,
    now_ms: u64,
    limit: usize,
) -> PushStorageResult<PushCleanupCounters>
where
    B: DocumentBackend,
{
    let rows = store
        .scan_json::<IdempotencyRecord>(FAMILY_IDEMPOTENCY, app_id)
        .await?;
    let mut counters = PushCleanupCounters::default();
    for (key, _, record) in rows {
        counters.scanned = counters.scanned.saturating_add(1);
        if !idempotency_record_expired(&record, now_ms) {
            continue;
        }
        if store
            .backend
            .delete(FAMILY_IDEMPOTENCY, app_id, &key, DEFAULT_SK)
            .await?
        {
            counters.deleted = counters.deleted.saturating_add(1);
            if counters.deleted as usize >= limit.max(1) {
                break;
            }
        }
    }
    Ok(counters)
}

async fn cleanup_scheduler_locks<B>(
    store: &DocumentPushStore<B>,
    app_id: &str,
    now_ms: u64,
    limit: usize,
) -> PushStorageResult<PushCleanupCounters>
where
    B: DocumentBackend,
{
    let rows = store
        .scan_json::<SchedulerLock>(FAMILY_SCHEDULER_LOCK, app_id)
        .await?;
    let mut counters = PushCleanupCounters::default();
    for (publish_id, _, lock) in rows {
        counters.scanned = counters.scanned.saturating_add(1);
        if lock.expires_at_ms > now_ms {
            continue;
        }
        if store
            .backend
            .delete(FAMILY_SCHEDULER_LOCK, app_id, &publish_id, DEFAULT_SK)
            .await?
        {
            counters.deleted = counters.deleted.saturating_add(1);
            if counters.deleted as usize >= limit.max(1) {
                break;
            }
        }
    }
    Ok(counters)
}

async fn cleanup_operator_invalidations<B>(
    store: &DocumentPushStore<B>,
    app_id: &str,
    cutoff_ms: u64,
    limit: usize,
) -> PushStorageResult<PushCleanupCounters>
where
    B: DocumentBackend,
{
    let rows = store
        .scan_json::<OperatorInvalidationEvent>(FAMILY_OPERATOR_INVALIDATION, app_id)
        .await?;
    let mut counters = PushCleanupCounters::default();
    for (event_id, position, event) in rows {
        counters.scanned = counters.scanned.saturating_add(1);
        if event.occurred_at_ms >= cutoff_ms {
            continue;
        }
        if store
            .backend
            .delete(FAMILY_OPERATOR_INVALIDATION, app_id, &event_id, &position)
            .await?
        {
            counters.deleted = counters.deleted.saturating_add(1);
            if counters.deleted as usize >= limit.max(1) {
                break;
            }
        }
    }
    Ok(counters)
}

fn merge(target: &mut PushCleanupCounters, counters: PushCleanupCounters) {
    target.scanned = target.scanned.saturating_add(counters.scanned);
    target.deleted = target.deleted.saturating_add(counters.deleted);
}
