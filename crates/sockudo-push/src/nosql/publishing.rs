#![allow(unused_imports)]
use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::*;
use crate::cleanup::terminal_publish_state;
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
impl<B> PushPublishStatusStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_publish_status(&self, status: PublishStatus) -> PushStorageResult<()> {
        let now_ms = crate::pipeline::now_ms();
        if let Some(previous_updated_at_ms) = self
            .get_json::<u64>(
                FAMILY_STATUS_UPDATED,
                &status.app_id,
                &status.publish_id,
                DEFAULT_SK,
            )
            .await?
        {
            self.backend
                .delete(
                    FAMILY_STATUS_UPDATED_TIME,
                    &status.app_id,
                    "time",
                    &status_updated_position(previous_updated_at_ms, &status.publish_id),
                )
                .await?;
        }
        self.remember_cleanup_app(&status.app_id).await?;
        self.put_json(
            FAMILY_STATUS_UPDATED,
            &status.app_id,
            &status.publish_id,
            DEFAULT_SK,
            &now_ms,
        )
        .await?;
        self.put_json(
            FAMILY_STATUS_UPDATED_TIME,
            &status.app_id,
            "time",
            &status_updated_position(now_ms, &status.publish_id),
            &status.publish_id,
        )
        .await?;
        self.put_json(
            FAMILY_STATUS,
            &status.app_id,
            &status.publish_id,
            DEFAULT_SK,
            &status,
        )
        .await
    }

    async fn get_publish_status(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<PublishStatus>> {
        self.get_json(FAMILY_STATUS, app_id, publish_id, DEFAULT_SK)
            .await
    }
}

#[async_trait]
impl<B> PushPublishLogStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn append_publish_log_event(&self, event: PublishLogEvent) -> PushStorageResult<()> {
        self.put_json(
            FAMILY_PUBLISH_LOG,
            &event.app_id,
            &event.publish_id,
            &format!("{:020}:{}", event.occurred_at_ms, event.event_id),
            &event,
        )
        .await
    }

    async fn list_publish_log_events(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<PublishLogEvent>> {
        let start = cursor_position(cursor, app_id)?;
        let mut rows = self
            .scan_json::<PublishLogEvent>(FAMILY_PUBLISH_LOG, app_id)
            .await?
            .into_iter()
            .map(|(_, position, event)| (position, event))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(page_from_rows(
            app_id,
            PushCursorKind::PublishLog,
            rows,
            limit,
            start,
        ))
    }
}

#[async_trait]
impl<B> PushFanoutShardStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_fanout_shard(&self, shard: ShardJob) -> PushStorageResult<()> {
        self.put_json(
            FAMILY_FANOUT_SHARD,
            &shard.app_id,
            &shard.publish_id,
            &shard.shard_id,
            &shard,
        )
        .await
    }

    async fn get_fanout_shard(
        &self,
        app_id: &str,
        publish_id: &str,
        shard_id: &str,
    ) -> PushStorageResult<Option<ShardJob>> {
        self.get_json(FAMILY_FANOUT_SHARD, app_id, publish_id, shard_id)
            .await
    }
}

#[async_trait]
impl<B> PushScheduleStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_scheduled_job(&self, job: ScheduledPushJob) -> PushStorageResult<()> {
        if let Some(existing) = self
            .get_json::<ScheduledPushJob>(
                FAMILY_SCHEDULED_JOB,
                &job.app_id,
                &job.publish_id,
                DEFAULT_SK,
            )
            .await?
        {
            self.delete_json(
                FAMILY_SCHEDULED_JOB_DUE,
                &existing.app_id,
                "due",
                &scheduled_due_position(&existing),
            )
            .await?;
        }
        self.put_json(
            FAMILY_SCHEDULED_JOB,
            &job.app_id,
            &job.publish_id,
            DEFAULT_SK,
            &job,
        )
        .await?;
        self.put_json(
            FAMILY_SCHEDULED_JOB_DUE,
            &job.app_id,
            "due",
            &scheduled_due_position(&job),
            &job,
        )
        .await?;
        self.put_json(
            FAMILY_SCHEDULED_APP,
            GLOBAL_APP_ID,
            "apps",
            &job.app_id,
            &job.app_id,
        )
        .await
    }

    async fn get_scheduled_job(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<ScheduledPushJob>> {
        self.get_json(FAMILY_SCHEDULED_JOB, app_id, publish_id, DEFAULT_SK)
            .await
    }

    async fn delete_scheduled_job(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        if let Some(existing) = self
            .get_json::<ScheduledPushJob>(FAMILY_SCHEDULED_JOB, app_id, publish_id, DEFAULT_SK)
            .await?
        {
            self.delete_json(
                FAMILY_SCHEDULED_JOB_DUE,
                app_id,
                "due",
                &scheduled_due_position(&existing),
            )
            .await?;
        }
        self.delete_json(FAMILY_SCHEDULED_JOB, app_id, publish_id, DEFAULT_SK)
            .await
    }

    async fn list_scheduled_apps(&self) -> PushStorageResult<Vec<String>> {
        Ok(self
            .scan_pk_json::<String>(FAMILY_SCHEDULED_APP, GLOBAL_APP_ID, "apps")
            .await?
            .into_iter()
            .map(|(_, _, app_id)| app_id)
            .collect())
    }

    async fn list_due_scheduled_jobs(
        &self,
        app_id: &str,
        due_minute_ms: u64,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ScheduledPushJob>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .scan_pk_page_json::<ScheduledPushJob>(
                FAMILY_SCHEDULED_JOB_DUE,
                app_id,
                "due",
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?
            .into_iter()
            .filter_map(|(_, position, job)| {
                (job.due_minute_ms <= due_minute_ms).then_some((position, job))
            })
            .collect::<Vec<_>>();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::ScheduledJob,
            rows,
            limit,
            start,
        ))
    }
}

#[async_trait]
impl<B> PushDeliveryEventStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn append_delivery_event(&self, event: DeliveryEvent) -> PushStorageResult<()> {
        self.remember_cleanup_app(&event.app_id).await?;
        let position = delivery_event_position(&event);
        self.put_json(
            FAMILY_DELIVERY_EVENT,
            &event.app_id,
            &event.publish_id,
            &position,
            &event,
        )
        .await?;
        self.put_json(
            FAMILY_DELIVERY_EVENT_TIME,
            &event.app_id,
            "time",
            &format!("{position}:{}", event.publish_id),
            &event,
        )
        .await
    }

    async fn list_delivery_events(
        &self,
        app_id: &str,
        publish_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeliveryEvent>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .scan_pk_page_json::<DeliveryEvent>(
                FAMILY_DELIVERY_EVENT,
                app_id,
                publish_id,
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?
            .into_iter()
            .map(|(_, position, event)| (position, event))
            .collect::<Vec<_>>();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::DeliveryEvent,
            rows,
            limit,
            start,
        ))
    }

    async fn purge_delivery_events_before(
        &self,
        app_id: &str,
        before_ms: u64,
    ) -> PushStorageResult<u64> {
        let events = self
            .scan_pk_json::<DeliveryEvent>(FAMILY_DELIVERY_EVENT_TIME, app_id, "time")
            .await?;
        let mut primary_keys = Vec::new();
        let mut time_keys = Vec::new();
        for (_, time_position, event) in events
            .into_iter()
            .filter(|(_, _, event)| event.occurred_at_ms < before_ms)
        {
            primary_keys.push((event.publish_id.clone(), delivery_event_position(&event)));
            time_keys.push(("time".to_owned(), time_position));
        }
        let deleted = self
            .backend
            .delete_many(FAMILY_DELIVERY_EVENT, app_id, &primary_keys)
            .await?;
        self.backend
            .delete_many(FAMILY_DELIVERY_EVENT_TIME, app_id, &time_keys)
            .await?;
        Ok(deleted)
    }
}

pub(super) async fn document_cleanup_publish_statuses<B>(
    store: &DocumentPushStore<B>,
    app_id: &str,
    cutoff_ms: u64,
    limit: usize,
) -> PushStorageResult<crate::cleanup::PushCleanupCounters>
where
    B: DocumentBackend,
{
    let rows = store
        .scan_pk_page_json::<String>(
            FAMILY_STATUS_UPDATED_TIME,
            app_id,
            "time",
            None,
            limit.max(1),
        )
        .await?;
    let mut counters = crate::cleanup::PushCleanupCounters::default();
    for (_, position, publish_id) in rows {
        counters.scanned = counters.scanned.saturating_add(1);
        let Some((updated_at_ms, _)) = parse_status_updated_position(&position) else {
            continue;
        };
        if updated_at_ms >= cutoff_ms {
            continue;
        }
        let Some(status) = store
            .get_json::<PublishStatus>(FAMILY_STATUS, app_id, &publish_id, DEFAULT_SK)
            .await?
        else {
            store
                .backend
                .delete(FAMILY_STATUS_UPDATED_TIME, app_id, "time", &position)
                .await?;
            continue;
        };
        if !terminal_publish_state(status.state) {
            continue;
        }
        if store
            .backend
            .delete(FAMILY_STATUS, app_id, &publish_id, DEFAULT_SK)
            .await?
        {
            counters.deleted = counters.deleted.saturating_add(1);
        }
        store
            .backend
            .delete(FAMILY_STATUS_UPDATED, app_id, &publish_id, DEFAULT_SK)
            .await?;
        store
            .backend
            .delete(FAMILY_STATUS_UPDATED_TIME, app_id, "time", &position)
            .await?;
    }
    Ok(counters)
}
