use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    ProviderCredential, PublishLogEvent, PublishStatus, PushCursor, PushCursorKind, ShardJob,
};
use crate::storage::{
    DeviceRegistrationChange, DeviceRegistrationOutcome, IdempotencyRecord,
    OperatorInvalidationEvent, Page, PushCredentialStore, PushDeliveryEventStore, PushDeviceStore,
    PushFanoutShardStore, PushIdempotencyStore, PushOperatorEventStore, PushPublishLogStore,
    PushPublishStatusStore, PushScheduleStore, PushSchedulerLockStore, PushStorageError,
    PushStorageResult, PushSubscriptionStore, PushTemplateStore, ScheduledPushJob, SchedulerLock,
};
#[cfg(feature = "surrealdb")]
use surrealdb_types::SurrealValue;

const FAMILY_DEVICE: &str = "device";
const FAMILY_SUBSCRIPTION: &str = "subscription";
const FAMILY_CREDENTIAL: &str = "credential";
const FAMILY_TEMPLATE: &str = "template";
const FAMILY_STATUS: &str = "status";
const FAMILY_PUBLISH_LOG: &str = "publish-log";
const FAMILY_FANOUT_SHARD: &str = "fanout-shard";
const FAMILY_SCHEDULED_JOB: &str = "scheduled-job";
const FAMILY_DELIVERY_EVENT: &str = "delivery-event";
const FAMILY_IDEMPOTENCY: &str = "idempotency";
const FAMILY_SCHEDULER_LOCK: &str = "scheduler-lock";
const FAMILY_OPERATOR_INVALIDATION: &str = "operator-invalidation";
const DEFAULT_SK: &str = "_";

#[derive(Clone, Debug)]
struct StoredDocument {
    pk: String,
    sk: String,
    data: String,
}

#[async_trait]
trait DocumentBackend: Clone + Send + Sync + 'static {
    async fn put(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<()>;

    async fn put_if_absent(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<bool>;

    async fn get(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>>;

    async fn delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<bool>;

    async fn scan_app(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<StoredDocument>>;
}

#[derive(Clone)]
pub struct DocumentPushStore<B> {
    backend: B,
}

impl<B> DocumentPushStore<B> {
    fn with_backend(backend: B) -> Self {
        Self { backend }
    }
}

#[cfg(feature = "dynamodb")]
pub type DynamoDbPushStore = DocumentPushStore<DynamoDbDocumentBackend>;
#[cfg(feature = "surrealdb")]
pub type SurrealDbPushStore = DocumentPushStore<SurrealDbDocumentBackend>;
#[cfg(feature = "scylladb")]
pub type ScyllaDbPushStore = DocumentPushStore<ScyllaDbDocumentBackend>;

#[allow(private_bounds)]
impl<B> DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_json<T: Serialize>(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        value: &T,
    ) -> PushStorageResult<()> {
        self.backend
            .put(family, app_id, pk, sk, to_json_string(value)?)
            .await
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<T>> {
        self.backend
            .get(family, app_id, pk, sk)
            .await?
            .map(|data| from_json_str(&data))
            .transpose()
    }

    async fn delete_json(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        delete_outcome(self.backend.delete(family, app_id, pk, sk).await?)
    }

    async fn scan_json<T: DeserializeOwned>(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<(String, String, T)>> {
        self.backend
            .scan_app(family, app_id)
            .await?
            .into_iter()
            .map(|doc| Ok((doc.pk, doc.sk, from_json_str(&doc.data)?)))
            .collect()
    }
}

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
        let existing = self.get_device(&device.app_id, &device.id).await?;
        let token_hash = device.push.recipient.token_hash();
        let change = registration_change(existing.as_ref(), &device);
        self.put_json(
            FAMILY_DEVICE,
            &device.app_id,
            &device.id,
            DEFAULT_SK,
            &device,
        )
        .await?;
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
            .scan_json::<DeviceDetails>(FAMILY_DEVICE, app_id)
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
            .scan_json::<DeviceDetails>(FAMILY_DEVICE, app_id)
            .await?;
        let mut deleted = 0_u64;
        for (device_id, _, device) in devices {
            if device.client_id.as_deref() == Some(client_id)
                && self.delete_device(app_id, &device_id).await? == DeleteDeviceOutcome::Deleted
            {
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
        let mut rows = self
            .scan_json::<DeviceDetails>(FAMILY_DEVICE, app_id)
            .await?
            .into_iter()
            .filter_map(|(_, _, device)| {
                (day_bucket_for_ms(device.last_active_at_ms) == day_bucket).then(|| {
                    (
                        format!("{:020}:{}", device.last_active_at_ms, device.id),
                        device,
                    )
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(page_from_rows(
            app_id,
            PushCursorKind::Device,
            rows,
            limit,
            start,
        ))
    }
}

#[async_trait]
impl<B> PushSubscriptionStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn upsert_subscription(
        &self,
        subscription: ChannelSubscription,
    ) -> PushStorageResult<()> {
        subscription.validate()?;
        self.put_json(
            FAMILY_SUBSCRIPTION,
            &subscription.app_id,
            &subscription.channel,
            &subscription.device_id,
            &subscription,
        )
        .await
    }

    async fn delete_subscription(
        &self,
        app_id: &str,
        channel: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        self.delete_json(FAMILY_SUBSCRIPTION, app_id, channel, device_id)
            .await
    }

    async fn list_channel_subscribers(
        &self,
        app_id: &str,
        channel: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .scan_json::<ChannelSubscription>(FAMILY_SUBSCRIPTION, app_id)
            .await?
            .into_iter()
            .filter_map(|(_, device_id, subscription)| {
                (subscription.channel == channel).then_some((device_id, subscription))
            })
            .collect();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::ChannelSubscription,
            rows,
            limit,
            start,
        ))
    }

    async fn list_device_channels(
        &self,
        app_id: &str,
        device_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        let start = cursor_position(cursor, app_id)?;
        let mut rows = self
            .scan_json::<ChannelSubscription>(FAMILY_SUBSCRIPTION, app_id)
            .await?
            .into_iter()
            .filter_map(|(_, _, subscription)| {
                (subscription.device_id == device_id)
                    .then(|| (subscription.channel.clone(), subscription))
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(page_from_rows(
            app_id,
            PushCursorKind::ChannelSubscription,
            rows,
            limit,
            start,
        ))
    }

    async fn list_subscriptions(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        let start = cursor_position(cursor, app_id)?;
        let mut rows = self
            .scan_json::<ChannelSubscription>(FAMILY_SUBSCRIPTION, app_id)
            .await?
            .into_iter()
            .map(|(_, _, subscription)| {
                (
                    format!("{}:{}", subscription.channel, subscription.device_id),
                    subscription,
                )
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(page_from_rows(
            app_id,
            PushCursorKind::ChannelSubscription,
            rows,
            limit,
            start,
        ))
    }

    async fn list_subscription_channels(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<String>> {
        let start = cursor_position(cursor, app_id)?;
        let mut channels = self
            .scan_json::<ChannelSubscription>(FAMILY_SUBSCRIPTION, app_id)
            .await?
            .into_iter()
            .map(|(_, _, subscription)| subscription.channel)
            .collect::<Vec<_>>();
        channels.sort();
        channels.dedup();
        let rows = channels
            .into_iter()
            .map(|channel| (channel.clone(), channel))
            .collect();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::ChannelSubscription,
            rows,
            limit,
            start,
        ))
    }

    async fn delete_subscriptions_by_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<u64> {
        let subscriptions = self
            .scan_json::<ChannelSubscription>(FAMILY_SUBSCRIPTION, app_id)
            .await?;
        let mut deleted = 0_u64;
        for (channel, sub_device_id, _) in subscriptions {
            if sub_device_id == device_id
                && self
                    .backend
                    .delete(FAMILY_SUBSCRIPTION, app_id, &channel, &sub_device_id)
                    .await?
            {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    async fn delete_subscriptions_by_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> PushStorageResult<u64> {
        let subscriptions = self
            .scan_json::<ChannelSubscription>(FAMILY_SUBSCRIPTION, app_id)
            .await?;
        let mut deleted = 0_u64;
        for (sub_channel, device_id, _) in subscriptions {
            if sub_channel == channel
                && self
                    .backend
                    .delete(FAMILY_SUBSCRIPTION, app_id, &sub_channel, &device_id)
                    .await?
            {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

#[async_trait]
impl<B> PushCredentialStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_credential(&self, credential: ProviderCredential) -> PushStorageResult<()> {
        credential.validate()?;
        self.put_json(
            FAMILY_CREDENTIAL,
            &credential.app_id,
            &credential.credential_id,
            DEFAULT_SK,
            &credential,
        )
        .await
    }

    async fn get_credential(
        &self,
        app_id: &str,
        credential_id: &str,
    ) -> PushStorageResult<Option<ProviderCredential>> {
        self.get_json(FAMILY_CREDENTIAL, app_id, credential_id, DEFAULT_SK)
            .await
    }

    async fn list_credentials(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ProviderCredential>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .scan_json::<ProviderCredential>(FAMILY_CREDENTIAL, app_id)
            .await?
            .into_iter()
            .map(|(credential_id, _, credential)| (credential_id, credential))
            .collect();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::Credential,
            rows,
            limit,
            start,
        ))
    }
}

#[async_trait]
impl<B> PushTemplateStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_template(&self, template: NotificationTemplate) -> PushStorageResult<()> {
        template.validate()?;
        self.put_json(
            FAMILY_TEMPLATE,
            &template.app_id,
            &template.template_id,
            DEFAULT_SK,
            &template,
        )
        .await
    }

    async fn get_template(
        &self,
        app_id: &str,
        template_id: &str,
    ) -> PushStorageResult<Option<NotificationTemplate>> {
        self.get_json(FAMILY_TEMPLATE, app_id, template_id, DEFAULT_SK)
            .await
    }

    async fn list_templates(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<NotificationTemplate>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .scan_json::<NotificationTemplate>(FAMILY_TEMPLATE, app_id)
            .await?
            .into_iter()
            .map(|(template_id, _, template)| (template_id, template))
            .collect();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::Template,
            rows,
            limit,
            start,
        ))
    }

    async fn delete_template(
        &self,
        app_id: &str,
        template_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        self.delete_json(FAMILY_TEMPLATE, app_id, template_id, DEFAULT_SK)
            .await
    }
}

#[async_trait]
impl<B> PushPublishStatusStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_publish_status(&self, status: PublishStatus) -> PushStorageResult<()> {
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
        self.put_json(
            FAMILY_SCHEDULED_JOB,
            &job.app_id,
            &job.publish_id,
            DEFAULT_SK,
            &job,
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
        self.delete_json(FAMILY_SCHEDULED_JOB, app_id, publish_id, DEFAULT_SK)
            .await
    }

    async fn list_due_scheduled_jobs(
        &self,
        app_id: &str,
        due_minute_ms: u64,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ScheduledPushJob>> {
        let start = cursor_position(cursor, app_id)?;
        let mut rows = self
            .scan_json::<ScheduledPushJob>(FAMILY_SCHEDULED_JOB, app_id)
            .await?
            .into_iter()
            .filter_map(|(_, _, job)| {
                (job.due_minute_ms <= due_minute_ms)
                    .then(|| (format!("{:020}:{}", job.due_at_ms, job.publish_id), job))
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
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
        self.put_json(
            FAMILY_DELIVERY_EVENT,
            &event.app_id,
            &event.publish_id,
            &format!("{:020}:{}", event.occurred_at_ms, event.event_id),
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
        let mut rows = self
            .scan_json::<DeliveryEvent>(FAMILY_DELIVERY_EVENT, app_id)
            .await?
            .into_iter()
            .filter_map(|(event_publish_id, position, event)| {
                (event_publish_id == publish_id).then_some((position, event))
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
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
            .scan_json::<DeliveryEvent>(FAMILY_DELIVERY_EVENT, app_id)
            .await?;
        let mut deleted = 0_u64;
        for (publish_id, position, event) in events {
            if event.occurred_at_ms < before_ms
                && self
                    .backend
                    .delete(FAMILY_DELIVERY_EVENT, app_id, &publish_id, &position)
                    .await?
            {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

#[async_trait]
impl<B> PushIdempotencyStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn put_idempotency_record_if_absent(
        &self,
        record: IdempotencyRecord,
    ) -> PushStorageResult<bool> {
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
        self.get_json(FAMILY_IDEMPOTENCY, app_id, key, DEFAULT_SK)
            .await
    }
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

#[cfg(feature = "dynamodb")]
#[derive(Clone)]
pub struct DynamoDbDocumentBackend {
    client: aws_sdk_dynamodb::Client,
    table: String,
}

#[cfg(feature = "dynamodb")]
impl DynamoDbPushStore {
    pub async fn new(
        client: aws_sdk_dynamodb::Client,
        table: impl Into<String>,
    ) -> PushStorageResult<Self> {
        let backend = DynamoDbDocumentBackend {
            client,
            table: table.into(),
        };
        backend.ensure_table().await?;
        Ok(Self::with_backend(backend))
    }
}

#[cfg(feature = "dynamodb")]
impl DynamoDbDocumentBackend {
    async fn ensure_table(&self) -> PushStorageResult<()> {
        use aws_sdk_dynamodb::types::{
            AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        };

        if self
            .client
            .describe_table()
            .table_name(&self.table)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }

        self.client
            .create_table()
            .table_name(&self.table)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("family_app")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(dynamodb_build_error)?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("doc_key")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(dynamodb_build_error)?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("family_app")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(dynamodb_build_error)?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("doc_key")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(dynamodb_build_error)?,
            )
            .send()
            .await
            .map_err(dynamodb_error)?;
        Ok(())
    }

    fn key(family: &'static str, app_id: &str, pk: &str, sk: &str) -> (String, String) {
        (family_app(family, app_id), document_key(pk, sk))
    }
}

#[cfg(feature = "dynamodb")]
#[async_trait]
impl DocumentBackend for DynamoDbDocumentBackend {
    async fn put(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<()> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let (family_app, doc_key) = Self::key(family, app_id, pk, sk);
        self.client
            .put_item()
            .table_name(&self.table)
            .item("family_app", AttributeValue::S(family_app))
            .item("doc_key", AttributeValue::S(doc_key))
            .item("app_id", AttributeValue::S(app_id.to_owned()))
            .item("pk", AttributeValue::S(pk.to_owned()))
            .item("sk", AttributeValue::S(sk.to_owned()))
            .item("data", AttributeValue::S(data))
            .send()
            .await
            .map_err(dynamodb_error)?;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<bool> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let (family_app, doc_key) = Self::key(family, app_id, pk, sk);
        let result = self
            .client
            .put_item()
            .table_name(&self.table)
            .condition_expression("attribute_not_exists(family_app)")
            .item("family_app", AttributeValue::S(family_app))
            .item("doc_key", AttributeValue::S(doc_key))
            .item("app_id", AttributeValue::S(app_id.to_owned()))
            .item("pk", AttributeValue::S(pk.to_owned()))
            .item("sk", AttributeValue::S(sk.to_owned()))
            .item("data", AttributeValue::S(data))
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error) if error.to_string().contains("ConditionalCheckFailed") => Ok(false),
            Err(error) => Err(dynamodb_error(error)),
        }
    }

    async fn get(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let (family_app, doc_key) = Self::key(family, app_id, pk, sk);
        Ok(self
            .client
            .get_item()
            .table_name(&self.table)
            .key("family_app", AttributeValue::S(family_app))
            .key("doc_key", AttributeValue::S(doc_key))
            .send()
            .await
            .map_err(dynamodb_error)?
            .item
            .and_then(|mut item| item.remove("data"))
            .and_then(|value| value.as_s().ok().cloned()))
    }

    async fn delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<bool> {
        use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
        let (family_app, doc_key) = Self::key(family, app_id, pk, sk);
        Ok(self
            .client
            .delete_item()
            .table_name(&self.table)
            .key("family_app", AttributeValue::S(family_app))
            .key("doc_key", AttributeValue::S(doc_key))
            .return_values(ReturnValue::AllOld)
            .send()
            .await
            .map_err(dynamodb_error)?
            .attributes
            .is_some_and(|attributes| !attributes.is_empty()))
    }

    async fn scan_app(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        use aws_sdk_dynamodb::types::AttributeValue;
        let family_app = family_app(family, app_id);
        let rows = self
            .client
            .query()
            .table_name(&self.table)
            .key_condition_expression("family_app = :family_app")
            .expression_attribute_values(":family_app", AttributeValue::S(family_app))
            .send()
            .await
            .map_err(dynamodb_error)?
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|mut item| {
                let pk = take_dynamodb_string(&mut item, "pk")?;
                let sk = take_dynamodb_string(&mut item, "sk")?;
                let data = take_dynamodb_string(&mut item, "data")?;
                Ok(StoredDocument { pk, sk, data })
            })
            .collect::<PushStorageResult<Vec<_>>>()?;
        Ok(rows)
    }
}

#[cfg(feature = "surrealdb")]
#[derive(Clone)]
pub struct SurrealDbDocumentBackend {
    db: surrealdb::Surreal<surrealdb::engine::any::Any>,
    table: String,
}

#[cfg(feature = "surrealdb")]
impl SurrealDbPushStore {
    pub async fn new(
        db: surrealdb::Surreal<surrealdb::engine::any::Any>,
        table: impl Into<String>,
    ) -> PushStorageResult<Self> {
        let backend = SurrealDbDocumentBackend {
            db,
            table: table.into(),
        };
        validate_identifier(&backend.table, "SurrealDB push table")?;
        backend.ensure_schema().await?;
        Ok(Self::with_backend(backend))
    }
}

#[cfg(feature = "surrealdb")]
#[derive(Debug, serde::Deserialize, surrealdb_types::SurrealValue)]
struct SurrealDocumentRow {
    pk: String,
    sk: String,
    data: String,
}

#[cfg(feature = "surrealdb")]
impl SurrealDbDocumentBackend {
    async fn ensure_schema(&self) -> PushStorageResult<()> {
        self.db
            .query(format!(
                "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
                 DEFINE INDEX IF NOT EXISTS {}_key ON TABLE {} FIELDS family_app, pk, sk UNIQUE;\
                 DEFINE INDEX IF NOT EXISTS {}_app ON TABLE {} FIELDS family_app;",
                self.table, self.table, self.table, self.table, self.table
            ))
            .await
            .map_err(surreal_error)?;
        Ok(())
    }

    fn record_id(family: &'static str, app_id: &str, pk: &str, sk: &str) -> String {
        let digest =
            crate::domain::stable_hash(format!("{family}\0{app_id}\0{pk}\0{sk}").as_bytes());
        digest.replace('-', "_")
    }
}

#[cfg(feature = "surrealdb")]
#[async_trait]
impl DocumentBackend for SurrealDbDocumentBackend {
    async fn put(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<()> {
        self.db
            .query("UPSERT type::record($table, $id) SET family_app = $family_app, app_id = $app_id, pk = $pk, sk = $sk, data = $data")
            .bind(("table", self.table.clone()))
            .bind(("id", Self::record_id(family, app_id, pk, sk)))
            .bind(("family_app", family_app(family, app_id)))
            .bind(("app_id", app_id.to_owned()))
            .bind(("pk", pk.to_owned()))
            .bind(("sk", sk.to_owned()))
            .bind(("data", data))
            .await
            .map_err(surreal_error)?;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<bool> {
        if self.get(family, app_id, pk, sk).await?.is_some() {
            return Ok(false);
        }
        self.put(family, app_id, pk, sk, data).await?;
        Ok(true)
    }

    async fn get(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT pk, sk, data FROM {} WHERE family_app = $family_app AND pk = $pk AND sk = $sk LIMIT 1",
                self.table
            ))
            .bind(("family_app", family_app(family, app_id)))
            .bind(("pk", pk.to_owned()))
            .bind(("sk", sk.to_owned()))
            .await
            .map_err(surreal_error)?;
        let rows: Vec<SurrealDocumentRow> = response.take(0usize).map_err(surreal_error)?;
        Ok(rows.into_iter().next().map(|row| row.data))
    }

    async fn delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<bool> {
        let existed = self.get(family, app_id, pk, sk).await?.is_some();
        if existed {
            self.db
                .query("DELETE type::record($table, $id)")
                .bind(("table", self.table.clone()))
                .bind(("id", Self::record_id(family, app_id, pk, sk)))
                .await
                .map_err(surreal_error)?;
        }
        Ok(existed)
    }

    async fn scan_app(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT pk, sk, data FROM {} WHERE family_app = $family_app ORDER BY pk ASC, sk ASC",
                self.table
            ))
            .bind(("family_app", family_app(family, app_id)))
            .await
            .map_err(surreal_error)?;
        let rows: Vec<SurrealDocumentRow> = response.take(0usize).map_err(surreal_error)?;
        Ok(rows
            .into_iter()
            .map(|row| StoredDocument {
                pk: row.pk,
                sk: row.sk,
                data: row.data,
            })
            .collect())
    }
}

#[cfg(feature = "scylladb")]
#[derive(Clone)]
pub struct ScyllaDbDocumentBackend {
    session: std::sync::Arc<scylla::client::session::Session>,
    keyspace: String,
    table: String,
}

#[cfg(feature = "scylladb")]
impl ScyllaDbPushStore {
    pub async fn new(
        session: std::sync::Arc<scylla::client::session::Session>,
        keyspace: impl Into<String>,
        table: impl Into<String>,
        replication_class: impl AsRef<str>,
        replication_factor: u32,
    ) -> PushStorageResult<Self> {
        let backend = ScyllaDbDocumentBackend {
            session,
            keyspace: keyspace.into(),
            table: table.into(),
        };
        validate_identifier(&backend.keyspace, "ScyllaDB push keyspace")?;
        validate_identifier(&backend.table, "ScyllaDB push table")?;
        backend
            .ensure_schema(replication_class.as_ref(), replication_factor)
            .await?;
        Ok(Self::with_backend(backend))
    }
}

#[cfg(feature = "scylladb")]
impl ScyllaDbDocumentBackend {
    fn fq_table(&self) -> String {
        format!("{}.{}", self.keyspace, self.table)
    }

    async fn ensure_schema(
        &self,
        replication_class: &str,
        replication_factor: u32,
    ) -> PushStorageResult<()> {
        validate_identifier(replication_class, "ScyllaDB replication class")?;
        self.session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': '{}', 'replication_factor': {}}}",
                    self.keyspace, replication_class, replication_factor
                ),
                (),
            )
            .await
            .map_err(scylla_error)?;
        self.session
            .query_unpaged(
                format!(
                    "CREATE TABLE IF NOT EXISTS {} (family_app text, pk text, sk text, app_id text, data text, PRIMARY KEY ((family_app), pk, sk)) WITH CLUSTERING ORDER BY (pk ASC, sk ASC)",
                    self.fq_table()
                ),
                (),
            )
            .await
            .map_err(scylla_error)?;
        Ok(())
    }
}

#[cfg(feature = "scylladb")]
#[async_trait]
impl DocumentBackend for ScyllaDbDocumentBackend {
    async fn put(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<()> {
        self.session
            .query_unpaged(
                format!(
                    "INSERT INTO {} (family_app, pk, sk, app_id, data) VALUES (?, ?, ?, ?, ?)",
                    self.fq_table()
                ),
                (family_app(family, app_id), pk, sk, app_id, data),
            )
            .await
            .map_err(scylla_error)?;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
        data: String,
    ) -> PushStorageResult<bool> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "INSERT INTO {} (family_app, pk, sk, app_id, data) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS",
                    self.fq_table()
                ),
                (family_app(family, app_id), pk, sk, app_id, data),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        let applied = rows
            .maybe_first_row::<(bool,)>()
            .map_err(scylla_error)?
            .map(|row| row.0)
            .unwrap_or(false);
        Ok(applied)
    }

    async fn get(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<Option<String>> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "SELECT data FROM {} WHERE family_app = ? AND pk = ? AND sk = ?",
                    self.fq_table()
                ),
                (family_app(family, app_id), pk, sk),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        rows.maybe_first_row::<(String,)>()
            .map_err(scylla_error)
            .map(|row| row.map(|row| row.0))
    }

    async fn delete(
        &self,
        family: &'static str,
        app_id: &str,
        pk: &str,
        sk: &str,
    ) -> PushStorageResult<bool> {
        let existed = self.get(family, app_id, pk, sk).await?.is_some();
        if existed {
            self.session
                .query_unpaged(
                    format!(
                        "DELETE FROM {} WHERE family_app = ? AND pk = ? AND sk = ?",
                        self.fq_table()
                    ),
                    (family_app(family, app_id), pk, sk),
                )
                .await
                .map_err(scylla_error)?;
        }
        Ok(existed)
    }

    async fn scan_app(
        &self,
        family: &'static str,
        app_id: &str,
    ) -> PushStorageResult<Vec<StoredDocument>> {
        let result = self
            .session
            .query_unpaged(
                format!(
                    "SELECT pk, sk, data FROM {} WHERE family_app = ?",
                    self.fq_table()
                ),
                (family_app(family, app_id),),
            )
            .await
            .map_err(scylla_error)?;
        let rows = result.into_rows_result().map_err(scylla_error)?;
        rows.rows::<(String, String, String)>()
            .map_err(scylla_error)?
            .map(|row| {
                row.map(|(pk, sk, data)| StoredDocument { pk, sk, data })
                    .map_err(scylla_error)
            })
            .collect()
    }
}

fn cursor_position(cursor: Option<PushCursor>, app_id: &str) -> PushStorageResult<Option<String>> {
    match cursor {
        Some(cursor) if cursor.app_id != app_id => {
            Err(crate::domain::PushDomainError::CursorAppMismatch {
                expected: app_id.to_owned(),
                found: cursor.app_id,
            }
            .into())
        }
        Some(cursor) => Ok(Some(cursor.position)),
        None => Ok(None),
    }
}

fn page_from_rows<T: Clone>(
    app_id: &str,
    kind: PushCursorKind,
    mut rows: Vec<(String, T)>,
    limit: usize,
    start_after: Option<String>,
) -> Page<T> {
    let limit = limit.max(1);
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    let filtered = rows
        .into_iter()
        .filter(|(position, _)| start_after.as_ref().is_none_or(|start| position > start))
        .collect::<Vec<_>>();

    let mut items = Vec::with_capacity(limit.min(filtered.len()));
    let mut last_position = None;
    for (position, item) in filtered.iter().take(limit) {
        last_position = Some(position.clone());
        items.push(item.clone());
    }

    let next_cursor = if filtered.len() > limit {
        last_position.map(|position| PushCursor {
            app_id: app_id.to_owned(),
            kind,
            position,
            issued_at_ms: 0,
        })
    } else {
        None
    };

    Page { items, next_cursor }
}

fn registration_change(
    existing: Option<&DeviceDetails>,
    incoming: &DeviceDetails,
) -> DeviceRegistrationChange {
    match existing {
        None => DeviceRegistrationChange::Inserted,
        Some(existing) if existing == incoming => DeviceRegistrationChange::Unchanged,
        Some(_) => DeviceRegistrationChange::Updated,
    }
}

fn delete_outcome(deleted: bool) -> PushStorageResult<DeleteDeviceOutcome> {
    Ok(if deleted {
        DeleteDeviceOutcome::Deleted
    } else {
        DeleteDeviceOutcome::NotFound
    })
}

fn day_bucket_for_ms(timestamp_ms: u64) -> String {
    (timestamp_ms / 86_400_000).to_string()
}

fn family_app(family: &'static str, app_id: &str) -> String {
    format!("{family}#{app_id}")
}

#[cfg(feature = "dynamodb")]
fn document_key(pk: &str, sk: &str) -> String {
    format!("{pk}\0{sk}")
}

fn to_json_string<T: Serialize>(value: &T) -> PushStorageResult<String> {
    serde_json::to_string(value).map_err(json_error)
}

fn from_json_str<T: DeserializeOwned>(value: &str) -> PushStorageResult<T> {
    serde_json::from_str(value).map_err(json_error)
}

fn json_error(error: serde_json::Error) -> PushStorageError {
    PushStorageError::Backend(format!("push document JSON mapping failed: {error}"))
}

#[cfg(any(feature = "surrealdb", feature = "scylladb"))]
fn validate_identifier(value: &str, label: &'static str) -> PushStorageResult<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(PushStorageError::Backend(format!(
            "{label} must contain only ASCII letters, numbers, and underscores"
        )));
    }
    Ok(())
}

#[cfg(feature = "dynamodb")]
fn take_dynamodb_string(
    item: &mut std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    name: &str,
) -> PushStorageResult<String> {
    item.remove(name)
        .ok_or_else(|| PushStorageError::Backend(format!("DynamoDB push item missing {name}")))?
        .as_s()
        .map(String::to_owned)
        .map_err(|_| {
            PushStorageError::Backend(format!("DynamoDB push item {name} is not a string"))
        })
}

#[cfg(feature = "dynamodb")]
fn dynamodb_error<E: std::fmt::Display>(error: E) -> PushStorageError {
    PushStorageError::Backend(format!("push DynamoDB backend error: {error}"))
}

#[cfg(feature = "dynamodb")]
fn dynamodb_build_error<E: std::fmt::Display>(error: E) -> PushStorageError {
    PushStorageError::Backend(format!("push DynamoDB schema build error: {error}"))
}

#[cfg(feature = "surrealdb")]
fn surreal_error<E: std::fmt::Display>(error: E) -> PushStorageError {
    PushStorageError::Backend(format!("push SurrealDB backend error: {error}"))
}

#[cfg(feature = "scylladb")]
fn scylla_error<E: std::fmt::Display>(error: E) -> PushStorageError {
    PushStorageError::Backend(format!("push ScyllaDB backend error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::PushStoreConformance;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    type TestDocumentKey = (String, String, String);
    type TestDocumentMap = Arc<RwLock<BTreeMap<TestDocumentKey, String>>>;

    #[derive(Clone, Default)]
    struct TestDocumentBackend {
        inner: TestDocumentMap,
    }

    #[async_trait]
    impl DocumentBackend for TestDocumentBackend {
        async fn put(
            &self,
            family: &'static str,
            app_id: &str,
            pk: &str,
            sk: &str,
            data: String,
        ) -> PushStorageResult<()> {
            self.inner.write().await.insert(
                (family_app(family, app_id), pk.to_owned(), sk.to_owned()),
                data,
            );
            Ok(())
        }

        async fn put_if_absent(
            &self,
            family: &'static str,
            app_id: &str,
            pk: &str,
            sk: &str,
            data: String,
        ) -> PushStorageResult<bool> {
            let key = (family_app(family, app_id), pk.to_owned(), sk.to_owned());
            let mut inner = self.inner.write().await;
            if inner.contains_key(&key) {
                return Ok(false);
            }
            inner.insert(key, data);
            Ok(true)
        }

        async fn get(
            &self,
            family: &'static str,
            app_id: &str,
            pk: &str,
            sk: &str,
        ) -> PushStorageResult<Option<String>> {
            Ok(self
                .inner
                .read()
                .await
                .get(&(family_app(family, app_id), pk.to_owned(), sk.to_owned()))
                .cloned())
        }

        async fn delete(
            &self,
            family: &'static str,
            app_id: &str,
            pk: &str,
            sk: &str,
        ) -> PushStorageResult<bool> {
            Ok(self
                .inner
                .write()
                .await
                .remove(&(family_app(family, app_id), pk.to_owned(), sk.to_owned()))
                .is_some())
        }

        async fn scan_app(
            &self,
            family: &'static str,
            app_id: &str,
        ) -> PushStorageResult<Vec<StoredDocument>> {
            Ok(self
                .inner
                .read()
                .await
                .iter()
                .filter(|((family_app_key, _, _), _)| family_app_key == &family_app(family, app_id))
                .map(|((_, pk, sk), data)| StoredDocument {
                    pk: pk.clone(),
                    sk: sk.clone(),
                    data: data.clone(),
                })
                .collect())
        }
    }

    fn test_store() -> DocumentPushStore<TestDocumentBackend> {
        DocumentPushStore::with_backend(TestDocumentBackend::default())
    }

    #[tokio::test]
    async fn document_store_satisfies_device_registration_idempotency() {
        PushStoreConformance::assert_device_registration_idempotency(test_store())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn document_store_satisfies_cursor_pagination_and_channel_fanout() {
        PushStoreConformance::assert_cursor_pagination_and_channel_fanout(test_store())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn document_store_satisfies_stale_cleanup_scans() {
        PushStoreConformance::assert_stale_cleanup_scans(test_store())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn document_store_satisfies_secondary_storage_contracts() {
        PushStoreConformance::assert_credentials_templates_schedule_events_and_idempotency(
            test_store(),
        )
        .await
        .unwrap();
    }
}
