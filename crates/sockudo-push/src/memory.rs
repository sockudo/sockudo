use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

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

#[derive(Clone, Default)]
pub struct MemoryPushStore {
    inner: Arc<RwLock<MemoryPushState>>,
}

#[derive(Default)]
struct MemoryPushState {
    devices_by_id: BTreeMap<(String, String), DeviceDetails>,
    subscriptions: BTreeMap<(String, String, String), ChannelSubscription>,
    credentials: BTreeMap<(String, String), ProviderCredential>,
    templates: BTreeMap<(String, String), NotificationTemplate>,
    publish_status: BTreeMap<(String, String), PublishStatus>,
    publish_log: BTreeMap<(String, u64, String), PublishLogEvent>,
    fanout_shards: BTreeMap<(String, String, String), ShardJob>,
    scheduled_by_id: BTreeMap<(String, String), ScheduledPushJob>,
    delivery_events: BTreeMap<(String, String, u64, String), DeliveryEvent>,
    idempotency: BTreeMap<(String, String), IdempotencyRecord>,
    scheduler_locks: BTreeMap<(String, String), SchedulerLock>,
    operator_invalidations: BTreeMap<(String, u64, String), OperatorInvalidationEvent>,
}

impl MemoryPushStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PushDeviceStore for MemoryPushStore {
    async fn upsert_device(
        &self,
        device: DeviceDetails,
    ) -> PushStorageResult<DeviceRegistrationOutcome> {
        device.validate()?;
        let token_hash = device.push.recipient.token_hash();
        let key = (device.app_id.clone(), device.id.clone());
        let mut inner = self.inner.write().await;
        let change = match inner.devices_by_id.get(&key) {
            None => DeviceRegistrationChange::Inserted,
            Some(existing) if existing == &device => DeviceRegistrationChange::Unchanged,
            Some(_) => DeviceRegistrationChange::Updated,
        };
        inner.devices_by_id.insert(key, device);
        Ok(DeviceRegistrationOutcome { change, token_hash })
    }

    async fn get_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<Option<DeviceDetails>> {
        Ok(self
            .inner
            .read()
            .await
            .devices_by_id
            .get(&(app_id.to_owned(), device_id.to_owned()))
            .cloned())
    }

    async fn delete_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        let mut inner = self.inner.write().await;
        let removed = inner
            .devices_by_id
            .remove(&(app_id.to_owned(), device_id.to_owned()))
            .is_some();
        inner
            .subscriptions
            .retain(|(sub_app_id, _, sub_device_id), _| {
                sub_app_id != app_id || sub_device_id != device_id
            });
        Ok(if removed {
            DeleteDeviceOutcome::Deleted
        } else {
            DeleteDeviceOutcome::NotFound
        })
    }

    async fn list_devices(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .inner
            .read()
            .await
            .devices_by_id
            .iter()
            .filter(|((device_app_id, _), _)| device_app_id == app_id)
            .map(|((_, device_id), device)| (device_id.clone(), device.clone()))
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
        let mut inner = self.inner.write().await;
        let device_ids = inner
            .devices_by_id
            .values()
            .filter(|device| {
                device.app_id == app_id && device.client_id.as_deref() == Some(client_id)
            })
            .map(|device| device.id.clone())
            .collect::<Vec<_>>();
        for device_id in &device_ids {
            inner
                .devices_by_id
                .remove(&(app_id.to_owned(), device_id.clone()));
        }
        inner
            .subscriptions
            .retain(|(sub_app_id, _, sub_device_id), _| {
                sub_app_id != app_id || !device_ids.contains(sub_device_id)
            });
        Ok(device_ids.len() as u64)
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
            .inner
            .read()
            .await
            .devices_by_id
            .values()
            .filter(|device| {
                device.app_id == app_id && day_bucket_for_ms(device.last_active_at_ms) == day_bucket
            })
            .map(|device| {
                (
                    format!("{:020}:{}", device.last_active_at_ms, device.id),
                    device.clone(),
                )
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
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
impl PushSubscriptionStore for MemoryPushStore {
    async fn upsert_subscription(
        &self,
        subscription: ChannelSubscription,
    ) -> PushStorageResult<()> {
        subscription.validate()?;
        self.inner.write().await.subscriptions.insert(
            (
                subscription.app_id.clone(),
                subscription.channel.clone(),
                subscription.device_id.clone(),
            ),
            subscription,
        );
        Ok(())
    }

    async fn delete_subscription(
        &self,
        app_id: &str,
        channel: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        let removed = self
            .inner
            .write()
            .await
            .subscriptions
            .remove(&(app_id.to_owned(), channel.to_owned(), device_id.to_owned()))
            .is_some();
        Ok(if removed {
            DeleteDeviceOutcome::Deleted
        } else {
            DeleteDeviceOutcome::NotFound
        })
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
            .inner
            .read()
            .await
            .subscriptions
            .iter()
            .filter(|((sub_app_id, sub_channel, _), _)| {
                sub_app_id == app_id && sub_channel == channel
            })
            .map(|((_, _, device_id), subscription)| (device_id.clone(), subscription.clone()))
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
            .inner
            .read()
            .await
            .subscriptions
            .iter()
            .filter(|((sub_app_id, _, sub_device_id), _)| {
                sub_app_id == app_id && sub_device_id == device_id
            })
            .map(|((_, channel, _), subscription)| (channel.clone(), subscription.clone()))
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
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
        let rows = self
            .inner
            .read()
            .await
            .subscriptions
            .iter()
            .filter(|((sub_app_id, _, _), _)| sub_app_id == app_id)
            .map(|((_, channel, device_id), subscription)| {
                (format!("{channel}:{device_id}"), subscription.clone())
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

    async fn list_subscription_channels(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<String>> {
        let start = cursor_position(cursor, app_id)?;
        let mut channels = self
            .inner
            .read()
            .await
            .subscriptions
            .keys()
            .filter(|(sub_app_id, _, _)| sub_app_id == app_id)
            .map(|(_, channel, _)| channel.clone())
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
        let mut inner = self.inner.write().await;
        let before = inner.subscriptions.len();
        inner
            .subscriptions
            .retain(|(sub_app_id, _, sub_device_id), _| {
                sub_app_id != app_id || sub_device_id != device_id
            });
        Ok((before - inner.subscriptions.len()) as u64)
    }

    async fn delete_subscriptions_by_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> PushStorageResult<u64> {
        let mut inner = self.inner.write().await;
        let before = inner.subscriptions.len();
        inner
            .subscriptions
            .retain(|(sub_app_id, sub_channel, _), _| {
                sub_app_id != app_id || sub_channel != channel
            });
        Ok((before - inner.subscriptions.len()) as u64)
    }
}

#[async_trait]
impl PushCredentialStore for MemoryPushStore {
    async fn put_credential(&self, credential: ProviderCredential) -> PushStorageResult<()> {
        credential.validate()?;
        self.inner.write().await.credentials.insert(
            (credential.app_id.clone(), credential.credential_id.clone()),
            credential,
        );
        Ok(())
    }

    async fn get_credential(
        &self,
        app_id: &str,
        credential_id: &str,
    ) -> PushStorageResult<Option<ProviderCredential>> {
        Ok(self
            .inner
            .read()
            .await
            .credentials
            .get(&(app_id.to_owned(), credential_id.to_owned()))
            .cloned())
    }

    async fn list_credentials(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ProviderCredential>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .inner
            .read()
            .await
            .credentials
            .iter()
            .filter(|((credential_app_id, _), _)| credential_app_id == app_id)
            .map(|((_, credential_id), credential)| (credential_id.clone(), credential.clone()))
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
impl PushTemplateStore for MemoryPushStore {
    async fn put_template(&self, template: NotificationTemplate) -> PushStorageResult<()> {
        template.validate()?;
        self.inner.write().await.templates.insert(
            (template.app_id.clone(), template.template_id.clone()),
            template,
        );
        Ok(())
    }

    async fn get_template(
        &self,
        app_id: &str,
        template_id: &str,
    ) -> PushStorageResult<Option<NotificationTemplate>> {
        Ok(self
            .inner
            .read()
            .await
            .templates
            .get(&(app_id.to_owned(), template_id.to_owned()))
            .cloned())
    }

    async fn list_templates(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<NotificationTemplate>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .inner
            .read()
            .await
            .templates
            .iter()
            .filter(|((template_app_id, _), _)| template_app_id == app_id)
            .map(|((_, template_id), template)| (template_id.clone(), template.clone()))
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
        let removed = self
            .inner
            .write()
            .await
            .templates
            .remove(&(app_id.to_owned(), template_id.to_owned()))
            .is_some();
        Ok(if removed {
            DeleteDeviceOutcome::Deleted
        } else {
            DeleteDeviceOutcome::NotFound
        })
    }
}

#[async_trait]
impl PushPublishStatusStore for MemoryPushStore {
    async fn put_publish_status(&self, status: PublishStatus) -> PushStorageResult<()> {
        self.inner
            .write()
            .await
            .publish_status
            .insert((status.app_id.clone(), status.publish_id.clone()), status);
        Ok(())
    }

    async fn get_publish_status(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<PublishStatus>> {
        Ok(self
            .inner
            .read()
            .await
            .publish_status
            .get(&(app_id.to_owned(), publish_id.to_owned()))
            .cloned())
    }
}

#[async_trait]
impl PushPublishLogStore for MemoryPushStore {
    async fn append_publish_log_event(&self, event: PublishLogEvent) -> PushStorageResult<()> {
        self.inner.write().await.publish_log.insert(
            (
                event.app_id.clone(),
                event.occurred_at_ms,
                event.event_id.clone(),
            ),
            event,
        );
        Ok(())
    }

    async fn list_publish_log_events(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<PublishLogEvent>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .inner
            .read()
            .await
            .publish_log
            .iter()
            .filter(|((event_app_id, _, _), _)| event_app_id == app_id)
            .map(|((_, occurred_at_ms, event_id), event)| {
                (format!("{occurred_at_ms:020}:{event_id}"), event.clone())
            })
            .collect();
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
impl PushFanoutShardStore for MemoryPushStore {
    async fn put_fanout_shard(&self, shard: ShardJob) -> PushStorageResult<()> {
        self.inner.write().await.fanout_shards.insert(
            (
                shard.app_id.clone(),
                shard.publish_id.clone(),
                shard.shard_id.clone(),
            ),
            shard,
        );
        Ok(())
    }

    async fn get_fanout_shard(
        &self,
        app_id: &str,
        publish_id: &str,
        shard_id: &str,
    ) -> PushStorageResult<Option<ShardJob>> {
        Ok(self
            .inner
            .read()
            .await
            .fanout_shards
            .get(&(
                app_id.to_owned(),
                publish_id.to_owned(),
                shard_id.to_owned(),
            ))
            .cloned())
    }
}

#[async_trait]
impl PushScheduleStore for MemoryPushStore {
    async fn put_scheduled_job(&self, job: ScheduledPushJob) -> PushStorageResult<()> {
        self.inner
            .write()
            .await
            .scheduled_by_id
            .insert((job.app_id.clone(), job.publish_id.clone()), job);
        Ok(())
    }

    async fn get_scheduled_job(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<ScheduledPushJob>> {
        Ok(self
            .inner
            .read()
            .await
            .scheduled_by_id
            .get(&(app_id.to_owned(), publish_id.to_owned()))
            .cloned())
    }

    async fn delete_scheduled_job(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        let removed = self
            .inner
            .write()
            .await
            .scheduled_by_id
            .remove(&(app_id.to_owned(), publish_id.to_owned()))
            .is_some();
        Ok(if removed {
            DeleteDeviceOutcome::Deleted
        } else {
            DeleteDeviceOutcome::NotFound
        })
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
            .inner
            .read()
            .await
            .scheduled_by_id
            .values()
            .filter(|job| job.app_id == app_id && job.due_minute_ms <= due_minute_ms)
            .map(|job| {
                (
                    format!("{:020}:{}", job.due_at_ms, job.publish_id),
                    job.clone(),
                )
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
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
impl PushDeliveryEventStore for MemoryPushStore {
    async fn append_delivery_event(&self, event: DeliveryEvent) -> PushStorageResult<()> {
        self.inner.write().await.delivery_events.insert(
            (
                event.app_id.clone(),
                event.publish_id.clone(),
                event.occurred_at_ms,
                event.event_id.clone(),
            ),
            event,
        );
        Ok(())
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
            .inner
            .read()
            .await
            .delivery_events
            .iter()
            .filter(|((event_app_id, event_publish_id, _, _), _)| {
                event_app_id == app_id && event_publish_id == publish_id
            })
            .map(|((_, _, occurred_at_ms, event_id), event)| {
                (format!("{occurred_at_ms:020}:{event_id}"), event.clone())
            })
            .collect();
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
        let mut inner = self.inner.write().await;
        let before = inner.delivery_events.len();
        inner
            .delivery_events
            .retain(|(event_app_id, _, occurred_at_ms, _), _| {
                event_app_id != app_id || *occurred_at_ms >= before_ms
            });
        Ok((before - inner.delivery_events.len()) as u64)
    }
}

#[async_trait]
impl PushIdempotencyStore for MemoryPushStore {
    async fn put_idempotency_record_if_absent(
        &self,
        record: IdempotencyRecord,
    ) -> PushStorageResult<bool> {
        let key = (record.app_id.clone(), record.key.clone());
        let mut inner = self.inner.write().await;
        if inner.idempotency.contains_key(&key) {
            return Ok(false);
        }
        inner.idempotency.insert(key, record);
        Ok(true)
    }

    async fn get_idempotency_record(
        &self,
        app_id: &str,
        key: &str,
    ) -> PushStorageResult<Option<IdempotencyRecord>> {
        Ok(self
            .inner
            .read()
            .await
            .idempotency
            .get(&(app_id.to_owned(), key.to_owned()))
            .cloned())
    }
}

#[async_trait]
impl PushSchedulerLockStore for MemoryPushStore {
    async fn acquire_scheduler_lock(
        &self,
        lock: SchedulerLock,
        now_ms: u64,
    ) -> PushStorageResult<bool> {
        let key = (lock.app_id.clone(), lock.publish_id.clone());
        let mut inner = self.inner.write().await;
        if let Some(existing) = inner.scheduler_locks.get(&key)
            && existing.owner_id != lock.owner_id
            && existing.expires_at_ms > now_ms
        {
            return Ok(false);
        }
        inner.scheduler_locks.insert(key, lock);
        Ok(true)
    }

    async fn release_scheduler_lock(
        &self,
        app_id: &str,
        publish_id: &str,
        owner_id: &str,
    ) -> PushStorageResult<()> {
        let key = (app_id.to_owned(), publish_id.to_owned());
        let mut inner = self.inner.write().await;
        if inner
            .scheduler_locks
            .get(&key)
            .is_some_and(|lock| lock.owner_id == owner_id)
        {
            inner.scheduler_locks.remove(&key);
        }
        Ok(())
    }
}

#[async_trait]
impl PushOperatorEventStore for MemoryPushStore {
    async fn append_operator_invalidation(
        &self,
        event: OperatorInvalidationEvent,
    ) -> PushStorageResult<()> {
        self.inner.write().await.operator_invalidations.insert(
            (
                event.app_id.clone(),
                event.occurred_at_ms,
                event.event_id.clone(),
            ),
            event,
        );
        Ok(())
    }

    async fn list_operator_invalidations(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<OperatorInvalidationEvent>> {
        let start = cursor_position(cursor, app_id)?;
        let rows = self
            .inner
            .read()
            .await
            .operator_invalidations
            .iter()
            .filter(|((event_app_id, _, _), _)| event_app_id == app_id)
            .map(|((_, occurred_at_ms, event_id), event)| {
                (format!("{occurred_at_ms:020}:{event_id}"), event.clone())
            })
            .collect();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::OperatorInvalidation,
            rows,
            limit,
            start,
        ))
    }
}

fn cursor_position(cursor: Option<PushCursor>, app_id: &str) -> PushStorageResult<Option<String>> {
    match cursor {
        Some(cursor) => {
            if cursor.app_id != app_id {
                return Err(crate::domain::PushDomainError::CursorAppMismatch {
                    expected: app_id.to_owned(),
                    found: cursor.app_id,
                }
                .into());
            }
            Ok(Some(cursor.position))
        }
        None => Ok(None),
    }
}

fn page_from_rows<T: Clone>(
    app_id: &str,
    kind: PushCursorKind,
    rows: Vec<(String, T)>,
    limit: usize,
    start_after: Option<String>,
) -> Page<T> {
    let limit = limit.max(1);
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

fn day_bucket_for_ms(timestamp_ms: u64) -> String {
    (timestamp_ms / 86_400_000).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::PushStoreConformance;

    #[tokio::test]
    async fn memory_store_satisfies_device_registration_idempotency() {
        PushStoreConformance::assert_device_registration_idempotency(MemoryPushStore::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn memory_store_satisfies_cursor_pagination_and_channel_fanout() {
        PushStoreConformance::assert_cursor_pagination_and_channel_fanout(MemoryPushStore::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn memory_store_satisfies_stale_cleanup_scans() {
        PushStoreConformance::assert_stale_cleanup_scans(MemoryPushStore::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn memory_store_satisfies_secondary_storage_contracts() {
        PushStoreConformance::assert_credentials_templates_schedule_events_and_idempotency(
            MemoryPushStore::new(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn memory_store_coordinates_scheduler_locks_and_operator_invalidations() {
        let store = MemoryPushStore::new();
        let lock = SchedulerLock {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            owner_id: "node-a".to_owned(),
            expires_at_ms: 1_000,
        };

        assert!(
            store
                .acquire_scheduler_lock(lock.clone(), 100)
                .await
                .unwrap()
        );
        assert!(
            !store
                .acquire_scheduler_lock(
                    SchedulerLock {
                        owner_id: "node-b".to_owned(),
                        ..lock.clone()
                    },
                    200,
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .acquire_scheduler_lock(
                    SchedulerLock {
                        owner_id: "node-b".to_owned(),
                        expires_at_ms: 2_000,
                        ..lock
                    },
                    1_001,
                )
                .await
                .unwrap()
        );

        let event = OperatorInvalidationEvent {
            app_id: "app-1".to_owned(),
            event_id: "invalidate-1".to_owned(),
            subject: "credential:fcm".to_owned(),
            occurred_at_ms: 10,
        };
        store
            .append_operator_invalidation(event.clone())
            .await
            .unwrap();
        let page = store
            .list_operator_invalidations("app-1", 10, None)
            .await
            .unwrap();
        assert_eq!(page.items, vec![event]);
    }

    #[tokio::test]
    async fn memory_store_handles_concurrent_registration_update() {
        PushStoreConformance::assert_concurrent_registration_update(MemoryPushStore::new())
            .await
            .unwrap();
    }

    #[test]
    fn non_memory_backends_return_explicit_startup_errors() {
        PushStoreConformance::assert_backend_startup_errors_are_explicit();
    }
}
