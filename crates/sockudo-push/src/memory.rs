mod lifecycle;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::cleanup::{
    PushCleanupCounters, PushCleanupReport, PushCleanupRequest, terminal_publish_state,
};
use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    ProviderCredential, PublishLogEvent, PublishStatus, PushCursor, PushCursorKind, ShardJob,
};
use crate::storage::{
    DeviceRegistrationChange, DeviceRegistrationOutcome, IdempotencyRecord,
    OperatorInvalidationEvent, Page, PublishStatusCasOutcome, PushCleanupStore,
    PushCredentialStore, PushDeliveryEventStore, PushDeviceStore, PushFanoutShardStore,
    PushIdempotencyStore, PushOperatorEventStore, PushPublishLogStore, PushPublishStatusStore,
    PushScheduleStore, PushSchedulerLockStore, PushStorageError, PushStorageResult,
    PushSubscriptionStore, PushTemplateStore, ScheduledPushJob, SchedulerLock,
    VersionedPublishStatus,
};

const MEMORY_DELIVERY_EVENT_CAP: usize = 100_000;

#[derive(Clone, Default)]
pub struct MemoryPushStore {
    inner: Arc<RwLock<MemoryPushState>>,
}

#[derive(Default)]
struct MemoryPushState {
    devices_by_id: BTreeMap<(String, String), DeviceDetails>,
    devices_by_client: BTreeMap<(String, String, String), ()>,
    devices_by_day: BTreeSet<(String, String, u64, String)>,
    subscriptions: BTreeMap<(String, String, String), ChannelSubscription>,
    subscription_positions: BTreeSet<(String, String, String, String)>,
    subscriptions_by_device: BTreeSet<(String, String, String)>,
    subscription_channels: BTreeMap<(String, String), usize>,
    credentials: BTreeMap<(String, String), ProviderCredential>,
    templates: BTreeMap<(String, String), NotificationTemplate>,
    publish_status: BTreeMap<(String, String), VersionedPublishStatus>,
    publish_log: BTreeMap<(String, u64, String), PublishLogEvent>,
    publish_log_by_publish: BTreeSet<(String, String, u64, String)>,
    lifecycle_scans: BTreeMap<(String, String), crate::lifecycle::LifecycleScan>,
    lifecycle_tombstones: BTreeMap<(String, String), crate::lifecycle::PublishTombstone>,
    lifecycle_cursor: Option<(String, String)>,
    lifecycle_retired_cursor: Option<(String, String)>,
    fanout_shards: BTreeMap<(String, String, String), ShardJob>,
    scheduled_by_id: BTreeMap<(String, String), ScheduledPushJob>,
    delivery_events: BTreeMap<(String, String, u64, String), DeliveryEvent>,
    idempotency: BTreeMap<(String, String), IdempotencyRecord>,
    scheduler_locks: BTreeMap<(String, String), SchedulerLock>,
    operator_invalidations: BTreeMap<(String, u64, String), OperatorInvalidationEvent>,
    status_cleanup_cursor: Option<(String, String)>,
    event_cleanup_cursor: Option<(String, String, u64, String)>,
    idempotency_cleanup_cursor: Option<(String, String)>,
    lock_cleanup_cursor: Option<(String, String)>,
    operator_cleanup_cursor: Option<(String, u64, String)>,
}

impl MemoryPushState {
    fn remove_device(&mut self, app_id: &str, device_id: &str) -> Option<DeviceDetails> {
        let device = self
            .devices_by_id
            .remove(&(app_id.to_owned(), device_id.to_owned()))?;
        if let Some(client_id) = &device.client_id {
            self.devices_by_client.remove(&(
                device.app_id.clone(),
                client_id.clone(),
                device.id.clone(),
            ));
        }
        self.devices_by_day.remove(&(
            device.app_id.clone(),
            day_bucket_for_ms(device.last_active_at_ms),
            device.last_active_at_ms,
            device.id.clone(),
        ));
        Some(device)
    }

    fn remove_subscription(&mut self, key: &(String, String, String)) -> bool {
        if self.subscriptions.remove(key).is_none() {
            return false;
        }
        self.subscription_positions.remove(&(
            key.0.clone(),
            format!("{}:{}", key.1, key.2),
            key.1.clone(),
            key.2.clone(),
        ));
        self.subscriptions_by_device
            .remove(&(key.0.clone(), key.2.clone(), key.1.clone()));
        let channel_key = (key.0.clone(), key.1.clone());
        if let Some(count) = self.subscription_channels.get_mut(&channel_key) {
            *count -= 1;
            if *count == 0 {
                self.subscription_channels.remove(&channel_key);
            }
        }
        true
    }

    fn remove_device_subscriptions(&mut self, app_id: &str, device_id: &str) -> u64 {
        let channels: Vec<_> = self
            .subscriptions_by_device
            .range((
                Excluded((app_id.to_owned(), device_id.to_owned(), String::new())),
                Unbounded,
            ))
            .take_while(|(app, device, _)| app == app_id && device == device_id)
            .map(|(_, _, channel)| channel.clone())
            .collect();
        for channel in &channels {
            self.remove_subscription(&(app_id.to_owned(), channel.clone(), device_id.to_owned()));
        }
        channels.len() as u64
    }
}

fn time_cursor_bound(start: Option<&str>) -> PushStorageResult<(u64, String)> {
    match start {
        None => Ok((0, String::new())),
        Some(position) => {
            let (timestamp, id) = position
                .split_once(':')
                .ok_or(crate::domain::PushDomainError::CursorDecode)?;
            let timestamp = timestamp
                .parse()
                .map_err(|_| crate::domain::PushDomainError::CursorDecode)?;
            Ok((timestamp, id.to_owned()))
        }
    }
}

impl MemoryPushStore {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(any(test, feature = "testing"))]
    pub async fn set_publish_status_updated_at_for_test(
        &self,
        app_id: &str,
        publish_id: &str,
        updated_at_ms: u64,
    ) {
        if let Some(status) = self
            .inner
            .write()
            .await
            .publish_status
            .get_mut(&(app_id.to_owned(), publish_id.to_owned()))
        {
            status.updated_at_ms = updated_at_ms;
        }
    }
}

#[async_trait]
impl PushDeviceStore for MemoryPushStore {
    async fn apply_device_feedback_once(
        &self,
        request: crate::storage::DeviceFeedbackRequest,
    ) -> PushStorageResult<crate::storage::DeviceFeedbackApplied> {
        use crate::storage::{
            DeviceFeedbackApplied, DeviceFeedbackEffect, apply_device_feedback_effect,
        };
        let mut inner = self.inner.write().await;
        let key = (
            request.app_id.clone(),
            format!("device-result:{}", request.receipt_id),
        );
        if inner
            .idempotency
            .get(&key)
            .is_some_and(|r| r.expires_at_ms > crate::pipeline::now_ms())
        {
            return Ok(DeviceFeedbackApplied::default());
        }
        let mut result = DeviceFeedbackApplied::default();
        if let Some(mut device) = inner.remove_device(&request.app_id, &request.device_id) {
            result.applied = true;
            result.previous = Some(device.push.state);
            if matches!(request.effect, DeviceFeedbackEffect::Delete) {
                inner.remove_device_subscriptions(&request.app_id, &request.device_id);
            } else {
                apply_device_feedback_effect(&mut device, &request);
                result.next = Some(device.push.state);
                if let Some(client) = &device.client_id {
                    inner.devices_by_client.insert(
                        (device.app_id.clone(), client.clone(), device.id.clone()),
                        (),
                    );
                }
                inner.devices_by_day.insert((
                    device.app_id.clone(),
                    day_bucket_for_ms(device.last_active_at_ms),
                    device.last_active_at_ms,
                    device.id.clone(),
                ));
                inner
                    .devices_by_id
                    .insert((device.app_id.clone(), device.id.clone()), device);
            }
        }
        inner.idempotency.insert(
            key.clone(),
            IdempotencyRecord {
                app_id: request.app_id,
                key: key.1,
                publish_id: request.publish_id,
                expires_at_ms: request.expires_at_ms,
            },
        );
        Ok(result)
    }

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
        inner.remove_device(&device.app_id, &device.id);
        inner.devices_by_day.insert((
            device.app_id.clone(),
            day_bucket_for_ms(device.last_active_at_ms),
            device.last_active_at_ms,
            device.id.clone(),
        ));
        if let Some(client_id) = &device.client_id {
            inner.devices_by_client.insert(
                (device.app_id.clone(), client_id.clone(), device.id.clone()),
                (),
            );
        }
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
        let removed = inner.remove_device(app_id, device_id).is_some();
        inner.remove_device_subscriptions(app_id, device_id);
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
            .range((
                Excluded((app_id.to_owned(), start.clone().unwrap_or_default())),
                Unbounded,
            ))
            .take_while(|((device_app_id, _), _)| device_app_id == app_id)
            .map(|((_, device_id), device)| (device_id.clone(), device.clone()))
            .take(limit.max(1).saturating_add(1))
            .collect();
        Ok(page_from_rows(
            app_id,
            PushCursorKind::Device,
            rows,
            limit,
            start,
        ))
    }

    async fn list_devices_by_client(
        &self,
        app_id: &str,
        client_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        use std::ops::Bound::{Excluded, Unbounded};
        let start = cursor_position(cursor, app_id)?;
        let inner = self.inner.read().await;
        let rows = inner
            .devices_by_client
            .range((
                Excluded((
                    app_id.to_owned(),
                    client_id.to_owned(),
                    start.clone().unwrap_or_default(),
                )),
                Unbounded,
            ))
            .take_while(|((app, client, _), _)| app == app_id && client == client_id)
            .filter_map(|((_, _, id), _)| {
                inner
                    .devices_by_id
                    .get(&(app_id.to_owned(), id.clone()))
                    .map(|device| (id.clone(), device.clone()))
            })
            .take(limit.max(1).saturating_add(1))
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
        let device_ids: Vec<_> = inner
            .devices_by_client
            .range((
                Excluded((app_id.to_owned(), client_id.to_owned(), String::new())),
                Unbounded,
            ))
            .take_while(|((app, client, _), _)| app == app_id && client == client_id)
            .map(|((_, _, id), _)| id.clone())
            .collect();
        for device_id in &device_ids {
            inner.remove_device(app_id, device_id);
            inner.remove_device_subscriptions(app_id, device_id);
        }
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
        let (timestamp, id) = time_cursor_bound(start.as_deref())?;
        let inner = self.inner.read().await;
        let rows = inner
            .devices_by_day
            .range((
                Excluded((app_id.to_owned(), day_bucket.to_owned(), timestamp, id)),
                Unbounded,
            ))
            .take_while(|(app, day, _, _)| app == app_id && day == day_bucket)
            .take(limit.max(1).saturating_add(1))
            .filter_map(|(_, _, timestamp, id)| {
                inner
                    .devices_by_id
                    .get(&(app_id.to_owned(), id.clone()))
                    .map(|device| (format!("{timestamp:020}:{id}"), device.clone()))
            })
            .collect();
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
        let mut inner = self.inner.write().await;
        let key = (
            subscription.app_id.clone(),
            subscription.channel.clone(),
            subscription.device_id.clone(),
        );
        if !inner.subscriptions.contains_key(&key) {
            *inner
                .subscription_channels
                .entry((subscription.app_id.clone(), subscription.channel.clone()))
                .or_default() += 1;
            inner.subscriptions_by_device.insert((
                subscription.app_id.clone(),
                subscription.device_id.clone(),
                subscription.channel.clone(),
            ));
        }
        inner.subscription_positions.insert((
            subscription.app_id.clone(),
            format!("{}:{}", subscription.channel, subscription.device_id),
            subscription.channel.clone(),
            subscription.device_id.clone(),
        ));
        inner.subscriptions.insert(
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
        let mut inner = self.inner.write().await;
        let removed = inner.remove_subscription(&(
            app_id.to_owned(),
            channel.to_owned(),
            device_id.to_owned(),
        ));
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
            .range((
                Excluded((
                    app_id.to_owned(),
                    channel.to_owned(),
                    start.clone().unwrap_or_default(),
                )),
                Unbounded,
            ))
            .take_while(|((sub_app_id, sub_channel, _), _)| {
                sub_app_id == app_id && sub_channel == channel
            })
            .map(|((_, _, device_id), subscription)| (device_id.clone(), subscription.clone()))
            .take(limit.max(1).saturating_add(1))
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
        let inner = self.inner.read().await;
        let rows = inner
            .subscriptions_by_device
            .range((
                Excluded((
                    app_id.to_owned(),
                    device_id.to_owned(),
                    start.clone().unwrap_or_default(),
                )),
                Unbounded,
            ))
            .take_while(|(app, device, _)| app == app_id && device == device_id)
            .take(limit.max(1).saturating_add(1))
            .filter_map(|(_, _, channel)| {
                inner
                    .subscriptions
                    .get(&(app_id.to_owned(), channel.clone(), device_id.to_owned()))
                    .map(|subscription| (channel.clone(), subscription.clone()))
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

    async fn list_subscriptions(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        let start = cursor_position(cursor, app_id)?;
        let inner = self.inner.read().await;
        let rows = inner
            .subscription_positions
            .range((
                Excluded((
                    app_id.to_owned(),
                    start.clone().unwrap_or_default(),
                    String::new(),
                    String::new(),
                )),
                Unbounded,
            ))
            .take_while(|(app, _, _, _)| app == app_id)
            .filter(|(_, position, _, _)| start.as_ref().is_none_or(|start| position > start))
            .take(limit.max(1).saturating_add(1))
            .filter_map(|(_, position, channel, id)| {
                inner
                    .subscriptions
                    .get(&(app_id.to_owned(), channel.clone(), id.clone()))
                    .map(|subscription| (position.clone(), subscription.clone()))
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
        let inner = self.inner.read().await;
        let rows = inner
            .subscription_channels
            .range((
                Excluded((app_id.to_owned(), start.clone().unwrap_or_default())),
                Unbounded,
            ))
            .take_while(|((app, _), _)| app == app_id)
            .take(limit.max(1).saturating_add(1))
            .map(|((_, channel), _)| (channel.clone(), channel.clone()))
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
        Ok(inner.remove_device_subscriptions(app_id, device_id))
    }

    async fn delete_subscriptions_by_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> PushStorageResult<u64> {
        let mut inner = self.inner.write().await;
        let keys: Vec<_> = inner
            .subscriptions
            .range((
                Excluded((app_id.to_owned(), channel.to_owned(), String::new())),
                Unbounded,
            ))
            .take_while(|((app, sub_channel, _), _)| app == app_id && sub_channel == channel)
            .map(|(key, _)| key.clone())
            .collect();
        for key in &keys {
            inner.remove_subscription(key);
        }
        Ok(keys.len() as u64)
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
            .range((
                Excluded((app_id.to_owned(), start.clone().unwrap_or_default())),
                Unbounded,
            ))
            .take_while(|((credential_app_id, _), _)| credential_app_id == app_id)
            .map(|((_, credential_id), credential)| (credential_id.clone(), credential.clone()))
            .take(limit.max(1).saturating_add(1))
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
            .range((
                Excluded((app_id.to_owned(), start.clone().unwrap_or_default())),
                Unbounded,
            ))
            .take_while(|((template_app_id, _), _)| template_app_id == app_id)
            .map(|((_, template_id), template)| (template_id.clone(), template.clone()))
            .take(limit.max(1).saturating_add(1))
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
    async fn is_publish_retired(&self, app_id: &str, publish_id: &str) -> PushStorageResult<bool> {
        Ok(self
            .inner
            .read()
            .await
            .lifecycle_tombstones
            .contains_key(&(app_id.to_owned(), publish_id.to_owned())))
    }

    async fn create_publish_status_if_absent(
        &self,
        status: PublishStatus,
    ) -> PushStorageResult<PublishStatusCasOutcome> {
        let key = (status.app_id.clone(), status.publish_id.clone());
        let mut inner = self.inner.write().await;
        if inner.lifecycle_tombstones.contains_key(&key) {
            return Ok(PublishStatusCasOutcome::Conflict);
        }
        if inner.publish_status.contains_key(&key) {
            return Ok(PublishStatusCasOutcome::Conflict);
        }
        inner.publish_status.insert(
            key,
            VersionedPublishStatus {
                status,
                revision: 1,
                updated_at_ms: crate::pipeline::now_ms(),
                pending_feedback: Default::default(),
                pending_children: Default::default(),
            },
        );
        Ok(PublishStatusCasOutcome::Inserted { revision: 1 })
    }

    async fn get_versioned_publish_status(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushStorageResult<Option<VersionedPublishStatus>> {
        Ok(self
            .inner
            .read()
            .await
            .publish_status
            .get(&(app_id.to_owned(), publish_id.to_owned()))
            .cloned())
    }

    async fn compare_and_swap_publish_status(
        &self,
        expected: &VersionedPublishStatus,
        next: PublishStatus,
    ) -> PushStorageResult<PublishStatusCasOutcome> {
        self.compare_and_swap_feedback_status(expected, next, expected.pending_feedback.clone())
            .await
    }

    async fn compare_and_swap_feedback_status(
        &self,
        expected: &VersionedPublishStatus,
        next: PublishStatus,
        pending: std::collections::BTreeMap<String, crate::storage::FeedbackReceipt>,
    ) -> PushStorageResult<PublishStatusCasOutcome> {
        if pending.len() > crate::storage::MAX_PENDING_FEEDBACK {
            return Err(PushStorageError::Backend(
                "feedback receipt capacity reached".into(),
            ));
        }
        if expected.status.app_id != next.app_id
            || expected.status.publish_id != next.publish_id
            || expected.revision == 0
        {
            return Err(PushStorageError::Backend(
                "invalid publish status compare-and-swap identity or revision".to_owned(),
            ));
        }
        let next_revision = expected.revision.checked_add(1).ok_or_else(|| {
            PushStorageError::Backend("publish status revision overflow".to_owned())
        })?;
        let key = (next.app_id.clone(), next.publish_id.clone());
        let mut inner = self.inner.write().await;
        let Some(current) = inner.publish_status.get_mut(&key) else {
            return Ok(PublishStatusCasOutcome::Missing);
        };
        if current.revision != expected.revision || current.updated_at_ms != expected.updated_at_ms
        {
            return Ok(PublishStatusCasOutcome::Conflict);
        }
        let updated_at_ms = crate::pipeline::now_ms().max(current.updated_at_ms.saturating_add(1));
        *current = VersionedPublishStatus {
            status: next,
            revision: next_revision,
            updated_at_ms,
            pending_feedback: pending,
            pending_children: current.pending_children.clone(),
        };
        Ok(PublishStatusCasOutcome::Updated {
            revision: next_revision,
        })
    }
}

#[async_trait]
impl PushPublishLogStore for MemoryPushStore {
    async fn append_publish_log_event(&self, event: PublishLogEvent) -> PushStorageResult<()> {
        let mut inner = self.inner.write().await;
        if inner
            .lifecycle_tombstones
            .contains_key(&(event.app_id.clone(), event.publish_id.clone()))
        {
            return Err(PushStorageError::Backend(
                "publish has been retired".to_owned(),
            ));
        }
        if let Some(status) = inner
            .publish_status
            .get_mut(&(event.app_id.clone(), event.publish_id.clone()))
        {
            status.revision = status.revision.checked_add(1).ok_or_else(|| {
                PushStorageError::Backend("publish status revision exhausted".to_owned())
            })?;
            status.updated_at_ms =
                crate::pipeline::now_ms().max(status.updated_at_ms.saturating_add(1));
        } else {
            return Err(PushStorageError::Backend(
                "publish status is missing; child write must retry after status repair".to_owned(),
            ));
        }
        inner.publish_log_by_publish.insert((
            event.app_id.clone(),
            event.publish_id.clone(),
            event.occurred_at_ms,
            event.event_id.clone(),
        ));
        inner.publish_log.insert(
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
        let (start_time, start_id) = time_cursor_bound(start.as_deref())?;
        let rows = self
            .inner
            .read()
            .await
            .publish_log
            .range((
                Excluded((app_id.to_owned(), start_time, start_id)),
                Unbounded,
            ))
            .take_while(|((event_app_id, _, _), _)| event_app_id == app_id)
            .map(|((_, occurred_at_ms, event_id), event)| {
                (format!("{occurred_at_ms:020}:{event_id}"), event.clone())
            })
            .take(limit.max(1).saturating_add(1))
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
        let mut inner = self.inner.write().await;
        if inner
            .lifecycle_tombstones
            .contains_key(&(shard.app_id.clone(), shard.publish_id.clone()))
        {
            return Err(PushStorageError::Backend(
                "publish has been retired".to_owned(),
            ));
        }
        if let Some(status) = inner
            .publish_status
            .get_mut(&(shard.app_id.clone(), shard.publish_id.clone()))
        {
            status.revision = status.revision.checked_add(1).ok_or_else(|| {
                PushStorageError::Backend("publish status revision exhausted".to_owned())
            })?;
            status.updated_at_ms =
                crate::pipeline::now_ms().max(status.updated_at_ms.saturating_add(1));
        } else {
            return Err(PushStorageError::Backend(
                "publish status is missing; child write must retry after status repair".to_owned(),
            ));
        }
        inner.fanout_shards.insert(
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

    async fn list_scheduled_apps(&self) -> PushStorageResult<Vec<String>> {
        let mut apps = self
            .inner
            .read()
            .await
            .scheduled_by_id
            .keys()
            .map(|(app_id, _)| app_id.clone())
            .collect::<Vec<_>>();
        apps.sort();
        apps.dedup();
        Ok(apps)
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
        prune_expired_records(&mut inner, crate::pipeline::now_ms());
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
        let mut inner = self.inner.write().await;
        prune_expired_records(&mut inner, crate::pipeline::now_ms());
        Ok(inner
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
        prune_expired_records(&mut inner, now_ms);
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

fn prune_expired_records(inner: &mut MemoryPushState, now_ms: u64) {
    inner.idempotency.retain(|_, record| {
        record.expires_at_ms < 1_000_000_000_000 || record.expires_at_ms > now_ms
    });
    inner
        .scheduler_locks
        .retain(|_, lock| lock.expires_at_ms > now_ms);
    if inner.delivery_events.len() > MEMORY_DELIVERY_EVENT_CAP {
        let remove_count = inner.delivery_events.len() - MEMORY_DELIVERY_EVENT_CAP;
        let keys = inner
            .delivery_events
            .keys()
            .take(remove_count)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            inner.delivery_events.remove(&key);
        }
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
        let (start_time, start_id) = time_cursor_bound(start.as_deref())?;
        let rows = self
            .inner
            .read()
            .await
            .operator_invalidations
            .range((
                Excluded((app_id.to_owned(), start_time, start_id)),
                Unbounded,
            ))
            .take_while(|((event_app_id, _, _), _)| event_app_id == app_id)
            .map(|((_, occurred_at_ms, event_id), event)| {
                (format!("{occurred_at_ms:020}:{event_id}"), event.clone())
            })
            .take(limit.max(1).saturating_add(1))
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

#[async_trait]
impl PushCleanupStore for MemoryPushStore {
    async fn cleanup_expired_push_data(
        &self,
        request: PushCleanupRequest,
    ) -> PushStorageResult<PushCleanupReport> {
        let mut inner = self.inner.write().await;
        let mut report = PushCleanupReport::default();
        let mut remaining = request.policy.max_deleted_per_tick;
        lifecycle::cleanup(&mut inner, &request, &mut report, &mut remaining)?;

        if remaining > 0
            && let Some(cutoff_ms) = request.publish_status_cutoff_ms()
        {
            let limit = request.limit_for(remaining);
            let (mut counters, mut keys, next) = cleanup_status_keys(&inner, cutoff_ms, limit);
            inner.status_cleanup_cursor = next;
            keys.retain(|key| {
                !inner.fanout_shards.contains_key(&(
                    key.0.clone(),
                    key.1.clone(),
                    crate::lifecycle::PLANNER_RECEIPT_ID.to_owned(),
                ))
            });
            counters.deleted = keys.len() as u64;
            for key in keys {
                inner.publish_status.remove(&key);
            }
            remaining = remaining.saturating_sub(counters.deleted as usize);
            report
                .publish_statuses
                .record(counters.scanned, counters.deleted);
        }

        if remaining > 0
            && let Some(cutoff_ms) = request.delivery_event_cutoff_ms()
        {
            let limit = request.limit_for(remaining);
            let (counters, keys, next) = cleanup_keys(
                inner.delivery_events.range((
                    inner
                        .event_cleanup_cursor
                        .clone()
                        .map(Excluded)
                        .unwrap_or(Unbounded),
                    Unbounded,
                )),
                limit,
                |item| {
                    let ((_, _, occurred_at_ms, _), _) = *item;
                    *occurred_at_ms < cutoff_ms
                },
                |item| item.0.clone(),
            );
            inner.event_cleanup_cursor = next;
            for key in keys {
                inner.delivery_events.remove(&key);
            }
            remaining = remaining.saturating_sub(counters.deleted as usize);
            report.delivery_events = counters;
        }

        if remaining > 0 {
            let limit = request.limit_for(remaining);
            let (counters, keys, next) = cleanup_keys(
                inner.idempotency.range((
                    inner
                        .idempotency_cleanup_cursor
                        .clone()
                        .map(Excluded)
                        .unwrap_or(Unbounded),
                    Unbounded,
                )),
                limit,
                |item| idempotency_record_expired(item.1, request.now_ms),
                |item| item.0.clone(),
            );
            inner.idempotency_cleanup_cursor = next;
            for key in keys {
                inner.idempotency.remove(&key);
            }
            remaining = remaining.saturating_sub(counters.deleted as usize);
            report.idempotency_records = counters;
        }

        if remaining > 0 {
            let limit = request.limit_for(remaining);
            let (counters, keys, next) = cleanup_keys(
                inner.scheduler_locks.range((
                    inner
                        .lock_cleanup_cursor
                        .clone()
                        .map(Excluded)
                        .unwrap_or(Unbounded),
                    Unbounded,
                )),
                limit,
                |item| item.1.expires_at_ms <= request.now_ms,
                |item| item.0.clone(),
            );
            inner.lock_cleanup_cursor = next;
            for key in keys {
                inner.scheduler_locks.remove(&key);
            }
            remaining = remaining.saturating_sub(counters.deleted as usize);
            report.scheduler_locks = counters;
        }

        if remaining > 0
            && let Some(cutoff_ms) = request.operator_event_cutoff_ms()
        {
            let limit = request.limit_for(remaining);
            let (counters, keys, next) = cleanup_keys(
                inner.operator_invalidations.range((
                    inner
                        .operator_cleanup_cursor
                        .clone()
                        .map(Excluded)
                        .unwrap_or(Unbounded),
                    Unbounded,
                )),
                limit,
                |item| {
                    let ((_, occurred_at_ms, _), _) = *item;
                    *occurred_at_ms < cutoff_ms
                },
                |item| item.0.clone(),
            );
            inner.operator_cleanup_cursor = next;
            for key in keys {
                inner.operator_invalidations.remove(&key);
            }
            report.operator_invalidations = counters;
        }

        Ok(report)
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
    // Cursor pagination compares `position` strings (`position > start_after`), so the page must be
    // emitted in ascending `position` order. Callers build `rows` in backing-map order, which does
    // not always match position-string order — e.g. subscription rows are keyed by the
    // `(channel, device_id)` tuple, but the position string `"{channel}:{device_id}"` orders
    // differently whenever one channel name is a prefix of another (`:` outranks digits). Sort here
    // so the helper owns the invariant its cursor contract depends on.
    let mut filtered = rows
        .into_iter()
        .filter(|(position, _)| start_after.as_ref().is_none_or(|start| position > start))
        .collect::<Vec<_>>();
    filtered.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut items = Vec::with_capacity(limit.min(filtered.len()));
    let mut last_position = None;
    let has_more = filtered.len() > limit;
    for (position, item) in filtered.into_iter().take(limit) {
        last_position = Some(position);
        items.push(item);
    }

    let next_cursor = if has_more {
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

type CleanupBatch<K> = (PushCleanupCounters, Vec<K>, Option<K>);

fn cleanup_status_keys(
    inner: &MemoryPushState,
    cutoff_ms: u64,
    limit: usize,
) -> CleanupBatch<(String, String)> {
    cleanup_keys(
        inner.publish_status.range((
            inner
                .status_cleanup_cursor
                .clone()
                .map(Excluded)
                .unwrap_or(Unbounded),
            Unbounded,
        )),
        limit,
        |item| {
            let (_, status) = *item;
            terminal_publish_state(status.status.state)
                && status.updated_at_ms < cutoff_ms
                && status.pending_feedback.is_empty()
                && status.pending_children.is_empty()
        },
        |item| item.0.clone(),
    )
}

fn cleanup_keys<I, T, K, F, M>(
    iter: I,
    limit: usize,
    should_delete: F,
    map_key: M,
) -> CleanupBatch<K>
where
    I: Iterator<Item = T>,
    T: Copy,
    K: Clone,
    F: Fn(&T) -> bool,
    M: Fn(T) -> K,
{
    let mut counters = PushCleanupCounters::default();
    let mut keys = Vec::new();
    let mut next = None;
    for item in iter.take(limit.max(1)) {
        let key = map_key(item);
        counters.scanned = counters.scanned.saturating_add(1);
        if should_delete(&item) {
            counters.deleted = counters.deleted.saturating_add(1);
            keys.push(key.clone());
        }
        next = Some(key);
    }
    (counters, keys, next)
}

fn idempotency_record_expired(record: &IdempotencyRecord, now_ms: u64) -> bool {
    record.expires_at_ms >= 1_000_000_000_000 && record.expires_at_ms <= now_ms
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

    #[tokio::test]
    async fn memory_status_cas_allows_exactly_one_writer_per_revision() {
        let store = MemoryPushStore::new();
        let status = PublishStatus {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            state: crate::domain::PublishLifecycleState::Queued,
            counters: crate::domain::PublishCounters::default(),
            fanout_regime: None,
            retry_after_ms: None,
            error_reason: None,
        };
        assert_eq!(
            store
                .create_publish_status_if_absent(status.clone())
                .await
                .unwrap(),
            PublishStatusCasOutcome::Inserted { revision: 1 }
        );
        let expected = store
            .get_versioned_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        let mut planning = status.clone();
        planning.state = crate::domain::PublishLifecycleState::Planning;
        let mut failed = status;
        failed.state = crate::domain::PublishLifecycleState::Failed;

        let (left, right) = tokio::join!(
            store.compare_and_swap_publish_status(&expected, planning),
            store.compare_and_swap_publish_status(&expected, failed),
        );
        let outcomes = [left.unwrap(), right.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, PublishStatusCasOutcome::Updated { .. }))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, PublishStatusCasOutcome::Conflict))
                .count(),
            1
        );
        assert_eq!(
            store
                .get_versioned_publish_status("app-1", "publish-1")
                .await
                .unwrap()
                .unwrap()
                .revision,
            2
        );
    }

    #[tokio::test]
    async fn memory_status_cas_rejects_stale_snapshot_after_recreation() {
        let store = MemoryPushStore::new();
        let status = PublishStatus {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            state: crate::domain::PublishLifecycleState::Queued,
            counters: crate::domain::PublishCounters::default(),
            fanout_regime: None,
            retry_after_ms: None,
            error_reason: None,
        };
        store
            .create_publish_status_if_absent(status.clone())
            .await
            .unwrap();
        store
            .set_publish_status_updated_at_for_test("app-1", "publish-1", 1)
            .await;
        let stale = store
            .get_versioned_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        store
            .inner
            .write()
            .await
            .publish_status
            .remove(&("app-1".to_owned(), "publish-1".to_owned()));
        store
            .create_publish_status_if_absent(status.clone())
            .await
            .unwrap();

        assert_eq!(
            store
                .compare_and_swap_publish_status(&stale, status)
                .await
                .unwrap(),
            PublishStatusCasOutcome::Conflict
        );
    }

    #[test]
    fn non_memory_backends_return_explicit_startup_errors() {
        PushStoreConformance::assert_backend_startup_errors_are_explicit();
    }
}
