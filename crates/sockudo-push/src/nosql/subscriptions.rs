#![allow(unused_imports)]
use super::constants::*;
use super::document::{DocumentBackend, DocumentPushStore};
use super::helpers::*;
use super::ordered::*;
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
impl<B> PushSubscriptionStore for DocumentPushStore<B>
where
    B: DocumentBackend,
{
    async fn upsert_subscription(
        &self,
        subscription: ChannelSubscription,
    ) -> PushStorageResult<()> {
        subscription.validate()?;
        self.write_ordered_reference(
            FAMILY_SUBSCRIPTION_ORDERED,
            &subscription.app_id,
            &format!("{}:{}", subscription.channel, subscription.device_id),
            &subscription.channel,
            &subscription.device_id,
        )
        .await?;
        self.put_json(
            FAMILY_SUBSCRIPTION,
            &subscription.app_id,
            &subscription.channel,
            &subscription.device_id,
            &subscription,
        )
        .await?;
        self.put_json(
            FAMILY_SUBSCRIPTION_BY_DEVICE,
            &subscription.app_id,
            &subscription.device_id,
            &subscription.channel,
            &subscription,
        )
        .await?;
        self.put_json(
            FAMILY_SUBSCRIPTION_CHANNEL,
            &subscription.app_id,
            &subscription.channel,
            DEFAULT_SK,
            &subscription.channel,
        )
        .await
    }

    async fn delete_subscription(
        &self,
        app_id: &str,
        channel: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        self.backend
            .delete(FAMILY_SUBSCRIPTION_BY_DEVICE, app_id, device_id, channel)
            .await?;
        let outcome = self
            .delete_json(FAMILY_SUBSCRIPTION, app_id, channel, device_id)
            .await?;
        self.delete_ordered_reference(
            FAMILY_SUBSCRIPTION_ORDERED,
            app_id,
            &format!("{channel}:{device_id}"),
            channel,
            device_id,
        )
        .await?;
        Ok(outcome)
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
            .scan_pk_page_json::<ChannelSubscription>(
                FAMILY_SUBSCRIPTION,
                app_id,
                channel,
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?
            .into_iter()
            .map(|(_, device_id, subscription)| (device_id, subscription))
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
        let rows = self
            .scan_pk_page_json::<ChannelSubscription>(
                FAMILY_SUBSCRIPTION_BY_DEVICE,
                app_id,
                device_id,
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?
            .into_iter()
            .map(|(_, channel, subscription)| (channel, subscription))
            .collect::<Vec<_>>();
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
        self.ordered_page(
            FAMILY_SUBSCRIPTION,
            FAMILY_SUBSCRIPTION_ORDERED,
            app_id,
            PushCursorKind::ChannelSubscription,
            limit,
            cursor,
        )
        .await
    }

    async fn list_subscription_channels(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<String>> {
        let start = cursor_position(cursor, app_id)?;
        let channels = self
            .scan_app_page_by_pk_json::<String>(
                FAMILY_SUBSCRIPTION_CHANNEL,
                app_id,
                start.as_deref(),
                limit_plus_one(limit),
            )
            .await?
            .into_iter()
            .map(|(channel, _, _)| channel)
            .collect::<Vec<_>>();
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
            .scan_pk_json::<ChannelSubscription>(FAMILY_SUBSCRIPTION_BY_DEVICE, app_id, device_id)
            .await?;
        let mut forward_keys = Vec::with_capacity(subscriptions.len());
        let mut reverse_keys = Vec::with_capacity(subscriptions.len());
        for (_, channel, _) in subscriptions {
            forward_keys.push((channel.clone(), device_id.to_owned()));
            reverse_keys.push((device_id.to_owned(), channel));
        }
        self.backend
            .delete_many(FAMILY_SUBSCRIPTION_BY_DEVICE, app_id, &reverse_keys)
            .await?;
        let deleted = self
            .backend
            .delete_many(FAMILY_SUBSCRIPTION, app_id, &forward_keys)
            .await?;
        for (channel, device_id) in &forward_keys {
            self.delete_ordered_reference(
                FAMILY_SUBSCRIPTION_ORDERED,
                app_id,
                &format!("{channel}:{device_id}"),
                channel,
                device_id,
            )
            .await?;
        }
        Ok(deleted)
    }

    async fn delete_subscriptions_by_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> PushStorageResult<u64> {
        let subscriptions = self
            .scan_pk_json::<ChannelSubscription>(FAMILY_SUBSCRIPTION, app_id, channel)
            .await?;
        let mut forward_keys = Vec::with_capacity(subscriptions.len());
        let mut reverse_keys = Vec::with_capacity(subscriptions.len());
        for (_, device_id, _) in subscriptions {
            forward_keys.push((channel.to_owned(), device_id.clone()));
            reverse_keys.push((device_id, channel.to_owned()));
        }
        self.backend
            .delete_many(FAMILY_SUBSCRIPTION_BY_DEVICE, app_id, &reverse_keys)
            .await?;
        let deleted = self
            .backend
            .delete_many(FAMILY_SUBSCRIPTION, app_id, &forward_keys)
            .await?;
        for (channel, device_id) in &forward_keys {
            self.delete_ordered_reference(
                FAMILY_SUBSCRIPTION_ORDERED,
                app_id,
                &format!("{channel}:{device_id}"),
                channel,
                device_id,
            )
            .await?;
        }
        self.backend
            .delete(FAMILY_SUBSCRIPTION_CHANNEL, app_id, channel, DEFAULT_SK)
            .await?;
        Ok(deleted)
    }
}
