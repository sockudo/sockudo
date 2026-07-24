//! Simulator-facing boundary for real Sockudo subsystem APIs.
//!
//! The deterministic simulator keeps its shadow model and fault scheduler, but
//! durable state changes should cross this module before the shadow advances.

use std::collections::BTreeSet;
use std::sync::Arc;

use sockudo_core::history::{MemoryHistoryStore, MemoryHistoryStoreConfig};
use sockudo_core::presence_history::{
    MemoryPresenceHistoryStore, MemoryPresenceHistoryStoreConfig,
};
use sockudo_core::version_store::MemoryVersionStore;
use sockudo_push::{
    ChannelSubscription, DeviceDetails, DevicePushDetails, DevicePushState, FanoutConfig,
    FormFactor, MemoryPushQueue, MemoryPushStore, Platform, PublishIntent, PublishStatus,
    PublishTarget, PushAcceptOutcome, PushAcceptRequest, PushDeviceStore, PushIdempotencyStore,
    PushPayload, PushPipeline, PushProviderKind, PushPublishLogStore, PushPublishStatusStore,
    PushRecipient, PushScheduleStore, PushSubscriptionStore, ScheduledPushJob, SecretString,
    publish_idempotency_key,
};
use sonic_rs::json;

use crate::config::SimulatorConfig;
use crate::error::SimulatorResult;

pub(crate) const APP_ID: &str = "sim-app";

pub(crate) struct RealSubsystemHarness {
    pub(crate) history: MemoryHistoryStore,
    pub(crate) version: MemoryVersionStore,
    pub(crate) presence: MemoryPresenceHistoryStore,
}

impl RealSubsystemHarness {
    pub(crate) fn new(config: &SimulatorConfig) -> Self {
        Self {
            history: MemoryHistoryStore::new(MemoryHistoryStoreConfig {
                retention_window: config.retention_window(),
                max_messages_per_channel: config.history_retention_messages,
                max_bytes_per_channel: None,
            }),
            version: MemoryVersionStore::new(),
            presence: MemoryPresenceHistoryStore::new(MemoryPresenceHistoryStoreConfig {
                retention_window: config.retention_window(),
                max_events_per_channel: config.presence_retention_events,
                max_bytes_per_channel: None,
                metrics: None,
            }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RealPushHarness {
    store: Arc<MemoryPushStore>,
    pipeline: Arc<PushPipeline>,
}

impl RealPushHarness {
    pub(crate) fn new() -> Self {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default())
            .with_max_publish_log_lag(u64::MAX);
        Self {
            store,
            pipeline: Arc::new(pipeline),
        }
    }

    pub(crate) async fn upsert_device(
        &self,
        device_id: &str,
        client_id: &str,
        provider: PushProviderKind,
        occurred_at_ms: u64,
    ) -> SimulatorResult<DeviceDetails> {
        let device = self.device_details(device_id, client_id, provider, occurred_at_ms)?;
        self.store.upsert_device(device.clone()).await?;
        Ok(device)
    }

    pub(crate) async fn delete_device(&self, device_id: &str) -> SimulatorResult<()> {
        self.store.delete_device(APP_ID, device_id).await?;
        Ok(())
    }

    pub(crate) async fn upsert_subscription(
        &self,
        channel: &str,
        device_id: &str,
    ) -> SimulatorResult<()> {
        let Some(device) = self.store.get_device(APP_ID, device_id).await? else {
            return Ok(());
        };
        self.store
            .upsert_subscription(ChannelSubscription::from_device(channel, &device))
            .await?;
        Ok(())
    }

    pub(crate) async fn delete_subscription(
        &self,
        channel: &str,
        device_id: &str,
    ) -> SimulatorResult<()> {
        self.store
            .delete_subscription(APP_ID, channel, device_id)
            .await?;
        Ok(())
    }

    pub(crate) async fn put_scheduled_publish(
        &self,
        publish_id: &str,
        channel: &str,
        due_tick: u64,
    ) -> SimulatorResult<()> {
        self.store
            .put_scheduled_job(ScheduledPushJob {
                app_id: APP_ID.to_string(),
                publish_id: publish_id.to_string(),
                due_at_ms: due_tick,
                due_minute_ms: due_tick,
                payload_json: json!({
                    "channel": channel,
                    "simulator": true,
                }),
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn delete_scheduled_publish(&self, publish_id: &str) -> SimulatorResult<()> {
        self.store.delete_scheduled_job(APP_ID, publish_id).await?;
        Ok(())
    }

    pub(crate) async fn accept_channel_publish(
        &self,
        publish_id: &str,
        channel: &str,
        expected_recipients: u64,
        occurred_at_ms: u64,
    ) -> SimulatorResult<PushAcceptOutcome> {
        let request = PushAcceptRequest {
            intent: PublishIntent {
                app_id: APP_ID.to_string(),
                publish_id: publish_id.to_string(),
                targets: vec![PublishTarget::Channel {
                    channel: channel.to_string(),
                }],
                payload: PushPayload {
                    template_id: None,
                    template_data: json!({
                        "channel": channel,
                        "publishId": publish_id,
                        "tick": occurred_at_ms,
                    }),
                    title: Some("Sockudo simulator".to_string()),
                    body: Some(format!("deterministic push {publish_id}")),
                    icon: None,
                    sound: None,
                    collapse_key: None,
                },
                provider_overrides: vec![],
                not_before_ms: None,
                expires_at_ms: None,
            },
            expected_recipients,
        };
        Ok(self
            .pipeline
            .accept_publish(request, occurred_at_ms)
            .await?)
    }

    pub(crate) async fn active_device_ids(
        &self,
        page_limit: usize,
    ) -> SimulatorResult<BTreeSet<String>> {
        let mut ids = BTreeSet::new();
        let mut cursor = None;
        loop {
            let page = self.store.list_devices(APP_ID, page_limit, cursor).await?;
            ids.extend(page.items.into_iter().map(|device| device.id));
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }
        Ok(ids)
    }

    pub(crate) async fn subscription_keys(
        &self,
        page_limit: usize,
    ) -> SimulatorResult<BTreeSet<(String, String)>> {
        let mut keys = BTreeSet::new();
        let mut cursor = None;
        loop {
            let page = self
                .store
                .list_subscriptions(APP_ID, page_limit, cursor)
                .await?;
            keys.extend(
                page.items
                    .into_iter()
                    .map(|subscription| (subscription.channel, subscription.device_id)),
            );
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }
        Ok(keys)
    }

    pub(crate) async fn publish_log_ids(
        &self,
        page_limit: usize,
    ) -> SimulatorResult<BTreeSet<String>> {
        let mut ids = BTreeSet::new();
        let mut cursor = None;
        loop {
            let page = self
                .store
                .list_publish_log_events(APP_ID, page_limit, cursor)
                .await?;
            ids.extend(page.items.into_iter().map(|event| event.publish_id));
            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }
        Ok(ids)
    }

    pub(crate) async fn publish_status(
        &self,
        publish_id: &str,
    ) -> SimulatorResult<Option<PublishStatus>> {
        Ok(self.store.get_publish_status(APP_ID, publish_id).await?)
    }

    pub(crate) async fn publish_idempotency_target(
        &self,
        publish_id: &str,
    ) -> SimulatorResult<Option<String>> {
        Ok(self
            .store
            .get_idempotency_record(APP_ID, &publish_idempotency_key(APP_ID, publish_id))
            .await?
            .map(|record| record.publish_id))
    }

    pub(crate) async fn scheduled_publish_exists(&self, publish_id: &str) -> SimulatorResult<bool> {
        Ok(self
            .store
            .get_scheduled_job(APP_ID, publish_id)
            .await?
            .is_some())
    }

    fn device_details(
        &self,
        device_id: &str,
        client_id: &str,
        provider: PushProviderKind,
        occurred_at_ms: u64,
    ) -> SimulatorResult<DeviceDetails> {
        Ok(DeviceDetails {
            app_id: APP_ID.to_string(),
            id: device_id.to_string(),
            client_id: Some(client_id.to_string()),
            form_factor: FormFactor::Phone,
            platform: platform_for_provider(provider),
            metadata: json!({
                "source": "sockudo-simulator",
            }),
            device_secret: SecretString::new(format!(
                "pbkdf2-sha256$120000$simulator${device_id}"
            ))?,
            timezone: "UTC".to_string(),
            locale: "en".to_string(),
            last_active_at_ms: occurred_at_ms,
            push: DevicePushDetails {
                recipient: recipient_for_provider(provider, device_id)?,
                state: DevicePushState::Active,
                failure_count: 0,
                error_reason: None,
            },
            push_rate_policy: None,
        })
    }
}

fn platform_for_provider(provider: PushProviderKind) -> Platform {
    match provider {
        PushProviderKind::Fcm | PushProviderKind::Hms => Platform::Android,
        PushProviderKind::Apns => Platform::Ios,
        PushProviderKind::WebPush => Platform::Browser,
        PushProviderKind::Wns => Platform::Windows,
        PushProviderKind::Realtime => Platform::Other,
    }
}

fn recipient_for_provider(
    provider: PushProviderKind,
    device_id: &str,
) -> SimulatorResult<PushRecipient> {
    let token = |prefix: &str| SecretString::new(format!("{prefix}-{device_id}"));
    Ok(match provider {
        PushProviderKind::Fcm => PushRecipient::Fcm {
            registration_token: token("fcm")?,
        },
        PushProviderKind::Apns => PushRecipient::Apns {
            device_token: token("apns")?,
        },
        PushProviderKind::WebPush => PushRecipient::Web {
            endpoint: SecretString::new(format!("https://push.example.com/{device_id}"))?,
            p256dh: token("p256dh")?,
            auth: token("auth")?,
        },
        PushProviderKind::Hms => PushRecipient::Hms {
            registration_token: token("hms")?,
        },
        PushProviderKind::Wns => PushRecipient::Wns {
            channel_uri: SecretString::new(format!("https://wns.example.com/{device_id}"))?,
        },
        PushProviderKind::Realtime => PushRecipient::Realtime {
            channel: format!("push:{device_id}"),
        },
    })
}
