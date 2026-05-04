use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;

use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeliveryOutcome, DeliveryResult,
    DeviceDetails, DevicePushDetails, DevicePushState, FormFactor, NotificationTemplate, Platform,
    ProviderCredential, ProviderCredentialMaterial, PublishCounters, PublishLifecycleState,
    PublishStatus, PushCursor, PushProviderKind, PushRecipient, SecretString, TemplateContent,
};
use crate::storage::{
    DeviceRegistrationChange, IdempotencyRecord, PushCredentialStore, PushDeliveryEventStore,
    PushDeviceStore, PushIdempotencyStore, PushPublishLogStore, PushPublishStatusStore,
    PushScheduleStore, PushStorageBackendKind, PushStorageError, PushStorageResult,
    PushSubscriptionStore, PushTemplateStore, ScheduledPushJob,
};

pub struct PushStoreConformance;

impl PushStoreConformance {
    pub async fn assert_device_registration_idempotency<S>(store: S) -> PushStorageResult<()>
    where
        S: PushDeviceStore,
    {
        let device = sample_device("device-1", 86_400_000);

        let first = store.upsert_device(device.clone()).await?;
        assert_eq!(first.change, DeviceRegistrationChange::Inserted);

        let second = store.upsert_device(device.clone()).await?;
        assert_eq!(second.change, DeviceRegistrationChange::Unchanged);

        let mut changed = device.clone();
        changed.push.recipient = PushRecipient::Fcm {
            registration_token: secret("new-registration-token"),
        };
        let third = store.upsert_device(changed.clone()).await?;
        assert_eq!(third.change, DeviceRegistrationChange::Updated);

        assert_eq!(
            store.get_device("app-1", "device-1").await?,
            Some(changed.clone())
        );
        assert!(store.delete_device("app-1", "missing").await?.is_success());
        assert_eq!(
            store.delete_device("app-1", "device-1").await?,
            DeleteDeviceOutcome::Deleted
        );
        Ok(())
    }

    pub async fn assert_cursor_pagination_and_channel_fanout<S>(store: S) -> PushStorageResult<()>
    where
        S: PushDeviceStore + PushSubscriptionStore,
    {
        for index in 0..5 {
            let device = sample_device(&format!("device-{index}"), 86_400_000 + index);
            store.upsert_device(device.clone()).await?;
            store
                .upsert_subscription(ChannelSubscription::from_device("private-room", &device))
                .await?;
        }

        let first = store
            .list_channel_subscribers("app-1", "private-room", 2, None)
            .await?;
        assert_eq!(first.items.len(), 2);
        let second = store
            .list_channel_subscribers("app-1", "private-room", 2, first.next_cursor)
            .await?;
        assert_eq!(second.items.len(), 2);
        let third = store
            .list_channel_subscribers("app-1", "private-room", 2, second.next_cursor)
            .await?;
        assert_eq!(third.items.len(), 1);
        assert!(third.next_cursor.is_none());

        let wrong_app_cursor = PushCursor {
            app_id: "other-app".to_owned(),
            kind: crate::domain::PushCursorKind::ChannelSubscription,
            position: "device-0".to_owned(),
            issued_at_ms: 0,
        };
        assert!(
            store
                .list_channel_subscribers("app-1", "private-room", 2, Some(wrong_app_cursor))
                .await
                .is_err()
        );
        Ok(())
    }

    pub async fn assert_stale_cleanup_scans<S>(store: S) -> PushStorageResult<()>
    where
        S: PushDeviceStore,
    {
        store
            .upsert_device(sample_device("stale-1", 2 * 86_400_000))
            .await?;
        store
            .upsert_device(sample_device("fresh-1", 3 * 86_400_000))
            .await?;

        let stale = store.list_stale_devices("app-1", "2", 10, None).await?;
        assert_eq!(stale.items.len(), 1);
        assert_eq!(stale.items[0].id, "stale-1");
        Ok(())
    }

    pub async fn assert_credentials_templates_schedule_events_and_idempotency<S>(
        store: S,
    ) -> PushStorageResult<()>
    where
        S: PushCredentialStore
            + PushTemplateStore
            + PushPublishStatusStore
            + PushPublishLogStore
            + PushScheduleStore
            + PushDeliveryEventStore
            + PushIdempotencyStore,
    {
        let credential = ProviderCredential {
            app_id: "app-1".to_owned(),
            credential_id: "fcm-main".to_owned(),
            provider: PushProviderKind::Fcm,
            version: 1,
            material: ProviderCredentialMaterial::Fcm {
                service_account_json: encrypted("encrypted-service-account-json"),
            },
        };
        store.put_credential(credential.clone()).await?;
        assert_eq!(
            store.get_credential("app-1", "fcm-main").await?,
            Some(credential)
        );

        let template = sample_template();
        store.put_template(template.clone()).await?;
        assert_eq!(
            store.get_template("app-1", "welcome").await?,
            Some(template)
        );

        let status = PublishStatus {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            state: PublishLifecycleState::Queued,
            counters: PublishCounters::default(),
            fanout_regime: None,
            retry_after_ms: None,
            error_reason: None,
        };
        store.put_publish_status(status.clone()).await?;
        assert_eq!(
            store.get_publish_status("app-1", "publish-1").await?,
            Some(status)
        );

        let event = crate::domain::PublishLogEvent {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            event_id: "event-1".to_owned(),
            occurred_at_ms: 1,
            intent: crate::domain::PublishIntent {
                app_id: "app-1".to_owned(),
                publish_id: "publish-1".to_owned(),
                targets: vec![crate::domain::PublishTarget::Device {
                    device_id: "device-1".to_owned(),
                }],
                payload: crate::domain::PushPayload {
                    template_id: None,
                    template_data: json!({}),
                    title: Some("Hello".to_owned()),
                    body: Some("Body".to_owned()),
                    icon: None,
                    sound: None,
                    collapse_key: None,
                },
                provider_overrides: vec![],
                not_before_ms: None,
                expires_at_ms: None,
            },
            fanout_regime: crate::domain::FanoutRegime::FastPath,
            expected_recipients: 1,
            fast_threshold: 10_000,
            shard_size: 100_000,
        };
        store.append_publish_log_event(event.clone()).await?;
        assert_eq!(
            store
                .list_publish_log_events("app-1", 10, None)
                .await?
                .items,
            vec![event]
        );

        let scheduled = ScheduledPushJob {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            due_at_ms: 120_000,
            due_minute_ms: 120_000,
            payload_json: json!({"templateId": "welcome"}),
        };
        store.put_scheduled_job(scheduled.clone()).await?;
        assert_eq!(
            store.get_scheduled_job("app-1", "publish-1").await?,
            Some(scheduled.clone())
        );
        let due = store
            .list_due_scheduled_jobs("app-1", 120_000, 10, None)
            .await?;
        assert_eq!(due.items, vec![scheduled]);

        let event = sample_delivery_event("event-1", 1000);
        store.append_delivery_event(event.clone()).await?;
        let events = store
            .list_delivery_events("app-1", "publish-1", 10, None)
            .await?;
        assert_eq!(events.items, vec![event]);
        assert_eq!(store.purge_delivery_events_before("app-1", 2000).await?, 1);
        assert!(
            store
                .list_delivery_events("app-1", "publish-1", 10, None)
                .await?
                .items
                .is_empty()
        );

        let idempotency = IdempotencyRecord {
            app_id: "app-1".to_owned(),
            key: "idem-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            expires_at_ms: 10_000,
        };
        assert!(
            store
                .put_idempotency_record_if_absent(idempotency.clone())
                .await?
        );
        assert!(
            !store
                .put_idempotency_record_if_absent(idempotency.clone())
                .await?
        );
        assert_eq!(
            store.get_idempotency_record("app-1", "idem-1").await?,
            Some(idempotency)
        );
        Ok(())
    }

    pub async fn assert_concurrent_registration_update<S>(store: S) -> PushStorageResult<()>
    where
        S: PushDeviceStore + Clone + Send + Sync + 'static,
    {
        let store = Arc::new(store);
        let mut handles = Vec::new();
        for index in 0..20 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let mut device = sample_device("shared-device", 86_400_000);
                device.push.recipient = PushRecipient::Fcm {
                    registration_token: secret(&format!("token-{index}")),
                };
                store.upsert_device(device).await
            }));
        }

        for handle in handles {
            handle
                .await
                .map_err(|error| PushStorageError::Backend(error.to_string()))??;
        }

        assert!(store.get_device("app-1", "shared-device").await?.is_some());
        Ok(())
    }

    pub fn assert_backend_startup_errors_are_explicit() {
        for backend in [
            PushStorageBackendKind::Postgres,
            PushStorageBackendKind::Mysql,
            PushStorageBackendKind::DynamoDb,
            PushStorageBackendKind::SurrealDb,
            PushStorageBackendKind::ScyllaDb,
        ] {
            let result = backend.startup_check();
            if matches!(backend, PushStorageBackendKind::Postgres) && cfg!(feature = "postgres")
                || matches!(backend, PushStorageBackendKind::Mysql) && cfg!(feature = "mysql")
                || matches!(backend, PushStorageBackendKind::DynamoDb) && cfg!(feature = "dynamodb")
                || matches!(backend, PushStorageBackendKind::SurrealDb)
                    && cfg!(feature = "surrealdb")
                || matches!(backend, PushStorageBackendKind::ScyllaDb) && cfg!(feature = "scylladb")
            {
                assert!(result.is_ok());
            } else {
                assert!(matches!(
                    result,
                    Err(PushStorageError::FeatureDisabled { .. })
                        | Err(PushStorageError::BackendDeferred { .. })
                ));
            }
        }
    }
}

fn sample_device(device_id: &str, last_active_at_ms: u64) -> DeviceDetails {
    DeviceDetails {
        app_id: "app-1".to_owned(),
        id: device_id.to_owned(),
        client_id: Some("client-1".to_owned()),
        form_factor: FormFactor::Phone,
        platform: Platform::Android,
        metadata: json!({"model": "Pixel"}),
        device_secret: crate::domain::hash_device_identity_token(&secret("device-identity-token")),
        timezone: "Europe/Madrid".to_owned(),
        locale: "en-US".to_owned(),
        last_active_at_ms,
        push: DevicePushDetails {
            recipient: PushRecipient::Fcm {
                registration_token: secret("fcm-registration-token"),
            },
            state: DevicePushState::Active,
            failure_count: 0,
            error_reason: None,
        },
        push_rate_policy: None,
    }
}

fn sample_template() -> NotificationTemplate {
    NotificationTemplate {
        app_id: "app-1".to_owned(),
        template_id: "welcome".to_owned(),
        default_locale: "en".to_owned(),
        locales: BTreeMap::from([(
            "en".to_owned(),
            TemplateContent {
                title: "Hello".to_owned(),
                body: "Welcome".to_owned(),
                icon: None,
                sound: None,
                collapse_key: None,
            },
        )]),
        provider_overrides: BTreeMap::new(),
    }
}

fn sample_delivery_event(event_id: &str, occurred_at_ms: u64) -> DeliveryEvent {
    DeliveryEvent {
        app_id: "app-1".to_owned(),
        publish_id: "publish-1".to_owned(),
        event_id: event_id.to_owned(),
        occurred_at_ms,
        result: DeliveryResult {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            provider: PushProviderKind::Fcm,
            batch_id: "batch-1".to_owned(),
            device_id: Some("device-1".to_owned()),
            outcome: DeliveryOutcome::Accepted,
            provider_message_id: Some("provider-message-1".to_owned()),
            error: None,
            attempt: 1,
        },
    }
}

fn secret(value: &str) -> SecretString {
    SecretString::new(value).unwrap()
}

fn encrypted(value: &str) -> crate::domain::EncryptedSecret {
    crate::domain::EncryptedSecret::new(value).unwrap()
}
