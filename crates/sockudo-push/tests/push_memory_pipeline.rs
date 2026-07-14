use std::sync::Arc;

use sockudo_push::{
    DeviceDetails, DevicePushDetails, DevicePushState, FanoutConfig, FormFactor, MemoryPushQueue,
    MemoryPushStore, Platform, PublishIntent, PublishTarget, PushAcceptRequest, PushDeviceStore,
    PushPayload, PushPipeline, PushPlanner, PushProviderKind, PushQueue, PushQueuePayload,
    PushQueueStage, PushRecipient, PushSubscriptionStore, SecretString, hash_device_identity_token,
};
use sonic_rs::json;

#[tokio::test]
async fn memory_pipeline_accepts_and_plans_channel_publish() {
    let store = Arc::new(MemoryPushStore::new());
    let queue = Arc::new(MemoryPushQueue::new());
    store
        .upsert_device(sample_device("device-1"))
        .await
        .unwrap();
    store
        .upsert_subscription(sockudo_push::ChannelSubscription::from_device(
            "news",
            &sample_device("device-1"),
        ))
        .await
        .unwrap();

    let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default());
    let accepted = pipeline
        .accept_publish(
            PushAcceptRequest {
                intent: sample_intent(vec![PublishTarget::Channel {
                    channel: "news".to_owned(),
                }]),
                expected_recipients: 1,
            },
            10,
        )
        .await
        .unwrap();
    assert!(!accepted.publish_log_message_id.is_empty());

    let planner = PushPlanner::new(store, queue.clone(), FanoutConfig::default());
    assert_eq!(planner.run_once("integration-planner").await.unwrap(), 1);
    assert_eq!(
        queue
            .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
            .await
            .unwrap()
            .ready_depth,
        1
    );
}

#[tokio::test]
async fn client_scoped_channel_subscription_expands_current_durable_devices() {
    let store = Arc::new(MemoryPushStore::new());
    let queue = Arc::new(MemoryPushQueue::new());
    store
        .upsert_device(sample_device("device-1"))
        .await
        .unwrap();
    store
        .upsert_device(sample_device("device-2"))
        .await
        .unwrap();
    store
        .upsert_subscription(sockudo_push::ChannelSubscription::from_client(
            "app-1", "news", "client-1",
        ))
        .await
        .unwrap();

    PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default())
        .accept_publish(
            PushAcceptRequest {
                intent: sample_intent(vec![PublishTarget::Channel {
                    channel: "news".to_owned(),
                }]),
                expected_recipients: 2,
            },
            10,
        )
        .await
        .unwrap();
    PushPlanner::new(store, queue.clone(), FanoutConfig::default())
        .run_once("client-scope-planner")
        .await
        .unwrap();

    let messages = queue
        .consume(
            PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
            "client-scope-provider",
            1,
            30_000,
        )
        .await
        .unwrap();
    let PushQueuePayload::DeliveryBatch(batch) = &messages[0].payload else {
        panic!("planner must emit a delivery batch");
    };
    assert_eq!(batch.jobs.len(), 2);
}

#[tokio::test]
async fn device_removed_after_admission_is_not_dispatched() {
    let store = Arc::new(MemoryPushStore::new());
    let queue = Arc::new(MemoryPushQueue::new());
    store
        .upsert_device(sample_device("device-1"))
        .await
        .unwrap();

    PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default())
        .accept_publish(
            PushAcceptRequest {
                intent: sample_intent(vec![PublishTarget::Device {
                    device_id: "device-1".to_owned(),
                }]),
                expected_recipients: 1,
            },
            10,
        )
        .await
        .unwrap();
    store.delete_device("app-1", "device-1").await.unwrap();
    PushPlanner::new(store, queue.clone(), FanoutConfig::default())
        .run_once("removal-race-planner")
        .await
        .unwrap();

    assert_eq!(
        queue
            .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
            .await
            .unwrap()
            .ready_depth,
        0
    );
}

#[tokio::test]
async fn two_planner_nodes_claim_shared_publish_log_once() {
    let store = Arc::new(MemoryPushStore::new());
    let queue = Arc::new(MemoryPushQueue::new());
    store
        .upsert_device(sample_device("device-1"))
        .await
        .unwrap();

    PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default())
        .accept_publish(
            PushAcceptRequest {
                intent: sample_intent(vec![PublishTarget::Device {
                    device_id: "device-1".to_owned(),
                }]),
                expected_recipients: 1,
            },
            10,
        )
        .await
        .unwrap();

    let node_a = PushPlanner::new(store.clone(), queue.clone(), FanoutConfig::default());
    let node_b = PushPlanner::new(store, queue.clone(), FanoutConfig::default());
    let (planned_a, planned_b) = tokio::join!(
        node_a.run_once("planner-node-a"),
        node_b.run_once("planner-node-b")
    );
    assert_eq!(planned_a.unwrap() + planned_b.unwrap(), 1);

    let batches = queue
        .consume(
            PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
            "provider-node",
            2,
            30_000,
        )
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let PushQueuePayload::DeliveryBatch(batch) = &batches[0].payload else {
        panic!("planner must emit a delivery batch");
    };
    assert_eq!(batch.jobs.len(), 1);
}

fn sample_intent(targets: Vec<PublishTarget>) -> PublishIntent {
    PublishIntent {
        app_id: "app-1".to_owned(),
        publish_id: "publish-1".to_owned(),
        targets,
        payload: PushPayload {
            template_id: None,
            template_data: json!({ "data": { "headline": "launch" } }),
            title: Some("Hello".to_owned()),
            body: Some("Body".to_owned()),
            icon: None,
            sound: None,
            collapse_key: None,
        },
        provider_overrides: vec![],
        not_before_ms: None,
        expires_at_ms: None,
    }
}

fn sample_device(device_id: &str) -> DeviceDetails {
    DeviceDetails {
        app_id: "app-1".to_owned(),
        id: device_id.to_owned(),
        client_id: Some("client-1".to_owned()),
        form_factor: FormFactor::Phone,
        platform: Platform::Android,
        metadata: json!({}),
        device_secret: hash_device_identity_token(&SecretString::new("device-token").unwrap()),
        timezone: "UTC".to_owned(),
        locale: "en".to_owned(),
        last_active_at_ms: 1,
        push: DevicePushDetails {
            recipient: PushRecipient::Fcm {
                registration_token: SecretString::new(format!("token-{device_id}")).unwrap(),
            },
            state: DevicePushState::Active,
            failure_count: 0,
            error_reason: None,
        },
        push_rate_policy: None,
    }
}
