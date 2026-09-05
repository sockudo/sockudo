use sockudo_push::*;
use sonic_rs::json;
use std::{sync::Arc, time::Instant};
const APP_ID: &str = "app-1";
fn sample_payload() -> PushPayload {
    PushPayload {
        template_id: None,
        template_data: json!({ "data": { "headline": "benchmark" } }),
        title: Some("Benchmark".to_owned()),
        body: Some("Body".to_owned()),
        icon: None,
        sound: None,
        collapse_key: None,
    }
}
fn sample_intent(publish_id: String, targets: Vec<PublishTarget>) -> PublishIntent {
    PublishIntent {
        app_id: APP_ID.to_owned(),
        publish_id,
        targets,
        payload: sample_payload(),
        provider_overrides: vec![],
        not_before_ms: None,
        expires_at_ms: None,
    }
}
fn sample_device(device_id: &str) -> DeviceDetails {
    DeviceDetails {
        app_id: APP_ID.to_owned(),
        id: device_id.to_owned(),
        client_id: Some("client-1".to_owned()),
        form_factor: FormFactor::Phone,
        platform: Platform::Android,
        metadata: json!({}),
        device_secret: SecretString::new(format!("pbkdf2-sha256$120000$bench${device_id}"))
            .unwrap(),
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
fn sample_delivery_batch(publish_id: String, jobs: usize) -> DeliveryBatch {
    let payload = Arc::new(sample_payload());
    DeliveryBatch {
        app_id: APP_ID.to_owned(),
        publish_id: publish_id.clone(),
        provider: PushProviderKind::Fcm,
        batch_id: "bench-batch-1".to_owned(),
        jobs: (0..jobs)
            .map(|index| DeliveryJob {
                app_id: APP_ID.to_owned(),
                publish_id: publish_id.clone(),
                provider: PushProviderKind::Fcm,
                batch_id: "bench-batch-1".to_owned(),
                device_id: Some(format!("device-{index}")),
                recipient: PushRecipient::Fcm {
                    registration_token: SecretString::new(format!("token-{index}")).unwrap(),
                },
                payload: Arc::clone(&payload),
                rendered_payload: None,
                attempt: 1,
                first_attempt_at_ms: None,
                not_before_ms: None,
                expires_at_ms: None,
            })
            .collect(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "p2".into());
    for n in [1_000usize, 10_000] {
        let store = Arc::new(MemoryPushStore::new());
        if mode == "p1" || mode == "p2" {
            for i in 0..n {
                let mut device = sample_device(&format!("device-{i:08}"));
                device.client_id = Some(if i < 10 { "selected" } else { "irrelevant" }.to_owned());
                store.upsert_device(device).await.unwrap();
            }
        }
        for rep in 0..7 {
            if mode == "p2" {
                let start = Instant::now();
                let mut cursor = None;
                let mut count = 0;
                loop {
                    let page = store.list_devices(APP_ID, 100, cursor).await.unwrap();
                    count += page.items.len();
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                assert_eq!(count, n);
                println!(
                    "p2,n={n},rep={rep},returned={count},us={}",
                    start.elapsed().as_micros()
                );
            } else if mode == "p1" {
                let queue = Arc::new(MemoryPushQueue::new());
                let config = FanoutConfig::default();
                let pipeline = PushPipeline::new(store.clone(), queue.clone(), config.clone());
                pipeline
                    .accept_publish(
                        PushAcceptRequest {
                            intent: sample_intent(
                                format!("bench-{n}-{rep}"),
                                vec![PublishTarget::Client {
                                    client_id: "selected".into(),
                                }],
                            ),
                            expected_recipients: 10,
                        },
                        1,
                    )
                    .await
                    .unwrap();
                let planner = PushPlanner::new(store.clone(), queue.clone(), config);
                let start = Instant::now();
                planner.run_once("bench").await.unwrap();
                let elapsed = start.elapsed().as_micros();
                let batches = queue
                    .consume(
                        PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                        "bench",
                        100,
                        30_000,
                    )
                    .await
                    .unwrap();
                let count: usize = batches
                    .iter()
                    .map(|m| match &m.payload {
                        PushQueuePayload::DeliveryBatch(b) => b.jobs.len(),
                        _ => panic!("wrong stage"),
                    })
                    .sum();
                assert_eq!(count, 10);
                println!("p1,n={n},rep={rep},returned={count},us={elapsed}");
            } else if mode == "p7" {
                let queue = MemoryPushQueue::new();
                let payload = PushQueuePayload::DeliveryBatch(Box::new(sample_delivery_batch(
                    "publish".into(),
                    1,
                )));
                for i in 0..n {
                    queue
                        .produce(
                            PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                            format!("job-{i}"),
                            payload.clone(),
                        )
                        .await
                        .unwrap();
                }
                let start = Instant::now();
                let mut count = 0;
                loop {
                    let batch = queue
                        .consume(
                            PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                            "bench",
                            32,
                            30_000,
                        )
                        .await
                        .unwrap();
                    if batch.is_empty() {
                        break;
                    }
                    count += batch.len();
                    for m in batch {
                        queue.ack(m.ack).await.unwrap();
                    }
                }
                assert_eq!(count, n);
                println!(
                    "p7,n={n},rep={rep},returned={count},us={}",
                    start.elapsed().as_micros()
                );
            } else if mode == "p8" {
                let metrics = PushMetrics::default();
                let start = Instant::now();
                for _ in 0..n {
                    metrics.observe(
                        "sockudo_push_dispatch_duration_seconds",
                        &[("provider", "fcm"), ("app", "app-1")],
                        0.001,
                    );
                }
                let elapsed = start.elapsed().as_micros();
                assert_eq!(metrics.snapshot().values().next().unwrap().count, n as u64);
                println!("p8,n={n},rep={rep},returned={n},us={elapsed}");
            }
        }
    }
}
