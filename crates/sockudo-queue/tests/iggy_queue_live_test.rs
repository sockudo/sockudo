#![cfg(feature = "iggy")]

use sockudo_core::options::IggyConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobPayload};
use sockudo_queue::IggyQueueManager;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use uuid::Uuid;

fn live_iggy_config() -> IggyConfig {
    let suffix = Uuid::new_v4().simple().to_string();
    IggyConfig {
        connection_string: std::env::var("SOCKUDO_IGGY_TEST_CONNECTION_STRING")
            .unwrap_or_else(|_| "iggy://iggy:iggy@127.0.0.1:18090".to_string()),
        stream: format!("sockudo-live-{suffix}"),
        queue_topic_prefix: format!("sockudo-live-queue-{suffix}"),
        consumer_group_prefix: format!("sockudo-live-workers-{suffix}"),
        request_timeout_ms: 5000,
        poll_interval_ms: 25,
        poll_batch_size: 10,
        ..Default::default()
    }
}

fn test_job(signature: &str) -> JobData {
    JobData {
        job_id: None,
        app_key: "test-key".to_string(),
        app_id: "test-app".to_string(),
        app_secret: "test-secret".to_string(),
        payload: JobPayload {
            time_ms: chrono::Utc::now().timestamp_millis(),
            events: vec![sonic_rs::json!({
                "name": "iggy.live",
                "channel": "test-channel",
                "data": { "ok": true }
            })],
        },
        original_signature: signature.to_string(),
    }
}

#[tokio::test]
#[ignore = "requires a running Apache Iggy broker; set SOCKUDO_IGGY_TEST_CONNECTION_STRING"]
async fn live_iggy_queue_publishes_and_processes_jobs() {
    let manager = IggyQueueManager::new(live_iggy_config()).await.unwrap();
    manager.check_health().await.unwrap();

    let queue_name = format!("webhooks-{}", Uuid::new_v4().simple());
    let expected_signature = format!("sig-{}", Uuid::new_v4().simple());
    let (tx, mut rx) = mpsc::channel::<JobData>(1);

    manager
        .process_queue(
            &queue_name,
            Box::new(move |job| {
                let tx = tx.clone();
                Box::pin(async move {
                    tx.send(job)
                        .await
                        .map_err(|error| sockudo_core::error::Error::Queue(error.to_string()))?;
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();

    manager
        .add_to_queue(&queue_name, test_job(&expected_signature))
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for Iggy queue job")
        .expect("Iggy queue worker stopped before receiving the job");
    assert_eq!(received.original_signature, expected_signature);

    manager.disconnect().await.unwrap();
}
