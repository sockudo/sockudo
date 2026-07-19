#![cfg(feature = "redis")]

use sockudo_core::error::Error;
use sockudo_core::options::QueueReliabilityConfig;
use sockudo_core::queue::{QueueInterface, QueueJobOptions, QueueJobRequest};
use sockudo_core::webhook_types::{JobData, JobPayload};
use sockudo_queue::RedisQueueManager;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SOCKUDO_REDIS_QUEUE_TEST_URL"]
async fn redis_queue_is_idempotent_retries_and_settles() {
    let url = std::env::var("SOCKUDO_REDIS_QUEUE_TEST_URL")
        .expect("SOCKUDO_REDIS_QUEUE_TEST_URL is required");
    let reliability = QueueReliabilityConfig {
        max_attempts: 3,
        retry_base_delay_ms: 0,
        retry_max_delay_ms: 0,
        retry_jitter: 0.0,
        lease_duration_ms: 2_000,
        lease_renew_interval_ms: 500,
        worker_poll_interval_ms: 25,
        worker_prefetch: 4,
        completed_retention: 0,
        event_retention: 0,
        max_batch_size: 64,
        ..QueueReliabilityConfig::default()
    };
    let manager = RedisQueueManager::new_with_config(
        &url,
        None,
        &format!("sockudo_live_test_{}", uuid::Uuid::new_v4().simple()),
        4,
        5_000,
        reliability,
    )
    .await
    .expect("Redis queue should connect");

    let data = job("same-payload");
    let options = QueueJobOptions {
        job_id: Some("stable-job".to_string()),
        ..QueueJobOptions::default()
    };
    let first = manager
        .enqueue("reliability", data.clone(), options.clone())
        .await
        .expect("first enqueue should succeed");
    let duplicate = manager
        .enqueue("reliability", data, options.clone())
        .await
        .expect("same stable job should be idempotent");
    assert_eq!(first, duplicate);
    assert!(
        manager
            .enqueue("reliability", job("different-payload"), options)
            .await
            .is_err(),
        "same ID with another payload must conflict"
    );

    let deliveries = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(Notify::new());
    let callback_deliveries = Arc::clone(&deliveries);
    let callback_completed = Arc::clone(&completed);
    manager
        .process_queue(
            "reliability",
            Box::new(move |_| {
                let deliveries = Arc::clone(&callback_deliveries);
                let completed = Arc::clone(&callback_completed);
                Box::pin(async move {
                    if deliveries.fetch_add(1, Ordering::AcqRel) == 0 {
                        return Err(Error::Queue("retry once".to_string()));
                    }
                    completed.notify_one();
                    Ok(())
                })
            }),
        )
        .await
        .expect("processor should start");

    tokio::time::timeout(Duration::from_secs(10), completed.notified())
        .await
        .expect("retried job should complete");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stats = manager.stats("reliability").await.expect("stats");
            if stats.ready == Some(0)
                && stats.active == Some(0)
                && stats.delayed == Some(0)
                && stats.dead_letter == Some(0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("queue should settle");
    assert_eq!(deliveries.load(Ordering::Acquire), 2);

    let generated_jobs = 256;
    let generated_deliveries = Arc::new(AtomicUsize::new(0));
    let generated_completed = Arc::new(Notify::new());
    let callback_deliveries = Arc::clone(&generated_deliveries);
    let callback_completed = Arc::clone(&generated_completed);
    manager
        .process_queue(
            "generated-batch",
            Box::new(move |_| {
                let deliveries = Arc::clone(&callback_deliveries);
                let completed = Arc::clone(&callback_completed);
                Box::pin(async move {
                    deliveries.fetch_add(1, Ordering::AcqRel);
                    completed.notify_one();
                    Ok(())
                })
            }),
        )
        .await
        .expect("generated batch processor should start");
    let requests = (0..generated_jobs)
        .map(|index| QueueJobRequest {
            data: job(&format!("generated-{index}")),
            options: QueueJobOptions::default(),
        })
        .collect();
    let generated_ids = manager
        .enqueue_batch("generated-batch", requests)
        .await
        .expect("generated batch should enqueue");
    assert_eq!(generated_ids.len(), generated_jobs);
    assert!(generated_ids.iter().all(|id| id.0.len() == 32));
    assert_eq!(
        generated_ids
            .iter()
            .map(|id| id.0.as_str())
            .collect::<HashSet<_>>()
            .len(),
        generated_jobs
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while generated_deliveries.load(Ordering::Acquire) < generated_jobs {
            generated_completed.notified().await;
        }
    })
    .await
    .expect("generated batch should complete");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stats = manager.stats("generated-batch").await.expect("stats");
            if stats.ready == Some(0)
                && stats.active == Some(0)
                && stats.delayed == Some(0)
                && stats.dead_letter == Some(0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("generated batch queue should settle");
    assert_eq!(generated_deliveries.load(Ordering::Acquire), generated_jobs);
    manager.disconnect().await.expect("queue should drain");
}

fn job(signature: &str) -> JobData {
    JobData {
        app_key: "key".to_string(),
        app_id: "app".to_string(),
        app_secret: "secret".to_string(),
        payload: JobPayload {
            time_ms: 0,
            events: Vec::new(),
        },
        original_signature: signature.to_string(),
    }
}
