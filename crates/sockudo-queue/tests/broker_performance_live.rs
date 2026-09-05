#![cfg(any(feature = "kafka", feature = "rabbitmq", feature = "iggy"))]
use sockudo_core::{
    error::Error,
    options::QueueReliabilityConfig,
    queue::QueueInterface,
    webhook_types::{JobData, JobPayload},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
fn job(app: usize, sequence: usize) -> JobData {
    JobData {
        job_id: Some(format!("{app}-{sequence}")),
        app_id: app.to_string(),
        app_key: "synthetic".into(),
        app_secret: "synthetic".into(),
        payload: JobPayload {
            time_ms: 1,
            events: vec![],
        },
        original_signature: sequence.to_string(),
    }
}

#[cfg(feature = "kafka")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SOCKUDO_KAFKA_TEST_BROKERS"]
async fn kafka_partition_workers_preserve_app_order_and_drain_on_shutdown() {
    use sockudo_core::options::KafkaAdapterConfig;
    let config = KafkaAdapterConfig {
        brokers: vec![std::env::var("SOCKUDO_KAFKA_TEST_BROKERS").unwrap()],
        prefix: format!("services-kafka-{}", uuid::Uuid::new_v4().simple()),
        partitions: 4,
        topic_epoch: Some("test-generation".into()),
        ..Default::default()
    };
    let manager = sockudo_queue::KafkaQueueManager::new(config).await.unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(tokio::sync::Mutex::new(HashMap::<String, Vec<usize>>::new()));
    let a = active.clone();
    let p = peak.clone();
    let c = completed.clone();
    let s = seen.clone();
    manager
        .process_queue(
            "ordered",
            Box::new(move |job| {
                let a = a.clone();
                let p = p.clone();
                let c = c.clone();
                let s = s.clone();
                Box::pin(async move {
                    let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                    p.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    s.lock()
                        .await
                        .entry(job.app_id)
                        .or_default()
                        .push(job.original_signature.parse().unwrap());
                    a.fetch_sub(1, Ordering::SeqCst);
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();
    // Give the new group a stable assignment before applying the ordered workload.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut jobs = Vec::new();
    for sequence in 0..10 {
        for app in 0..16 {
            jobs.push(job(app, sequence));
        }
    }
    let started = Instant::now();
    manager.add_batch_to_queue("ordered", jobs).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while completed.load(Ordering::SeqCst) < 160 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    manager.disconnect().await.unwrap();
    assert!(peak.load(Ordering::SeqCst) >= 2);
    assert!(peak.load(Ordering::SeqCst) <= 4);
    for sequences in seen.lock().await.values() {
        assert_eq!(sequences, &(0..10).collect::<Vec<_>>());
    }
    assert_eq!(seen.lock().await.len(), 16);
    println!(
        "kafka_delivered=160 peak={} elapsed_ms={}",
        peak.load(Ordering::SeqCst),
        started.elapsed().as_millis()
    );
}

#[cfg(feature = "rabbitmq")]
#[tokio::test]
#[ignore = "requires SOCKUDO_RABBITMQ_TEST_URL"]
async fn rabbit_retries_are_paced_and_keep_accepted_jobs_until_success() {
    let manager = sockudo_queue::RabbitMqQueueManager::new_with_reliability(
        sockudo_core::options::RabbitMqAdapterConfig {
            url: std::env::var("SOCKUDO_RABBITMQ_TEST_URL").unwrap(),
            prefix: format!("services-rabbit-{}", uuid::Uuid::new_v4().simple()),
            ..Default::default()
        },
        QueueReliabilityConfig {
            retry_base_delay_ms: 100,
            retry_max_delay_ms: 100,
            retry_jitter: 0.0,
            max_attempts: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let calls = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let completed = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let done = completed.clone();
    manager
        .process_queue(
            "retry",
            Box::new(move |_| {
                let c = c.clone();
                let done = done.clone();
                Box::pin(async move {
                    let mut calls = c.lock().await;
                    calls.push(Instant::now());
                    if calls.len() < 3 {
                        Err(Error::Queue("injected dependency outage".into()))
                    } else {
                        done.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
            }),
        )
        .await
        .unwrap();
    manager.add_to_queue("retry", job(0, 0)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while completed.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    manager.disconnect().await.unwrap();
    let calls = calls.lock().await;
    assert_eq!(calls.len(), 3);
    for pair in calls.windows(2) {
        assert!(pair[1].duration_since(pair[0]) >= Duration::from_millis(90));
    }
    println!(
        "rabbit_attempts=3 retry_span_ms={}",
        calls[2].duration_since(calls[0]).as_millis()
    );
}

#[cfg(feature = "iggy")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SOCKUDO_IGGY_TEST_CONNECTION_STRING"]
async fn iggy_partition_workers_deliver_every_job_with_bounded_concurrency() {
    let config = sockudo_core::options::IggyConfig {
        connection_string: std::env::var("SOCKUDO_IGGY_TEST_CONNECTION_STRING").unwrap(),
        stream: format!("services-iggy-{}", uuid::Uuid::new_v4().simple()),
        partitions_count: 4,
        poll_interval_ms: 5,
        poll_batch_size: 4,
        ..Default::default()
    };
    let manager = sockudo_queue::IggyQueueManager::new(config).await.unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
    let a = active.clone();
    let p = peak.clone();
    let s = seen.clone();
    manager
        .process_queue(
            "parallel",
            Box::new(move |job| {
                let a = a.clone();
                let p = p.clone();
                let s = s.clone();
                Box::pin(async move {
                    let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                    p.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    s.lock().await.insert(job.job_id);
                    a.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    manager
        .add_batch_to_queue("parallel", (0..100).map(|n| job(n, 0)).collect())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while seen.lock().await.len() < 100 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    manager.disconnect().await.unwrap();
    assert!(peak.load(Ordering::SeqCst) >= 2);
    assert!(peak.load(Ordering::SeqCst) <= 4);
    assert_eq!(seen.lock().await.len(), 100);
}
