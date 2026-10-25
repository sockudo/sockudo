#![cfg(feature = "sqs")]
//! Live SQS redrive tests. Run against ElasticMQ or LocalStack:
//!
//! SOCKUDO_SQS_TEST_ENDPOINT_URL=http://localhost:9324 \
//! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//!   cargo test -p sockudo-queue --features sqs --test sqs_redrive_live_test -- --ignored

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aws_sdk_sqs::types::QueueAttributeName;
use sockudo_core::error::Error;
use sockudo_core::options::{QueueReliabilityConfig, SqsQueueConfig};
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobPayload};
use sockudo_queue::SqsQueueManager;

const REGION: &str = "us-east-1";

fn endpoint() -> String {
    std::env::var("SOCKUDO_SQS_TEST_ENDPOINT_URL")
        .expect("SOCKUDO_SQS_TEST_ENDPOINT_URL is required")
}

fn unique(name: &str) -> String {
    format!("sockudo-it-{name}-{}", uuid::Uuid::new_v4().simple())
}

async fn admin_client() -> aws_sdk_sqs::Client {
    let config = aws_config::from_env()
        .region(aws_sdk_sqs::config::Region::new(REGION))
        .endpoint_url(endpoint())
        .load()
        .await;
    aws_sdk_sqs::Client::new(&config)
}

/// Creates a queue the way an operator would, outside Sockudo.
async fn create_queue(
    client: &aws_sdk_sqs::Client,
    name: &str,
    attributes: HashMap<QueueAttributeName, String>,
) -> (String, String) {
    let url = client
        .create_queue()
        .queue_name(name)
        .set_attributes(Some(attributes))
        .send()
        .await
        .expect("create queue")
        .queue_url()
        .expect("queue url")
        .to_owned();
    let arn = client
        .get_queue_attributes()
        .queue_url(&url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .expect("queue attributes")
        .attributes()
        .and_then(|attributes| attributes.get(&QueueAttributeName::QueueArn).cloned())
        .expect("queue arn");
    (url, arn)
}

fn prefix_of(queue_url: &str) -> String {
    let (base, _) = queue_url.rsplit_once('/').expect("queue url has a path");
    format!("{base}/")
}

fn sqs_config(queue_url_prefix: Option<String>, require_redrive_policy: bool) -> SqsQueueConfig {
    SqsQueueConfig {
        region: REGION.to_string(),
        endpoint_url: Some(endpoint()),
        queue_url_prefix,
        visibility_timeout: 2,
        wait_time_seconds: 1,
        concurrency: 1,
        require_redrive_policy,
        ..SqsQueueConfig::default()
    }
}

fn reliability(max_attempts: u32) -> QueueReliabilityConfig {
    QueueReliabilityConfig {
        max_attempts,
        // 0 is clamped to the 1 s SQS minimum, never to visibility 0.
        retry_base_delay_ms: 0,
        retry_max_delay_ms: 0,
        retry_jitter: 0.0,
        ..QueueReliabilityConfig::default()
    }
}

fn job() -> JobData {
    JobData {
        app_id: "live-app".to_string(),
        app_key: "live-key".to_string(),
        app_secret: "live-secret".to_string(),
        payload: JobPayload {
            time_ms: 1_700_000_000_000,
            events: Vec::new(),
        },
        original_signature: "sig".to_string(),
        ..JobData::default()
    }
}

/// Registers a processor that always fails and counts deliveries.
async fn consume_failing(manager: &SqsQueueManager, queue: &str) -> Arc<AtomicUsize> {
    let deliveries = Arc::new(AtomicUsize::new(0));
    let counter = deliveries.clone();
    manager
        .process_queue(
            queue,
            Box::new(move |_| {
                let counter = counter.clone();
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Err(Error::Other("always fails".to_string()))
                })
            }),
        )
        .await
        .expect("start workers");
    deliveries
}

async fn eventually(timeout: Duration, mut condition: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met in {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires SOCKUDO_SQS_TEST_ENDPOINT_URL (ElasticMQ or LocalStack)"]
async fn redrive_policy_moves_exhausted_jobs_to_the_dead_letter_queue() {
    let client = admin_client().await;
    let source = unique("redrive");
    let (_, dlq_arn) = create_queue(&client, &format!("{source}-dlq"), HashMap::new()).await;
    let (source_url, _) = create_queue(
        &client,
        &source,
        HashMap::from([(
            QueueAttributeName::RedrivePolicy,
            format!(r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":"2"}}"#),
        )]),
    )
    .await;

    let manager = SqsQueueManager::new_with_reliability(
        sqs_config(Some(prefix_of(&source_url)), false),
        reliability(5),
    )
    .await
    .unwrap();
    manager.validate_delivery_guarantees(&source).await.unwrap();
    assert!(manager.capabilities().dead_letter);

    let deliveries = consume_failing(&manager, &source).await;
    manager.add_to_queue(&source, job()).await.unwrap();

    eventually(Duration::from_secs(60), async || {
        manager.stats(&source).await.unwrap().dead_letter == Some(1)
    })
    .await;
    // SQS bounds delivery at maxReceiveCount (2), not max_attempts (5).
    assert_eq!(deliveries.load(Ordering::SeqCst), 2);
    manager.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires SOCKUDO_SQS_TEST_ENDPOINT_URL (ElasticMQ or LocalStack)"]
async fn missing_redrive_policy_deletes_after_max_attempts() {
    let client = admin_client().await;
    let source = unique("fallback");
    let (source_url, _) = create_queue(&client, &source, HashMap::new()).await;

    let manager = SqsQueueManager::new_with_reliability(
        sqs_config(Some(prefix_of(&source_url)), false),
        reliability(2),
    )
    .await
    .unwrap();
    manager.validate_delivery_guarantees(&source).await.unwrap();
    assert!(!manager.capabilities().dead_letter);

    let deliveries = consume_failing(&manager, &source).await;
    manager.add_to_queue(&source, job()).await.unwrap();

    eventually(Duration::from_secs(30), async || {
        let stats = manager.stats(&source).await.unwrap();
        deliveries.load(Ordering::SeqCst) == 2 && stats.ready == Some(0) && stats.active == Some(0)
    })
    .await;
    // Deleted, not looping until retention: no third delivery after the backoff.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(deliveries.load(Ordering::SeqCst), 2);
    manager.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires SOCKUDO_SQS_TEST_ENDPOINT_URL (ElasticMQ or LocalStack)"]
async fn strict_mode_rejects_a_queue_without_redrive_policy() {
    let client = admin_client().await;
    let source = unique("strict");
    let (source_url, _) = create_queue(&client, &source, HashMap::new()).await;

    let manager = SqsQueueManager::new_with_reliability(
        sqs_config(Some(prefix_of(&source_url)), true),
        reliability(5),
    )
    .await
    .unwrap();
    let error = manager
        .validate_delivery_guarantees(&source)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Config(_)), "{error:?}");
    assert!(
        !error.to_string().contains(&source_url),
        "error must not carry the queue URL"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires SOCKUDO_SQS_TEST_ENDPOINT_URL (ElasticMQ or LocalStack)"]
async fn sockudo_created_queue_starts_in_fallback_mode() {
    // No prefix: Sockudo creates the queue itself, without a RedrivePolicy.
    let source = unique("created");
    let manager = SqsQueueManager::new_with_reliability(sqs_config(None, false), reliability(5))
        .await
        .unwrap();

    manager.validate_delivery_guarantees(&source).await.unwrap();
    assert!(!manager.capabilities().dead_letter);
}
