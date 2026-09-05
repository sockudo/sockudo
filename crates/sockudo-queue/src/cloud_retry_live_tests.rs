use sockudo_core::error::Error;
use sockudo_core::options::QueueReliabilityConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobPayload};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn reliability() -> QueueReliabilityConfig {
    QueueReliabilityConfig {
        worker_prefetch: 1,
        retry_base_delay_ms: 1000,
        retry_max_delay_ms: 2000,
        retry_jitter: 0.0,
        ..Default::default()
    }
}

async fn paced(manager: &dyn QueueInterface, name: &str, minimum_span_ms: u64) {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let observed = attempts.clone();
    manager
        .process_queue(
            name,
            Box::new(move |job| {
                assert_eq!(job.app_id, "synthetic-retry");
                let mut attempts = observed.lock().unwrap();
                attempts.push(Instant::now());
                let attempt = attempts.len();
                Box::pin(async move {
                    if attempt < 3 {
                        Err(Error::Queue("synthetic retry failure".into()))
                    } else {
                        Ok(())
                    }
                })
            }),
        )
        .await
        .unwrap();
    manager
        .add_to_queue(
            name,
            JobData {
                job_id: Some("synthetic-retry-job".into()),
                app_id: "synthetic-retry".into(),
                app_key: "synthetic-key".into(),
                app_secret: "synthetic-secret".into(),
                original_signature: String::new(),
                payload: JobPayload {
                    time_ms: 1,
                    events: vec![sonic_rs::json!({"name":"test"})],
                },
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(45), async {
        while attempts.lock().unwrap().len() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let elapsed = {
        let times = attempts.lock().unwrap();
        assert_eq!(
            times.len(),
            3,
            "exactly two failed attempts and one success"
        );
        times[2].duration_since(times[0])
    };
    assert!(
        elapsed >= Duration::from_millis(minimum_span_ms),
        "retry amplification: {elapsed:?}"
    );
    manager.disconnect().await.unwrap();
    println!(
        "attempts=3 successes=1 retry_span_ms={}",
        elapsed.as_millis()
    );
}

#[cfg(feature = "sqs")]
#[tokio::test]
#[ignore = "requires SOCKUDO_SQS_TEST_ENDPOINT and synthetic AWS credentials"]
async fn sqs_emulator_receive_count_paces_retries() {
    use aws_sdk_sqs::config::{Credentials, Region};
    let endpoint = std::env::var("SOCKUDO_SQS_TEST_ENDPOINT").unwrap();
    let client = aws_sdk_sqs::Client::from_conf(
        aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "synthetic",
                "synthetic",
                None,
                None,
                "fixture",
            ))
            .endpoint_url(&endpoint)
            .build(),
    );
    let name = format!("retry-{}", uuid::Uuid::new_v4().simple());
    let url = client
        .create_queue()
        .queue_name(&name)
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    let manager = crate::SqsQueueManager::new_with_reliability(
        sockudo_core::options::SqsQueueConfig {
            endpoint_url: Some(endpoint),
            region: "us-east-1".into(),
            visibility_timeout: 1,
            wait_time_seconds: 1,
            concurrency: 1,
            max_messages: 1,
            fifo: false,
            ..Default::default()
        },
        reliability(),
    )
    .await
    .unwrap();
    paced(&manager, &name, 2800).await;
    let remaining = client
        .receive_message()
        .queue_url(&url)
        .wait_time_seconds(1)
        .send()
        .await
        .unwrap();
    assert!(
        remaining.messages().is_empty(),
        "successful source must be acknowledged"
    );
    client.delete_queue().queue_url(url).send().await.unwrap();
}

#[cfg(feature = "google-pubsub")]
#[tokio::test]
#[ignore = "requires SOCKUDO_PUBSUB_TEST_ENDPOINT"]
async fn pubsub_emulator_existing_unpaced_subscription_uses_bounded_retry_delay() {
    pubsub_retry_case(true).await;
}

#[cfg(feature = "google-pubsub")]
#[tokio::test]
#[ignore = "requires SOCKUDO_PUBSUB_TEST_ENDPOINT with retry policy support"]
async fn pubsub_emulator_new_subscription_installs_and_honors_retry_policy() {
    pubsub_retry_case(false).await;
}

#[cfg(feature = "google-pubsub")]
async fn pubsub_retry_case(existing_subscription: bool) {
    use google_cloud_auth::credentials::anonymous::Builder;
    use google_cloud_pubsub::client::{SubscriptionAdmin, TopicAdmin};
    let host = std::env::var("SOCKUDO_PUBSUB_TEST_ENDPOINT").unwrap();
    let endpoint = format!("http://{host}");
    let topics = TopicAdmin::builder()
        .with_endpoint(&endpoint)
        .with_credentials(Builder::new().build())
        .build()
        .await
        .unwrap();
    let subscriptions = SubscriptionAdmin::builder()
        .with_endpoint(&endpoint)
        .with_credentials(Builder::new().build())
        .build()
        .await
        .unwrap();
    let prefix = format!("retry-{}", uuid::Uuid::new_v4().simple());
    let topic = format!("projects/synthetic-audit/topics/{prefix}-queue-work");
    let subscription =
        format!("projects/synthetic-audit/subscriptions/{prefix}-queue-workers-work");
    topics.create_topic().set_name(&topic).send().await.unwrap();
    if existing_subscription {
        subscriptions
            .create_subscription()
            .set_name(&subscription)
            .set_topic(&topic)
            .send()
            .await
            .unwrap();
    }
    let manager = crate::GooglePubSubQueueManager::new_with_reliability(
        sockudo_core::options::GooglePubSubAdapterConfig {
            project_id: "synthetic-audit".into(),
            prefix,
            emulator_host: Some(host),
            ..Default::default()
        },
        reliability(),
    )
    .await
    .unwrap();
    paced(&manager, "work", 1800).await;
    let policy = subscriptions
        .get_subscription()
        .set_subscription(&subscription)
        .send()
        .await
        .unwrap();
    if existing_subscription {
        assert!(
            policy.retry_policy.is_none(),
            "existing subscription policy remains externally owned"
        );
    } else {
        let retry = policy.retry_policy.expect("new subscription retry policy");
        let minimum = retry.minimum_backoff.unwrap();
        let maximum = retry.maximum_backoff.unwrap();
        assert_eq!((minimum.seconds(), minimum.nanos()), (1, 0));
        assert_eq!((maximum.seconds(), maximum.nanos()), (2, 0));
    }
    subscriptions
        .delete_subscription()
        .set_subscription(subscription)
        .send()
        .await
        .unwrap();
    topics.delete_topic().set_topic(topic).send().await.unwrap();
}
