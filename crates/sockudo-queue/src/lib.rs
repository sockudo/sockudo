#[cfg(any(
    feature = "google-pubsub",
    feature = "iggy",
    feature = "kafka",
    feature = "nats",
    feature = "pulsar",
    feature = "rabbitmq",
    feature = "sns"
))]
mod broker_batch;
#[cfg(feature = "google-pubsub")]
pub mod google_pubsub_queue_manager;
#[cfg(feature = "iggy")]
pub mod iggy_queue_manager;
#[cfg(feature = "kafka")]
pub mod kafka_queue_manager;
pub mod manager;
pub mod memory_queue_manager;
#[cfg(feature = "nats")]
pub mod nats_queue_manager;
#[cfg(feature = "pulsar")]
pub mod pulsar_queue_manager;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq_queue_manager;
#[cfg(feature = "redis")]
mod redis_backend;
#[cfg(feature = "redis-cluster")]
pub mod redis_cluster_queue_manager;
#[cfg(feature = "redis")]
mod redis_connection;
#[cfg(feature = "redis")]
pub mod redis_queue_manager;
#[cfg(feature = "redis")]
mod redis_scripts;
#[cfg(feature = "sns")]
pub mod sns_queue_manager;
#[cfg(feature = "sqs")]
pub mod sqs_queue_manager;
#[cfg(any(
    feature = "google-pubsub",
    feature = "iggy",
    feature = "kafka",
    feature = "nats",
    feature = "pulsar",
    feature = "rabbitmq"
))]
mod worker_registry;

#[cfg(feature = "google-pubsub")]
pub use google_pubsub_queue_manager::GooglePubSubQueueManager;
#[cfg(feature = "iggy")]
pub use iggy_queue_manager::IggyQueueManager;
#[cfg(feature = "kafka")]
pub use kafka_queue_manager::KafkaQueueManager;
pub use manager::{QueueManager, QueueManagerFactory};
pub use memory_queue_manager::MemoryQueueManager;
#[cfg(feature = "nats")]
pub use nats_queue_manager::NatsJetStreamQueueManager;
#[cfg(feature = "pulsar")]
pub use pulsar_queue_manager::PulsarQueueManager;
#[cfg(feature = "rabbitmq")]
pub use rabbitmq_queue_manager::RabbitMqQueueManager;
#[cfg(feature = "redis-cluster")]
pub use redis_cluster_queue_manager::RedisClusterQueueManager;
#[cfg(feature = "redis")]
pub use redis_queue_manager::RedisQueueManager;
#[cfg(feature = "sns")]
pub use sns_queue_manager::SnsQueueManager;
#[cfg(feature = "sqs")]
pub use sqs_queue_manager::SqsQueueManager;

use sockudo_core::error::Result;
use sockudo_core::webhook_types::JobData;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[cfg(all(test, any(feature = "sqs", feature = "google-pubsub")))]
mod cloud_retry_live_tests;

/// Type alias for the Arc'd async job processor callback used across queue managers
pub(crate) type ArcJobProcessorFn = Arc<
    Box<
        dyn Fn(JobData) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
    >,
>;

#[cfg(any(
    feature = "rabbitmq",
    feature = "iggy",
    feature = "sqs",
    feature = "kafka",
    feature = "google-pubsub"
))]
fn broker_retry_delay(
    config: &sockudo_core::options::QueueReliabilityConfig,
    attempt: u32,
) -> std::time::Duration {
    let base = config
        .retry_base_delay_ms
        .saturating_mul(1_u64 << attempt.saturating_sub(1).min(63))
        .min(config.retry_max_delay_ms);
    let random = (uuid::Uuid::new_v4().as_u128() as u64) as f64 / u64::MAX as f64;
    let multiplier = 1.0 + (random * 2.0 - 1.0) * config.retry_jitter;
    std::time::Duration::from_millis(
        ((base as f64 * multiplier) as u64)
            .min(config.retry_max_delay_ms)
            .max(1),
    )
}
