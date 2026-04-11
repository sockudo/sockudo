#[cfg(feature = "google-pubsub")]
pub mod google_pubsub_queue_manager;
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
#[cfg(feature = "redis-cluster")]
pub mod redis_cluster_queue_manager;
#[cfg(feature = "redis")]
pub mod redis_queue_manager;
#[cfg(feature = "sns")]
pub mod sns_queue_manager;
#[cfg(feature = "sqs")]
pub mod sqs_queue_manager;

#[cfg(feature = "google-pubsub")]
pub use google_pubsub_queue_manager::GooglePubSubQueueManager;
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

/// Type alias for the Arc'd async job processor callback used across queue managers
pub(crate) type ArcJobProcessorFn = Arc<
    Box<
        dyn Fn(JobData) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
    >,
>;
