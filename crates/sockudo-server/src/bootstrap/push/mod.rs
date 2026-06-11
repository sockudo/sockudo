mod queue;
mod stores;
#[cfg(all(feature = "push", feature = "monolith"))]
pub(crate) mod workers;

use self::queue::QueueManagerPushQueue;
use self::stores::{
    create_dynamodb_push_store, create_mysql_push_store, create_postgres_push_store,
    create_scylladb_push_store, create_surrealdb_push_store,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::ServerOptions;
#[cfg(feature = "push")]
use sockudo_core::options::{PushQueueDriver, PushStorageDriver};
use sockudo_webhook::integration::QueueManager;
use std::sync::Arc;
use tracing::warn;

#[cfg(feature = "push")]
pub(crate) async fn create_push_store(
    config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    match config.push.storage_driver {
        PushStorageDriver::Memory => {
            if config.mode.eq_ignore_ascii_case("production") {
                warn!(
                    "Push storage driver is memory; this is intended only for tests and local development"
                );
            }
            Ok(Arc::new(sockudo_push::MemoryPushStore::new()))
        }
        PushStorageDriver::Postgres => create_postgres_push_store(config).await,
        PushStorageDriver::Mysql => create_mysql_push_store(config).await,
        PushStorageDriver::DynamoDb => create_dynamodb_push_store(config).await,
        PushStorageDriver::SurrealDb => create_surrealdb_push_store(config).await,
        PushStorageDriver::ScyllaDb => create_scylladb_push_store(config).await,
    }
}

#[cfg(feature = "push")]
pub(crate) fn create_push_queue(
    config: &ServerOptions,
    queue_manager: Option<Arc<QueueManager>>,
) -> Result<sockudo_push::DynPushQueue> {
    let backend = match config.push.queue_driver {
        PushQueueDriver::Memory => sockudo_push::PushQueueBackendKind::Memory,
        PushQueueDriver::Redis => sockudo_push::PushQueueBackendKind::Redis,
        PushQueueDriver::RedisCluster => sockudo_push::PushQueueBackendKind::RedisCluster,
        PushQueueDriver::Nats => sockudo_push::PushQueueBackendKind::Nats,
        PushQueueDriver::Pulsar => sockudo_push::PushQueueBackendKind::Pulsar,
        PushQueueDriver::RabbitMq => sockudo_push::PushQueueBackendKind::RabbitMq,
        PushQueueDriver::GooglePubsub => sockudo_push::PushQueueBackendKind::GooglePubsub,
        PushQueueDriver::Kafka => sockudo_push::PushQueueBackendKind::Kafka,
        PushQueueDriver::Iggy => sockudo_push::PushQueueBackendKind::Iggy,
        PushQueueDriver::Sqs => sockudo_push::PushQueueBackendKind::Sqs,
        PushQueueDriver::Sns => sockudo_push::PushQueueBackendKind::Sns,
    };
    backend
        .startup_check()
        .map_err(|e| Error::Internal(format!("Failed to create push queue: {e}")))?;
    if backend == sockudo_push::PushQueueBackendKind::Memory {
        return Ok(Arc::new(sockudo_push::MemoryPushQueue::new()));
    }
    let queue_manager = queue_manager.ok_or_else(|| {
        Error::Internal(format!(
            "push queue driver {:?} requires an initialized queue manager",
            backend
        ))
    })?;
    Ok(Arc::new(QueueManagerPushQueue::new(backend, queue_manager)))
}
