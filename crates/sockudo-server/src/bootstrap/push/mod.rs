mod capability;
mod queue;
mod stores;
#[cfg(all(feature = "push", feature = "monolith"))]
pub(crate) mod workers;

use self::queue::QueueManagerPushQueue;
use self::stores::{
    create_dynamodb_push_store, create_mysql_push_store, create_postgres_push_store,
    create_scylladb_push_store, create_surrealdb_push_store,
};
pub(crate) use capability::{PushAdmissionRejection, PushAdmissionSnapshot};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::ServerOptions;
#[cfg(feature = "push")]
use sockudo_core::options::{PushQueueDriver, PushStorageDriver};
use sockudo_webhook::integration::QueueManager;
use std::sync::Arc;
use tracing::warn;

#[cfg(feature = "push")]
fn production_memory_drivers_allowed(config: &ServerOptions) -> bool {
    !config.mode.eq_ignore_ascii_case("production") || config.push.allow_memory_drivers
}

#[cfg(feature = "push")]
pub(crate) async fn create_push_store(
    config: &ServerOptions,
) -> Result<sockudo_push::DynPushStore> {
    match config.push.storage_driver {
        PushStorageDriver::Memory => {
            if !production_memory_drivers_allowed(config) {
                return Err(Error::Internal(
                    "push storage driver memory is unsafe in production; set push.allow_memory_drivers=true or PUSH_ALLOW_MEMORY_DRIVERS=true only for local/dev acknowledgment"
                        .to_owned(),
                ));
            }
            if config.mode.eq_ignore_ascii_case("production") {
                warn!(
                    "push storage driver is memory; intended for tests and local development only"
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
    admission: &PushAdmissionSnapshot,
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
        if !production_memory_drivers_allowed(config) {
            return Err(Error::Internal(
                "push queue driver memory is unsafe in production; set push.allow_memory_drivers=true or PUSH_ALLOW_MEMORY_DRIVERS=true only for local/dev acknowledgment"
                    .to_owned(),
            ));
        }
        return Ok(Arc::new(sockudo_push::MemoryPushQueue::new()));
    }
    let queue_manager = queue_manager.ok_or_else(|| {
        Error::Internal(format!(
            "push queue driver {:?} requires an initialized queue manager",
            backend
        ))
    })?;
    Ok(Arc::new(QueueManagerPushQueue::new(
        backend,
        queue_manager,
        queue::QueueManagerPushQueueStageOwnership::from_config(config, admission),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn production_memory_push_store_requires_explicit_allow() {
        let mut config = ServerOptions {
            mode: "production".to_owned(),
            ..Default::default()
        };
        config.push.storage_driver = PushStorageDriver::Memory;
        config.push.allow_memory_drivers = false;

        let error = match create_push_store(&config).await {
            Ok(_) => panic!("memory push store should be rejected in production"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("memory is unsafe in production"));
    }

    #[tokio::test]
    async fn production_memory_push_queue_requires_explicit_allow() {
        let mut config = ServerOptions {
            mode: "production".to_owned(),
            ..Default::default()
        };
        config.push.queue_driver = PushQueueDriver::Memory;
        config.push.allow_memory_drivers = false;

        let store: sockudo_push::DynPushStore = Arc::new(sockudo_push::MemoryPushStore::new());
        let admission = PushAdmissionSnapshot::from_config(&config, &store).await;
        let error = match create_push_queue(&config, None, &admission) {
            Ok(_) => panic!("memory push queue should be rejected in production"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("memory is unsafe in production"));
    }

    #[tokio::test]
    async fn memory_push_drivers_can_be_explicitly_allowed_for_tests() {
        let mut config = ServerOptions {
            mode: "production".to_owned(),
            ..Default::default()
        };
        config.push.storage_driver = PushStorageDriver::Memory;
        config.push.queue_driver = PushQueueDriver::Memory;
        config.push.allow_memory_drivers = true;

        let store = create_push_store(&config).await.unwrap();
        let admission = PushAdmissionSnapshot::from_config(&config, &store).await;
        create_push_queue(&config, None, &admission).unwrap();
    }
}
