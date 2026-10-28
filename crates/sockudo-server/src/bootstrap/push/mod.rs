#[cfg(feature = "push-apns")]
mod apns_channels;
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
#[cfg(all(feature = "push-apns", feature = "monolith"))]
pub(crate) use apns_channels::apns_provider_http_options;
#[cfg(feature = "push-apns")]
pub(crate) use apns_channels::create_apns_channel_manager;
pub(crate) use capability::{PushAdmissionRejection, PushAdmissionSnapshot};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::ServerOptions;
#[cfg(feature = "push")]
use sockudo_core::options::{PushQueueDriver, PushStorageDriver};
#[cfg(feature = "push")]
use sockudo_core::queue::QueueBackendKind;
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
    let queue_backend = queue_manager.backend();
    if let Some(expected) = push_queue_driver_backend(&config.push.queue_driver)
        && !push_queue_backend_matches(expected, queue_backend)
    {
        return Err(Error::Internal(format!(
            "push.queue_driver={} does not match the {} queue backend built from queue.driver: push queue work runs on the shared [queue] backend, so set push.queue_driver (PUSH_QUEUE_DRIVER) and queue.driver (QUEUE_DRIVER) to the same driver",
            expected.as_str(),
            queue_backend.as_str()
        )));
    }
    if !queue_manager.capabilities().delayed_delivery {
        return Err(Error::Internal(if queue_backend == QueueBackendKind::Sqs {
            "push queue cannot use an SQS FIFO queue: FIFO queues have no per-message delay, which push needs for scheduled sends and retries; set queue.sqs.fifo=false (QUEUE_SQS_FIFO=false)"
                    .to_owned()
        } else {
            format!(
                "push queue cannot use the {} queue backend: it has no per-message delayed delivery, which push needs for scheduled sends and retries; use redis, redis-cluster, sqs (standard queue) or pulsar",
                queue_backend.as_str()
            )
        }));
    }
    Ok(Arc::new(QueueManagerPushQueue::new(
        backend,
        queue_manager,
        queue::QueueManagerPushQueueStageOwnership::from_config(config, admission),
    )))
}

/// The queue backend a `push.queue_driver` names; `None` for the node-local memory queue,
/// which does not use the queue manager.
#[cfg(feature = "push")]
fn push_queue_driver_backend(driver: &PushQueueDriver) -> Option<QueueBackendKind> {
    Some(match driver {
        PushQueueDriver::Memory => return None,
        PushQueueDriver::Redis => QueueBackendKind::Redis,
        PushQueueDriver::RedisCluster => QueueBackendKind::RedisCluster,
        PushQueueDriver::Nats => QueueBackendKind::Nats,
        PushQueueDriver::Pulsar => QueueBackendKind::Pulsar,
        PushQueueDriver::RabbitMq => QueueBackendKind::RabbitMq,
        PushQueueDriver::GooglePubsub => QueueBackendKind::GooglePubSub,
        PushQueueDriver::Kafka => QueueBackendKind::Kafka,
        PushQueueDriver::Iggy => QueueBackendKind::Iggy,
        PushQueueDriver::Sqs => QueueBackendKind::Sqs,
        PushQueueDriver::Sns => QueueBackendKind::Sns,
    })
}

/// `queue.driver = "redis"` reports `redis-sentinel` when Sentinel is configured.
#[cfg(feature = "push")]
fn push_queue_backend_matches(expected: QueueBackendKind, actual: QueueBackendKind) -> bool {
    expected == actual
        || (expected == QueueBackendKind::Redis && actual == QueueBackendKind::RedisSentinel)
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

    /// A queue driver that only reports a backend and its delayed-delivery capability.
    struct CapabilityOnlyQueueDriver {
        backend: QueueBackendKind,
        delayed_delivery: bool,
    }

    #[async_trait::async_trait]
    impl sockudo_core::queue::QueueInterface for CapabilityOnlyQueueDriver {
        async fn add_to_queue(
            &self,
            _queue_name: &str,
            _data: sockudo_core::webhook_types::JobData,
        ) -> Result<()> {
            Ok(())
        }

        async fn process_queue(
            &self,
            _queue_name: &str,
            _callback: sockudo_core::webhook_types::JobProcessorFnAsync,
        ) -> Result<()> {
            Ok(())
        }

        async fn disconnect(&self) -> Result<()> {
            Ok(())
        }

        async fn check_health(&self) -> Result<()> {
            Ok(())
        }

        fn backend(&self) -> QueueBackendKind {
            self.backend
        }

        fn capabilities(&self) -> sockudo_core::queue::QueueCapabilities {
            sockudo_core::queue::QueueCapabilities {
                delayed_delivery: self.delayed_delivery,
                ..Default::default()
            }
        }
    }

    async fn create_push_queue_on(
        queue_driver: PushQueueDriver,
        backend: QueueBackendKind,
        delayed_delivery: bool,
    ) -> Result<sockudo_push::DynPushQueue> {
        let mut config = ServerOptions::default();
        config.push.queue_driver = queue_driver;
        let store: sockudo_push::DynPushStore = Arc::new(sockudo_push::MemoryPushStore::new());
        let admission = PushAdmissionSnapshot::from_config(&config, &store).await;
        let manager = Arc::new(QueueManager::new(Box::new(CapabilityOnlyQueueDriver {
            backend,
            delayed_delivery,
        })));
        create_push_queue(&config, Some(manager), &admission)
    }

    #[cfg(feature = "redis")]
    #[tokio::test]
    async fn push_queue_driver_must_match_queue_driver() {
        let error = match create_push_queue_on(
            PushQueueDriver::Redis,
            QueueBackendKind::Memory,
            true,
        )
        .await
        {
            Ok(_) => panic!("push.queue_driver=redis on a memory queue should be rejected"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains("push.queue_driver=redis does not match the memory queue backend"),
            "{message}"
        );

        create_push_queue_on(PushQueueDriver::Redis, QueueBackendKind::Redis, true)
            .await
            .unwrap();
        create_push_queue_on(
            PushQueueDriver::Redis,
            QueueBackendKind::RedisSentinel,
            true,
        )
        .await
        .unwrap();
    }

    #[cfg(feature = "kafka")]
    #[tokio::test]
    async fn push_queue_rejects_backends_without_delayed_delivery() {
        let error = match create_push_queue_on(
            PushQueueDriver::Kafka,
            QueueBackendKind::Kafka,
            false,
        )
        .await
        {
            Ok(_) => panic!("a queue backend without delayed delivery should be rejected"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains("cannot use the kafka queue backend")
                && message.contains("no per-message delayed delivery"),
            "{message}"
        );
    }

    #[test]
    fn push_queue_driver_names_its_queue_backend() {
        for (driver, backend) in [
            (PushQueueDriver::Redis, QueueBackendKind::Redis),
            (
                PushQueueDriver::RedisCluster,
                QueueBackendKind::RedisCluster,
            ),
            (PushQueueDriver::Nats, QueueBackendKind::Nats),
            (PushQueueDriver::Pulsar, QueueBackendKind::Pulsar),
            (PushQueueDriver::RabbitMq, QueueBackendKind::RabbitMq),
            (
                PushQueueDriver::GooglePubsub,
                QueueBackendKind::GooglePubSub,
            ),
            (PushQueueDriver::Kafka, QueueBackendKind::Kafka),
            (PushQueueDriver::Iggy, QueueBackendKind::Iggy),
            (PushQueueDriver::Sqs, QueueBackendKind::Sqs),
            (PushQueueDriver::Sns, QueueBackendKind::Sns),
        ] {
            assert_eq!(push_queue_driver_backend(&driver), Some(backend));
        }
        assert_eq!(push_queue_driver_backend(&PushQueueDriver::Memory), None);
        assert!(!push_queue_backend_matches(
            QueueBackendKind::RedisCluster,
            QueueBackendKind::Redis
        ));
    }
}
