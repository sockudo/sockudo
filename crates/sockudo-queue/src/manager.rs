use sockudo_core::error::Result;
use sockudo_core::options::{QueueReliabilityConfig, SentinelSpec};

#[cfg(feature = "google-pubsub")]
use sockudo_core::options::GooglePubSubAdapterConfig;
#[cfg(feature = "iggy")]
use sockudo_core::options::IggyConfig;
#[cfg(feature = "kafka")]
use sockudo_core::options::KafkaAdapterConfig;
#[cfg(feature = "nats")]
use sockudo_core::options::NatsAdapterConfig;
#[cfg(feature = "pulsar")]
use sockudo_core::options::PulsarAdapterConfig;
#[cfg(feature = "rabbitmq")]
use sockudo_core::options::RabbitMqAdapterConfig;
#[cfg(feature = "sns")]
use sockudo_core::options::SnsQueueConfig;
#[cfg(feature = "sqs")]
use sockudo_core::options::SqsQueueConfig;
use sockudo_core::queue::{
    QueueCapabilities, QueueHealth, QueueInterface, QueueJobId, QueueJobOptions, QueueJobRequest,
    QueueStats,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};

#[cfg(feature = "google-pubsub")]
use crate::google_pubsub_queue_manager::GooglePubSubQueueManager;
#[cfg(feature = "iggy")]
use crate::iggy_queue_manager::IggyQueueManager;
#[cfg(feature = "kafka")]
use crate::kafka_queue_manager::KafkaQueueManager;
use crate::memory_queue_manager::MemoryQueueManager;
#[cfg(feature = "nats")]
use crate::nats_queue_manager::NatsJetStreamQueueManager;
#[cfg(feature = "pulsar")]
use crate::pulsar_queue_manager::PulsarQueueManager;
#[cfg(feature = "rabbitmq")]
use crate::rabbitmq_queue_manager::RabbitMqQueueManager;
#[cfg(feature = "redis-cluster")]
use crate::redis_cluster_queue_manager::RedisClusterQueueManager;
#[cfg(feature = "redis")]
use crate::redis_queue_manager::RedisQueueManager;
#[cfg(feature = "sns")]
use crate::sns_queue_manager::SnsQueueManager;
#[cfg(feature = "sqs")]
use crate::sqs_queue_manager::SqsQueueManager;
use tracing::*;

/// General Queue Manager interface wrapper
pub struct QueueManagerFactory;

impl QueueManagerFactory {
    /// Creates a queue manager instance based on the specified driver.
    #[allow(unused_variables)]
    pub async fn create(
        driver: &str,
        redis_url: Option<&str>,
        prefix: Option<&str>,
        concurrency: Option<usize>,
        response_timeout_ms: Option<u64>,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_with_reliability(
            driver,
            redis_url,
            prefix,
            concurrency,
            response_timeout_ms,
            QueueReliabilityConfig::default(),
            None,
        )
        .await
    }

    /// Creates a queue manager with explicit Queue v2 reliability and optional
    /// native Redis Sentinel topology configuration.
    #[allow(unused_variables)]
    pub async fn create_with_reliability(
        driver: &str,
        redis_url: Option<&str>,
        prefix: Option<&str>,
        concurrency: Option<usize>,
        response_timeout_ms: Option<u64>,
        reliability: QueueReliabilityConfig,
        sentinel: Option<SentinelSpec>,
    ) -> Result<Box<dyn QueueInterface>> {
        reliability
            .validate()
            .map_err(sockudo_core::error::Error::Config)?;
        match driver {
            #[cfg(feature = "redis")]
            "redis" => {
                let url = redis_url.unwrap_or("redis://127.0.0.1:6379/");
                let prefix_str = prefix.unwrap_or("sockudo");
                let concurrency_val = concurrency.unwrap_or(5);
                let response_timeout_ms = response_timeout_ms.unwrap_or(5000);
                info!(
                    "Creating Redis queue manager (Prefix: {}, Concurrency: {}, Response timeout: {}ms)",
                    prefix_str, concurrency_val, response_timeout_ms
                );
                let manager = RedisQueueManager::new_with_config(
                    url,
                    sentinel,
                    prefix_str,
                    concurrency_val,
                    response_timeout_ms,
                    reliability,
                )
                .await?;
                Ok(Box::new(manager))
            }
            #[cfg(feature = "redis-cluster")]
            "redis-cluster" => {
                let nodes_str = redis_url.unwrap_or(
                    "redis://127.0.0.1:7000,redis://127.0.0.1:7001,redis://127.0.0.1:7002",
                );
                let cluster_nodes: Vec<String> =
                    nodes_str.split(',').map(|s| s.trim().to_string()).collect();
                let prefix_str = prefix.unwrap_or("sockudo");
                let concurrency_val = concurrency.unwrap_or(5);
                let response_timeout_ms = response_timeout_ms.unwrap_or(5000);

                info!(
                    "Creating Redis Cluster queue manager (Prefix: {}, Concurrency: {}, Request timeout: {}ms)",
                    prefix_str, concurrency_val, response_timeout_ms
                );
                let manager = RedisClusterQueueManager::new_with_config(
                    cluster_nodes,
                    prefix_str,
                    concurrency_val,
                    response_timeout_ms,
                    reliability,
                )
                .await?;
                Ok(Box::new(manager))
            }
            #[cfg(feature = "nats")]
            "nats" => {
                Err(sockudo_core::error::Error::Config(
                    "NATS queue manager requires typed configuration via create_nats".to_string(),
                ))
            }
            "memory" => {
                info!("{}", "Creating Memory queue manager".to_string());
                let manager = MemoryQueueManager::new_with_config(reliability)?;
                manager.start_processing();
                Ok(Box::new(manager))
            }
            #[cfg(not(feature = "redis"))]
            "redis" => {
                Err(sockudo_core::error::Error::Config(
                    "Redis queue manager requested but the redis feature is not compiled in"
                        .to_string(),
                ))
            }
            #[cfg(not(feature = "redis-cluster"))]
            "redis-cluster" => {
                Err(sockudo_core::error::Error::Config(
                    "Redis Cluster queue manager requested but the redis-cluster feature is not compiled in"
                        .to_string(),
                ))
            }
            #[cfg(feature = "sqs")]
            "sqs" => {
                Err(sockudo_core::error::Error::Config(
                    "SQS queue manager requires typed configuration via create_sqs".to_string(),
                ))
            }
            #[cfg(not(feature = "sqs"))]
            "sqs" => {
                Err(sockudo_core::error::Error::Config(
                    "SQS queue manager requested but the sqs feature is not compiled in".to_string(),
                ))
            }
            #[cfg(feature = "sns")]
            "sns" => {
                Err(sockudo_core::error::Error::Config(
                    "SNS queue manager requires typed configuration via create_sns".to_string(),
                ))
            }
            #[cfg(not(feature = "sns"))]
            "sns" => {
                Err(sockudo_core::error::Error::Config(
                    "SNS queue manager requested but the sns feature is not compiled in".to_string(),
                ))
            }
            other => Err(sockudo_core::error::Error::Queue(format!(
                "Unsupported queue driver: {other}"
            ))),
        }
    }

    /// Creates an SQS queue manager instance with the given configuration.
    #[cfg(feature = "sqs")]
    pub async fn create_sqs(config: SqsQueueConfig) -> Result<Box<dyn QueueInterface>> {
        Self::create_sqs_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    #[cfg(feature = "sqs")]
    pub async fn create_sqs_with_reliability(
        config: SqsQueueConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        info!(
            "Creating SQS queue manager (Region: {}, Concurrency: {}, FIFO: {})",
            config.region, config.concurrency, config.fifo
        );
        if let Some(ref url_prefix) = config.queue_url_prefix {
            debug!("SQS queue URL prefix: {}", url_prefix);
        }
        let manager = SqsQueueManager::new_with_reliability(config, reliability).await?;
        Ok(Box::new(manager))
    }

    #[cfg(not(feature = "sqs"))]
    #[allow(unused_variables)]
    pub async fn create_sqs(
        config: sockudo_core::options::SqsQueueConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Err(sockudo_core::error::Error::Config(
            "SQS queue manager requested but the sqs feature is not compiled in".to_string(),
        ))
    }

    #[cfg(not(feature = "sqs"))]
    #[allow(unused_variables)]
    pub async fn create_sqs_with_reliability(
        config: sockudo_core::options::SqsQueueConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_sqs(config).await
    }

    #[cfg(feature = "sns")]
    pub async fn create_sns(config: SnsQueueConfig) -> Result<Box<dyn QueueInterface>> {
        Self::create_sns_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    #[cfg(feature = "sns")]
    pub async fn create_sns_with_reliability(
        config: SnsQueueConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        info!(
            "Creating SNS queue manager (Region: {}, Topic: {})",
            config.region, config.topic_arn
        );
        let manager = SnsQueueManager::new_with_reliability(config, reliability).await?;
        Ok(Box::new(manager))
    }

    #[cfg(not(feature = "sns"))]
    #[allow(unused_variables)]
    pub async fn create_sns(
        config: sockudo_core::options::SnsQueueConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Err(sockudo_core::error::Error::Config(
            "SNS queue manager requested but the sns feature is not compiled in".to_string(),
        ))
    }

    #[cfg(not(feature = "sns"))]
    #[allow(unused_variables)]
    pub async fn create_sns_with_reliability(
        config: sockudo_core::options::SnsQueueConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_sns(config).await
    }

    #[cfg(feature = "rabbitmq")]
    pub async fn create_rabbitmq(config: RabbitMqAdapterConfig) -> Result<Box<dyn QueueInterface>> {
        Self::create_rabbitmq_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    #[cfg(feature = "rabbitmq")]
    pub async fn create_rabbitmq_with_reliability(
        config: RabbitMqAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        let manager = RabbitMqQueueManager::new_with_reliability(config, reliability).await?;
        Ok(Box::new(manager))
    }

    #[cfg(not(feature = "rabbitmq"))]
    #[allow(unused_variables)]
    pub async fn create_rabbitmq(
        config: sockudo_core::options::RabbitMqAdapterConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Err(sockudo_core::error::Error::Config(
            "RabbitMQ queue manager requested but the rabbitmq feature is not compiled in"
                .to_string(),
        ))
    }

    #[cfg(not(feature = "rabbitmq"))]
    #[allow(unused_variables)]
    pub async fn create_rabbitmq_with_reliability(
        config: sockudo_core::options::RabbitMqAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_rabbitmq(config).await
    }

    #[cfg(feature = "kafka")]
    pub async fn create_kafka(config: KafkaAdapterConfig) -> Result<Box<dyn QueueInterface>> {
        Self::create_kafka_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    #[cfg(feature = "kafka")]
    pub async fn create_kafka_with_reliability(
        config: KafkaAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        let manager = KafkaQueueManager::new_with_reliability(config, reliability).await?;
        Ok(Box::new(manager))
    }

    #[cfg(not(feature = "kafka"))]
    #[allow(unused_variables)]
    pub async fn create_kafka(
        config: sockudo_core::options::KafkaAdapterConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Err(sockudo_core::error::Error::Config(
            "Kafka queue manager requested but the kafka feature is not compiled in".to_string(),
        ))
    }

    #[cfg(not(feature = "kafka"))]
    #[allow(unused_variables)]
    pub async fn create_kafka_with_reliability(
        config: sockudo_core::options::KafkaAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_kafka(config).await
    }

    #[cfg(feature = "iggy")]
    pub async fn create_iggy(config: IggyConfig) -> Result<Box<dyn QueueInterface>> {
        Self::create_iggy_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    #[cfg(feature = "iggy")]
    pub async fn create_iggy_with_reliability(
        config: IggyConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        let manager = IggyQueueManager::new_with_reliability(config, reliability).await?;
        Ok(Box::new(manager))
    }

    #[cfg(not(feature = "iggy"))]
    #[allow(unused_variables)]
    pub async fn create_iggy(
        config: sockudo_core::options::IggyConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Err(sockudo_core::error::Error::Config(
            "Apache Iggy queue manager requested but the iggy feature is not compiled in"
                .to_string(),
        ))
    }

    #[cfg(not(feature = "iggy"))]
    #[allow(unused_variables)]
    pub async fn create_iggy_with_reliability(
        config: sockudo_core::options::IggyConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_iggy(config).await
    }

    #[cfg(feature = "pulsar")]
    pub async fn create_pulsar(config: PulsarAdapterConfig) -> Result<Box<dyn QueueInterface>> {
        Self::create_pulsar_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    #[cfg(feature = "pulsar")]
    pub async fn create_pulsar_with_reliability(
        config: PulsarAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        let manager = PulsarQueueManager::new_with_reliability(config, reliability).await?;
        Ok(Box::new(manager))
    }

    #[cfg(not(feature = "pulsar"))]
    #[allow(unused_variables)]
    pub async fn create_pulsar(
        config: sockudo_core::options::PulsarAdapterConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Err(sockudo_core::error::Error::Config(
            "Pulsar queue manager requested but the pulsar feature is not compiled in".to_string(),
        ))
    }

    #[cfg(not(feature = "pulsar"))]
    #[allow(unused_variables)]
    pub async fn create_pulsar_with_reliability(
        config: sockudo_core::options::PulsarAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_pulsar(config).await
    }

    #[cfg(feature = "google-pubsub")]
    pub async fn create_google_pubsub(
        config: GooglePubSubAdapterConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_google_pubsub_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    #[cfg(feature = "google-pubsub")]
    pub async fn create_google_pubsub_with_reliability(
        config: GooglePubSubAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        let manager = GooglePubSubQueueManager::new_with_reliability(config, reliability).await?;
        Ok(Box::new(manager))
    }

    #[cfg(not(feature = "google-pubsub"))]
    #[allow(unused_variables)]
    pub async fn create_google_pubsub(
        config: sockudo_core::options::GooglePubSubAdapterConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Err(sockudo_core::error::Error::Config(
            "Google Pub/Sub queue manager requested but the google-pubsub feature is not compiled in"
                .to_string(),
        ))
    }

    #[cfg(not(feature = "google-pubsub"))]
    #[allow(unused_variables)]
    pub async fn create_google_pubsub_with_reliability(
        config: sockudo_core::options::GooglePubSubAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_google_pubsub(config).await
    }

    #[cfg(feature = "nats")]
    pub async fn create_nats(config: NatsAdapterConfig) -> Result<Box<dyn QueueInterface>> {
        Self::create_nats_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    #[cfg(feature = "nats")]
    pub async fn create_nats_with_reliability(
        config: NatsAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        let manager = NatsJetStreamQueueManager::new_with_reliability(config, reliability).await?;
        Ok(Box::new(manager))
    }

    #[cfg(not(feature = "nats"))]
    #[allow(unused_variables)]
    pub async fn create_nats(
        config: sockudo_core::options::NatsAdapterConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Err(sockudo_core::error::Error::Config(
            "NATS queue manager requested but the nats feature is not compiled in".to_string(),
        ))
    }

    #[cfg(not(feature = "nats"))]
    #[allow(unused_variables)]
    pub async fn create_nats_with_reliability(
        config: sockudo_core::options::NatsAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Box<dyn QueueInterface>> {
        Self::create_nats(config).await
    }
}

pub struct QueueManager {
    driver: Box<dyn QueueInterface>,
}

impl QueueManager {
    /// Creates a new QueueManager wrapping a specific driver implementation.
    pub fn new(driver: Box<dyn QueueInterface>) -> Self {
        Self { driver }
    }

    /// Adds data to the specified queue via the underlying driver.
    pub async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        self.driver.add_to_queue(queue_name, data).await
    }

    /// Enqueues a Queue v2 job with stable identity, delay, and deduplication controls.
    pub async fn enqueue(
        &self,
        queue_name: &str,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<QueueJobId> {
        self.driver.enqueue(queue_name, data, options).await
    }

    /// Adds multiple jobs to the specified queue via the underlying driver.
    pub async fn add_batch_to_queue(&self, queue_name: &str, data: Vec<JobData>) -> Result<()> {
        self.driver.add_batch_to_queue(queue_name, data).await
    }

    pub async fn enqueue_batch(
        &self,
        queue_name: &str,
        jobs: Vec<QueueJobRequest>,
    ) -> Result<Vec<QueueJobId>> {
        self.driver.enqueue_batch(queue_name, jobs).await
    }

    /// Registers a processor for the specified queue and starts processing (if applicable for the driver).
    pub async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> Result<()> {
        self.driver.process_queue(queue_name, callback).await
    }

    /// Disconnects the underlying driver (if necessary).
    pub async fn disconnect(&self) -> Result<()> {
        self.driver.disconnect().await
    }

    /// Checks the health of the underlying queue driver.
    pub async fn check_health(&self) -> Result<()> {
        self.driver.check_health().await
    }

    pub fn capabilities(&self) -> QueueCapabilities {
        self.driver.capabilities()
    }

    pub async fn health(&self) -> Result<QueueHealth> {
        self.driver.health().await
    }

    pub async fn stats(&self, queue_name: &str) -> Result<QueueStats> {
        self.driver.stats(queue_name).await
    }

    pub async fn replay_dead_letters(&self, queue_name: &str, limit: u32) -> Result<u64> {
        self.driver.replay_dead_letters(queue_name, limit).await
    }
}
