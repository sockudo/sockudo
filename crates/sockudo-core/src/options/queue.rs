use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisClusterQueueConfig {
    pub concurrency: u32,
    pub prefix: Option<String>,
    pub nodes: Vec<String>,
    pub request_timeout_ms: u64,
}

impl Default for RedisClusterQueueConfig {
    fn default() -> Self {
        Self {
            concurrency: 5,
            prefix: Some("sockudo_queue:".to_string()),
            nodes: vec!["redis://127.0.0.1:6379".to_string()],
            request_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SqsQueueConfig {
    pub region: String,
    pub queue_url_prefix: Option<String>,
    pub visibility_timeout: i32,
    pub endpoint_url: Option<String>,
    pub max_messages: i32,
    pub wait_time_seconds: i32,
    pub concurrency: u32,
    pub fifo: bool,
    pub message_group_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SnsQueueConfig {
    pub region: String,
    pub topic_arn: String,
    pub endpoint_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct QueueConfig {
    pub driver: QueueDriver,
    pub reliability: QueueReliabilityConfig,
    pub redis: RedisQueueConfig,
    pub redis_cluster: RedisClusterQueueConfig,
    pub nats: NatsAdapterConfig,
    pub pulsar: PulsarAdapterConfig,
    pub rabbitmq: RabbitMqAdapterConfig,
    pub google_pubsub: GooglePubSubAdapterConfig,
    pub kafka: KafkaAdapterConfig,
    pub iggy: IggyConfig,
    pub sqs: SqsQueueConfig,
    pub sns: SnsQueueConfig,
}

/// Delivery guarantees shared by all queue backends. Individual brokers may
/// enforce stricter platform limits, exposed through queue capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueReliabilityConfig {
    /// Maximum deliveries before a job moves to the dead-letter queue.
    pub max_attempts: u32,
    /// Initial retry backoff.
    pub retry_base_delay_ms: u64,
    /// Maximum retry backoff.
    pub retry_max_delay_ms: u64,
    /// Symmetric retry jitter as a fraction in the inclusive range 0.0..=1.0.
    pub retry_jitter: f64,
    /// Lease duration for an in-flight delivery.
    pub lease_duration_ms: u64,
    /// Interval used to renew a live delivery lease.
    pub lease_renew_interval_ms: u64,
    /// Maximum number of expired deliveries reclaimed per atomic claim.
    pub stalled_batch_size: u32,
    /// Worker wait/poll interval. Redis workers use blocking notification plus
    /// this interval as a recovery and delayed-job promotion bound.
    pub worker_poll_interval_ms: u64,
    /// Maximum leased jobs buffered ahead of each Redis callback worker. This
    /// bounds memory and redelivery exposure while allowing batched I/O.
    pub worker_prefetch: usize,
    /// Grace period for in-flight processors during shutdown.
    pub shutdown_timeout_ms: u64,
    /// Completed job metadata retained per logical queue. Zero removes on ack.
    pub completed_retention: u32,
    /// Failed/dead-letter jobs retained per logical queue.
    pub failed_retention: u32,
    /// Approximate maximum Redis queue lifecycle events retained per queue.
    pub event_retention: u32,
    /// Lifetime of an application deduplication key.
    pub deduplication_ttl_ms: u64,
    /// Maximum jobs accepted by the in-memory backend per logical queue.
    pub memory_capacity: usize,
    /// Maximum jobs encoded into one backend-native/atomic enqueue batch.
    pub max_batch_size: usize,
}

impl Default for QueueReliabilityConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            retry_base_delay_ms: 1_000,
            retry_max_delay_ms: 60_000,
            retry_jitter: 0.2,
            lease_duration_ms: 30_000,
            lease_renew_interval_ms: 10_000,
            stalled_batch_size: 100,
            worker_poll_interval_ms: 500,
            worker_prefetch: 16,
            shutdown_timeout_ms: 30_000,
            completed_retention: 1_000,
            failed_retention: 10_000,
            event_retention: 10_000,
            deduplication_ttl_ms: 300_000,
            memory_capacity: 100_000,
            max_batch_size: 1_000,
        }
    }
}

impl QueueReliabilityConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_attempts == 0 {
            return Err("queue.reliability.max_attempts must be greater than zero".to_string());
        }
        if self.lease_duration_ms == 0 {
            return Err(
                "queue.reliability.lease_duration_ms must be greater than zero".to_string(),
            );
        }
        if self.lease_renew_interval_ms == 0
            || self.lease_renew_interval_ms >= self.lease_duration_ms
        {
            return Err(
                "queue.reliability.lease_renew_interval_ms must be greater than zero and less than lease_duration_ms"
                    .to_string(),
            );
        }
        if self.retry_base_delay_ms > self.retry_max_delay_ms {
            return Err(
                "queue.reliability.retry_base_delay_ms must not exceed retry_max_delay_ms"
                    .to_string(),
            );
        }
        if !(0.0..=1.0).contains(&self.retry_jitter) {
            return Err("queue.reliability.retry_jitter must be in 0.0..=1.0".to_string());
        }
        if self.stalled_batch_size == 0
            || self.worker_prefetch == 0
            || self.memory_capacity == 0
            || self.max_batch_size == 0
        {
            return Err(
                "queue reliability batch size and memory capacity must be greater than zero"
                    .to_string(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisQueueConfig {
    pub concurrency: u32,
    pub prefix: Option<String>,
    pub url_override: Option<String>,
    pub cluster_mode: bool,
    pub response_timeout_ms: u64,
}

impl Default for SqsQueueConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            queue_url_prefix: None,
            visibility_timeout: 30,
            endpoint_url: None,
            max_messages: 10,
            wait_time_seconds: 5,
            concurrency: 5,
            fifo: false,
            message_group_id: Some("default".to_string()),
        }
    }
}

impl Default for SnsQueueConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            topic_arn: String::new(),
            endpoint_url: None,
        }
    }
}

impl Default for RedisQueueConfig {
    fn default() -> Self {
        Self {
            concurrency: 5,
            prefix: Some("sockudo_queue:".to_string()),
            url_override: None,
            cluster_mode: false,
            response_timeout_ms: 5000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_queue_default_response_timeout_is_failover_budget() {
        assert_eq!(RedisQueueConfig::default().response_timeout_ms, 5000);
        assert_eq!(RedisClusterQueueConfig::default().request_timeout_ms, 5000);
    }

    #[test]
    fn default_reliability_config_is_valid() {
        QueueReliabilityConfig::default().validate().unwrap();
    }

    #[test]
    fn lease_renewal_must_precede_expiry() {
        let config = QueueReliabilityConfig {
            lease_duration_ms: 1_000,
            lease_renew_interval_ms: 1_000,
            ..QueueReliabilityConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn retry_jitter_is_bounded() {
        let config = QueueReliabilityConfig {
            retry_jitter: 1.01,
            ..QueueReliabilityConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn worker_prefetch_must_be_nonzero() {
        let config = QueueReliabilityConfig {
            worker_prefetch: 0,
            ..QueueReliabilityConfig::default()
        };
        assert!(config.validate().is_err());
    }
}
