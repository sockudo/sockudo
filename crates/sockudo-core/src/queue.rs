//! Queue contracts shared by Sockudo's webhook, push, and broker backends.
//!
//! [`QueueInterface`] retains the original `JobData` convenience methods so rolling
//! upgrades and third-party backends keep compiling. New code should use
//! [`QueueInterface::enqueue`] with [`QueueJobOptions`] and inspect backend
//! capabilities before depending on optional broker features.

use crate::error::{Error, Result};
use crate::webhook_types::{JobData, JobProcessorFnAsync};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Stable identifier returned for an enqueued logical job.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QueueJobId(pub String);

impl QueueJobId {
    #[must_use]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl Default for QueueJobId {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-job controls understood by reliable queue backends.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueJobOptions {
    /// Caller-supplied stable ID. Supplying the same ID makes an ambiguous enqueue
    /// retry idempotent when the backend advertises `deduplication`.
    pub job_id: Option<String>,
    /// Optional application-level deduplication key.
    pub deduplication_key: Option<String>,
    /// Delay before the first delivery.
    pub delay_ms: u64,
    /// Override the queue's configured maximum delivery attempts.
    pub max_attempts: Option<u32>,
}

/// One item in a batch enqueue request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueJobRequest {
    pub data: JobData,
    #[serde(default)]
    pub options: QueueJobOptions,
}

/// Queue implementation selected at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueueBackendKind {
    Unknown,
    Memory,
    Redis,
    RedisSentinel,
    RedisCluster,
    Nats,
    RabbitMq,
    Kafka,
    Iggy,
    Pulsar,
    GooglePubSub,
    Sqs,
    Sns,
}

impl QueueBackendKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Memory => "memory",
            Self::Redis => "redis",
            Self::RedisSentinel => "redis-sentinel",
            Self::RedisCluster => "redis-cluster",
            Self::Nats => "nats",
            Self::RabbitMq => "rabbitmq",
            Self::Kafka => "kafka",
            Self::Iggy => "iggy",
            Self::Pulsar => "pulsar",
            Self::GooglePubSub => "google-pubsub",
            Self::Sqs => "sqs",
            Self::Sns => "sns",
        }
    }
}

/// Machine-readable backend behavior. Unsupported operations must fail instead
/// of silently degrading to a weaker backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueCapabilities {
    pub consume: bool,
    pub acknowledgements: bool,
    pub delayed_delivery: bool,
    pub retries: bool,
    pub dead_letter: bool,
    pub deduplication: bool,
    pub leasing: bool,
    pub durable: bool,
    pub batch_enqueue: bool,
    pub observable_lag: bool,
}

/// Point-in-time queue depths. Backends return `None` for values they cannot
/// obtain without an expensive or unsupported broker query.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStats {
    pub ready: Option<u64>,
    pub active: Option<u64>,
    pub delayed: Option<u64>,
    pub dead_letter: Option<u64>,
    pub completed: Option<u64>,
    pub oldest_ready_age_ms: Option<u64>,
}

/// Health and worker state returned by queue implementations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueHealth {
    pub backend: QueueBackendKind,
    pub healthy: bool,
    pub accepting_jobs: bool,
    pub workers: usize,
    pub capabilities: QueueCapabilities,
}

impl Default for QueueHealth {
    fn default() -> Self {
        Self {
            backend: QueueBackendKind::Unknown,
            healthy: true,
            accepting_jobs: true,
            workers: 0,
            capabilities: QueueCapabilities::default(),
        }
    }
}

#[async_trait]
pub trait QueueInterface: Send + Sync {
    /// Enqueues one job using the original compatibility surface.
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()>;

    /// Enqueues a logical job and returns its stable ID.
    ///
    /// Backends with native Queue v2 support override this method. The default
    /// implementation preserves compatibility but does not promise deduplication.
    async fn enqueue(
        &self,
        queue_name: &str,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<QueueJobId> {
        if options.delay_ms > 0
            || options.deduplication_key.is_some()
            || options.max_attempts.is_some()
        {
            return Err(Error::Queue(format!(
                "queue backend {} does not implement Queue v2 job options",
                self.backend().as_str()
            )));
        }
        let id = QueueJobId(options.job_id.unwrap_or_else(|| QueueJobId::new().0));
        self.add_to_queue(queue_name, data).await?;
        Ok(id)
    }

    async fn add_batch_to_queue(&self, queue_name: &str, data: Vec<JobData>) -> Result<()> {
        for job in data {
            self.add_to_queue(queue_name, job).await?;
        }
        Ok(())
    }

    /// Enqueues a batch. Reliable backends should override this to perform one
    /// atomic or broker-native batch operation.
    async fn enqueue_batch(
        &self,
        queue_name: &str,
        jobs: Vec<QueueJobRequest>,
    ) -> Result<Vec<QueueJobId>> {
        let mut ids = Vec::with_capacity(jobs.len());
        for job in jobs {
            ids.push(self.enqueue(queue_name, job.data, job.options).await?);
        }
        Ok(ids)
    }

    /// Registers a processor. A successful callback acknowledges a delivery;
    /// an error requests retry according to backend/configured policy.
    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()>;

    /// Stops claiming new jobs and drains or releases in-flight deliveries.
    /// This operation must never delete durable shared queue contents.
    async fn disconnect(&self) -> Result<()>;

    async fn check_health(&self) -> Result<()>;

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::Unknown
    }

    fn capabilities(&self) -> QueueCapabilities {
        QueueCapabilities::default()
    }

    async fn health(&self) -> Result<QueueHealth> {
        self.check_health().await?;
        Ok(QueueHealth {
            backend: self.backend(),
            healthy: true,
            accepting_jobs: true,
            workers: 0,
            capabilities: self.capabilities(),
        })
    }

    async fn stats(&self, _queue_name: &str) -> Result<QueueStats> {
        Ok(QueueStats::default())
    }

    /// Moves up to `limit` oldest dead-letter jobs back to the ready state.
    async fn replay_dead_letters(&self, _queue_name: &str, _limit: u32) -> Result<u64> {
        Err(crate::error::Error::Queue(
            "dead-letter replay is not supported by this queue backend".to_string(),
        ))
    }
}
