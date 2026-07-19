use crate::redis_backend::ReliableRedisQueue;
use crate::redis_connection::ClusterRedisProvider;
use async_trait::async_trait;
use sockudo_core::error::Result;
use sockudo_core::options::QueueReliabilityConfig;
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueHealth, QueueInterface, QueueJobId, QueueJobOptions,
    QueueJobRequest, QueueStats,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;

/// Reliable Redis Cluster queue. Every logical queue is deliberately bound to
/// one hash slot so all Lua state transitions remain atomic.
pub struct RedisClusterQueueManager {
    backend: ReliableRedisQueue<ClusterRedisProvider>,
}

impl RedisClusterQueueManager {
    pub async fn new(
        cluster_nodes: Vec<String>,
        prefix: &str,
        concurrency: usize,
        request_timeout_ms: u64,
    ) -> Result<Self> {
        Self::new_with_config(
            cluster_nodes,
            prefix,
            concurrency,
            request_timeout_ms,
            QueueReliabilityConfig::default(),
        )
        .await
    }

    pub async fn new_with_config(
        cluster_nodes: Vec<String>,
        prefix: &str,
        concurrency: usize,
        request_timeout_ms: u64,
        reliability: QueueReliabilityConfig,
    ) -> Result<Self> {
        let provider = ClusterRedisProvider::connect(cluster_nodes, request_timeout_ms).await?;
        Ok(Self {
            backend: ReliableRedisQueue::new(
                provider,
                prefix,
                concurrency,
                request_timeout_ms,
                reliability,
            )?,
        })
    }
}

#[async_trait]
impl QueueInterface for RedisClusterQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        self.backend
            .enqueue(queue_name, data, QueueJobOptions::default())
            .await
            .map(|_| ())
    }

    async fn enqueue(
        &self,
        queue_name: &str,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<QueueJobId> {
        self.backend.enqueue(queue_name, data, options).await
    }

    async fn add_batch_to_queue(&self, queue_name: &str, data: Vec<JobData>) -> Result<()> {
        let jobs = data
            .into_iter()
            .map(|data| QueueJobRequest {
                data,
                options: QueueJobOptions::default(),
            })
            .collect();
        self.backend
            .enqueue_batch(queue_name, jobs)
            .await
            .map(|_| ())
    }

    async fn enqueue_batch(
        &self,
        queue_name: &str,
        jobs: Vec<QueueJobRequest>,
    ) -> Result<Vec<QueueJobId>> {
        self.backend.enqueue_batch(queue_name, jobs).await
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
        self.backend
            .process_queue(queue_name, Arc::from(callback))
            .await
    }

    async fn disconnect(&self) -> Result<()> {
        self.backend.disconnect().await
    }

    async fn check_health(&self) -> Result<()> {
        self.backend.check_health().await
    }

    fn backend(&self) -> QueueBackendKind {
        self.backend.backend()
    }

    fn capabilities(&self) -> QueueCapabilities {
        self.backend.capabilities()
    }

    async fn health(&self) -> Result<QueueHealth> {
        self.backend.health().await
    }

    async fn stats(&self, queue_name: &str) -> Result<QueueStats> {
        self.backend.stats(queue_name).await
    }

    async fn replay_dead_letters(&self, queue_name: &str, limit: u32) -> Result<u64> {
        self.backend.replay_dead_letters(queue_name, limit).await
    }
}
