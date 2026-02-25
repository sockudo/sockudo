use crate::queue::{JobProcessorFnAsync, QueueInterface};
use async_trait::async_trait;
use sockudo_queue_redis::QueueBackend as _;
use sockudo_queue_redis::backend::RedisQueueManager as RedisQueueBackend;
use sockudo_types::webhook::JobData;

pub struct RedisQueueManager {
    inner: RedisQueueBackend,
}

impl RedisQueueManager {
    pub async fn new(
        redis_url: &str,
        prefix: &str,
        concurrency: usize,
    ) -> crate::error::Result<Self> {
        let inner = RedisQueueBackend::new(redis_url, prefix, concurrency)
            .await
            .map_err(crate::error::Error::Queue)?;
        Ok(Self { inner })
    }

    #[allow(dead_code)]
    pub fn start_processing(&self) {
        self.inner.start_processing();
    }
}

#[async_trait]
impl QueueInterface for RedisQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> crate::error::Result<()> {
        self.inner
            .add_to_queue(queue_name, data)
            .await
            .map_err(crate::error::Error::Queue)
    }

    async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> crate::error::Result<()> {
        let adapted_callback: sockudo_queue_redis::JobProcessorFnAsync = Box::new(move |job| {
            let fut = callback(job);
            Box::pin(async move { fut.await.map_err(|e| e.to_string()) })
        });

        self.inner
            .process_queue(queue_name, adapted_callback)
            .await
            .map_err(crate::error::Error::Queue)
    }

    async fn disconnect(&self) -> crate::error::Result<()> {
        self.inner
            .disconnect()
            .await
            .map_err(crate::error::Error::Queue)
    }

    async fn check_health(&self) -> crate::error::Result<()> {
        self.inner
            .check_health()
            .await
            .map_err(crate::error::Error::Queue)
    }
}
