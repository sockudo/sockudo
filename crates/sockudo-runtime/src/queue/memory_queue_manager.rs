use crate::queue::{JobProcessorFnAsync, QueueInterface};
use async_trait::async_trait;
use sockudo_queue_memory::QueueBackend as _;
use sockudo_queue_memory::backend::MemoryQueueManager as MemoryQueueBackend;
use sockudo_types::webhook::JobData;

/// Adapter over `sockudo-queue-memory` implementation.
pub struct MemoryQueueManager {
    inner: MemoryQueueBackend,
}

impl Default for MemoryQueueManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryQueueManager {
    pub fn new() -> Self {
        Self {
            inner: MemoryQueueBackend::new(),
        }
    }

    pub fn start_processing(&self) {
        self.inner.start_processing();
    }
}

#[async_trait]
impl QueueInterface for MemoryQueueManager {
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
        let adapted_callback: sockudo_queue_memory::JobProcessorFnAsync = Box::new(move |job| {
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
