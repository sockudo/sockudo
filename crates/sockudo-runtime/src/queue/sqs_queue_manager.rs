use crate::queue::{JobProcessorFnAsync, QueueInterface};
use async_trait::async_trait;
use sockudo_config::queue::SqsQueueConfig;
use sockudo_queue_sqs::QueueBackend as _;
use sockudo_queue_sqs::SqsQueueConfig as ExternalSqsQueueConfig;
use sockudo_queue_sqs::backend::SqsQueueManager as SqsQueueBackend;
use sockudo_types::webhook::JobData;

pub struct SqsQueueManager {
    inner: SqsQueueBackend,
}

impl SqsQueueManager {
    pub async fn new(config: SqsQueueConfig) -> crate::error::Result<Self> {
        let external_config = ExternalSqsQueueConfig {
            region: config.region,
            queue_url_prefix: config.queue_url_prefix,
            visibility_timeout: config.visibility_timeout,
            endpoint_url: config.endpoint_url,
            max_messages: config.max_messages,
            wait_time_seconds: config.wait_time_seconds,
            concurrency: config.concurrency,
            fifo: config.fifo,
            message_group_id: config.message_group_id,
        };

        let inner = SqsQueueBackend::new(external_config)
            .await
            .map_err(crate::error::Error::Queue)?;

        Ok(Self { inner })
    }
}

#[async_trait]
impl QueueInterface for SqsQueueManager {
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
        let adapted_callback: sockudo_queue_sqs::JobProcessorFnAsync = Box::new(move |job| {
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
