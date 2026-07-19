use crate::broker_batch::prepare_default_batch;
use async_trait::async_trait;
use aws_sdk_sns as sns;
use aws_sdk_sns::config::Region;
use aws_sdk_sns::types::PublishBatchRequestEntry;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{QueueReliabilityConfig, SnsQueueConfig};
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueInterface, QueueJobId, QueueJobRequest,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use tracing::debug;

pub struct SnsQueueManager {
    client: sns::Client,
    config: SnsQueueConfig,
    reliability: QueueReliabilityConfig,
}

impl SnsQueueManager {
    pub async fn new(config: SnsQueueConfig) -> Result<Self> {
        Self::new_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    pub async fn new_with_reliability(
        config: SnsQueueConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Self> {
        reliability.validate().map_err(Error::Config)?;
        if config.topic_arn.is_empty() {
            return Err(Error::Queue("SNS topic_arn is not configured".to_string()));
        }

        let aws_config = aws_config::from_env().load().await;

        let mut sns_config_builder =
            sns::config::Builder::from(&aws_config).region(Region::new(config.region.clone()));

        if let Some(ref endpoint) = config.endpoint_url {
            sns_config_builder = sns_config_builder.endpoint_url(endpoint);
        }

        let client = sns::Client::from_conf(sns_config_builder.build());

        Ok(Self {
            client,
            config,
            reliability,
        })
    }

    async fn publish_batch(&self, queue_name: &str, jobs: Vec<JobData>) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        debug!("SNS batch publish called for queue: {queue_name}");
        let batch_size = self.reliability.max_batch_size.min(10);
        for chunk in jobs.chunks(batch_size) {
            let entries = chunk
                .iter()
                .enumerate()
                .map(|(index, data)| {
                    let message = sonic_rs::to_string(data).map_err(|e| {
                        Error::Queue(format!("Failed to serialize SNS job data: {e}"))
                    })?;
                    PublishBatchRequestEntry::builder()
                        .id(format!("job-{index}"))
                        .message(message)
                        .build()
                        .map_err(|e| Error::Queue(format!("Failed to build SNS batch entry: {e}")))
                })
                .collect::<Result<Vec<_>>>()?;
            let output = self
                .client
                .publish_batch()
                .topic_arn(&self.config.topic_arn)
                .set_publish_batch_request_entries(Some(entries))
                .send()
                .await
                .map_err(|e| Error::Queue(format!("Failed to publish SNS batch: {e}")))?;
            if !output.failed().is_empty() {
                let failures = output
                    .failed()
                    .iter()
                    .map(|failure| format!("{}:{}", failure.id(), failure.code()))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Error::Queue(format!(
                    "SNS accepted the batch request but rejected entries: {failures}"
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl QueueInterface for SnsQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        self.publish_batch(queue_name, vec![data]).await
    }

    async fn add_batch_to_queue(&self, queue_name: &str, data: Vec<JobData>) -> Result<()> {
        self.publish_batch(queue_name, data).await
    }

    async fn enqueue_batch(
        &self,
        queue_name: &str,
        jobs: Vec<QueueJobRequest>,
    ) -> Result<Vec<QueueJobId>> {
        let prepared = prepare_default_batch(self.backend(), jobs)?;
        self.publish_batch(queue_name, prepared.data).await?;
        Ok(prepared.ids)
    }

    async fn process_queue(&self, _queue_name: &str, _callback: JobProcessorFnAsync) -> Result<()> {
        Err(Error::Queue(
            "SNS is publish-only; configure SQS or another consumer-capable queue backend"
                .to_string(),
        ))
    }

    async fn disconnect(&self) -> Result<()> {
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        self.client
            .get_topic_attributes()
            .topic_arn(&self.config.topic_arn)
            .send()
            .await
            .map_err(|e| Error::Queue(format!("Queue SNS health check failed: {e}")))?;
        Ok(())
    }

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::Sns
    }

    fn capabilities(&self) -> QueueCapabilities {
        QueueCapabilities {
            consume: false,
            acknowledgements: false,
            delayed_delivery: false,
            retries: false,
            dead_letter: false,
            deduplication: false,
            leasing: false,
            durable: true,
            batch_enqueue: true,
            observable_lag: false,
        }
    }
}
