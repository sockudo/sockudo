use crate::error::{Error, Result};
use crate::options::SnsQueueConfig;
use crate::queue::QueueInterface;
use crate::webhook::sender::JobProcessorFnAsync;
use async_trait::async_trait;
use aws_sdk_sns as sns;
use aws_sdk_sns::config::Region;
use tracing::{debug, info};

pub struct SnsQueueManager {
    client: sns::Client,
    config: SnsQueueConfig,
}

impl SnsQueueManager {
    pub async fn new(config: SnsQueueConfig) -> Result<Self> {
        if config.topic_arn.is_empty() {
            return Err(Error::Queue(
                "SNS topic_arn is not configured".to_string(),
            ));
        }

        // Load shared AWS config WITHOUT setting region — STS must resolve
        // credentials using the local AWS_REGION env var, not the SNS target region.
        let aws_config = aws_config::from_env().load().await;

        // Set region ONLY on the SNS service-specific config builder.
        // This keeps STS credential resolution local while SNS targets the configured region.
        let mut sns_config_builder = sns::config::Builder::from(&aws_config)
            .region(Region::new(config.region.clone()));

        if let Some(ref endpoint) = config.endpoint_url {
            sns_config_builder = sns_config_builder.endpoint_url(endpoint);
        }

        let client = sns::Client::from_conf(sns_config_builder.build());

        Ok(Self { client, config })
    }
}

#[async_trait]
impl QueueInterface for SnsQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: crate::webhook::types::JobData) -> Result<()> {
        // queue_name is unused — all publishes go to the configured topic_arn.
        // Logged for debugging traceability.
        debug!("SNS add_to_queue called for queue: {}", queue_name);

        let json = sonic_rs::to_string(&data)
            .map_err(|e| Error::Queue(format!("Failed to serialize job data: {e}")))?;

        let result = self
            .client
            .publish()
            .topic_arn(&self.config.topic_arn)
            .message(json)
            .send()
            .await
            .map_err(|e| Error::Queue(format!("Failed to publish to SNS topic: {e}")))?;

        info!(
            "Published job to SNS topic {} with message ID: {}",
            self.config.topic_arn,
            result.message_id().unwrap_or("unknown")
        );

        Ok(())
    }

    async fn process_queue(&self, _queue_name: &str, _callback: JobProcessorFnAsync) -> Result<()> {
        // SNS is publish-only. Consumption is handled by the SQS driver on consumer pods.
        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        // No background workers to shut down.
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
}
