use crate::{ArcJobProcessorFn, JobProcessorFnAsync, QueueBackend, QueueResult, SqsQueueConfig};
use ahash::AHashMap;
use async_trait::async_trait;
use aws_sdk_sqs as sqs;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{error, info};

pub struct SqsQueueManager {
    client: sqs::Client,
    config: SqsQueueConfig,
    queue_urls: Arc<Mutex<AHashMap<String, String>>>,
    worker_handles: Arc<Mutex<AHashMap<String, Vec<tokio::task::JoinHandle<()>>>>>,
    shutdown: Arc<Mutex<bool>>,
}

impl SqsQueueManager {
    pub async fn new(config: SqsQueueConfig) -> QueueResult<Self> {
        let mut aws_config_builder =
            aws_config::from_env().region(aws_sdk_sqs::config::Region::new(config.region.clone()));

        if let Some(endpoint) = &config.endpoint_url {
            aws_config_builder = aws_config_builder.endpoint_url(endpoint);
        }

        let aws_config = aws_config_builder.load().await;
        let client = sqs::Client::new(&aws_config);

        Ok(Self {
            client,
            config,
            queue_urls: Arc::new(Mutex::new(AHashMap::new())),
            worker_handles: Arc::new(Mutex::new(AHashMap::new())),
            shutdown: Arc::new(Mutex::new(false)),
        })
    }

    async fn get_queue_url(&self, queue_name: &str) -> QueueResult<String> {
        let cached_url = {
            let queue_urls = self.queue_urls.lock().await;
            queue_urls.get(queue_name).cloned()
        };

        if let Some(url) = cached_url {
            return Ok(url);
        }

        if let Some(prefix) = &self.config.queue_url_prefix {
            let queue_url = if self.config.fifo {
                format!("{prefix}/{queue_name}.fifo")
            } else {
                format!("{prefix}/{queue_name}")
            };

            let mut queue_urls = self.queue_urls.lock().await;
            queue_urls.insert(queue_name.to_string(), queue_url.clone());
            return Ok(queue_url);
        }

        let actual_queue_name = if self.config.fifo && !queue_name.ends_with(".fifo") {
            format!("{queue_name}.fifo")
        } else {
            queue_name.to_string()
        };

        match self
            .client
            .get_queue_url()
            .queue_name(&actual_queue_name)
            .send()
            .await
        {
            Ok(output) => {
                if let Some(url) = output.queue_url() {
                    let mut queue_urls = self.queue_urls.lock().await;
                    queue_urls.insert(queue_name.to_string(), url.to_string());
                    Ok(url.to_string())
                } else {
                    Err(format!("No URL returned for queue: {queue_name}"))
                }
            }
            Err(e) => {
                if e.to_string().contains("QueueDoesNotExist")
                    || e.to_string().contains("NonExistentQueue")
                {
                    self.create_queue(queue_name).await
                } else {
                    Err(format!("Failed to get queue URL: {e}"))
                }
            }
        }
    }

    async fn create_queue(&self, queue_name: &str) -> QueueResult<String> {
        info!("Creating SQS queue: {queue_name}");

        let actual_queue_name = if self.config.fifo && !queue_name.ends_with(".fifo") {
            format!("{queue_name}.fifo")
        } else {
            queue_name.to_string()
        };

        let mut attributes = HashMap::new();
        use aws_sdk_sqs::types::QueueAttributeName;

        attributes.insert(
            QueueAttributeName::VisibilityTimeout,
            self.config.visibility_timeout.to_string(),
        );

        if self.config.fifo {
            attributes.insert(QueueAttributeName::FifoQueue, "true".to_string());
            attributes.insert(
                QueueAttributeName::ContentBasedDeduplication,
                "true".to_string(),
            );
        }

        let result = self
            .client
            .create_queue()
            .queue_name(&actual_queue_name)
            .set_attributes(Some(attributes))
            .send()
            .await
            .map_err(|e| format!("Failed to create SQS queue {queue_name}: {e}"))?;

        if let Some(url) = result.queue_url() {
            let mut queue_urls = self.queue_urls.lock().await;
            queue_urls.insert(queue_name.to_string(), url.to_string());
            Ok(url.to_string())
        } else {
            Err(format!(
                "No URL returned after creating queue: {queue_name}"
            ))
        }
    }

    async fn start_worker(
        &self,
        queue_name: &str,
        queue_url: String,
        processor: ArcJobProcessorFn,
        worker_id: usize,
    ) -> tokio::task::JoinHandle<()> {
        let client = self.client.clone();
        let config = self.config.clone();
        let shutdown = self.shutdown.clone();
        let queue_name = queue_name.to_string();

        tokio::spawn(async move {
            info!("Starting SQS worker #{worker_id} for queue: {queue_name}");

            let mut interval = interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                if *shutdown.lock().await {
                    info!("SQS worker #{worker_id} for queue {queue_name} shutting down");
                    break;
                }

                let result = client
                    .receive_message()
                    .queue_url(&queue_url)
                    .max_number_of_messages(config.max_messages)
                    .visibility_timeout(config.visibility_timeout)
                    .wait_time_seconds(config.wait_time_seconds)
                    .send()
                    .await;

                match result {
                    Ok(response) => {
                        let messages = response.messages();
                        if messages.is_empty() {
                            continue;
                        }

                        info!(
                            "SQS worker #{worker_id} received {} messages from queue {queue_name}",
                            messages.len()
                        );

                        for message in messages {
                            if let Some(body) = message.body() {
                                match sonic_rs::from_str::<sockudo_types::webhook::JobData>(body) {
                                    Ok(job_data) => match processor(job_data).await {
                                        Ok(_) => {
                                            if let Some(receipt_handle) = message.receipt_handle()
                                                && let Err(e) = client
                                                    .delete_message()
                                                    .queue_url(&queue_url)
                                                    .receipt_handle(receipt_handle)
                                                    .send()
                                                    .await
                                            {
                                                error!(
                                                    "Failed to delete message from SQS queue {queue_name}: {e}"
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "Error processing message from SQS queue {queue_name}: {e}"
                                            );
                                        }
                                    },
                                    Err(e) => {
                                        error!(
                                            "Failed to deserialize message from SQS queue {queue_name}: {e}"
                                        );

                                        if let Some(receipt_handle) = message.receipt_handle()
                                            && let Err(e) = client
                                                .delete_message()
                                                .queue_url(&queue_url)
                                                .receipt_handle(receipt_handle)
                                                .send()
                                                .await
                                        {
                                            error!(
                                                "Failed to delete malformed message from SQS queue {queue_name}: {e}"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to receive messages from SQS queue {queue_name}: {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        })
    }
}

#[async_trait]
impl QueueBackend for SqsQueueManager {
    async fn add_to_queue(
        &self,
        queue_name: &str,
        data: sockudo_types::webhook::JobData,
    ) -> QueueResult<()> {
        let queue_url = self.get_queue_url(queue_name).await?;

        let data_json =
            sonic_rs::to_string(&data).map_err(|e| format!("Failed to serialize job data: {e}"))?;

        let mut send_message_request = self
            .client
            .send_message()
            .queue_url(queue_url)
            .message_body(data_json);

        if self.config.fifo
            && let Some(group_id) = &self.config.message_group_id
        {
            send_message_request = send_message_request.message_group_id(group_id);
        }

        let result = send_message_request
            .send()
            .await
            .map_err(|e| format!("Failed to send message to SQS queue {queue_name}: {e}"))?;

        info!(
            "Added job to SQS queue {queue_name} with ID: {}",
            result.message_id().unwrap_or("unknown")
        );

        Ok(())
    }

    async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> QueueResult<()> {
        let queue_url = self.get_queue_url(queue_name).await?;
        let processor: ArcJobProcessorFn = Arc::from(callback);

        let mut worker_handles = Vec::new();
        let concurrency = self.config.concurrency as usize;
        for worker_id in 0..concurrency {
            let handle = self
                .start_worker(queue_name, queue_url.clone(), processor.clone(), worker_id)
                .await;

            worker_handles.push(handle);
        }

        let mut handles = self.worker_handles.lock().await;
        handles.insert(queue_name.to_string(), worker_handles);

        info!("Started {concurrency} workers for SQS queue: {queue_name}");

        Ok(())
    }

    async fn disconnect(&self) -> QueueResult<()> {
        {
            let mut shutdown = self.shutdown.lock().await;
            *shutdown = true;
        }

        {
            let mut handles = self.worker_handles.lock().await;
            for (queue_name, workers) in handles.drain() {
                info!("Waiting for SQS queue {queue_name} workers to shutdown");
                for handle in workers {
                    handle.abort();
                }
            }
        }

        Ok(())
    }

    async fn check_health(&self) -> QueueResult<()> {
        self.client
            .list_queues()
            .send()
            .await
            .map_err(|e| format!("Queue SQS connection failed: {e}"))?;
        Ok(())
    }
}

impl Drop for SqsQueueManager {
    fn drop(&mut self) {
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut lock = shutdown.lock().await;
            *lock = true;
        });
    }
}
