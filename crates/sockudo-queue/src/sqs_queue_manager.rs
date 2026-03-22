use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use aws_sdk_sqs as sqs;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::SqsQueueConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

/// SQS-based implementation of the QueueInterface
pub struct SqsQueueManager {
    /// SQS client
    client: sqs::Client,
    /// Configuration
    config: SqsQueueConfig,

    /// Cache of queue URLs
    queue_urls: Arc<RwLock<HashMap<String, String>>>,
    /// Active workers
    worker_handles: Arc<Mutex<HashMap<String, Vec<tokio::task::JoinHandle<()>>>>>,
    /// Flag to control worker shutdown
    shutdown: Arc<AtomicBool>,
}

impl SqsQueueManager {
    /// Create a new SQS queue manager
    pub async fn new(config: SqsQueueConfig) -> Result<Self> {
        let mut aws_config_builder = aws_config::from_env();

        aws_config_builder =
            aws_config_builder.region(aws_sdk_sqs::config::Region::new(config.region.clone()));

        if let Some(endpoint) = &config.endpoint_url {
            aws_config_builder = aws_config_builder.endpoint_url(endpoint);
        }

        let aws_config = aws_config_builder.load().await;
        let client = sqs::Client::new(&aws_config);

        Ok(Self {
            client,
            config,
            queue_urls: Arc::new(RwLock::new(HashMap::new())),
            worker_handles: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Get or create the URL for a queue
    async fn get_queue_url(&self, queue_name: &str) -> Result<String> {
        if let Some(url) = self.queue_urls.read().unwrap().get(queue_name).cloned() {
            return Ok(url);
        }

        if let Some(prefix) = &self.config.queue_url_prefix {
            let queue_url = if self.config.fifo {
                format!("{prefix}{queue_name}.fifo")
            } else {
                format!("{prefix}{queue_name}")
            };

            self.queue_urls
                .write()
                .unwrap()
                .insert(queue_name.to_string(), queue_url.clone());

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
                    self.queue_urls
                        .write()
                        .unwrap()
                        .insert(queue_name.to_string(), url.to_string());

                    Ok(url.to_string())
                } else {
                    Err(Error::Queue(format!(
                        "No URL returned for queue: {queue_name}"
                    )))
                }
            }
            Err(e) => {
                if e.to_string().contains("QueueDoesNotExist")
                    || e.to_string().contains("NonExistentQueue")
                {
                    self.create_queue(queue_name).await
                } else {
                    Err(Error::Queue(format!("Failed to get queue URL: {e}")))
                }
            }
        }
    }

    /// Create a queue if it doesn't exist
    async fn create_queue(&self, queue_name: &str) -> Result<String> {
        info!("{}", format!("Creating SQS queue: {}", queue_name));

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
            .map_err(|e| Error::Queue(format!("Failed to create SQS queue {queue_name}: {e}")))?;

        if let Some(url) = result.queue_url() {
            self.queue_urls
                .write()
                .unwrap()
                .insert(queue_name.to_string(), url.to_string());

            Ok(url.to_string())
        } else {
            Err(Error::Queue(format!(
                "No URL returned after creating queue: {queue_name}"
            )))
        }
    }

    /// Start a worker for processing messages from the queue
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
            info!(
                "{}",
                format!(
                    "Starting SQS worker #{} for queue: {}",
                    worker_id, queue_name
                )
            );

            let mut interval = interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                if shutdown.load(Ordering::Relaxed) {
                    info!(
                        "{}",
                        format!(
                            "SQS worker #{} for queue {} shutting down",
                            worker_id, queue_name
                        )
                    );
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

                        if !messages.is_empty() {
                            info!(
                                "{}",
                                format!(
                                    "SQS worker #{} received {} messages from queue {}",
                                    worker_id,
                                    messages.len(),
                                    queue_name
                                )
                            );

                            for message in messages {
                                if let Some(body) = message.body() {
                                    match sonic_rs::from_str::<JobData>(body) {
                                        Ok(job_data) => match processor(job_data).await {
                                            Ok(_) => {
                                                if let Some(receipt_handle) =
                                                    message.receipt_handle()
                                                    && let Err(e) = client
                                                        .delete_message()
                                                        .queue_url(&queue_url)
                                                        .receipt_handle(receipt_handle)
                                                        .send()
                                                        .await
                                                {
                                                    error!(
                                                        "{}",
                                                        format!(
                                                            "Failed to delete message from SQS queue {}: {}",
                                                            queue_name, e
                                                        )
                                                    );
                                                }
                                            }
                                            Err(e) => {
                                                error!(
                                                    "{}",
                                                    format!(
                                                        "Error processing message from SQS queue {}: {}",
                                                        queue_name, e
                                                    )
                                                );
                                            }
                                        },
                                        Err(e) => {
                                            error!(
                                                "{}",
                                                format!(
                                                    "Failed to deserialize message from SQS queue {}: {}",
                                                    queue_name, e
                                                )
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
                                                    "{}",
                                                    format!(
                                                        "Failed to delete malformed message from SQS queue {}: {}",
                                                        queue_name, e
                                                    )
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "{}",
                            format!(
                                "Failed to receive messages from SQS queue {}: {}",
                                queue_name, e
                            )
                        );

                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        })
    }
}

#[async_trait]
impl QueueInterface for SqsQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        let queue_url = self.get_queue_url(queue_name).await?;

        let data_json = sonic_rs::to_string(&data)
            .map_err(|e| Error::Queue(format!("Failed to serialize job data: {e}")))?;

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

        let result = send_message_request.send().await.map_err(|e| {
            Error::Queue(format!(
                "Failed to send message to SQS queue {queue_name}: {e}"
            ))
        })?;

        info!(
            "{}",
            format!(
                "Added job to SQS queue {} with ID: {}",
                queue_name,
                result.message_id().unwrap_or("unknown")
            )
        );

        Ok(())
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
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

        self.worker_handles
            .lock()
            .unwrap()
            .insert(queue_name.to_string(), worker_handles);

        info!(
            "{}",
            format!(
                "Started {} workers for SQS queue: {}",
                concurrency, queue_name
            )
        );

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::Relaxed);

        let worker_handles = std::mem::take(&mut *self.worker_handles.lock().unwrap());
        for (queue_name, workers) in worker_handles {
            info!(
                "{}",
                format!("Waiting for SQS queue {} workers to shutdown", queue_name)
            );

            for handle in workers {
                handle.abort();
            }
        }

        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        self.client
            .list_queues()
            .send()
            .await
            .map_err(|e| Error::Queue(format!("Queue SQS connection failed: {e}")))?;
        Ok(())
    }
}

impl Drop for SqsQueueManager {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}
