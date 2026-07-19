use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use aws_sdk_sqs as sqs;
use aws_sdk_sqs::types::{QueueAttributeName, SendMessageBatchRequestEntry};
use futures_util::{StreamExt, stream};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{QueueReliabilityConfig, SqsQueueConfig};
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueInterface, QueueJobId, QueueJobOptions,
    QueueJobRequest, QueueStats,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;
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
    reliability: QueueReliabilityConfig,
}

impl SqsQueueManager {
    /// Create a new SQS queue manager
    pub async fn new(config: SqsQueueConfig) -> Result<Self> {
        Self::new_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    pub async fn new_with_reliability(
        config: SqsQueueConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Self> {
        reliability.validate().map_err(Error::Config)?;
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
            reliability,
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

    async fn send_jobs(
        &self,
        queue_name: &str,
        jobs: Vec<(JobData, QueueJobOptions)>,
    ) -> Result<Vec<QueueJobId>> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let queue_url = self.get_queue_url(queue_name).await?;
        let mut prepared = Vec::with_capacity(jobs.len());

        for (data, options) in jobs {
            if options.max_attempts.is_some() {
                return Err(Error::Queue(
                    "SQS max_attempts is configured by the queue redrive policy, not per job"
                        .to_string(),
                ));
            }
            if options.delay_ms > 900_000 {
                return Err(Error::Queue(
                    "SQS delay_ms must not exceed 900000 (15 minutes)".to_string(),
                ));
            }
            if self.config.fifo && options.delay_ms > 0 {
                return Err(Error::Queue(
                    "SQS FIFO queues do not support per-message delay".to_string(),
                ));
            }
            if !self.config.fifo && options.deduplication_key.is_some() {
                return Err(Error::Queue(
                    "SQS deduplication keys require a FIFO queue".to_string(),
                ));
            }

            let supplied_job_id = options.job_id.is_some();
            let id = QueueJobId(
                options
                    .job_id
                    .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()),
            );
            let deduplication_id = options
                .deduplication_key
                .or_else(|| supplied_job_id.then(|| id.0.clone()));
            let body = sonic_rs::to_string(&data)
                .map_err(|e| Error::Queue(format!("Failed to serialize SQS job data: {e}")))?;
            prepared.push((id, body, options.delay_ms, deduplication_id));
        }

        let batch_size = self.reliability.max_batch_size.min(10);
        for chunk in prepared.chunks(batch_size) {
            let entries = chunk
                .iter()
                .enumerate()
                .map(|(index, (_, body, delay_ms, deduplication_id))| {
                    let mut builder = SendMessageBatchRequestEntry::builder()
                        .id(format!("job-{index}"))
                        .message_body(body)
                        .set_delay_seconds(
                            (*delay_ms > 0).then(|| delay_ms.div_ceil(1_000).min(900) as i32),
                        );
                    if self.config.fifo {
                        builder = builder
                            .set_message_group_id(self.config.message_group_id.clone())
                            .set_message_deduplication_id(deduplication_id.clone());
                    }
                    builder
                        .build()
                        .map_err(|e| Error::Queue(format!("Failed to build SQS batch entry: {e}")))
                })
                .collect::<Result<Vec<_>>>()?;
            let response = self
                .client
                .send_message_batch()
                .queue_url(&queue_url)
                .set_entries(Some(entries))
                .send()
                .await
                .map_err(|e| {
                    Error::Queue(format!(
                        "Failed to send message batch to SQS queue {queue_name}: {e}"
                    ))
                })?;
            if !response.failed().is_empty() {
                let failures = response
                    .failed()
                    .iter()
                    .map(|failure| format!("{}:{}", failure.id(), failure.code()))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Error::Queue(format!(
                    "SQS accepted the batch request but rejected entries: {failures}"
                )));
            }
        }

        Ok(prepared.into_iter().map(|(id, ..)| id).collect())
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
        let worker_prefetch = self.reliability.worker_prefetch;

        tokio::spawn(async move {
            info!(
                "{}",
                format!(
                    "Starting SQS worker #{} for queue: {}",
                    worker_id, queue_name
                )
            );

            loop {
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

                            let message_concurrency = if config.fifo {
                                1
                            } else {
                                worker_prefetch.min(messages.len()).max(1)
                            };
                            stream::iter(messages.iter().cloned())
                                .for_each_concurrent(message_concurrency, |message| {
                                    process_sqs_message(
                                        client.clone(),
                                        queue_url.clone(),
                                        queue_name.clone(),
                                        config.visibility_timeout,
                                        processor.clone(),
                                        message,
                                    )
                                })
                                .await;
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
        self.send_jobs(queue_name, vec![(data, QueueJobOptions::default())])
            .await?;
        Ok(())
    }

    async fn enqueue(
        &self,
        queue_name: &str,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<QueueJobId> {
        self.send_jobs(queue_name, vec![(data, options)])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Queue("SQS enqueue returned no job ID".to_string()))
    }

    async fn add_batch_to_queue(&self, queue_name: &str, data: Vec<JobData>) -> Result<()> {
        self.send_jobs(
            queue_name,
            data.into_iter()
                .map(|data| (data, QueueJobOptions::default()))
                .collect(),
        )
        .await?;
        Ok(())
    }

    async fn enqueue_batch(
        &self,
        queue_name: &str,
        jobs: Vec<QueueJobRequest>,
    ) -> Result<Vec<QueueJobId>> {
        self.send_jobs(
            queue_name,
            jobs.into_iter()
                .map(|job| (job.data, job.options))
                .collect(),
        )
        .await
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

            for mut handle in workers {
                if tokio::time::timeout(
                    Duration::from_millis(self.reliability.shutdown_timeout_ms),
                    &mut handle,
                )
                .await
                .is_err()
                {
                    handle.abort();
                    let _ = handle.await;
                }
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

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::Sqs
    }

    fn capabilities(&self) -> QueueCapabilities {
        QueueCapabilities {
            consume: true,
            acknowledgements: true,
            delayed_delivery: true,
            retries: true,
            dead_letter: false,
            deduplication: self.config.fifo,
            leasing: true,
            durable: true,
            batch_enqueue: true,
            observable_lag: true,
        }
    }

    async fn stats(&self, queue_name: &str) -> Result<QueueStats> {
        let queue_url = self.get_queue_url(queue_name).await?;
        let output = self
            .client
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesDelayed)
            .send()
            .await
            .map_err(|e| Error::Queue(format!("Failed to read SQS queue stats: {e}")))?;
        let attributes = output.attributes();
        let parse = |name: &QueueAttributeName| {
            attributes
                .and_then(|values| values.get(name))
                .and_then(|value| value.parse::<u64>().ok())
        };
        Ok(QueueStats {
            ready: parse(&QueueAttributeName::ApproximateNumberOfMessages),
            active: parse(&QueueAttributeName::ApproximateNumberOfMessagesNotVisible),
            delayed: parse(&QueueAttributeName::ApproximateNumberOfMessagesDelayed),
            ..QueueStats::default()
        })
    }
}

async fn process_sqs_message(
    client: sqs::Client,
    queue_url: String,
    queue_name: String,
    visibility_timeout: i32,
    processor: ArcJobProcessorFn,
    message: sqs::types::Message,
) {
    let Some(body) = message.body() else {
        return;
    };
    let job_data = match sonic_rs::from_str::<JobData>(body) {
        Ok(job_data) => job_data,
        Err(error) => {
            error!("Failed to deserialize message from SQS queue {queue_name}: {error}");
            if let Some(receipt_handle) = message.receipt_handle() {
                let _ = client
                    .delete_message()
                    .queue_url(&queue_url)
                    .receipt_handle(receipt_handle)
                    .send()
                    .await;
            }
            return;
        }
    };

    match process_with_visibility_renewal(
        &client,
        &queue_url,
        message.receipt_handle(),
        visibility_timeout,
        processor(job_data),
    )
    .await
    {
        Ok(()) => {
            if let Some(receipt_handle) = message.receipt_handle()
                && let Err(error) = client
                    .delete_message()
                    .queue_url(&queue_url)
                    .receipt_handle(receipt_handle)
                    .send()
                    .await
            {
                error!("Failed to delete message from SQS queue {queue_name}: {error}");
            }
        }
        Err(error) => {
            error!("Error processing message from SQS queue {queue_name}: {error}");
            if let Some(receipt_handle) = message.receipt_handle()
                && let Err(change_error) = client
                    .change_message_visibility()
                    .queue_url(&queue_url)
                    .receipt_handle(receipt_handle)
                    .visibility_timeout(0)
                    .send()
                    .await
            {
                error!("Failed to release failed SQS queue message: {change_error}");
            }
        }
    }
}

async fn process_with_visibility_renewal<F>(
    client: &sqs::Client,
    queue_url: &str,
    receipt_handle: Option<&str>,
    visibility_timeout: i32,
    future: F,
) -> Result<()>
where
    F: std::future::Future<Output = Result<()>>,
{
    let Some(receipt_handle) = receipt_handle else {
        return future.await;
    };
    tokio::pin!(future);
    let renew_seconds = u64::try_from(visibility_timeout.max(2) / 2).unwrap_or(1);
    let mut interval = tokio::time::interval(Duration::from_secs(renew_seconds));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            result = &mut future => return result,
            _ = interval.tick() => {
                client
                    .change_message_visibility()
                    .queue_url(queue_url)
                    .receipt_handle(receipt_handle)
                    .visibility_timeout(visibility_timeout)
                    .send()
                    .await
                    .map_err(|error| Error::Queue(format!(
                        "failed to renew SQS message visibility: {error}"
                    )))?;
            }
        }
    }
}

impl Drop for SqsQueueManager {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}
