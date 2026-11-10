use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use aws_sdk_sqs as sqs;
use aws_sdk_sqs::error::ProvideErrorMetadata;
use aws_sdk_sqs::types::{
    MessageSystemAttributeName, QueueAttributeName, SendMessageBatchRequestEntry,
};
use futures_util::{StreamExt, stream};
use serde::Deserialize;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{QueueReliabilityConfig, SqsQueueConfig};
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueInterface, QueueJobId, QueueJobOptions,
    QueueJobRequest, QueueStats,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use sonic_rs::{JsonValueTrait, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;
use tracing::{debug, error, info, warn};

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
    /// Redrive state per validated queue. Unvalidated queues (push stages) are
    /// absent and keep backoff-only release.
    redrive: Arc<RwLock<HashMap<String, Redrive>>>,
    /// True once a validated queue has an operator-managed `RedrivePolicy`.
    redrive_validated: AtomicBool,
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
            redrive: Arc::new(RwLock::new(HashMap::new())),
            redrive_validated: AtomicBool::new(false),
        })
    }

    /// Resolves the queue URL and records the redrive state. Split from the
    /// trait hook so strict-mode wrapping can stay in one place.
    async fn validate_queue_delivery(&self, queue_name: &str) -> Result<()> {
        let queue_url = self.get_queue_url(queue_name).await?;
        self.validate_redrive_policy(queue_name, &queue_url).await
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
                let code = e.code().unwrap_or_default();
                if code.contains("QueueDoesNotExist")
                    || code.contains("NonExistentQueue")
                    || e.to_string().contains("QueueDoesNotExist")
                    || e.to_string().contains("NonExistentQueue")
                {
                    self.create_queue(queue_name).await
                } else {
                    Err(Error::Queue(format!("Failed to get queue URL: {code}")))
                }
            }
        }
    }

    /// Create a queue if it doesn't exist
    async fn create_queue(&self, queue_name: &str) -> Result<String> {
        debug!(queue = %queue_name, "creating sqs queue");

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

        for (mut data, options) in jobs {
            if options.max_attempts.is_some() {
                return Err(Error::Queue(
                    "SQS max_attempts is configured by the queue redrive policy, not per job"
                        .to_string(),
                ));
            }
            if let Some(max_delay_ms) = QueueBackendKind::Sqs.max_delay_ms()
                && options.delay_ms > max_delay_ms
            {
                return Err(Error::Queue(format!(
                    "SQS delay_ms must not exceed {max_delay_ms} (15 minutes)"
                )));
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

            let supplied_job_id = options.job_id.is_some() || data.job_id.is_some();
            let id = QueueJobId(
                options
                    .job_id
                    .or_else(|| data.job_id.clone())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()),
            );
            data.job_id = Some(id.0.clone());
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

    /// Records how failed receives on `queue_name` are bounded. Never modifies
    /// the operator's queue.
    async fn validate_redrive_policy(&self, queue_name: &str, queue_url: &str) -> Result<()> {
        let require = self.config.require_redrive_policy;
        let raw = match self
            .client
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(QueueAttributeName::RedrivePolicy)
            .send()
            .await
        {
            Ok(output) => output
                .attributes()
                .and_then(|attributes| attributes.get(&QueueAttributeName::RedrivePolicy))
                .cloned(),
            // SDK Display may contain the queue URL; report the AWS error code only.
            Err(error) if require => {
                return Err(Error::Config(format!(
                    "cannot read RedrivePolicy for SQS queue {queue_name} ({}); grant \
                     sqs:GetQueueAttributes or set QUEUE_SQS_REQUIRE_REDRIVE_POLICY=false",
                    error.code().unwrap_or("unknown")
                )));
            }
            Err(error) => {
                warn!(
                    queue = %queue_name,
                    error_code = error.code().unwrap_or(""),
                    max_attempts = self.reliability.max_attempts,
                    "cannot read sqs redrive policy; failed jobs are retried with backoff \
                     and never deleted until the policy can be read"
                );
                self.record_redrive(queue_name, Redrive::Unreadable);
                return Ok(());
            }
        };

        let policy = raw
            .as_deref()
            .and_then(|raw| sonic_rs::from_str::<RedrivePolicy>(raw).ok())
            .and_then(|policy| policy.max_receive_count().map(|count| (count, policy)));
        let redrive = match policy {
            Some((max_receive_count, policy)) => {
                info!(
                    queue = %queue_name,
                    max_receive_count,
                    max_attempts = self.reliability.max_attempts,
                    "sqs redrive policy found; max_receive_count bounds delivery"
                );
                Redrive::Policy {
                    max_receive_count,
                    dead_letter_url: dead_letter_queue_url(
                        queue_url,
                        &policy.dead_letter_target_arn,
                    ),
                }
            }
            None if require => {
                return Err(Error::Config(format!(
                    "SQS queue {queue_name} has no valid RedrivePolicy; attach a dead-letter \
                     queue with maxReceiveCount or set QUEUE_SQS_REQUIRE_REDRIVE_POLICY=false"
                )));
            }
            None => {
                warn!(
                    queue = %queue_name,
                    max_attempts = self.reliability.max_attempts,
                    "sqs queue has no redrive policy; failed jobs are deleted after max_attempts"
                );
                Redrive::Missing
            }
        };
        self.record_redrive(queue_name, redrive);
        Ok(())
    }

    fn record_redrive(&self, queue_name: &str, redrive: Redrive) {
        if matches!(redrive, Redrive::Policy { .. }) {
            self.redrive_validated.store(true, Ordering::Relaxed);
        }
        self.redrive
            .write()
            .unwrap()
            .insert(queue_name.to_owned(), redrive);
    }

    fn release_policy(&self, queue_name: &str) -> ReleasePolicy {
        let redrive = self.redrive.read().unwrap().get(queue_name).cloned();
        ReleasePolicy {
            retry: RetryPolicy {
                base_delay_ms: self.reliability.retry_base_delay_ms,
                max_delay_ms: self.reliability.retry_max_delay_ms,
                jitter: self.reliability.retry_jitter,
            },
            max_receive_count: match &redrive {
                Some(Redrive::Policy {
                    max_receive_count, ..
                }) => Some(*max_receive_count),
                _ => None,
            },
            // An unreadable policy is treated as absent for the attempt bound
            // (no redrive known), but `exhausted` still refuses to delete so a
            // real policy with a higher `maxReceiveCount` cannot be undercut.
            drop_after: matches!(redrive, Some(Redrive::Missing))
                .then_some(self.reliability.max_attempts),
            unreadable: matches!(redrive, Some(Redrive::Unreadable)),
        }
    }

    async fn dead_letter_depth(&self, queue_name: &str) -> Option<u64> {
        let dead_letter_url = match self.redrive.read().unwrap().get(queue_name) {
            Some(Redrive::Policy {
                dead_letter_url: Some(url),
                ..
            }) => url.clone(),
            _ => return None,
        };
        let output = match self
            .client
            .get_queue_attributes()
            .queue_url(dead_letter_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) => {
                warn!(
                    queue = %queue_name,
                    error_code = error.code().unwrap_or(""),
                    "failed to read sqs dead-letter depth"
                );
                return None;
            }
        };
        output
            .attributes()
            .and_then(|attributes| attributes.get(&QueueAttributeName::ApproximateNumberOfMessages))
            .and_then(|value| value.parse::<u64>().ok())
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
        let release = self.release_policy(&queue_name);

        tokio::spawn(async move {
            debug!(worker_id = %worker_id, queue = %queue_name, "starting sqs worker");

            loop {
                if shutdown.load(Ordering::Relaxed) {
                    debug!(worker_id = %worker_id, queue = %queue_name, "sqs worker shutting down");
                    break;
                }

                let result = client
                    .receive_message()
                    .queue_url(&queue_url)
                    .max_number_of_messages(config.max_messages)
                    .visibility_timeout(config.visibility_timeout)
                    .wait_time_seconds(config.wait_time_seconds)
                    .message_system_attribute_names(
                        MessageSystemAttributeName::ApproximateReceiveCount,
                    )
                    .send()
                    .await;

                match result {
                    Ok(response) => {
                        let messages = response.messages();

                        if !messages.is_empty() {
                            debug!(worker_id = %worker_id, queue = %queue_name, message_count = messages.len(), "sqs worker received messages");

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
                                        release,
                                        processor.clone(),
                                        message,
                                    )
                                })
                                .await;
                        }
                    }
                    Err(e) => {
                        error!(queue = %queue_name, error = %e, "failed to receive messages from sqs queue");
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

        info!(queue = %queue_name, worker_count = concurrency, "started sqs queue workers");

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::Relaxed);

        let worker_handles = std::mem::take(&mut *self.worker_handles.lock().unwrap());
        for (queue_name, workers) in worker_handles {
            info!(queue = %queue_name, "waiting for sqs queue workers to shutdown");

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

    async fn validate_delivery_guarantees(&self, queue_name: &str) -> Result<()> {
        // Without a prefix this creates the queue first. A Sockudo-created queue has
        // no RedrivePolicy, so it runs with the max_attempts fallback unless required.
        // Strict mode treats any failure here — including queue setup — as fatal:
        // the operator asked to fail closed, not to run with disabled webhooks.
        // Error text stays AWS-code-only: the SDK Display may embed the queue URL.
        if self.config.require_redrive_policy {
            match self.validate_queue_delivery(queue_name).await {
                Ok(()) => Ok(()),
                Err(Error::Config(error)) => Err(Error::Config(error)),
                Err(error) => Err(Error::Config(format!(
                    "SQS queue {queue_name} validation failed: {error}"
                ))),
            }
        } else {
            self.validate_queue_delivery(queue_name).await
        }
    }

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::Sqs
    }

    fn capabilities(&self) -> QueueCapabilities {
        QueueCapabilities {
            consume: true,
            acknowledgements: true,
            delayed_delivery: !self.config.fifo,
            retries: true,
            dead_letter: self.redrive_validated.load(Ordering::Relaxed),
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
            dead_letter: self.dead_letter_depth(queue_name).await,
            ..QueueStats::default()
        })
    }
}

async fn process_sqs_message(
    client: sqs::Client,
    queue_url: String,
    queue_name: String,
    visibility_timeout: i32,
    release: ReleasePolicy,
    processor: ArcJobProcessorFn,
    message: sqs::types::Message,
) {
    let Some(body) = message.body() else {
        return;
    };
    let job_data = match sonic_rs::from_str::<JobData>(body) {
        Ok(job_data) => job_data,
        Err(error) => {
            error!(
                queue = %queue_name,
                error = %error,
                "failed to deserialize sqs queue message"
            );
            if let Some(receipt_handle) = message.receipt_handle()
                && let Err(error) = client
                    .delete_message()
                    .queue_url(&queue_url)
                    .receipt_handle(receipt_handle)
                    .send()
                    .await
            {
                error!(
                    queue = %queue_name,
                    error = %error,
                    "failed to delete malformed sqs queue message"
                );
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
                error!(
                    queue = %queue_name,
                    error = %error,
                    "failed to delete sqs queue message"
                );
            }
        }
        // Do not log the processor error: its Display may embed the webhook URL.
        Err(_) => {
            let attempt = receive_count(&message);
            if release.exhausted(attempt) {
                error!(
                    queue = %queue_name,
                    receive_count = attempt,
                    "sqs queue job exhausted max_attempts without a redrive policy; deleting"
                );
                if let Some(receipt_handle) = message.receipt_handle()
                    && let Err(delete_error) = client
                        .delete_message()
                        .queue_url(&queue_url)
                        .receipt_handle(receipt_handle)
                        .send()
                        .await
                {
                    error!(
                        queue = %queue_name,
                        error_code = delete_error.code().unwrap_or(""),
                        "failed to delete exhausted sqs queue message"
                    );
                }
                return;
            }
            if release.final_receive(attempt) {
                error!(
                    queue = %queue_name,
                    receive_count = attempt,
                    "sqs queue job failed its final receive; redrive moves it to the dead-letter queue"
                );
            }
            let delay_secs = release
                .retry
                .delay_secs(message.message_id().unwrap_or_default(), attempt);
            warn!(
                queue = %queue_name,
                receive_count = attempt,
                attempt_limit = release.attempt_limit(),
                retry_delay_ms = i64::from(delay_secs) * 1_000,
                "sqs queue processor failed; releasing for retry"
            );
            if let Some(receipt_handle) = message.receipt_handle()
                && let Err(change_error) = client
                    .change_message_visibility()
                    .queue_url(&queue_url)
                    .receipt_handle(receipt_handle)
                    .visibility_timeout(delay_secs)
                    .send()
                    .await
            {
                error!(
                    queue = %queue_name,
                    error_code = change_error.code().unwrap_or(""),
                    "failed to release sqs queue message for retry"
                );
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
                if let Err(error) = client
                    .change_message_visibility()
                    .queue_url(queue_url)
                    .receipt_handle(receipt_handle)
                    .visibility_timeout(visibility_timeout)
                    .send()
                    .await
                {
                    // A failed renewal only risks redelivery. Returning here would drop the
                    // processor while its spawned webhook tasks keep running, and the
                    // immediate release would re-POST the same job concurrently.
                    warn!(
                        error_code = error.code().unwrap_or(""),
                        "failed to renew sqs message visibility"
                    );
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct RetryPolicy {
    base_delay_ms: u64,
    max_delay_ms: u64,
    jitter: f64,
}

impl RetryPolicy {
    /// Exponential backoff with deterministic per-message jitter, mirroring
    /// `memory_queue_manager` and `redis_backend`.
    fn delay_secs(self, message_id: &str, attempt: u32) -> i32 {
        let exponent = attempt.saturating_sub(1).min(31);
        let base = self
            .base_delay_ms
            .saturating_mul(1_u64 << exponent)
            .min(self.max_delay_ms);
        let delay_ms = if base == 0 || self.jitter == 0.0 {
            base
        } else {
            let hash = message_id
                .bytes()
                .fold(0xcbf29ce484222325_u64, |value, byte| {
                    (value ^ u64::from(byte)).wrapping_mul(0x100000001b3)
                });
            let unit = (hash as f64) / (u64::MAX as f64);
            let factor = 1.0 - self.jitter + (2.0 * self.jitter * unit);
            ((base as f64) * factor).round() as u64
        };
        i32::try_from(delay_ms.div_ceil(1_000).clamp(1, 43_200)).unwrap_or(43_200)
    }
}

/// How a failed receive on one queue is released. Built once per worker.
#[derive(Clone, Copy)]
struct ReleasePolicy {
    retry: RetryPolicy,
    /// Receives after which SQS moves the message (operator `RedrivePolicy`).
    max_receive_count: Option<u32>,
    /// Receives after which the driver deletes the message itself. Set only for a
    /// validated queue without a `RedrivePolicy`, from `reliability.max_attempts`.
    drop_after: Option<u32>,
    /// The policy could not be read at startup: never delete, redrive may still
    /// exist with a higher `maxReceiveCount` than `max_attempts`.
    unreadable: bool,
}

impl ReleasePolicy {
    fn exhausted(self, attempt: u32) -> bool {
        !self.unreadable
            && self
                .drop_after
                .is_some_and(|max_attempts| attempt >= max_attempts)
    }

    fn final_receive(self, attempt: u32) -> bool {
        self.max_receive_count
            .is_some_and(|max_receive_count| attempt >= max_receive_count)
    }

    /// Attempt limit for logs; 0 means unbounded (queue was never validated).
    fn attempt_limit(self) -> u32 {
        self.max_receive_count.or(self.drop_after).unwrap_or(0)
    }
}

fn receive_count(message: &sqs::types::Message) -> u32 {
    message
        .attributes()
        .and_then(|attributes| attributes.get(&MessageSystemAttributeName::ApproximateReceiveCount))
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
}

#[derive(Debug, Deserialize)]
struct RedrivePolicy {
    #[serde(rename = "deadLetterTargetArn")]
    dead_letter_target_arn: String,
    #[serde(rename = "maxReceiveCount")]
    max_receive_count: Value,
}

impl RedrivePolicy {
    /// AWS returns `maxReceiveCount` as a number or a numeric string.
    fn max_receive_count(&self) -> Option<u32> {
        self.max_receive_count
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .or_else(|| {
                self.max_receive_count
                    .as_str()
                    .and_then(|value| value.parse().ok())
            })
            .filter(|count| *count > 0)
    }
}

/// Redrive state recorded by `validate_delivery_guarantees`.
#[derive(Clone)]
enum Redrive {
    /// SQS moves the message to the DLQ after `max_receive_count` receives.
    Policy {
        max_receive_count: u32,
        dead_letter_url: Option<String>,
    },
    /// No usable policy: the driver deletes after `reliability.max_attempts`.
    Missing,
    /// A policy may exist but could not be read (`GetQueueAttributes` failed).
    /// The driver retries with backoff and never deletes: deleting could race
    /// SQS's own redrive when `maxReceiveCount` exceeds `max_attempts`.
    Unreadable,
}

/// SQS requires a DLQ in the source queue's account and region, so only the
/// queue name differs. Deriving the URL avoids a `GetQueueUrl` call and its grant.
fn dead_letter_queue_url(queue_url: &str, target_arn: &str) -> Option<String> {
    let name = target_arn
        .rsplit(':')
        .next()
        .filter(|name| !name.is_empty())?;
    let (base, _) = queue_url.rsplit_once('/')?;
    Some(format!("{base}/{name}"))
}

impl Drop for SqsQueueManager {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry(base_delay_ms: u64, jitter: f64) -> RetryPolicy {
        RetryPolicy {
            base_delay_ms,
            max_delay_ms: 60_000,
            jitter,
        }
    }

    fn release(
        max_receive_count: Option<u32>,
        drop_after: Option<u32>,
        unreadable: bool,
    ) -> ReleasePolicy {
        ReleasePolicy {
            retry: retry(1_000, 0.0),
            max_receive_count,
            drop_after,
            unreadable,
        }
    }

    fn parsed_max_receive_count(raw: &str) -> Option<u32> {
        sonic_rs::from_str::<RedrivePolicy>(raw)
            .ok()?
            .max_receive_count()
    }

    // A queue URL prefix skips the GetQueueUrl round trip, so these exercise the
    // per-job validation in `send_jobs` without reaching SQS.
    async fn offline_manager(fifo: bool) -> SqsQueueManager {
        SqsQueueManager::new(SqsQueueConfig {
            region: "us-east-1".to_string(),
            queue_url_prefix: Some("http://127.0.0.1:1/000000000000/".to_string()),
            endpoint_url: Some("http://127.0.0.1:1".to_string()),
            fifo,
            ..SqsQueueConfig::default()
        })
        .await
        .unwrap()
    }

    fn job() -> JobData {
        JobData {
            job_id: None,
            app_key: "test-key".to_string(),
            app_id: "test-id".to_string(),
            app_secret: "test-secret".to_string(),
            trace_context: Default::default(),
            payload: sockudo_core::webhook_types::JobPayload::default(),
            original_signature: "signature".to_string(),
        }
    }

    #[test]
    fn retry_backoff_doubles_and_caps() {
        let retry = retry(1_000, 0.0);
        assert_eq!(retry.delay_secs("message-1", 1), 1);
        assert_eq!(retry.delay_secs("message-1", 2), 2);
        assert_eq!(retry.delay_secs("message-1", 3), 4);
        assert_eq!(retry.delay_secs("message-1", 10), 60);
        assert_eq!(retry.delay_secs("message-1", u32::MAX), 60);
    }

    #[test]
    fn retry_backoff_never_releases_immediately() {
        // visibility 0 is the hot loop this change removes.
        assert_eq!(retry(0, 0.0).delay_secs("message-1", 1), 1);
    }

    #[test]
    fn retry_backoff_jitter_is_bounded_and_deterministic() {
        let retry = retry(10_000, 0.2);
        for id in ["a", "b", "message-1", "0f1e2d3c"] {
            let delay = retry.delay_secs(id, 1);
            assert!((8..=12).contains(&delay), "{id}: {delay}");
            assert_eq!(delay, retry.delay_secs(id, 1));
        }
    }

    #[test]
    fn receive_count_reads_the_system_attribute() {
        let message = sqs::types::Message::builder()
            .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "4")
            .build();
        assert_eq!(receive_count(&message), 4);
        assert_eq!(receive_count(&sqs::types::Message::builder().build()), 1);
    }

    #[test]
    fn redrive_policy_accepts_number_or_string() {
        let arn = "arn:aws:sqs:us-east-1:123456789012:webhooks-dlq";
        let policy =
            |count: &str| format!(r#"{{"deadLetterTargetArn":"{arn}","maxReceiveCount":{count}}}"#);
        assert_eq!(parsed_max_receive_count(&policy("3")), Some(3));
        assert_eq!(parsed_max_receive_count(&policy(r#""3""#)), Some(3));
        assert_eq!(parsed_max_receive_count(&policy("0")), None);
        assert_eq!(parsed_max_receive_count(&policy(r#""-1""#)), None);
        assert_eq!(parsed_max_receive_count(&policy(r#""many""#)), None);
        assert_eq!(
            parsed_max_receive_count(&format!(r#"{{"deadLetterTargetArn":"{arn}"}}"#)),
            None
        );
        assert_eq!(parsed_max_receive_count("not json"), None);
    }

    #[test]
    fn dead_letter_url_replaces_the_queue_name() {
        assert_eq!(
            dead_letter_queue_url(
                "https://sqs.eu-west-1.amazonaws.com/123456789012/prod-sockudo-webhooks",
                "arn:aws:sqs:eu-west-1:123456789012:prod-sockudo-webhooks-dlq",
            )
            .as_deref(),
            Some("https://sqs.eu-west-1.amazonaws.com/123456789012/prod-sockudo-webhooks-dlq")
        );
        assert_eq!(
            dead_letter_queue_url(
                "http://localhost:9324/000000000000/webhooks.fifo",
                "arn:aws:sqs:elasticmq:000000000000:webhooks-dlq.fifo",
            )
            .as_deref(),
            Some("http://localhost:9324/000000000000/webhooks-dlq.fifo")
        );
        assert_eq!(
            dead_letter_queue_url(
                "https://sqs.us-east-1.amazonaws.com/1/q",
                "arn:aws:sqs:us-east-1:1:"
            ),
            None
        );
    }

    #[test]
    fn release_policy_bounds_each_redrive_state() {
        // No RedrivePolicy: the driver deletes at max_attempts.
        let missing = release(None, Some(5), false);
        assert!(!missing.exhausted(4));
        assert!(missing.exhausted(5));
        assert!(!missing.final_receive(5));
        assert_eq!(missing.attempt_limit(), 5);

        // Operator policy: SQS moves the message; the driver never deletes it.
        let policy = release(Some(3), None, false);
        assert!(!policy.exhausted(100));
        assert!(!policy.final_receive(2));
        assert!(policy.final_receive(3));
        assert_eq!(policy.attempt_limit(), 3);

        // Never validated (push stages): backoff only.
        let unvalidated = release(None, None, false);
        assert!(!unvalidated.exhausted(u32::MAX));
        assert!(!unvalidated.final_receive(u32::MAX));
        assert_eq!(unvalidated.attempt_limit(), 0);

        // Unreadable policy: a policy may exist with a higher maxReceiveCount
        // than max_attempts, so the driver never deletes and SQS still redrives.
        let unreadable = release(None, Some(5), true);
        assert!(!unreadable.exhausted(5));
        assert!(!unreadable.exhausted(u32::MAX));
        assert!(!unreadable.final_receive(5));
        assert_eq!(unreadable.attempt_limit(), 5);
    }

    #[tokio::test]
    async fn standard_queue_rejects_deduplication_keys() {
        let error = offline_manager(false)
            .await
            .enqueue(
                "push.publish.v1",
                job(),
                QueueJobOptions {
                    deduplication_key: Some("push.publish.v1:push-1".to_string()),
                    ..QueueJobOptions::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("require a FIFO queue"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn fifo_queue_rejects_per_message_delay() {
        let error = offline_manager(true)
            .await
            .enqueue(
                "push.publish.v1",
                job(),
                QueueJobOptions {
                    delay_ms: 1_000,
                    ..QueueJobOptions::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("do not support per-message delay"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn delays_beyond_fifteen_minutes_are_rejected() {
        let error = offline_manager(false)
            .await
            .enqueue(
                "push.publish.v1",
                job(),
                QueueJobOptions {
                    delay_ms: 900_001,
                    ..QueueJobOptions::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("must not exceed 900000"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn capabilities_match_send_validation() {
        let standard = offline_manager(false).await.capabilities();
        assert!(standard.delayed_delivery);
        assert!(!standard.deduplication);

        let fifo = offline_manager(true).await.capabilities();
        assert!(!fifo.delayed_delivery);
        assert!(fifo.deduplication);
    }
}
