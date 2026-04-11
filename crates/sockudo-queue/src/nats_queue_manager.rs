use crate::ArcJobProcessorFn;
use async_nats::jetstream;
use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::stream;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::NatsAdapterConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use tracing::{error, info, warn};

pub struct NatsJetStreamQueueManager {
    client: async_nats::Client,
    jetstream: jetstream::Context,
    prefix: String,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
}

impl NatsJetStreamQueueManager {
    pub async fn new(config: NatsAdapterConfig) -> Result<Self> {
        let mut options = async_nats::ConnectOptions::new();
        if let (Some(username), Some(password)) =
            (config.username.as_deref(), config.password.as_deref())
        {
            options = options.user_and_password(username.to_string(), password.to_string());
        } else if let Some(token) = config.token.as_deref() {
            options = options.token(token.to_string());
        }
        options = options.connection_timeout(std::time::Duration::from_millis(
            config.connection_timeout_ms,
        ));
        let client = options
            .connect(&config.servers)
            .await
            .map_err(|e| Error::Queue(format!("Failed to connect to NATS JetStream: {e}")))?;
        let jetstream = jetstream::new(client.clone());

        Ok(Self {
            client,
            jetstream,
            prefix: config.prefix,
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    fn normalize(value: &str) -> String {
        value
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect()
    }

    fn stream_name(&self, queue_name: &str) -> String {
        format!(
            "{}_q_{}",
            Self::normalize(&self.prefix),
            Self::normalize(queue_name)
        )
    }

    fn subject_name(&self, queue_name: &str) -> String {
        format!("{}.queue.{}", self.prefix, queue_name)
    }

    async fn ensure_stream(&self, queue_name: &str) -> Result<stream::Stream> {
        let stream_name = self.stream_name(queue_name);
        let subject = self.subject_name(queue_name);
        self.jetstream
            .get_or_create_stream(stream::Config {
                name: stream_name,
                subjects: vec![subject],
                max_messages: 100_000,
                ..Default::default()
            })
            .await
            .map_err(|e| Error::Queue(format!("Failed to ensure NATS JetStream queue stream: {e}")))
    }
}

#[async_trait]
impl QueueInterface for NatsJetStreamQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        let _stream = self.ensure_stream(queue_name).await?;
        let subject = self.subject_name(queue_name);
        let payload = sonic_rs::to_vec(&data)
            .map_err(|e| Error::Queue(format!("Failed to serialize NATS queue job: {e}")))?;
        self.jetstream
            .publish(subject, payload.into())
            .await
            .map_err(|e| Error::Queue(format!("Failed to publish NATS queue job: {e}")))?
            .await
            .map_err(|e| Error::Queue(format!("Failed to await NATS queue publish ack: {e}")))?;
        Ok(())
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
        let stream = self.ensure_stream(queue_name).await?;
        let consumer_name = format!("{}_consumer", self.stream_name(queue_name));
        let consumer = stream
            .get_or_create_consumer(
                consumer_name.as_str(),
                pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: std::time::Duration::from_secs(30),
                    max_deliver: 10,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| Error::Queue(format!("Failed to create NATS queue consumer: {e}")))?;

        let callback: ArcJobProcessorFn = Arc::from(callback);
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                let messages = match consumer.messages().await {
                    Ok(messages) => messages,
                    Err(e) => {
                        error!("Failed to open NATS JetStream message stream: {}", e);
                        break;
                    }
                };
                tokio::pin!(messages);
                loop {
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }
                    let message = tokio::select! {
                        _ = shutdown.notified() => break,
                        message = messages.try_next() => message,
                    };
                    match message {
                        Ok(Some(message)) => {
                            match sonic_rs::from_slice::<JobData>(&message.payload) {
                                Ok(job) => {
                                    if callback(job).await.is_ok() {
                                        if let Err(e) = message.ack().await {
                                            error!("Failed to ack NATS queue job: {}", e);
                                        }
                                    } else {
                                        warn!(
                                            "NATS queue job failed; message left unacked for redelivery"
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to deserialize NATS queue job: {}", e);
                                    let _ = message.ack().await;
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            error!("NATS queue consumer error: {}", e);
                            break;
                        }
                    }
                }
            }
            info!("NATS JetStream queue consumer stopped");
        });

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
        self.client
            .drain()
            .await
            .map_err(|e| Error::Queue(format!("Failed to drain NATS queue client: {e}")))?;
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        self.jetstream
            .query_account()
            .await
            .map(|_| ())
            .map_err(|e| Error::Queue(format!("NATS JetStream queue health check failed: {e}")))
    }
}
