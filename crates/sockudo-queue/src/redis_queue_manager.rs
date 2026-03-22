use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, RedisResult};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};

pub struct RedisQueueManager {
    redis_client: redis::Client,
    redis_connection: ConnectionManager,
    prefix: String,
    concurrency: usize,
}

impl RedisQueueManager {
    pub async fn new(
        redis_url: &str,
        prefix: &str,
        concurrency: usize,
    ) -> sockudo_core::error::Result<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| {
            sockudo_core::error::Error::Config(format!("Failed to open Redis client: {e}"))
        })?;

        let connection = Self::create_connection_manager(&client).await?;

        Ok(Self {
            redis_client: client,
            redis_connection: connection,
            prefix: prefix.to_string(),
            concurrency,
        })
    }

    #[allow(dead_code)]
    pub fn start_processing(&self) {}

    async fn create_connection_manager(
        client: &redis::Client,
    ) -> sockudo_core::error::Result<ConnectionManager> {
        let connection_manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_number_of_retries(5)
            .set_exponent_base(2.0)
            .set_max_delay(std::time::Duration::from_millis(5000));

        client
            .get_connection_manager_with_config(connection_manager_config)
            .await
            .map_err(|e| {
                sockudo_core::error::Error::Connection(format!(
                    "Failed to get Redis connection: {e}"
                ))
            })
    }

    async fn format_key(&self, queue_name: &str) -> String {
        format!("{}:queue:{}", self.prefix, queue_name)
    }

    async fn start_worker(
        &self,
        queue_name: &str,
        queue_key: String,
        processor: ArcJobProcessorFn,
        worker_id: usize,
    ) -> sockudo_core::error::Result<tokio::task::JoinHandle<()>> {
        let mut worker_conn = Self::create_connection_manager(&self.redis_client).await?;
        let worker_queue_name = queue_name.to_string();

        Ok(tokio::spawn(async move {
            debug!(
                "{}",
                format!(
                    "Starting Redis queue worker {} for queue: {}",
                    worker_id, worker_queue_name
                )
            );

            loop {
                let blpop_result: RedisResult<Option<(String, String)>> =
                    worker_conn.blpop(&queue_key, 0.01).await;

                match blpop_result {
                    Ok(Some((_key, job_data_str))) => {
                        match sonic_rs::from_str::<JobData>(&job_data_str) {
                            Ok(job_data) => {
                                if let Err(e) = processor(job_data).await {
                                    error!("{}", format!("Worker error: {}", e));
                                } else {
                                    debug!("{}", "Worker finished".to_string());
                                }
                            }
                            Err(e) => {
                                error!(
                                    "{}",
                                    format!(
                                        "[Worker {}] Error deserializing job data from Redis queue {}: {}. Data: '{}'",
                                        worker_id, worker_queue_name, e, job_data_str
                                    )
                                );
                            }
                        }
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        error!(
                            "{}",
                            format!(
                                "[Worker {}] Redis BLPOP error on queue {}: {}",
                                worker_id, worker_queue_name, e
                            )
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }))
    }
}

#[async_trait]
impl QueueInterface for RedisQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> sockudo_core::error::Result<()>
    where
        JobData: Serialize,
    {
        let queue_key = self.format_key(queue_name).await;
        let data_json = sonic_rs::to_string(&data)?;
        let mut conn = self.redis_connection.clone();

        conn.rpush::<_, _, ()>(&queue_key, data_json)
            .await
            .map_err(|e| {
                sockudo_core::error::Error::Queue(format!(
                    "Redis RPUSH failed for queue {queue_name}: {e}"
                ))
            })?;

        Ok(())
    }

    async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> sockudo_core::error::Result<()>
    where
        JobData: DeserializeOwned + Send + 'static,
    {
        let queue_key = self.format_key(queue_name).await;
        let processor_arc: ArcJobProcessorFn = Arc::from(callback);

        debug!(
            "{}",
            format!(
                "Registered processor and starting workers for Redis queue: {}",
                queue_name
            )
        );

        for worker_id in 0..self.concurrency {
            self.start_worker(
                queue_name,
                queue_key.clone(),
                processor_arc.clone(),
                worker_id,
            )
            .await?;
        }

        Ok(())
    }

    async fn disconnect(&self) -> sockudo_core::error::Result<()> {
        let mut conn = self.redis_connection.clone();
        let pattern = format!("{}:queue:*", self.prefix);

        let keys = {
            let mut keys = Vec::new();
            let mut iter: redis::AsyncIter<String> =
                conn.scan_match(&pattern).await.map_err(|e| {
                    sockudo_core::error::Error::Queue(format!(
                        "Redis scan error during disconnect: {e}"
                    ))
                })?;

            while let Some(key) = iter.next_item().await {
                let key = key.map_err(|e| {
                    sockudo_core::error::Error::Queue(format!(
                        "Redis scan iteration error during disconnect: {e}"
                    ))
                })?;
                keys.push(key);
            }
            keys
        };

        for key in keys {
            conn.del::<_, ()>(&key).await.map_err(|e| {
                sockudo_core::error::Error::Queue(format!(
                    "Redis delete error during disconnect: {e}"
                ))
            })?;
        }
        Ok(())
    }

    async fn check_health(&self) -> sockudo_core::error::Result<()> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                sockudo_core::error::Error::Redis(format!("Queue Redis connection failed: {e}"))
            })?;

        let response = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| {
                sockudo_core::error::Error::Redis(format!("Queue Redis PING failed: {e}"))
            })?;

        if response == "PONG" {
            Ok(())
        } else {
            Err(sockudo_core::error::Error::Redis(format!(
                "Queue Redis PING returned unexpected response: {response}"
            )))
        }
    }
}
