use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, RedisResult};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{debug, error};

const MAX_PRODUCER_CONNECTIONS: usize = 8;
const HEALTH_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const RPUSH_ATTEMPTS: usize = 3;
const RPUSH_RETRY_BASE_DELAY: Duration = Duration::from_millis(50);
const WORKER_BLPOP_TIMEOUT_SECONDS: f64 = 1.0;

pub struct RedisQueueManager {
    redis_client: redis::Client,
    redis_connection: ConnectionManager,
    producer_connections: Box<[ConnectionManager]>,
    next_producer_connection: AtomicUsize,
    health_connection: ConnectionManager,
    prefix: String,
    concurrency: usize,
}

impl RedisQueueManager {
    pub async fn new(
        redis_url: &str,
        prefix: &str,
        concurrency: usize,
        response_timeout_ms: u64,
    ) -> sockudo_core::error::Result<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| {
            sockudo_core::error::Error::Config(format!("Failed to open Redis client: {e}"))
        })?;

        let response_timeout = Self::response_timeout_from_ms(response_timeout_ms);
        let connection = Self::create_connection_manager(&client, response_timeout).await?;
        let health_connection =
            Self::create_connection_manager(&client, Some(HEALTH_RESPONSE_TIMEOUT)).await?;
        let producer_connections =
            Self::create_producer_connections(&client, concurrency, response_timeout).await?;

        Ok(Self {
            redis_client: client,
            redis_connection: connection,
            producer_connections,
            next_producer_connection: AtomicUsize::new(0),
            health_connection,
            prefix: prefix.to_string(),
            concurrency,
        })
    }

    #[allow(dead_code)]
    pub fn start_processing(&self) {}

    async fn create_connection_manager(
        client: &redis::Client,
        response_timeout: Option<Duration>,
    ) -> sockudo_core::error::Result<ConnectionManager> {
        let connection_manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_number_of_retries(5)
            .set_exponent_base(2.0)
            .set_max_delay(Duration::from_millis(5000))
            .set_response_timeout(response_timeout);

        client
            .get_connection_manager_with_config(connection_manager_config)
            .await
            .map_err(|e| {
                sockudo_core::error::Error::Connection(format!(
                    "Failed to get Redis connection: {e}"
                ))
            })
    }

    async fn create_producer_connections(
        client: &redis::Client,
        concurrency: usize,
        response_timeout: Option<Duration>,
    ) -> sockudo_core::error::Result<Box<[ConnectionManager]>> {
        let producer_count = concurrency.clamp(1, MAX_PRODUCER_CONNECTIONS);
        let mut producer_connections = Vec::with_capacity(producer_count);

        for _ in 0..producer_count {
            producer_connections
                .push(Self::create_connection_manager(client, response_timeout).await?);
        }

        Ok(producer_connections.into_boxed_slice())
    }

    fn response_timeout_from_ms(response_timeout_ms: u64) -> Option<Duration> {
        if response_timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(response_timeout_ms))
        }
    }

    fn producer_connection(&self) -> ConnectionManager {
        let connection_index = self
            .next_producer_connection
            .fetch_add(1, Ordering::Relaxed)
            % self.producer_connections.len();

        self.producer_connections[connection_index].clone()
    }

    fn format_key(&self, queue_name: &str) -> String {
        format!("{}:queue:{}", self.prefix, queue_name)
    }

    async fn rpush_serialized_jobs(
        &self,
        queue_name: &str,
        queue_key: &str,
        data_json: &[String],
    ) -> sockudo_core::error::Result<()> {
        let mut last_error = None;

        for attempt in 0..RPUSH_ATTEMPTS {
            let mut conn = self.producer_connection();
            match conn.rpush::<_, _, ()>(queue_key, data_json.to_vec()).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 < RPUSH_ATTEMPTS {
                        tokio::time::sleep(RPUSH_RETRY_BASE_DELAY * (attempt as u32 + 1)).await;
                    }
                }
            }
        }

        let error = last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown Redis error".to_string());
        Err(sockudo_core::error::Error::Queue(format!(
            "Redis RPUSH failed for queue {queue_name}: {error}"
        )))
    }

    async fn start_worker(
        &self,
        queue_name: &str,
        queue_key: String,
        processor: ArcJobProcessorFn,
        worker_id: usize,
    ) -> sockudo_core::error::Result<tokio::task::JoinHandle<()>> {
        let mut worker_conn = Self::create_connection_manager(&self.redis_client, None).await?;
        let worker_queue_name = queue_name.to_string();

        Ok(tokio::spawn(async move {
            debug!(worker_id = %worker_id, queue = %worker_queue_name, "starting redis queue worker");

            loop {
                let blpop_result: RedisResult<Option<(String, String)>> = worker_conn
                    .blpop(&queue_key, WORKER_BLPOP_TIMEOUT_SECONDS)
                    .await;

                match blpop_result {
                    Ok(Some((_key, job_data_str))) => {
                        match sonic_rs::from_str::<JobData>(&job_data_str) {
                            Ok(job_data) => {
                                if let Err(e) = processor(job_data).await {
                                    error!(worker_id = %worker_id, queue = %worker_queue_name, error = %e, "worker processing error");
                                } else {
                                    debug!(worker_id = %worker_id, queue = %worker_queue_name, "worker processed job");
                                }
                            }
                            Err(e) => {
                                error!(worker_id = %worker_id, queue = %worker_queue_name, error = %e, "worker job deserialization error");
                            }
                        }
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        if e.to_string().contains("timed out") {
                            continue;
                        }
                        error!(worker_id = %worker_id, queue = %worker_queue_name, error = %e, "worker redis blpop error");
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
        let queue_key = self.format_key(queue_name);
        let data_json = vec![sonic_rs::to_string(&data)?];
        self.rpush_serialized_jobs(queue_name, &queue_key, &data_json)
            .await
    }

    async fn add_batch_to_queue(
        &self,
        queue_name: &str,
        data: Vec<JobData>,
    ) -> sockudo_core::error::Result<()>
    where
        JobData: Serialize,
    {
        if data.is_empty() {
            return Ok(());
        }

        let queue_key = self.format_key(queue_name);
        let data_json = data
            .iter()
            .map(sonic_rs::to_string)
            .collect::<Result<Vec<_>, _>>()?;
        self.rpush_serialized_jobs(queue_name, &queue_key, &data_json)
            .await
    }

    async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> sockudo_core::error::Result<()>
    where
        JobData: DeserializeOwned + Send + 'static,
    {
        let queue_key = self.format_key(queue_name);
        let processor_arc: ArcJobProcessorFn = Arc::from(callback);

        debug!(queue = %queue_name, worker_count = self.concurrency, "registered processor and starting redis queue workers");

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
        let mut conn = self.health_connection.clone();

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_response_timeout_disables_redis_client_deadline() {
        assert_eq!(RedisQueueManager::response_timeout_from_ms(0), None);
        assert_eq!(
            RedisQueueManager::response_timeout_from_ms(5000),
            Some(Duration::from_millis(5000))
        );
    }
}
