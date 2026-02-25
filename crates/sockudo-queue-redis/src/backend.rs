use crate::{ArcJobProcessorFn, JobProcessorFnAsync, QueueBackend, QueueResult};
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, RedisResult};
use sockudo_types::webhook::JobData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error};

pub struct RedisQueueManager {
    redis_client: redis::Client,
    redis_connection: Arc<Mutex<ConnectionManager>>,
    job_processors: dashmap::DashMap<String, ArcJobProcessorFn, ahash::RandomState>,
    prefix: String,
    concurrency: usize,
}

impl RedisQueueManager {
    pub async fn new(redis_url: &str, prefix: &str, concurrency: usize) -> QueueResult<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| format!("Failed to open Redis client: {e}"))?;

        let connection_manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_number_of_retries(5)
            .set_exponent_base(2.0)
            .set_max_delay(std::time::Duration::from_millis(5000));

        let connection = client
            .get_connection_manager_with_config(connection_manager_config)
            .await
            .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

        Ok(Self {
            redis_client: client,
            redis_connection: Arc::new(Mutex::new(connection)),
            job_processors: dashmap::DashMap::with_hasher(ahash::RandomState::new()),
            prefix: prefix.to_string(),
            concurrency,
        })
    }

    pub fn start_processing(&self) {}

    async fn format_key(&self, queue_name: &str) -> String {
        format!("{}:queue:{}", self.prefix, queue_name)
    }
}

#[async_trait]
impl QueueBackend for RedisQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> QueueResult<()> {
        let queue_key = self.format_key(queue_name).await;
        let data_json = sonic_rs::to_string(&data).map_err(|e| e.to_string())?;

        let mut conn = self.redis_connection.lock().await;
        conn.rpush::<_, _, ()>(&queue_key, data_json)
            .await
            .map_err(|e| format!("Redis RPUSH failed for queue {queue_name}: {e}"))?;

        Ok(())
    }

    async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> QueueResult<()> {
        let queue_key = self.format_key(queue_name).await;

        let processor_arc: ArcJobProcessorFn = Arc::from(callback);
        self.job_processors
            .insert(queue_name.to_string(), processor_arc.clone());
        debug!("Registered processor and starting workers for Redis queue: {queue_name}");

        for i in 0..self.concurrency {
            let worker_queue_key = queue_key.clone();
            let worker_redis_conn = self.redis_connection.clone();
            let worker_processor = processor_arc.clone();
            let worker_queue_name = queue_name.to_string();

            tokio::spawn(async move {
                debug!("Starting Redis queue worker {i} for queue: {worker_queue_name}");

                loop {
                    let blpop_result: RedisResult<Option<(String, String)>> = {
                        let mut conn = worker_redis_conn.lock().await;
                        conn.blpop(&worker_queue_key, 0.01).await
                    };

                    match blpop_result {
                        Ok(Some((_key, job_data_str))) => {
                            match sonic_rs::from_str::<JobData>(&job_data_str) {
                                Ok(job_data) => {
                                    if let Err(e) = worker_processor(job_data).await {
                                        error!("Worker error: {e}");
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "[Worker {i}] Error deserializing job data from Redis queue {worker_queue_name}: {e}. Data: '{job_data_str}'"
                                    );
                                }
                            }
                        }
                        Ok(None) => continue,
                        Err(e) => {
                            error!(
                                "[Worker {i}] Redis BLPOP error on queue {worker_queue_name}: {e}"
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
        }

        Ok(())
    }

    async fn disconnect(&self) -> QueueResult<()> {
        let mut conn = self.redis_connection.lock().await;
        let pattern = format!("{}:queue:*", self.prefix);

        let keys = {
            let mut keys: Vec<String> = Vec::new();
            let mut iter: redis::AsyncIter<String> = conn
                .scan_match(&pattern)
                .await
                .map_err(|e| format!("Redis scan error during disconnect: {e}"))?;

            while let Some(key) = iter.next_item().await {
                let key =
                    key.map_err(|e| format!("Redis scan iteration error during disconnect: {e}"))?;
                keys.push(key);
            }
            keys
        };

        for key in keys {
            conn.del::<_, ()>(&key)
                .await
                .map_err(|e| format!("Redis delete error during disconnect: {e}"))?;
        }
        Ok(())
    }

    async fn check_health(&self) -> QueueResult<()> {
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("Queue Redis connection failed: {e}"))?;

        let response = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| format!("Queue Redis PING failed: {e}"))?;

        if response == "PONG" {
            Ok(())
        } else {
            Err(format!(
                "Queue Redis PING returned unexpected response: {response}"
            ))
        }
    }
}
