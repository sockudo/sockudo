use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use redis::cluster::{ClusterClient, ClusterClientBuilder};
use redis::cluster_async::ClusterConnection;
use redis::{AsyncCommands, RedisResult};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{debug, error, info};

const MAX_PRODUCER_CONNECTIONS: usize = 8;
const RPUSH_ATTEMPTS: usize = 3;
const RPUSH_RETRY_BASE_DELAY: Duration = Duration::from_millis(50);
const WORKER_BLPOP_TIMEOUT_SECONDS: f64 = 1.0;

pub struct RedisClusterQueueManager {
    redis_client: ClusterClient,
    redis_connection: ClusterConnection,
    producer_connections: Box<[ClusterConnection]>,
    next_producer_connection: AtomicUsize,
    prefix: String,
    concurrency: usize,
}

impl RedisClusterQueueManager {
    pub async fn new(
        cluster_nodes: Vec<String>,
        prefix: &str,
        concurrency: usize,
        request_timeout_ms: u64,
    ) -> sockudo_core::error::Result<Self> {
        let client = Self::create_cluster_client(cluster_nodes.clone(), request_timeout_ms)?;

        let connection = client.get_async_connection().await.map_err(|e| {
            sockudo_core::error::Error::Connection(format!(
                "Failed to get Redis cluster connection: {e}"
            ))
        })?;
        let producer_connections = Self::create_producer_connections(&client, concurrency).await?;

        info!(
            "Connected to Redis cluster with {} nodes, prefix: {}, concurrency: {}, request timeout: {}ms",
            cluster_nodes.len(),
            prefix,
            concurrency,
            request_timeout_ms
        );

        Ok(Self {
            redis_client: client,
            redis_connection: connection,
            producer_connections,
            next_producer_connection: AtomicUsize::new(0),
            prefix: prefix.to_string(),
            concurrency,
        })
    }

    #[allow(dead_code)]
    pub fn start_processing(&self) {}

    fn create_cluster_client(
        cluster_nodes: Vec<String>,
        request_timeout_ms: u64,
    ) -> sockudo_core::error::Result<ClusterClient> {
        let builder = ClusterClientBuilder::new(cluster_nodes);
        let builder = if request_timeout_ms == 0 {
            builder.overall_response_timeout(None)
        } else {
            let request_timeout = Duration::from_millis(request_timeout_ms);
            builder
                .response_timeout(request_timeout)
                .overall_response_timeout(Some(request_timeout))
        };

        builder.build().map_err(|e| {
            sockudo_core::error::Error::Config(format!(
                "Failed to create Redis cluster client: {e}"
            ))
        })
    }

    async fn create_producer_connections(
        client: &ClusterClient,
        concurrency: usize,
    ) -> sockudo_core::error::Result<Box<[ClusterConnection]>> {
        let producer_count = concurrency.clamp(1, MAX_PRODUCER_CONNECTIONS);
        let mut producer_connections = Vec::with_capacity(producer_count);

        for _ in 0..producer_count {
            producer_connections.push(client.get_async_connection().await.map_err(|e| {
                sockudo_core::error::Error::Connection(format!(
                    "Failed to get Redis cluster producer connection: {e}"
                ))
            })?);
        }

        Ok(producer_connections.into_boxed_slice())
    }

    fn producer_connection(&self) -> ClusterConnection {
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
            .unwrap_or_else(|| "unknown Redis Cluster error".to_string());
        Err(sockudo_core::error::Error::Queue(format!(
            "Redis Cluster RPUSH failed for queue {queue_name}: {error}"
        )))
    }

    async fn start_worker(
        &self,
        queue_name: &str,
        queue_key: String,
        processor: ArcJobProcessorFn,
        worker_id: usize,
    ) -> sockudo_core::error::Result<tokio::task::JoinHandle<()>> {
        let mut worker_conn = self
            .redis_client
            .get_async_connection()
            .await
            .map_err(|e| {
                sockudo_core::error::Error::Connection(format!(
                    "Failed to get Redis cluster worker connection: {e}"
                ))
            })?;
        let worker_queue_name = queue_name.to_string();

        Ok(tokio::spawn(async move {
            debug!(
                "{}",
                format!(
                    "Starting Redis cluster queue worker {} for queue: {}",
                    worker_id, worker_queue_name
                )
            );

            loop {
                let blpop_result: RedisResult<Option<(String, String)>> = worker_conn
                    .blpop(&queue_key, WORKER_BLPOP_TIMEOUT_SECONDS)
                    .await;

                match blpop_result {
                    Ok(Some((_key, job_data_str))) => {
                        match sonic_rs::from_str::<JobData>(&job_data_str) {
                            Ok(job_data) => {
                                if let Err(e) = processor(job_data).await {
                                    error!("{}", format!("Cluster worker error: {}", e));
                                } else {
                                    debug!("{}", "Cluster worker finished".to_string());
                                }
                            }
                            Err(e) => {
                                error!(
                                    "{}",
                                    format!(
                                        "[Cluster Worker {}] Error deserializing job data from Redis cluster queue {}: {}. Data: '{}'",
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
                                "[Cluster Worker {}] Redis cluster BLPOP error on queue {}: {}",
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
impl QueueInterface for RedisClusterQueueManager {
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

        debug!(
            "{}",
            format!(
                "Registered processor and starting workers for Redis cluster queue: {}",
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
                        "Redis cluster scan error during disconnect: {e}"
                    ))
                })?;

            while let Some(key) = iter.next_item().await {
                let key = key.map_err(|e| {
                    sockudo_core::error::Error::Queue(format!(
                        "Redis cluster scan iteration error during disconnect: {e}"
                    ))
                })?;
                keys.push(key);
            }
            keys
        };

        for key in keys {
            conn.del::<_, ()>(&key).await.map_err(|e| {
                sockudo_core::error::Error::Queue(format!(
                    "Redis cluster delete error during disconnect: {e}"
                ))
            })?;
        }
        Ok(())
    }

    async fn check_health(&self) -> sockudo_core::error::Result<()> {
        let mut conn = self
            .redis_client
            .get_async_connection()
            .await
            .map_err(|e| {
                sockudo_core::error::Error::Redis(format!(
                    "Queue Redis Cluster connection failed: {e}"
                ))
            })?;

        let response = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| {
                sockudo_core::error::Error::Redis(format!("Queue Redis Cluster PING failed: {e}"))
            })?;

        if response == "PONG" {
            Ok(())
        } else {
            Err(sockudo_core::error::Error::Redis(format!(
                "Queue Redis Cluster PING returned unexpected response: {response}"
            )))
        }
    }
}
