use crate::cache::manager::CacheManager;
use crate::error::{Error, Result};
use async_trait::async_trait;
use redis::AsyncCommands;
use redis::cluster::{ClusterClient, ClusterClientBuilder};
use redis::cluster_async::ClusterConnection;
use std::time::Duration;
use tokio::sync::Mutex;

/// Configuration for the Redis Cluster cache manager
#[derive(Clone, Debug)]
pub struct RedisClusterCacheConfig {
    /// Redis cluster nodes (array of "host:port" strings)
    pub nodes: Vec<String>,
    /// Key prefix
    pub prefix: String,
    /// Response timeout
    pub response_timeout: Option<Duration>,
    /// Read from replicas (if supported)
    pub read_from_replicas: bool,
}

impl Default for RedisClusterCacheConfig {
    fn default() -> Self {
        Self {
            nodes: vec!["127.0.0.1:6379".to_string()],
            prefix: "cache".to_string(),
            response_timeout: Some(Duration::from_secs(5)),
            read_from_replicas: false,
        }
    }
}

/// A Redis Cluster-based implementation of the CacheManager trait
pub struct RedisClusterCacheManager {
    client: ClusterClient,
    connection: Mutex<ClusterConnection>,
    prefix: String,
}

impl RedisClusterCacheManager {
    pub async fn new(config: RedisClusterCacheConfig) -> Result<Self> {
        let mut builder = ClusterClientBuilder::new(config.nodes.clone());
        if let Some(timeout) = config.response_timeout {
            builder = builder.response_timeout(timeout)
        }

        if config.read_from_replicas {
            builder = builder.read_from_replicas();
        }

        let client = builder
            .build()
            .map_err(|e| Error::Cache(format!("Failed to create Redis Cluster client: {e}")))?;

        let connection = client
            .get_async_connection()
            .await
            .map_err(|e| Error::Cache(format!("Failed to connect to Redis Cluster: {e}")))?;

        Ok(Self {
            client,
            connection: Mutex::new(connection),
            prefix: config.prefix,
        })
    }

    pub async fn with_nodes(nodes: Vec<String>, prefix: Option<&str>) -> Result<Self> {
        let config = RedisClusterCacheConfig {
            nodes,
            prefix: prefix.unwrap_or("cache").to_string(),
            ..Default::default()
        };

        Self::new(config).await
    }

    fn prefixed_key(&self, key: &str) -> String {
        format!("{}:{}", self.prefix, key)
    }
}

#[async_trait]
impl CacheManager for RedisClusterCacheManager {
    async fn has(&self, key: &str) -> Result<bool> {
        let mut connection = self.connection.lock().await;
        let exists: bool = connection
            .exists(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster exists error: {e}")))?;
        Ok(exists)
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        let mut connection = self.connection.lock().await;
        let value: Option<String> = connection
            .get(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster get error: {e}")))?;
        Ok(value)
    }

    async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<()> {
        let prefixed_key = self.prefixed_key(key);
        let mut connection = self.connection.lock().await;

        if ttl_seconds > 0 {
            connection
                .set_ex::<_, _, ()>(prefixed_key, value, ttl_seconds)
                .await
                .map_err(|e| Error::Cache(format!("Redis Cluster set error: {e}")))?;
        } else {
            connection
                .set::<_, _, ()>(prefixed_key, value)
                .await
                .map_err(|e| Error::Cache(format!("Redis Cluster set error: {e}")))?;
        }

        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        let mut connection = self.connection.lock().await;
        let deleted: i32 = connection
            .del(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster delete error: {e}")))?;
        if deleted == 0 {
            return Err(Error::Cache(format!("Key '{key}' not found")));
        }
        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.clear_prefix().await?;
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        let mut connection =
            self.client.get_async_connection().await.map_err(|e| {
                Error::Cache(format!("Failed to acquire health check connection: {e}"))
            })?;

        let response = redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map_err(|e| {
                Error::Cache(format!("Cache Redis Cluster health check PING failed: {e}"))
            })?;

        if response == "PONG" {
            Ok(())
        } else {
            Err(Error::Cache(format!(
                "Cache Redis Cluster PING returned unexpected response: {response}"
            )))
        }
    }

    async fn ttl(&self, key: &str) -> Result<Option<Duration>> {
        let mut connection = self.connection.lock().await;
        let ttl: i64 = connection
            .ttl(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster TTL error: {e}")))?;
        if ttl < 0 {
            return Ok(None);
        }
        Ok(Some(Duration::from_secs(ttl as u64)))
    }
}

impl RedisClusterCacheManager {
    pub async fn delete(&self, key: &str) -> Result<bool> {
        let mut connection = self.connection.lock().await;
        let deleted: i32 = connection
            .del(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster delete error: {e}")))?;
        Ok(deleted > 0)
    }

    pub async fn clear_prefix(&self) -> Result<usize> {
        let pattern = format!("{}:*", self.prefix);
        let mut connection = self.connection.lock().await;

        let keys = {
            let mut keys = Vec::new();
            let mut iter: redis::AsyncIter<String> = connection
                .scan_match(&pattern)
                .await
                .map_err(|e| Error::Cache(format!("Redis Cluster scan error: {e}")))?;

            while let Some(key) = iter.next_item().await {
                let key = key.map_err(|e| {
                    Error::Cache(format!("Redis Cluster scan iteration error: {e}"))
                })?;
                keys.push(key);
            }
            keys
        };

        if keys.is_empty() {
            return Ok(0);
        }

        let mut deleted_count = 0;
        for key in keys {
            let deleted: i32 = connection
                .del(&key)
                .await
                .map_err(|e| Error::Cache(format!("Redis Cluster delete error: {e}")))?;
            deleted_count += deleted as usize;
        }

        Ok(deleted_count)
    }

    pub async fn set_many(&self, pairs: &[(&str, &str)], ttl_seconds: u64) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }

        let prefixed_pairs: Vec<(String, &str)> = pairs
            .iter()
            .map(|(k, v)| (self.prefixed_key(k), *v))
            .collect();

        let mut connection = self.connection.lock().await;
        for (key, value) in &prefixed_pairs {
            if ttl_seconds > 0 {
                connection
                    .set_ex::<_, _, ()>(key, *value, ttl_seconds)
                    .await
                    .map_err(|e| Error::Cache(format!("Redis Cluster set_ex error: {e}")))?;
            } else {
                connection
                    .set::<_, _, ()>(key, *value)
                    .await
                    .map_err(|e| Error::Cache(format!("Redis Cluster set error: {e}")))?;
            }
        }

        Ok(())
    }

    pub async fn increment(&self, key: &str, by: i64) -> Result<i64> {
        let mut connection = self.connection.lock().await;
        let value: i64 = connection
            .incr(self.prefixed_key(key), by)
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster increment error: {e}")))?;
        Ok(value)
    }

    pub async fn get_remaining_ttl() {
        todo!()
    }

    pub async fn get_many(&self, keys: &[&str]) -> Result<Vec<Option<String>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(keys.len());
        let mut connection = self.connection.lock().await;
        for key in keys {
            let value: Option<String> = connection
                .get(self.prefixed_key(key))
                .await
                .map_err(|e| Error::Cache(format!("Redis Cluster get error: {e}")))?;
            results.push(value);
        }

        Ok(results)
    }

    pub fn get_client(&self) -> ClusterClient {
        self.client.clone()
    }

    pub async fn get_connection(&self) -> Result<ClusterConnection> {
        self.client
            .get_async_connection()
            .await
            .map_err(|e| Error::Cache(format!("Failed to get Redis Cluster connection: {e}")))
    }

    pub async fn get_cluster_info(&self) -> Result<String> {
        let mut connection = self.connection.lock().await;
        let info: String = redis::cmd("CLUSTER")
            .arg("INFO")
            .query_async(&mut *connection)
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster info error: {e}")))?;

        Ok(info)
    }

    pub async fn get_cluster_nodes(&self) -> Result<String> {
        let mut connection = self.connection.lock().await;
        let nodes: String = redis::cmd("CLUSTER")
            .arg("NODES")
            .query_async(&mut *connection)
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster nodes error: {e}")))?;

        Ok(nodes)
    }
}

/// Factory for creating cache managers
pub struct ClusterCacheManagerFactory;

impl ClusterCacheManagerFactory {
    pub async fn create_redis_cluster(
        nodes: Vec<String>,
        prefix: Option<&str>,
        response_timeout: Option<Duration>,
        read_from_replicas: bool,
    ) -> Result<Box<dyn CacheManager + Send>> {
        let config = RedisClusterCacheConfig {
            nodes,
            prefix: prefix.unwrap_or("cache").to_string(),
            response_timeout,
            read_from_replicas,
        };

        let cache_manager = RedisClusterCacheManager::new(config).await?;
        Ok(Box::new(cache_manager))
    }
}
