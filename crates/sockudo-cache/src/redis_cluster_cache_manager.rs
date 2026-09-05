use async_trait::async_trait;
use redis::AsyncCommands;
use redis::cluster::{ClusterClient, ClusterClientBuilder};
use redis::cluster_async::ClusterConnection;
use redis::cluster_read_routing::RandomReplicaStrategy;
use redis::cluster_routing::{Route, RoutingInfo, SingleNodeRoutingInfo, SlotAddr};
use sockudo_core::cache::{CacheManager, CacheScanPage};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::RedisTlsOptions;
use sockudo_core::redis_client::configure_cluster_builder;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;

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
    /// TLS settings for cluster data connections.
    pub tls: RedisTlsOptions,
}

impl Default for RedisClusterCacheConfig {
    fn default() -> Self {
        Self {
            nodes: vec!["127.0.0.1:6379".to_string()],
            prefix: "cache".to_string(),
            response_timeout: Some(Duration::from_secs(5)),
            read_from_replicas: false,
            tls: RedisTlsOptions::default(),
        }
    }
}

/// A Redis Cluster-based implementation of the CacheManager trait
pub struct RedisClusterCacheManager {
    client: ClusterClient,
    /// ClusterConnection is internally multiplexed (backed by MultiplexedConnection per node).
    /// Clone is cheap -- clones share the same internal per-node connection pool.
    connection: ClusterConnection,
    prefix: String,
}

impl RedisClusterCacheManager {
    pub async fn new(config: RedisClusterCacheConfig) -> Result<Self> {
        let mut builder = ClusterClientBuilder::new(config.nodes.clone());
        if let Some(timeout) = config.response_timeout {
            builder = builder.response_timeout(timeout)
        }

        if config.read_from_replicas {
            builder = builder.read_routing_strategy(RandomReplicaStrategy);
        }
        builder = configure_cluster_builder(builder, &config.tls).await?;

        let client = builder
            .build()
            .map_err(|e| Error::Cache(format!("Failed to create Redis Cluster client: {e}")))?;

        let connection = sockudo_core::redis_client::cluster_connect_with_retry(&client).await?;

        Ok(Self {
            client,
            connection,
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
        let mut connection = self.connection.clone();
        let exists: bool = connection
            .exists(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster exists error: {e}")))?;
        Ok(exists)
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        let mut connection = self.connection.clone();
        let value: Option<String> = connection
            .get(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster get error: {e}")))?;
        Ok(value)
    }

    async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<()> {
        let prefixed_key = self.prefixed_key(key);
        let mut connection = self.connection.clone();

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
        let mut connection = self.connection.clone();
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
        // ClusterConnection resources are released on drop. Shared TTL-managed
        // state must survive an individual node's graceful shutdown.
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        let mut connection = self.connection.clone();

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
        let mut connection = self.connection.clone();
        let ttl: i64 = connection
            .ttl(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster TTL error: {e}")))?;
        if ttl < 0 {
            return Ok(None);
        }
        Ok(Some(Duration::from_secs(ttl as u64)))
    }

    async fn scan_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
        let mut entries = Vec::with_capacity(limit.min(256));
        let mut cursor = None;
        while entries.len() < limit {
            let page = self
                .scan_prefix_page(prefix, cursor, limit - entries.len())
                .await?;
            entries.extend(page.entries.into_iter().take(limit - entries.len()));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(entries)
    }

    async fn scan_prefix_page(
        &self,
        prefix: &str,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<CacheScanPage> {
        if limit == 0 {
            return Ok(CacheScanPage::default());
        }

        let mut connection = self.connection.clone();
        let (primaries, generation) = read_scan_topology(&mut connection).await?;
        let (node_index, scan_cursor) = match cursor.as_deref() {
            None | Some("0") => (0, 0),
            Some(cursor) => {
                let mut fields = cursor.split(':');
                let version = fields.next();
                let expected = fields.next().and_then(|value| value.parse::<u64>().ok());
                let node = fields.next().and_then(|value| value.parse::<usize>().ok());
                let scan = fields.next().and_then(|value| value.parse::<u64>().ok());
                if version != Some("v1") || expected != Some(generation) || fields.next().is_some()
                {
                    return Err(Error::Cache(
                        "Redis Cluster scan cursor expired or topology changed; restart scan"
                            .into(),
                    ));
                }
                match (node, scan) {
                    (Some(node), Some(scan)) if node < primaries.len() => (node, scan),
                    _ => return Err(Error::Cache("invalid Redis Cluster scan cursor".into())),
                }
            }
        };
        let slot = *primaries
            .values()
            .nth(node_index)
            .expect("validated cluster scan primary");
        let pattern = format!("{}:{}*", self.prefix, prefix);
        let cache_prefix = format!("{}:", self.prefix);
        let mut command = redis::cmd("SCAN");
        command
            .arg(scan_cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(limit.min(256));
        let result = connection
            .route_command(
                command,
                RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(Route::new(
                    slot,
                    SlotAddr::Master,
                ))),
            )
            .await
            .map_err(|error| Error::Cache(format!("Redis Cluster scan page error: {error}")))?;
        let (next_cursor, keys): (u64, Vec<String>) = redis::from_redis_value(result)
            .map_err(|error| Error::Cache(format!("Redis Cluster scan result error: {error}")))?;
        let continuation = if next_cursor != 0 {
            Some(format!("v1:{generation}:{node_index}:{next_cursor}"))
        } else if node_index + 1 < primaries.len() {
            Some(format!("v1:{generation}:{}:0", node_index + 1))
        } else {
            None
        };

        let mut entries = Vec::with_capacity(keys.len());
        for keys in keys.chunks(256) {
            let mut pipeline = redis::pipe();
            for key in keys {
                pipeline.cmd("GET").arg(key);
            }
            let response = connection
                .route_pipeline(
                    pipeline,
                    0,
                    keys.len(),
                    SingleNodeRoutingInfo::SpecificNode(Route::new(slot, SlotAddr::Master)),
                )
                .await
                .map_err(|error| {
                    Error::Cache(format!("Redis Cluster scan values error: {error}"))
                })?;
            let values: Vec<Option<String>> = response
                .into_iter()
                .map(redis::from_redis_value)
                .collect::<std::result::Result<_, _>>()
                .map_err(|error| {
                    Error::Cache(format!("Redis Cluster scan value decode error: {error}"))
                })?;
            for (key, value) in keys.iter().zip(values) {
                if let Some(value) = value
                    && let Some(unprefixed_key) = key.strip_prefix(&cache_prefix)
                {
                    entries.push((unprefixed_key.to_owned(), value));
                }
            }
        }

        // A topology change during the final page must also invalidate the sweep.
        // Routing a representative slot alone cannot prove that all scanned keys
        // remained on their original primary during resharding.
        if read_scan_topology(&mut connection).await?.1 != generation {
            return Err(Error::Cache(
                "Redis Cluster topology changed during scan; restart scan".into(),
            ));
        }

        Ok(CacheScanPage {
            entries,
            next_cursor: continuation,
        })
    }

    async fn set_if_not_exists(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        let mut connection = self.connection.clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(&prefixed_key)
            .arg(value)
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut connection)
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster SET NX error: {e}")))?;
        Ok(result.is_some())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: &str,
        value: &str,
        ttl_seconds: u64,
    ) -> Result<bool> {
        let mut connection = self.connection.clone();
        let result: i32 = redis::Script::new(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3]); return 1 else return 0 end",
        )
        .key(self.prefixed_key(key))
        .arg(expected)
        .arg(value)
        .arg(ttl_seconds.max(1))
        .invoke_async(&mut connection)
        .await
        .map_err(|e| Error::Cache(format!("Redis Cluster compare-and-swap error: {e}")))?;
        Ok(result == 1)
    }

    async fn compare_and_remove(&self, key: &str, expected: &str) -> Result<bool> {
        let mut connection = self.connection.clone();
        let result: i32 = redis::Script::new(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
        )
        .key(self.prefixed_key(key))
        .arg(expected)
        .invoke_async(&mut connection)
        .await
        .map_err(|e| Error::Cache(format!("Redis Cluster compare-and-remove error: {e}")))?;
        Ok(result == 1)
    }

    async fn increment_by(&self, key: &str, delta: i64, ttl_seconds: u64) -> Result<i64> {
        let prefixed_key = self.prefixed_key(key);
        let mut connection = self.connection.clone();
        let value: i64 = connection
            .incr(&prefixed_key, delta)
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster increment error: {e}")))?;
        if ttl_seconds > 0 {
            let _: bool = connection
                .expire(&prefixed_key, ttl_seconds as i64)
                .await
                .map_err(|e| Error::Cache(format!("Redis Cluster expire error: {e}")))?;
        }
        Ok(value)
    }
}

impl RedisClusterCacheManager {
    pub async fn delete(&self, key: &str) -> Result<bool> {
        let mut connection = self.connection.clone();
        let deleted: i32 = connection
            .del(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster delete error: {e}")))?;
        Ok(deleted > 0)
    }

    pub async fn clear_prefix(&self) -> Result<usize> {
        let pattern = format!("{}:*", self.prefix);
        let mut connection = self.connection.clone();

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

        let mut connection = self.connection.clone();
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
        let mut connection = self.connection.clone();
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
        let mut connection = self.connection.clone();
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

    pub fn get_connection(&self) -> ClusterConnection {
        self.connection.clone()
    }

    pub async fn get_cluster_info(&self) -> Result<String> {
        let mut connection = self.connection.clone();
        let info: String = redis::cmd("CLUSTER")
            .arg("INFO")
            .query_async(&mut connection)
            .await
            .map_err(|e| Error::Cache(format!("Redis Cluster info error: {e}")))?;

        Ok(info)
    }

    pub async fn get_cluster_nodes(&self) -> Result<String> {
        let mut connection = self.connection.clone();
        let nodes: String = redis::cmd("CLUSTER")
            .arg("NODES")
            .query_async(&mut connection)
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
            tls: RedisTlsOptions::default(),
        };

        let cache_manager = RedisClusterCacheManager::new(config).await?;
        Ok(Box::new(cache_manager))
    }
}

async fn read_scan_topology(
    connection: &mut ClusterConnection,
) -> Result<(BTreeMap<String, u16>, u64)> {
    let slots: redis::Value = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(connection)
        .await
        .map_err(|error| Error::Cache(format!("Redis Cluster scan topology error: {error}")))?;
    scan_topology(slots)
}

fn scan_topology(slots: redis::Value) -> Result<(BTreeMap<String, u16>, u64)> {
    let invalid = || Error::Cache("invalid Redis Cluster slot topology".into());
    let redis::Value::Array(slots) = slots else {
        return Err(invalid());
    };
    let mut ranges = Vec::with_capacity(slots.len());
    for slot in slots {
        let redis::Value::Array(fields) = slot else {
            return Err(invalid());
        };
        let (
            Some(redis::Value::Int(start)),
            Some(redis::Value::Int(end)),
            Some(redis::Value::Array(primary)),
        ) = (fields.first(), fields.get(1), fields.get(2))
        else {
            return Err(invalid());
        };
        let node_id: String = redis::from_redis_value(primary.get(2).ok_or_else(invalid)?.clone())
            .map_err(|_| invalid())?;
        let start = u16::try_from(*start).map_err(|_| invalid())?;
        let end = u16::try_from(*end).map_err(|_| invalid())?;
        if start > end || end >= 16384 || node_id.is_empty() {
            return Err(invalid());
        }
        ranges.push((start, end, node_id));
    }
    ranges.sort_unstable();
    let mut next = 0;
    let mut primaries = BTreeMap::new();
    for (start, end, node) in &ranges {
        if *start != next {
            return Err(invalid());
        }
        next = end + 1;
        primaries.entry(node.clone()).or_insert(*start);
    }
    if next != 16384 {
        return Err(invalid());
    }
    // Include every interval, not just the first representative slot per node:
    // an interior slot can move while every primary's representative stays put.
    let mut hasher = std::hash::DefaultHasher::new();
    ranges.hash(&mut hasher);
    Ok((primaries, hasher.finish()))
}

#[cfg(test)]
mod scan_topology_tests {
    use super::scan_topology;
    use redis::Value;

    fn slots(ranges: &[(i64, i64, &str)]) -> Value {
        Value::Array(
            ranges
                .iter()
                .map(|(start, end, node)| {
                    Value::Array(vec![
                        Value::Int(*start),
                        Value::Int(*end),
                        Value::Array(vec![
                            Value::BulkString(b"127.0.0.1".to_vec()),
                            Value::Int(6379),
                            Value::BulkString(node.as_bytes().to_vec()),
                        ]),
                    ])
                })
                .collect(),
        )
    }

    #[test]
    fn interior_reshard_changes_generation_without_changing_representatives() {
        let before = scan_topology(slots(&[(0, 8191, "a"), (8192, 16383, "b")])).unwrap();
        let after_same_representatives = scan_topology(slots(&[
            (0, 8191, "a"),
            (8192, 8194, "b"),
            (8195, 8195, "a"),
            (8196, 16383, "b"),
        ]))
        .unwrap();
        assert_eq!(before.0, after_same_representatives.0);
        assert_ne!(before.1, after_same_representatives.1);
    }

    #[test]
    fn response_order_is_irrelevant_and_partial_maps_fail_closed() {
        let forward = scan_topology(slots(&[(0, 8191, "a"), (8192, 16383, "b")])).unwrap();
        let reverse = scan_topology(slots(&[(8192, 16383, "b"), (0, 8191, "a")])).unwrap();
        assert_eq!(forward, reverse);
        for invalid in [
            slots(&[]),
            slots(&[(0, 1, "a")]),
            slots(&[(0, 8192, "a"), (8192, 16383, "b")]),
        ] {
            assert!(scan_topology(invalid).is_err());
        }
    }
}
