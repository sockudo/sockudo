use async_trait::async_trait;
use redis::AsyncCommands;
use redis::cluster::ClusterClientBuilder;
use redis::cluster_async::ClusterConnection;
use redis::cluster_read_routing::RandomReplicaStrategy;
use sockudo_core::delta_types::ClusterCoordinator;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{RedisTlsOptions, SentinelSpec};
use sockudo_core::redis_client::{RedisClient, RedisClientOptions, configure_cluster_builder};
use std::sync::OnceLock;
use tracing::debug;

// One key keeps the atomic operation compatible with Redis Cluster slot routing.
// Preserve the legacy counter key and TTL, including interval=0 and interval=1.
const INCREMENT_SCRIPT: &str = r#"
local count = redis.call('INCR', KEYS[1])
local interval = tonumber(ARGV[1])
if count >= interval then
    redis.call('SET', KEYS[1], 0, 'EX', ARGV[2])
    return {1, interval}
end
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return {0, count}
"#;

#[derive(Clone)]
enum RedisCoordinationConnection {
    Standard(redis::aio::ConnectionManager),
    Cluster(ClusterConnection),
}

impl RedisCoordinationConnection {
    async fn increment_and_check(
        &mut self,
        key: &str,
        interval: u32,
        ttl_seconds: u64,
    ) -> redis::RedisResult<(bool, u32)> {
        static SCRIPT: OnceLock<redis::Script> = OnceLock::new();
        let script = SCRIPT.get_or_init(|| redis::Script::new(INCREMENT_SCRIPT));
        let mut invocation = script.prepare_invoke();
        invocation.key(key).arg(interval).arg(ttl_seconds);
        match self {
            Self::Standard(conn) => invocation.invoke_async(conn).await,
            Self::Cluster(conn) => invocation.invoke_async(conn).await,
        }
    }

    async fn del(&mut self, key: &str) -> redis::RedisResult<()> {
        match self {
            Self::Standard(conn) => conn.del(key).await,
            Self::Cluster(conn) => conn.del(key).await,
        }
    }

    async fn get(&mut self, key: &str) -> redis::RedisResult<Option<u32>> {
        match self {
            Self::Standard(conn) => conn.get(key).await,
            Self::Cluster(conn) => conn.get(key).await,
        }
    }
}

/// Redis-based cluster coordinator for delta interval synchronization
pub struct RedisClusterCoordinator {
    connection: RedisCoordinationConnection,
    prefix: String,
    ttl_seconds: u64,
    backend_name: &'static str,
}

impl RedisClusterCoordinator {
    /// Create a new Redis cluster coordinator
    pub async fn new(redis_url: &str, prefix: Option<&str>) -> Result<Self> {
        Self::new_with_connection_options(redis_url, None, RedisTlsOptions::default(), prefix).await
    }

    /// Create a coordinator using direct TLS or a native Sentinel topology.
    pub async fn new_with_connection_options(
        redis_url: &str,
        sentinel: Option<SentinelSpec>,
        tls: RedisTlsOptions,
        prefix: Option<&str>,
    ) -> Result<Self> {
        let client = RedisClient::connect_with_options(
            redis_url,
            RedisClientOptions {
                sentinel,
                tls,
                response_timeout: None,
            },
        )
        .await?;
        let connection = client.command_connection().await?;

        Ok(Self {
            connection: RedisCoordinationConnection::Standard(connection),
            prefix: prefix.unwrap_or("sockudo").to_string(),
            ttl_seconds: 300,
            backend_name: "redis",
        })
    }

    /// Create a new Redis Cluster coordinator from seed nodes.
    pub async fn new_cluster(nodes: Vec<String>, prefix: Option<&str>) -> Result<Self> {
        Self::new_cluster_with_tls(nodes, RedisTlsOptions::default(), prefix).await
    }

    /// Create a Redis Cluster coordinator with data-plane TLS settings.
    pub async fn new_cluster_with_tls(
        nodes: Vec<String>,
        tls: RedisTlsOptions,
        prefix: Option<&str>,
    ) -> Result<Self> {
        let builder = ClusterClientBuilder::new(nodes)
            .retries(3)
            .read_routing_strategy(RandomReplicaStrategy);
        let client = configure_cluster_builder(builder, &tls)
            .await?
            .build()
            .map_err(|e| Error::Redis(format!("Failed to create Redis Cluster client: {}", e)))?;

        let connection = sockudo_core::redis_client::cluster_connect_with_retry(&client).await?;

        Ok(Self {
            connection: RedisCoordinationConnection::Cluster(connection),
            prefix: prefix.unwrap_or("sockudo").to_string(),
            ttl_seconds: 300,
            backend_name: "redis_cluster",
        })
    }

    fn get_key(&self, app_id: &str, channel: &str, conflation_key: &str) -> String {
        format!(
            "{}:delta_count:{}:{}:{}",
            self.prefix, app_id, channel, conflation_key
        )
    }
}

#[async_trait]
impl ClusterCoordinator for RedisClusterCoordinator {
    fn backend_name(&self) -> &'static str {
        self.backend_name
    }

    async fn increment_and_check(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
        interval: u32,
    ) -> Result<(bool, u32)> {
        let key = self.get_key(app_id, channel, conflation_key);
        let mut conn = self.connection.clone();

        let (should_send_full, count) = conn
            .increment_and_check(&key, interval, self.ttl_seconds)
            .await
            .map_err(|e| Error::Redis(format!("Failed to update delta counter: {e}")))?;
        debug!(
            app_id,
            channel, count, interval, should_send_full, "delta coordination counter updated"
        );
        Ok((should_send_full, count))
    }

    async fn reset_counter(&self, app_id: &str, channel: &str, conflation_key: &str) -> Result<()> {
        let key = self.get_key(app_id, channel, conflation_key);
        let mut conn = self.connection.clone();

        let _: () = conn
            .del(&key)
            .await
            .map_err(|e| Error::Redis(format!("Failed to delete counter: {}", e)))?;

        debug!(app_id, channel, "cluster coordination: reset counter");
        Ok(())
    }

    async fn get_counter(&self, app_id: &str, channel: &str, conflation_key: &str) -> Result<u32> {
        let key = self.get_key(app_id, channel, conflation_key);
        let mut conn = self.connection.clone();

        let count: Option<u32> = conn
            .get(&key)
            .await
            .map_err(|e| Error::Redis(format!("Failed to get counter: {}", e)))?;

        Ok(count.unwrap_or(0))
    }
}

impl Clone for RedisClusterCoordinator {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            prefix: self.prefix.clone(),
            ttl_seconds: self.ttl_seconds,
            backend_name: self.backend_name,
        }
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    #[ignore = "requires isolated Redis at SOCKUDO_REDIS_TEST_URL"]
    async fn independent_coordinators_atomically_reset_and_expire() {
        let url = std::env::var("SOCKUDO_REDIS_TEST_URL").expect("explicit test Redis URL");
        let prefix = format!("sockudo-fanout-atomic-{}", std::process::id());
        let first = Arc::new(
            RedisClusterCoordinator::new(&url, Some(&prefix))
                .await
                .unwrap(),
        );
        let second = Arc::new(
            RedisClusterCoordinator::new(&url, Some(&prefix))
                .await
                .unwrap(),
        );
        first.reset_counter("app", "channel", "key").await.unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..32 {
            let coordinator = if index % 2 == 0 {
                first.clone()
            } else {
                second.clone()
            };
            tasks.spawn(async move {
                let mut full = 0;
                for _ in 0..32 {
                    let (reset, count) = coordinator
                        .increment_and_check("app", "channel", "key", 16)
                        .await
                        .unwrap();
                    assert_eq!(reset, count == 16);
                    full += usize::from(reset);
                }
                full
            });
        }
        let mut full = 0;
        while let Some(result) = tasks.join_next().await {
            full += result.unwrap();
        }
        assert_eq!(full, 64);
        assert_eq!(first.get_counter("app", "channel", "key").await.unwrap(), 0);
        for interval in [0, 1] {
            assert_eq!(
                first
                    .increment_and_check("app", "channel", "key", interval)
                    .await
                    .unwrap(),
                (true, interval)
            );
        }
        first.reset_counter("app", "channel", "key").await.unwrap();
        assert_eq!(
            first
                .increment_and_check("app", "channel", "key", u32::MAX)
                .await
                .unwrap(),
            (false, 1)
        );
        let mut expiry = RedisClusterCoordinator::new(&url, Some(&prefix))
            .await
            .unwrap();
        expiry.ttl_seconds = 1;
        expiry.reset_counter("app", "expiry", "key").await.unwrap();
        assert_eq!(
            expiry
                .increment_and_check("app", "expiry", "key", 16)
                .await
                .unwrap(),
            (false, 1)
        );
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        assert_eq!(
            expiry
                .increment_and_check("app", "expiry", "key", 16)
                .await
                .unwrap(),
            (false, 1)
        );
        first.reset_counter("app", "channel", "key").await.unwrap();
        expiry.reset_counter("app", "expiry", "key").await.unwrap();
    }
}
