use ahash::AHashMap;
use serde::{Deserialize, Serialize};

use crate::drivers::AdapterDriver;

#[cfg(feature = "nats")]
const NATS_DEFAULT_PREFIX: &str = "sockudo";
#[cfg(feature = "redis-cluster")]
const REDIS_CLUSTER_DEFAULT_PREFIX: &str = "sockudo";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AdapterConfig {
    pub driver: AdapterDriver,
    pub redis: RedisAdapterConfig,
    pub cluster: RedisClusterAdapterConfig,
    pub nats: NatsAdapterConfig,
    #[serde(default = "default_buffer_multiplier_per_cpu")]
    pub buffer_multiplier_per_cpu: usize,
    pub cluster_health: ClusterHealthConfig,
    #[serde(default = "default_enable_socket_counting")]
    pub enable_socket_counting: bool,
}

fn default_enable_socket_counting() -> bool {
    true
}

fn default_buffer_multiplier_per_cpu() -> usize {
    64
}

impl Default for AdapterConfig {
    fn default() -> Self {
        Self {
            driver: AdapterDriver::default(),
            redis: RedisAdapterConfig::default(),
            cluster: RedisClusterAdapterConfig::default(),
            nats: NatsAdapterConfig::default(),
            buffer_multiplier_per_cpu: default_buffer_multiplier_per_cpu(),
            cluster_health: ClusterHealthConfig::default(),
            enable_socket_counting: default_enable_socket_counting(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisAdapterConfig {
    pub requests_timeout: u64,
    pub prefix: String,
    pub redis_pub_options: AHashMap<String, sonic_rs::Value>,
    pub redis_sub_options: AHashMap<String, sonic_rs::Value>,
    pub cluster_mode: bool,
}

impl Default for RedisAdapterConfig {
    fn default() -> Self {
        Self {
            requests_timeout: 5000,
            prefix: "sockudo_adapter:".to_string(),
            redis_pub_options: AHashMap::new(),
            redis_sub_options: AHashMap::new(),
            cluster_mode: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisClusterAdapterConfig {
    pub nodes: Vec<String>,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub use_connection_manager: bool,
    /// Use sharded pub/sub (SSUBSCRIBE/SPUBLISH) for Redis 7.0+
    /// This routes messages to specific nodes instead of broadcasting to all.
    #[serde(default)]
    pub use_sharded_pubsub: bool,
}

impl Default for RedisClusterAdapterConfig {
    fn default() -> Self {
        Self {
            nodes: vec![],
            #[cfg(feature = "redis-cluster")]
            prefix: REDIS_CLUSTER_DEFAULT_PREFIX.to_string(),
            #[cfg(not(feature = "redis-cluster"))]
            prefix: "sockudo_adapter:".to_string(),
            request_timeout_ms: 1000,
            use_connection_manager: true,
            use_sharded_pubsub: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NatsAdapterConfig {
    pub servers: Vec<String>,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub connection_timeout_ms: u64,
    pub nodes_number: Option<u32>,
}

impl Default for NatsAdapterConfig {
    fn default() -> Self {
        Self {
            servers: vec!["nats://localhost:4222".to_string()],
            #[cfg(feature = "nats")]
            prefix: NATS_DEFAULT_PREFIX.to_string(),
            #[cfg(not(feature = "nats"))]
            prefix: "sockudo_adapter:".to_string(),
            request_timeout_ms: 5000,
            username: None,
            password: None,
            token: None,
            connection_timeout_ms: 5000,
            nodes_number: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterHealthConfig {
    pub enabled: bool,
    pub heartbeat_interval_ms: u64,
    pub node_timeout_ms: u64,
    pub cleanup_interval_ms: u64,
}

impl Default for ClusterHealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            heartbeat_interval_ms: 10000,
            node_timeout_ms: 30000,
            cleanup_interval_ms: 10000,
        }
    }
}

impl ClusterHealthConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.heartbeat_interval_ms == 0 {
            return Err("heartbeat_interval_ms must be greater than 0".to_string());
        }
        if self.node_timeout_ms == 0 {
            return Err("node_timeout_ms must be greater than 0".to_string());
        }
        if self.cleanup_interval_ms == 0 {
            return Err("cleanup_interval_ms must be greater than 0".to_string());
        }

        if self.heartbeat_interval_ms > self.node_timeout_ms / 3 {
            return Err(format!(
                "heartbeat_interval_ms ({}) should be at least 3x smaller than node_timeout_ms ({}) to avoid false positive dead node detection. Recommended: heartbeat_interval_ms <= {}",
                self.heartbeat_interval_ms,
                self.node_timeout_ms,
                self.node_timeout_ms / 3
            ));
        }

        if self.cleanup_interval_ms > self.node_timeout_ms {
            return Err(format!(
                "cleanup_interval_ms ({}) should not be larger than node_timeout_ms ({}) to ensure timely dead node detection",
                self.cleanup_interval_ms, self.node_timeout_ms
            ));
        }

        Ok(())
    }
}
