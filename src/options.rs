use crate::app::config::App;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
// Assuming DEFAULT_PREFIX is pub const in nats_adapter or imported appropriately
use crate::adapter::nats_adapter::DEFAULT_PREFIX as NATS_DEFAULT_PREFIX;
use crate::adapter::redis_cluster_adapter::DEFAULT_PREFIX as REDIS_CLUSTER_DEFAULT_PREFIX;


// --- Enums for Driver Types ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AdapterDriver {
    Local,
    Redis,
    #[serde(rename = "redis-cluster")]
    RedisCluster,
    Nats,
}

impl Default for AdapterDriver {
    fn default() -> Self { AdapterDriver::Redis }
}

impl std::str::FromStr for AdapterDriver {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "local" => Ok(AdapterDriver::Local),
            "redis" => Ok(AdapterDriver::Redis),
            "redis-cluster" => Ok(AdapterDriver::RedisCluster),
            "nats" => Ok(AdapterDriver::Nats),
            _ => Err(format!("Unknown adapter driver: {}", s)),
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AppManagerDriver {
    Memory,
    Mysql,
    Dynamodb,
}

impl Default for AppManagerDriver {
    fn default() -> Self { AppManagerDriver::Memory }
}

impl std::str::FromStr for AppManagerDriver {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(AppManagerDriver::Memory),
            "mysql" => Ok(AppManagerDriver::Mysql),
            "dynamodb" => Ok(AppManagerDriver::Dynamodb),
            _ => Err(format!("Unknown app manager driver: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CacheDriver {
    Memory,
    Redis,
    #[serde(rename = "redis-cluster")]
    RedisCluster,
    None,
}

impl Default for CacheDriver {
    fn default() -> Self { CacheDriver::Memory }
}

impl std::str::FromStr for CacheDriver {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(CacheDriver::Memory),
            "redis" => Ok(CacheDriver::Redis),
            "redis-cluster" => Ok(CacheDriver::RedisCluster),
            "none" => Ok(CacheDriver::None),
            _ => Err(format!("Unknown cache driver: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum QueueDriver {
    Memory,
    Redis,
    Sqs,
    None,
}

impl Default for QueueDriver {
    fn default() -> Self { QueueDriver::Redis }
}

impl std::str::FromStr for QueueDriver {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(QueueDriver::Memory),
            "redis" => Ok(QueueDriver::Redis),
            "sqs" => Ok(QueueDriver::Sqs),
            "none" => Ok(QueueDriver::None),
            _ => Err(format!("Unknown queue driver: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MetricsDriver {
    Prometheus,
}

impl Default for MetricsDriver {
    fn default() -> Self { MetricsDriver::Prometheus }
}

impl std::str::FromStr for MetricsDriver {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "prometheus" => Ok(MetricsDriver::Prometheus),
            _ => Err(format!("Unknown metrics driver: {}", s)),
        }
    }
}


// --- Main Configuration Struct ---
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerOptions {
    pub adapter: AdapterConfig,
    pub app_manager: AppManagerConfig,
    pub cache: CacheConfig,
    pub channel_limits: ChannelLimits,
    pub cluster: ClusterConfig,
    pub cors: CorsConfig,
    pub database: DatabaseConfig,
    pub database_pooling: DatabasePooling,
    pub debug: bool,
    pub event_limits: EventLimits,
    pub host: String,
    pub http_api: HttpApiConfig,
    pub instance: InstanceConfig,
    pub metrics: MetricsConfig,
    pub mode: String,
    pub port: u16,
    pub path_prefix: String,
    pub presence: PresenceConfig,
    pub queue: QueueConfig,
    pub rate_limiter: RateLimiterConfig,
    pub shutdown_grace_period: u64,
    pub ssl: SslConfig,
    pub user_authentication_timeout: u64,
    pub webhooks: WebhooksConfig,
    pub websocket_max_payload_kb: u32,
}

// --- Configuration Sub-Structs ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SqsQueueConfig {
    pub region: String,
    pub queue_url_prefix: Option<String>,
    pub visibility_timeout: i32,
    pub endpoint_url: Option<String>,
    pub max_messages: i32,
    pub wait_time_seconds: i32,
    pub concurrency: u32,
    pub fifo: bool,
    pub message_group_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AdapterConfig {
    pub driver: AdapterDriver,
    pub redis: RedisAdapterConfig,
    pub cluster: RedisClusterAdapterConfig,
    pub nats: NatsAdapterConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisAdapterConfig {
    pub requests_timeout: u64,
    pub prefix: String,
    pub redis_pub_options: HashMap<String, serde_json::Value>,
    pub redis_sub_options: HashMap<String, serde_json::Value>,
    pub cluster_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisClusterAdapterConfig {
    pub nodes: Vec<String>,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub use_connection_manager: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppManagerConfig {
    pub driver: AppManagerDriver,
    pub array: ArrayConfig,
    pub cache: CacheSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ArrayConfig {
    pub apps: Vec<App>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheSettings {
    pub enabled: bool,
    pub ttl: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MemoryCacheOptions {
    pub ttl: u64,
    pub cleanup_interval: u64,
    pub max_capacity: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    pub driver: CacheDriver,
    pub redis: RedisConfig,
    pub memory: MemoryCacheOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisConfig {
    pub prefix: Option<String>,
    pub url_override: Option<String>,
    pub cluster_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ChannelLimits {
    pub max_name_length: u32,
    pub cache_ttl: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub hostname: String,
    pub hello_interval: u64,
    pub check_interval: u64,
    pub node_timeout: u64,
    pub master_timeout: u64,
    pub port: u16,
    pub prefix: String,
    pub ignore_process: bool,
    pub broadcast: String,
    pub unicast: Option<String>,
    pub multicast: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CorsConfig {
    pub credentials: bool,
    pub origin: Vec<String>,
    pub methods: Vec<String>,
    pub allowed_headers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    pub mysql: DatabaseConnection,
    pub postgres: DatabaseConnection,
    pub redis: RedisConnection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabaseConnection {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub table_name: String,
    pub connection_pool_size: u32,
    pub cache_ttl: u64,
    pub cache_cleanup_interval: u64,
    pub cache_max_capacity: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisConnection {
    pub host: String,
    pub port: u16,
    pub db: u32,
    pub username: Option<String>,
    pub password: Option<String>,
    pub key_prefix: String,
    pub sentinels: Vec<RedisSentinel>,
    pub sentinel_password: Option<String>,
    pub name: String,
    pub cluster_nodes: Vec<ClusterNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisSentinel {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterNode {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabasePooling {
    pub enabled: bool,
    pub min: u32,
    pub max: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EventLimits {
    pub max_channels_at_once: u32,
    pub max_name_length: u32,
    pub max_payload_in_kb: u32,
    pub max_batch_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpApiConfig {
    pub request_limit_in_mb: u32,
    pub accept_traffic: AcceptTraffic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AcceptTraffic {
    pub memory_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InstanceConfig {
    pub process_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub driver: MetricsDriver,
    pub host: String,
    pub prometheus: PrometheusConfig,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrometheusConfig {
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PresenceConfig {
    pub max_members_per_channel: u32,
    pub max_member_size_in_kb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    pub driver: QueueDriver,
    pub redis: RedisQueueConfig, // This will use the updated RedisQueueConfig
    pub sqs: SqsQueueConfig,
}

// Updated RedisQueueConfig for type safety
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisQueueConfig {
    pub concurrency: u32,
    pub prefix: Option<String>,       // Optional prefix for this specific queue
    pub url_override: Option<String>, // Optional URL to override the global DatabaseConfig.redis.url
    pub cluster_mode: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimit {
    pub max_requests: u32,
    pub window_seconds: u64,
    pub identifier: Option<String>,
    pub trust_hops: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimiterConfig {
    pub enabled: bool,
    pub driver: CacheDriver,
    pub api_rate_limit: RateLimit,
    pub websocket_rate_limit: RateLimit,
    pub redis: RedisConfig,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SslConfig {
    pub enabled: bool,
    pub cert_path: String,
    pub key_path: String,
    pub passphrase: Option<String>,
    pub ca_path: Option<String>,
    pub redirect_http: bool,
    pub http_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhooksConfig {
    pub batching: BatchingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchingConfig {
    pub enabled: bool,
    pub duration: u64, // ms
}

// --- Default Implementations ---

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            adapter: AdapterConfig::default(),
            app_manager: AppManagerConfig::default(),
            cache: CacheConfig::default(),
            channel_limits: ChannelLimits::default(),
            cluster: ClusterConfig::default(),
            cors: CorsConfig::default(),
            database: DatabaseConfig::default(),
            database_pooling: DatabasePooling::default(),
            debug: false,
            event_limits: EventLimits::default(),
            host: "0.0.0.0".to_string(),
            http_api: HttpApiConfig::default(),
            instance: InstanceConfig::default(),
            metrics: MetricsConfig::default(),
            mode: "production".to_string(),
            port: 6001,
            path_prefix: "/".to_string(),
            presence: PresenceConfig::default(),
            queue: QueueConfig::default(),
            rate_limiter: RateLimiterConfig::default(),
            shutdown_grace_period: 10,
            ssl: SslConfig::default(),
            user_authentication_timeout: 3600,
            webhooks: WebhooksConfig::default(),
            websocket_max_payload_kb: 64,
        }
    }
}

impl Default for SqsQueueConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            queue_url_prefix: None,
            visibility_timeout: 30,
            endpoint_url: None,
            max_messages: 10,
            wait_time_seconds: 5,
            concurrency: 5,
            fifo: false,
            message_group_id: Some("default".to_string()),
        }
    }
}

impl Default for AdapterConfig {
    fn default() -> Self {
        Self {
            driver: AdapterDriver::default(),
            redis: RedisAdapterConfig::default(),
            cluster: RedisClusterAdapterConfig::default(),
            nats: NatsAdapterConfig::default(),
        }
    }
}

impl Default for RedisAdapterConfig {
    fn default() -> Self {
        Self {
            requests_timeout: 5000,
            prefix: "sockudo_adapter:".to_string(),
            redis_pub_options: HashMap::new(),
            redis_sub_options: HashMap::new(),
            cluster_mode: false,
        }
    }
}

impl Default for RedisClusterAdapterConfig {
    fn default() -> Self {
        Self {
            nodes: vec![],
            prefix: REDIS_CLUSTER_DEFAULT_PREFIX.to_string(),
            request_timeout_ms: 5000,
            use_connection_manager: true,
        }
    }
}

impl Default for NatsAdapterConfig {
    fn default() -> Self {
        Self {
            servers: vec!["nats://localhost:4222".to_string()],
            prefix: NATS_DEFAULT_PREFIX.to_string(),
            request_timeout_ms: 5000,
            username: None,
            password: None,
            token: None,
            connection_timeout_ms: 5000,
            nodes_number: None,
        }
    }
}


impl Default for AppManagerConfig {
    fn default() -> Self {
        Self {
            driver: AppManagerDriver::default(),
            array: ArrayConfig::default(),
            cache: CacheSettings::default(),
        }
    }
}

impl Default for ArrayConfig {
    fn default() -> Self { Self { apps: Vec::new() } }
}

impl Default for CacheSettings {
    fn default() -> Self { Self { enabled: true, ttl: 300 } }
}

impl Default for MemoryCacheOptions {
    fn default() -> Self {
        Self {
            ttl: 300,
            cleanup_interval: 60,
            max_capacity: 10000,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            driver: CacheDriver::default(),
            redis: RedisConfig {
                prefix: Some("sockudo_cache:".to_string()),
                url_override: None,
                cluster_mode: false,
            },
            memory: MemoryCacheOptions::default(),
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            prefix: None,
            url_override: None,
            cluster_mode: false,
        }
    }
}

impl Default for ChannelLimits {
    fn default() -> Self {
        Self {
            max_name_length: 200,
            cache_ttl: 3600,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            hostname: "localhost".to_string(),
            hello_interval: 5000,
            check_interval: 10000,
            node_timeout: 30000,
            master_timeout: 60000,
            port: 6002,
            prefix: "sockudo_cluster:".to_string(),
            ignore_process: false,
            broadcast: "cluster:broadcast".to_string(),
            unicast: Some("cluster:unicast".to_string()),
            multicast: Some("cluster:multicast".to_string()),
        }
    }
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            credentials: true,
            origin: vec!["*".to_string()],
            methods: vec!["GET".to_string(), "POST".to_string(), "OPTIONS".to_string()],
            allowed_headers: vec![
                "Authorization".to_string(),
                "Content-Type".to_string(),
                "X-Requested-With".to_string(),
                "Accept".to_string(),
            ],
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            mysql: DatabaseConnection::default(),
            postgres: DatabaseConnection::default(),
            redis: RedisConnection::default(),
        }
    }
}

impl Default for DatabaseConnection {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: "".to_string(),
            database: "sockudo".to_string(),
            table_name: "applications".to_string(),
            connection_pool_size: 10,
            cache_ttl: 300,
            cache_cleanup_interval: 60,
            cache_max_capacity: 100,
        }
    }
}

impl Default for RedisConnection {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster_nodes: Vec::new(),
        }
    }
}

impl Default for RedisSentinel {
    fn default() -> Self { Self { host: "localhost".to_string(), port: 26379 } }
}

impl Default for ClusterNode {
    fn default() -> Self { Self { host: "127.0.0.1".to_string(), port: 7000 } }
}

impl Default for DatabasePooling {
    fn default() -> Self { Self { enabled: true, min: 2, max: 10 } }
}

impl Default for EventLimits {
    fn default() -> Self {
        Self {
            max_channels_at_once: 100,
            max_name_length: 200,
            max_payload_in_kb: 100,
            max_batch_size: 10,
        }
    }
}

impl Default for HttpApiConfig {
    fn default() -> Self {
        Self {
            request_limit_in_mb: 10,
            accept_traffic: AcceptTraffic::default(),
        }
    }
}

impl Default for AcceptTraffic {
    fn default() -> Self { Self { memory_threshold: 0.90 } }
}

impl Default for InstanceConfig {
    fn default() -> Self { Self { process_id: uuid::Uuid::new_v4().to_string() } }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            driver: MetricsDriver::default(),
            host: "0.0.0.0".to_string(),
            prometheus: PrometheusConfig::default(),
            port: 9601,
        }
    }
}

impl Default for PrometheusConfig {
    fn default() -> Self { Self { prefix: "sockudo_".to_string() } }
}

impl Default for PresenceConfig {
    fn default() -> Self {
        Self {
            max_members_per_channel: 100,
            max_member_size_in_kb: 2,
        }
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            driver: QueueDriver::default(),
            redis: RedisQueueConfig::default(), // Uses updated RedisQueueConfig default
            sqs: SqsQueueConfig::default(),
        }
    }
}

// Updated Default for new RedisQueueConfig structure
impl Default for RedisQueueConfig {
    fn default() -> Self {
        Self {
            concurrency: 5,
            prefix: Some("sockudo_queue:".to_string()), // Default prefix for queue
            url_override: None,
            cluster_mode: false,
        }
    }
}

impl Default for RateLimit {
    fn default() -> Self {
        Self {
            max_requests: 60,
            window_seconds: 60,
            identifier: Some("default".to_string()),
            trust_hops: Some(0),
        }
    }
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            driver: CacheDriver::Memory,
            api_rate_limit: RateLimit {
                max_requests: 100,
                window_seconds: 60,
                identifier: Some("api".to_string()),
                trust_hops: Some(0),
            },
            websocket_rate_limit: RateLimit {
                max_requests: 20,
                window_seconds: 60,
                identifier: Some("websocket_connect".to_string()),
                trust_hops: Some(0),
            },
            redis: RedisConfig {
                prefix: Some("sockudo_rl:".to_string()),
                url_override: None,
                cluster_mode: false,
            },
        }
    }
}

impl Default for SslConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: "".to_string(),
            key_path: "".to_string(),
            passphrase: None,
            ca_path: None,
            redirect_http: false,
            http_port: Some(80),
        }
    }
}

impl Default for WebhooksConfig {
    fn default() -> Self { Self { batching: BatchingConfig::default() } }
}

impl Default for BatchingConfig {
    fn default() -> Self { Self { enabled: true, duration: 50 } }
}
