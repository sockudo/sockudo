use crate::app::config::App;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)] // This makes all fields use default values when missing
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SqsQueueConfig {
    /// AWS region
    pub region: String,
    /// URL prefix for SQS queues, omitting queue name
    pub queue_url_prefix: Option<String>,
    /// Visibility timeout in seconds (how long a message is invisible after being received)
    pub visibility_timeout: i32,
    /// Optional endpoint URL (for local testing with LocalStack)
    pub endpoint_url: Option<String>,
    /// How many messages to receive at once
    pub max_messages: i32,
    /// Wait time in seconds for long polling (0-20)
    pub wait_time_seconds: i32,
    /// Processing concurrency per queue
    pub concurrency: u32,
    /// Use standard queue (false) or FIFO queue (true)
    pub fifo: bool,
    /// Message group ID for FIFO queues
    pub message_group_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AdapterConfig {
    pub driver: String,
    pub redis: RedisAdapterConfig,
    pub cluster: ClusterAdapterConfig,
    pub nats: NatsConfig,
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
pub struct ClusterAdapterConfig {
    pub requests_timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NatsConfig {
    pub requests_timeout: u64,
    pub prefix: String,
    pub servers: Vec<String>,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub token: Option<String>,
    pub timeout: u64,
    pub nodes_number: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppManagerConfig {
    pub driver: String,
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
pub struct CacheConfig {
    pub driver: String,
    pub redis: RedisConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisConfig {
    pub redis_options: HashMap<String, serde_json::Value>,
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
    pub cache_ttl: u64,              // in seconds
    pub cache_cleanup_interval: u64, // in seconds
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
    pub max_channels_at_once: String,
    pub max_name_length: String,
    pub max_payload_in_kb: String,
    pub max_batch_size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpApiConfig {
    pub request_limit_in_mb: String,
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
    pub driver: String,
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
    pub max_members_per_channel: String,
    pub max_member_size_in_kb: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    pub driver: String,
    pub redis: RedisQueueConfig,
    pub sqs: SqsQueueConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisQueueConfig {
    pub concurrency: u32,
    pub redis_options: HashMap<String, serde_json::Value>,
    pub cluster_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimiterConfig {
    pub driver: String,
    pub default_limit_per_second: u32,
    pub default_window_seconds: u64,
    pub api_rate_limit: RateLimit,
    pub websocket_rate_limit: RateLimit,
    pub redis: RedisConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimit {
    pub max_requests: u32,
    pub window_seconds: u64,
    pub identifier: Option<String>,
}

// Default implementation for RateLimiterConfig
impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            driver: "memory".to_string(),
            default_limit_per_second: 60,
            default_window_seconds: 60,
            api_rate_limit: RateLimit {
                max_requests: 60,
                window_seconds: 60,
                identifier: Some("api".to_string()),
            },
            websocket_rate_limit: RateLimit {
                max_requests: 10,
                window_seconds: 60,
                identifier: Some("websocket".to_string()),
            },
            redis: RedisConfig {
                redis_options: HashMap::new(),
                cluster_mode: false,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SslConfig {
    pub cert_path: String,
    pub key_path: String,
    pub passphrase: String,
    pub ca_path: String,
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
    pub duration: u64,
}

// Make sure all struct types have Default implementation
impl Default for RateLimit {
    fn default() -> Self {
        Self {
            max_requests: 60,
            window_seconds: 60,
            identifier: None,
        }
    }
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            adapter: AdapterConfig {
                driver: "redis".to_string(),
                redis: RedisAdapterConfig {
                    requests_timeout: 5000,
                    prefix: "sockudo".to_string(),
                    redis_pub_options: HashMap::new(),
                    redis_sub_options: HashMap::new(),
                    cluster_mode: false,
                },
                cluster: ClusterAdapterConfig {
                    requests_timeout: 5000,
                },
                nats: NatsConfig {
                    requests_timeout: 5000,
                    prefix: "sockudo".to_string(),
                    servers: vec!["nats://localhost:4222".to_string()],
                    user: None,
                    pass: None,
                    token: None,
                    timeout: 5000,
                    nodes_number: None,
                },
            },
            app_manager: AppManagerConfig {
                driver: "memory".to_string(),
                array: ArrayConfig { apps: vec![] },
                cache: CacheSettings {
                    enabled: true,
                    ttl: 300,
                },
            },
            cache: CacheConfig {
                driver: "redis".to_string(),
                redis: RedisConfig {
                    redis_options: HashMap::new(),
                    cluster_mode: false,
                },
            },
            channel_limits: ChannelLimits {
                max_name_length: 200,
                cache_ttl: 3600,
            },
            cluster: ClusterConfig {
                hostname: "localhost".to_string(),
                hello_interval: 5000,
                check_interval: 10000,
                node_timeout: 30000,
                master_timeout: 60000,
                port: 6002, // Different from main port
                prefix: "sockudo".to_string(),
                ignore_process: false,
                broadcast: "cluster:broadcast".to_string(),
                unicast: Some("cluster:unicast".to_string()),
                multicast: Some("cluster:multicast".to_string()),
            },
            cors: CorsConfig {
                credentials: true,
                origin: vec!["*".to_string()],
                methods: vec!["GET".to_string(), "POST".to_string(), "OPTIONS".to_string()],
                allowed_headers: vec![
                    "Authorization".to_string(),
                    "Content-Type".to_string(),
                    "X-Requested-With".to_string(),
                    "Accept".to_string(),
                ],
            },
            database: DatabaseConfig {
                mysql: DatabaseConnection {
                    host: "localhost".to_string(),
                    port: 3306,
                    username: "root".to_string(),
                    password: "".to_string(),
                    database: "sockudo".to_string(),
                    table_name: "applications".to_string(),
                    connection_pool_size: 0,
                    cache_ttl: 0,
                    cache_cleanup_interval: 0,
                    cache_max_capacity: 0,
                },
                postgres: DatabaseConnection {
                    host: "localhost".to_string(),
                    port: 5432,
                    username: "postgres".to_string(),
                    password: "".to_string(),
                    database: "sockudo".to_string(),
                    table_name: "applications".to_string(),
                    connection_pool_size: 4,
                    cache_ttl: 60,
                    cache_cleanup_interval: 60,
                    cache_max_capacity: 60,
                },
                redis: RedisConnection {
                    host: "localhost".to_string(),
                    port: 6379,
                    db: 0,
                    username: None,
                    password: None,
                    key_prefix: "sockudo:".to_string(),
                    sentinels: vec![],
                    sentinel_password: None,
                    name: "master".to_string(),
                    cluster_nodes: vec![],
                },
            },
            database_pooling: DatabasePooling {
                enabled: true,
                min: 2,
                max: 10,
            },
            debug: false,
            event_limits: EventLimits {
                max_channels_at_once: "100".to_string(),
                max_name_length: "200".to_string(),
                max_payload_in_kb: "100".to_string(),
                max_batch_size: "10".to_string(),
            },
            host: "0.0.0.0".to_string(),
            http_api: HttpApiConfig {
                request_limit_in_mb: "10".to_string(),
                accept_traffic: AcceptTraffic {
                    memory_threshold: 0.90,
                },
            },
            instance: InstanceConfig {
                process_id: uuid::Uuid::new_v4().to_string(),
            },
            metrics: MetricsConfig {
                enabled: true,
                driver: "prometheus".to_string(),
                host: "0.0.0.0".to_string(),
                prometheus: PrometheusConfig {
                    prefix: "sockudo_".to_string(),
                },
                port: 9601,
            },
            mode: "production".to_string(),
            path_prefix: "/".to_string(),
            port: 6001,
            presence: PresenceConfig {
                max_members_per_channel: "100".to_string(),
                max_member_size_in_kb: "2".to_string(),
            },
            queue: QueueConfig {
                driver: "redis".to_string(),
                redis: RedisQueueConfig {
                    concurrency: 5,
                    redis_options: HashMap::new(),
                    cluster_mode: false,
                },
                sqs: SqsQueueConfig {
                    region: "us-east-1".to_string(),
                    queue_url_prefix: None,
                    visibility_timeout: 30,
                    endpoint_url: None,
                    max_messages: 10,
                    wait_time_seconds: 5,
                    concurrency: 5,
                    fifo: false,
                    message_group_id: Some("default".to_string()),
                },
            },
            rate_limiter: RateLimiterConfig::default(),
            shutdown_grace_period: 10,
            ssl: SslConfig {
                cert_path: "".to_string(),
                key_path: "".to_string(),
                passphrase: "".to_string(),
                ca_path: "".to_string(),
            },
            user_authentication_timeout: 3600,
            webhooks: WebhooksConfig {
                batching: BatchingConfig {
                    enabled: true,
                    duration: 50,
                },
            },
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

// Add default implementations for all other structs
impl Default for AdapterConfig {
    fn default() -> Self {
        Self {
            driver: "redis".to_string(),
            redis: RedisAdapterConfig::default(),
            cluster: ClusterAdapterConfig::default(),
            nats: NatsConfig::default(),
        }
    }
}

impl Default for RedisAdapterConfig {
    fn default() -> Self {
        Self {
            requests_timeout: 5000,
            prefix: "sockudo".to_string(),
            redis_pub_options: HashMap::new(),
            redis_sub_options: HashMap::new(),
            cluster_mode: false,
        }
    }
}

impl Default for ClusterAdapterConfig {
    fn default() -> Self {
        Self {
            requests_timeout: 5000,
        }
    }
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            requests_timeout: 5000,
            prefix: "sockudo".to_string(),
            servers: vec!["nats://localhost:4222".to_string()],
            user: None,
            pass: None,
            token: None,
            timeout: 5000,
            nodes_number: None,
        }
    }
}

impl Default for AppManagerConfig {
    fn default() -> Self {
        Self {
            driver: "memory".to_string(),
            array: ArrayConfig::default(),
            cache: CacheSettings::default(),
        }
    }
}

impl Default for ArrayConfig {
    fn default() -> Self {
        Self { apps: Vec::new() }
    }
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl: 300,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            driver: "redis".to_string(),
            redis: RedisConfig::default(),
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            redis_options: HashMap::new(),
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
            prefix: "sockudo".to_string(),
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
            cache_ttl: 300,             // 5 minutes
            cache_cleanup_interval: 60, // 1 minute
            cache_max_capacity: 100,
        }
    }
}

impl Default for RedisConnection {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "master".to_string(),
            cluster_nodes: Vec::new(),
        }
    }
}

impl Default for RedisSentinel {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 26379,
        }
    }
}

impl Default for ClusterNode {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 6379,
        }
    }
}

impl Default for DatabasePooling {
    fn default() -> Self {
        Self {
            enabled: true,
            min: 2,
            max: 10,
        }
    }
}

impl Default for EventLimits {
    fn default() -> Self {
        Self {
            max_channels_at_once: "100".to_string(),
            max_name_length: "200".to_string(),
            max_payload_in_kb: "100".to_string(),
            max_batch_size: "10".to_string(),
        }
    }
}

impl Default for HttpApiConfig {
    fn default() -> Self {
        Self {
            request_limit_in_mb: "10".to_string(),
            accept_traffic: AcceptTraffic::default(),
        }
    }
}

impl Default for AcceptTraffic {
    fn default() -> Self {
        Self {
            memory_threshold: 0.90,
        }
    }
}

impl Default for InstanceConfig {
    fn default() -> Self {
        Self {
            process_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            driver: "prometheus".to_string(),
            host: "0.0.0.0".to_string(),
            prometheus: PrometheusConfig::default(),
            port: 9601,
        }
    }
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            prefix: "sockudo_".to_string(),
        }
    }
}

impl Default for PresenceConfig {
    fn default() -> Self {
        Self {
            max_members_per_channel: "100".to_string(),
            max_member_size_in_kb: "2".to_string(),
        }
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            driver: "redis".to_string(),
            redis: RedisQueueConfig::default(),
            sqs: SqsQueueConfig::default(),
        }
    }
}

impl Default for RedisQueueConfig {
    fn default() -> Self {
        Self {
            concurrency: 5,
            redis_options: HashMap::new(),
            cluster_mode: false,
        }
    }
}

impl Default for SslConfig {
    fn default() -> Self {
        Self {
            cert_path: "".to_string(),
            key_path: "".to_string(),
            passphrase: "".to_string(),
            ca_path: "".to_string(),
        }
    }
}

impl Default for WebhooksConfig {
    fn default() -> Self {
        Self {
            batching: BatchingConfig::default(),
        }
    }
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            duration: 50,
        }
    }
}
