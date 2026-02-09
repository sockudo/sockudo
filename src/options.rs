#[cfg(feature = "nats")]
use crate::adapter::nats_adapter::DEFAULT_PREFIX as NATS_DEFAULT_PREFIX;
#[cfg(feature = "redis-cluster")]
use crate::adapter::redis_cluster_adapter::DEFAULT_PREFIX as REDIS_CLUSTER_DEFAULT_PREFIX;
use crate::app::config::App;
use crate::utils::{parse_bool_env, parse_env, parse_env_optional};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{info, warn};
use url::Url;

// Custom deserializer for octal permission mode (string format only, like chmod)
fn deserialize_octal_permission<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self};

    let s = String::deserialize(deserializer)?;

    // Validate that the string contains only octal digits
    if !s.chars().all(|c| c.is_digit(8)) {
        return Err(de::Error::custom(format!(
            "invalid octal permission mode '{}': must contain only digits 0-7",
            s
        )));
    }

    // Parse as octal
    let mode = u32::from_str_radix(&s, 8)
        .map_err(|_| de::Error::custom(format!("invalid octal permission mode: {}", s)))?;

    // Validate it's within valid Unix permission range
    if mode > 0o777 {
        return Err(de::Error::custom(format!(
            "permission mode '{}' exceeds maximum value 777",
            s
        )));
    }

    Ok(mode)
}

// Helper function to parse driver enums with fallback behavior (matches main.rs)
fn parse_driver_enum<T: FromStr + Clone + std::fmt::Debug>(
    driver_str: String,
    default_driver: T,
    driver_name: &str,
) -> T
where
    <T as FromStr>::Err: std::fmt::Debug,
{
    match T::from_str(&driver_str.to_lowercase()) {
        Ok(driver_enum) => driver_enum,
        Err(e) => {
            warn!(
                "Failed to parse {} driver from string '{}': {:?}. Using default: {:?}.",
                driver_name, driver_str, e, default_driver
            );
            default_driver
        }
    }
}

fn override_db_pool_settings(db_conn: &mut DatabaseConnection, prefix: &str) {
    if let Some(min) = parse_env_optional::<u32>(&format!("{}_POOL_MIN", prefix)) {
        db_conn.pool_min = Some(min);
    }
    if let Some(max) = parse_env_optional::<u32>(&format!("{}_POOL_MAX", prefix)) {
        db_conn.pool_max = Some(max);
    }
}

// --- Enums for Driver Types ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AdapterDriver {
    #[default]
    Local,
    Redis,
    #[serde(rename = "redis-cluster")]
    RedisCluster,
    Nats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DynamoDbSettings {
    pub region: String,
    pub table_name: String,
    pub endpoint_url: Option<String>, // For local testing
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub aws_profile_name: Option<String>,
}

impl Default for DynamoDbSettings {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            table_name: "sockudo-applications".to_string(), // Default table name
            endpoint_url: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_profile_name: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScyllaDbSettings {
    pub nodes: Vec<String>,
    pub keyspace: String,
    pub table_name: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub replication_class: String,
    pub replication_factor: u32,
}

impl Default for ScyllaDbSettings {
    fn default() -> Self {
        Self {
            nodes: vec!["127.0.0.1:9042".to_string()],
            keyspace: "sockudo".to_string(),
            table_name: "applications".to_string(),
            username: None,
            password: None,
            replication_class: "SimpleStrategy".to_string(),
            replication_factor: 3,
        }
    }
}

impl FromStr for AdapterDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "local" => Ok(AdapterDriver::Local),
            "redis" => Ok(AdapterDriver::Redis),
            "redis-cluster" => Ok(AdapterDriver::RedisCluster),
            "nats" => Ok(AdapterDriver::Nats),
            _ => Err(format!("Unknown adapter driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AppManagerDriver {
    #[default]
    Memory,
    Mysql,
    Dynamodb,
    PgSql,
    ScyllaDb,
}
impl FromStr for AppManagerDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(AppManagerDriver::Memory),
            "mysql" => Ok(AppManagerDriver::Mysql),
            "dynamodb" => Ok(AppManagerDriver::Dynamodb),
            "pgsql" | "postgres" | "postgresql" => Ok(AppManagerDriver::PgSql),
            "scylladb" | "scylla" => Ok(AppManagerDriver::ScyllaDb),
            _ => Err(format!("Unknown app manager driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CacheDriver {
    #[default]
    Memory,
    Redis,
    #[serde(rename = "redis-cluster")]
    RedisCluster,
    None,
}

impl FromStr for CacheDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(CacheDriver::Memory),
            "redis" => Ok(CacheDriver::Redis),
            "redis-cluster" => Ok(CacheDriver::RedisCluster),
            "none" => Ok(CacheDriver::None),
            _ => Err(format!("Unknown cache driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum QueueDriver {
    Memory,
    #[default]
    Redis,
    #[serde(rename = "redis-cluster")] // Add this variant
    RedisCluster,
    Sqs,
    None,
}

impl FromStr for QueueDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(QueueDriver::Memory),
            "redis" => Ok(QueueDriver::Redis),
            "redis-cluster" => Ok(QueueDriver::RedisCluster), // Add this case
            "sqs" => Ok(QueueDriver::Sqs),
            "none" => Ok(QueueDriver::None),
            _ => Err(format!("Unknown queue driver: {s}")),
        }
    }
}

// Corrected: Added AsRef<str> implementation for QueueDriver
impl AsRef<str> for QueueDriver {
    fn as_ref(&self) -> &str {
        match self {
            QueueDriver::Memory => "memory",
            QueueDriver::Redis => "redis",
            QueueDriver::RedisCluster => "redis-cluster", // Add this case
            QueueDriver::Sqs => "sqs",
            QueueDriver::None => "none",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisClusterQueueConfig {
    pub concurrency: u32,
    pub prefix: Option<String>,
    pub nodes: Vec<String>, // Cluster node URLs
    pub request_timeout_ms: u64,
}

impl Default for RedisClusterQueueConfig {
    fn default() -> Self {
        Self {
            concurrency: 5,
            prefix: Some("sockudo_queue:".to_string()),
            nodes: vec!["redis://127.0.0.1:6379".to_string()],
            request_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MetricsDriver {
    #[default]
    Prometheus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogOutputFormat {
    #[default]
    Human,
    Json,
}

impl FromStr for LogOutputFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "human" => Ok(LogOutputFormat::Human),
            "json" => Ok(LogOutputFormat::Json),
            _ => Err(format!("Unknown log output format: {s}")),
        }
    }
}

impl FromStr for MetricsDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "prometheus" => Ok(MetricsDriver::Prometheus),
            _ => Err(format!("Unknown metrics driver: {s}")),
        }
    }
}

// Corrected: Added AsRef<str> implementation for MetricsDriver
impl AsRef<str> for MetricsDriver {
    fn as_ref(&self) -> &str {
        match self {
            MetricsDriver::Prometheus => "prometheus",
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
    pub cors: CorsConfig,
    pub database: DatabaseConfig,
    pub database_pooling: DatabasePooling,
    pub debug: bool,
    pub event_limits: EventLimits,
    pub host: String,
    pub http_api: HttpApiConfig,
    pub instance: InstanceConfig,
    pub logging: Option<LoggingConfig>,
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
    pub cleanup: crate::cleanup::CleanupConfig,
    pub activity_timeout: u64,
    pub cluster_health: ClusterHealthConfig,
    pub unix_socket: UnixSocketConfig,
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
    #[serde(default = "default_buffer_multiplier_per_cpu")]
    pub buffer_multiplier_per_cpu: usize,
    pub cluster_health: ClusterHealthConfig,
}

fn default_buffer_multiplier_per_cpu() -> usize {
    64 // 64 concurrent operations per CPU core
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
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AppManagerConfig {
    pub driver: AppManagerDriver,
    pub array: ArrayConfig,
    pub cache: CacheSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
pub struct CorsConfig {
    pub credentials: bool,
    pub origin: Vec<String>,
    pub methods: Vec<String>,
    pub allowed_headers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DatabaseConfig {
    pub mysql: DatabaseConnection,
    pub postgres: DatabaseConnection,
    pub redis: RedisConnection,
    pub dynamodb: DynamoDbSettings,
    pub scylladb: ScyllaDbSettings,
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
    // Optional per-DB pooling overrides; fall back to ServerOptions.database_pooling
    pub pool_min: Option<u32>,
    pub pool_max: Option<u32>,
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
    pub cluster: RedisClusterConnection,
    /// Legacy field kept for backward compatibility. Prefer `database.redis.cluster.nodes`.
    pub cluster_nodes: Vec<ClusterNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisSentinel {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RedisClusterConnection {
    pub nodes: Vec<ClusterNode>,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(alias = "useTLS")]
    pub use_tls: bool,
}

impl RedisConnection {
    /// Returns true if Redis Sentinel is configured.
    pub fn is_sentinel_configured(&self) -> bool {
        !self.sentinels.is_empty()
    }

    /// Builds a Redis connection URL based on the configuration.
    /// If sentinels are configured, returns a sentinel URL.
    /// Otherwise, returns a standard Redis URL.
    ///
    /// Sentinel URL format: `redis+sentinel://[[username:]password@]host1[:port1][,host2[:port2]][,hostN[:portN]]/service-name[/db][?params]`
    ///
    /// # Examples
    ///
    /// Standard Redis:
    /// - `redis://127.0.0.1:6379/0`
    /// - `redis://:password@127.0.0.1:6379/0`
    /// - `redis://user:password@127.0.0.1:6379/0`
    ///
    /// Sentinel:
    /// - `redis+sentinel://sentinel1:26379,sentinel2:26379/mymaster/0`
    /// - `redis+sentinel://:sentinelpass@sentinel1:26379,sentinel2:26379/mymaster/0`
    pub fn to_url(&self) -> String {
        if self.is_sentinel_configured() {
            self.build_sentinel_url()
        } else {
            self.build_standard_url()
        }
    }

    /// Builds a standard Redis URL.
    fn build_standard_url(&self) -> String {
        let mut url = String::from("redis://");

        // Add authentication if present
        if let Some(ref username) = self.username {
            url.push_str(username);
            if let Some(ref password) = self.password {
                url.push(':');
                url.push_str(&urlencoding::encode(password));
            }
            url.push('@');
        } else if let Some(ref password) = self.password {
            // Password without username
            url.push(':');
            url.push_str(&urlencoding::encode(password));
            url.push('@');
        }

        // Add host and port
        url.push_str(&self.host);
        url.push(':');
        url.push_str(&self.port.to_string());

        // Add database
        url.push('/');
        url.push_str(&self.db.to_string());

        url
    }

    /// Builds a Redis Sentinel URL.
    /// Format: `redis+sentinel://[[username:]password@]host1[:port1][,host2[:port2]][,hostN[:portN]]/service-name[/db][?params]`
    fn build_sentinel_url(&self) -> String {
        let mut url = String::from("redis+sentinel://");

        // Add sentinel authentication if present (for connecting to sentinels)
        if let Some(ref sentinel_password) = self.sentinel_password {
            url.push(':');
            url.push_str(&urlencoding::encode(sentinel_password));
            url.push('@');
        }

        // Add sentinel hosts
        let sentinel_hosts: Vec<String> = self
            .sentinels
            .iter()
            .map(|s| format!("{}:{}", s.host, s.port))
            .collect();
        url.push_str(&sentinel_hosts.join(","));

        // Add service name (master name)
        url.push('/');
        url.push_str(&self.name);

        // Add database
        url.push('/');
        url.push_str(&self.db.to_string());

        // Add master authentication as query parameter if present
        let mut params = Vec::new();
        if let Some(ref password) = self.password {
            params.push(format!("password={}", urlencoding::encode(password)));
        }
        if let Some(ref username) = self.username {
            params.push(format!("username={}", urlencoding::encode(username)));
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        url
    }

    /// Returns true when cluster nodes are configured via either the new (`cluster.nodes`)
    /// or legacy (`cluster_nodes`) field.
    pub fn has_cluster_nodes(&self) -> bool {
        !self.cluster.nodes.is_empty() || !self.cluster_nodes.is_empty()
    }

    /// Returns normalized Redis Cluster seed URLs from the canonical cluster configuration.
    /// Falls back to legacy `cluster_nodes` for backward compatibility.
    pub fn cluster_node_urls(&self) -> Vec<String> {
        if !self.cluster.nodes.is_empty() {
            return self.build_cluster_urls(&self.cluster.nodes);
        }
        self.build_cluster_urls(&self.cluster_nodes)
    }

    /// Normalizes any list of seed strings (`host:port`, `redis://...`, `rediss://...`) using
    /// shared cluster auth/TLS options.
    pub fn normalize_cluster_seed_urls(&self, seeds: &[String]) -> Vec<String> {
        self.build_cluster_urls(
            &seeds
                .iter()
                .filter_map(|seed| ClusterNode::from_seed(seed))
                .collect::<Vec<ClusterNode>>(),
        )
    }

    fn build_cluster_urls(&self, nodes: &[ClusterNode]) -> Vec<String> {
        let username = self
            .cluster
            .username
            .as_deref()
            .or(self.username.as_deref());
        let password = self
            .cluster
            .password
            .as_deref()
            .or(self.password.as_deref());
        let use_tls = self.cluster.use_tls;

        nodes
            .iter()
            .map(|node| node.to_url_with_options(use_tls, username, password))
            .collect()
    }
}

impl RedisSentinel {
    /// Converts the sentinel to a host:port string.
    pub fn to_host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterNode {
    pub host: String,
    pub port: u16,
}

impl ClusterNode {
    /// Builds a Redis URL from the cluster node configuration.
    /// Supports protocol detection in the host field for TLS/SSL connections.
    ///
    /// Examples:
    /// - `host: "localhost", port: 6379` -> `"redis://localhost:6379"`
    /// - `host: "rediss://secure.example.com", port: 6379` -> `"rediss://secure.example.com:6379"`
    /// - `host: "rediss://secure.example.com:7000", port: 6379` -> `"rediss://secure.example.com:7000"`
    pub fn to_url(&self) -> String {
        self.to_url_with_options(false, None, None)
    }

    pub fn to_url_with_options(
        &self,
        use_tls: bool,
        username: Option<&str>,
        password: Option<&str>,
    ) -> String {
        let host = self.host.trim();

        if host.starts_with("redis://") || host.starts_with("rediss://") {
            if let Ok(parsed) = Url::parse(host)
                && let Some(host_str) = parsed.host_str()
            {
                let scheme = parsed.scheme();
                let port = parsed.port_or_known_default().unwrap_or(self.port);
                let parsed_username = (!parsed.username().is_empty()).then_some(parsed.username());
                let parsed_password = parsed.password();
                let has_embedded_auth = parsed_username.is_some() || parsed_password.is_some();
                let (effective_username, effective_password) = if has_embedded_auth {
                    (parsed_username, parsed_password)
                } else {
                    (username, password)
                };

                return build_redis_url(
                    scheme,
                    host_str,
                    port,
                    effective_username,
                    effective_password,
                );
            }

            // Fallback for malformed URLs: keep old behavior, then inject auth if missing.
            let has_port = if let Some(bracket_pos) = host.rfind(']') {
                host[bracket_pos..].contains(':')
            } else {
                host.split(':').count() >= 3
            };
            let base = if has_port {
                host.to_string()
            } else {
                format!("{}:{}", host, self.port)
            };

            if let Ok(parsed) = Url::parse(&base) {
                let parsed_username = (!parsed.username().is_empty()).then_some(parsed.username());
                let parsed_password = parsed.password();
                if let Some(host_str) = parsed.host_str() {
                    let port = parsed.port_or_known_default().unwrap_or(self.port);
                    let has_embedded_auth = parsed_username.is_some() || parsed_password.is_some();
                    let (effective_username, effective_password) = if has_embedded_auth {
                        (parsed_username, parsed_password)
                    } else {
                        (username, password)
                    };
                    return build_redis_url(
                        parsed.scheme(),
                        host_str,
                        port,
                        effective_username,
                        effective_password,
                    );
                }
            }
            return base;
        }

        let (normalized_host, normalized_port) = split_plain_host_and_port(host, self.port);
        let scheme = if use_tls { "rediss" } else { "redis" };
        build_redis_url(
            scheme,
            &normalized_host,
            normalized_port,
            username,
            password,
        )
    }

    pub fn from_seed(seed: &str) -> Option<Self> {
        let trimmed = seed.trim();
        if trimmed.is_empty() {
            return None;
        }

        if trimmed.starts_with("redis://") || trimmed.starts_with("rediss://") {
            let port = Url::parse(trimmed)
                .ok()
                .and_then(|parsed| parsed.port_or_known_default())
                .unwrap_or(6379);
            return Some(Self {
                host: trimmed.to_string(),
                port,
            });
        }

        let (host, port) = split_plain_host_and_port(trimmed, 6379);
        Some(Self { host, port })
    }
}

fn split_plain_host_and_port(raw_host: &str, default_port: u16) -> (String, u16) {
    let host = raw_host.trim();

    // Handle bracketed IPv6: [::1]:6379
    if host.starts_with('[') {
        if let Some(end_bracket) = host.find(']') {
            let host_part = host[1..end_bracket].to_string();
            let remainder = &host[end_bracket + 1..];
            if let Some(port_str) = remainder.strip_prefix(':')
                && let Ok(port) = port_str.parse::<u16>()
            {
                return (host_part, port);
            }
            return (host_part, default_port);
        }
        return (host.to_string(), default_port);
    }

    // Handle hostname/IP with port: host:6379
    if host.matches(':').count() == 1
        && let Some((host_part, port_part)) = host.rsplit_once(':')
        && let Ok(port) = port_part.parse::<u16>()
    {
        return (host_part.to_string(), port);
    }

    (host.to_string(), default_port)
}

fn build_redis_url(
    scheme: &str,
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
) -> String {
    let mut url = format!("{scheme}://");

    if let Some(user) = username {
        url.push_str(&urlencoding::encode(user));
        if let Some(pass) = password {
            url.push(':');
            url.push_str(&urlencoding::encode(pass));
        }
        url.push('@');
    } else if let Some(pass) = password {
        url.push(':');
        url.push_str(&urlencoding::encode(pass));
        url.push('@');
    }

    if host.contains(':') && !host.starts_with('[') {
        url.push('[');
        url.push_str(host);
        url.push(']');
    } else {
        url.push_str(host);
    }
    url.push(':');
    url.push_str(&port.to_string());
    url
}

#[cfg(test)]
mod redis_connection_tests {
    use super::{ClusterNode, RedisClusterConnection, RedisConnection, RedisSentinel};

    #[test]
    fn test_standard_url_basic() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(conn.to_url(), "redis://127.0.0.1:6379/0");
    }

    #[test]
    fn test_standard_url_with_password() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 2,
            username: None,
            password: Some("secret".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(conn.to_url(), "redis://:secret@127.0.0.1:6379/2");
    }

    #[test]
    fn test_standard_url_with_username_and_password() {
        let conn = RedisConnection {
            host: "redis.example.com".to_string(),
            port: 6380,
            db: 1,
            username: Some("admin".to_string()),
            password: Some("pass123".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis://admin:pass123@redis.example.com:6380/1"
        );
    }

    #[test]
    fn test_standard_url_with_special_chars_in_password() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: Some("pass@word#123".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: Vec::new(),
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(conn.to_url(), "redis://:pass%40word%23123@127.0.0.1:6379/0");
    }

    #[test]
    fn test_is_sentinel_configured_false() {
        let conn = RedisConnection::default();
        assert!(!conn.is_sentinel_configured());
    }

    #[test]
    fn test_is_sentinel_configured_true() {
        let conn = RedisConnection {
            sentinels: vec![RedisSentinel {
                host: "sentinel1".to_string(),
                port: 26379,
            }],
            ..Default::default()
        };
        assert!(conn.is_sentinel_configured());
    }

    #[test]
    fn test_sentinel_url_basic() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: vec![
                RedisSentinel {
                    host: "sentinel1".to_string(),
                    port: 26379,
                },
                RedisSentinel {
                    host: "sentinel2".to_string(),
                    port: 26379,
                },
            ],
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis+sentinel://sentinel1:26379,sentinel2:26379/mymaster/0"
        );
    }

    #[test]
    fn test_sentinel_url_with_sentinel_password() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: vec![RedisSentinel {
                host: "sentinel1".to_string(),
                port: 26379,
            }],
            sentinel_password: Some("sentinelpass".to_string()),
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis+sentinel://:sentinelpass@sentinel1:26379/mymaster/0"
        );
    }

    #[test]
    fn test_sentinel_url_with_master_password() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 1,
            username: None,
            password: Some("masterpass".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: vec![RedisSentinel {
                host: "sentinel1".to_string(),
                port: 26379,
            }],
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis+sentinel://sentinel1:26379/mymaster/1?password=masterpass"
        );
    }

    #[test]
    fn test_sentinel_url_with_all_auth() {
        let conn = RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 2,
            username: Some("redisuser".to_string()),
            password: Some("redispass".to_string()),
            key_prefix: "sockudo:".to_string(),
            sentinels: vec![
                RedisSentinel {
                    host: "sentinel1".to_string(),
                    port: 26379,
                },
                RedisSentinel {
                    host: "sentinel2".to_string(),
                    port: 26380,
                },
            ],
            sentinel_password: Some("sentinelauth".to_string()),
            name: "production-master".to_string(),
            cluster: RedisClusterConnection::default(),
            cluster_nodes: Vec::new(),
        };
        assert_eq!(
            conn.to_url(),
            "redis+sentinel://:sentinelauth@sentinel1:26379,sentinel2:26380/production-master/2?password=redispass&username=redisuser"
        );
    }

    #[test]
    fn test_sentinel_to_host_port() {
        let sentinel = RedisSentinel {
            host: "sentinel.example.com".to_string(),
            port: 26379,
        };
        assert_eq!(sentinel.to_host_port(), "sentinel.example.com:26379");
    }

    #[test]
    fn test_cluster_node_urls_with_shared_cluster_auth_and_tls() {
        let conn = RedisConnection {
            cluster: RedisClusterConnection {
                nodes: vec![
                    ClusterNode {
                        host: "node1.secure-cluster.com".to_string(),
                        port: 7000,
                    },
                    ClusterNode {
                        host: "redis://node2.secure-cluster.com:7001".to_string(),
                        port: 7001,
                    },
                    ClusterNode {
                        host: "rediss://node3.secure-cluster.com".to_string(),
                        port: 7002,
                    },
                ],
                username: None,
                password: Some("cluster-secret".to_string()),
                use_tls: true,
            },
            ..Default::default()
        };

        assert_eq!(
            conn.cluster_node_urls(),
            vec![
                "rediss://:cluster-secret@node1.secure-cluster.com:7000",
                "redis://:cluster-secret@node2.secure-cluster.com:7001",
                "rediss://:cluster-secret@node3.secure-cluster.com:7002",
            ]
        );
    }

    #[test]
    fn test_cluster_node_urls_fallback_to_legacy_nodes() {
        let conn = RedisConnection {
            password: Some("fallback-secret".to_string()),
            cluster_nodes: vec![ClusterNode {
                host: "legacy-node.example.com".to_string(),
                port: 7000,
            }],
            ..Default::default()
        };

        assert_eq!(
            conn.cluster_node_urls(),
            vec!["redis://:fallback-secret@legacy-node.example.com:7000"]
        );
    }

    #[test]
    fn test_normalize_cluster_seed_urls() {
        let conn = RedisConnection {
            cluster: RedisClusterConnection {
                nodes: Vec::new(),
                username: Some("svc-user".to_string()),
                password: Some("svc-pass".to_string()),
                use_tls: true,
            },
            ..Default::default()
        };

        let seeds = vec![
            "node1.example.com:7000".to_string(),
            "redis://node2.example.com:7001".to_string(),
            "rediss://node3.example.com".to_string(),
        ];

        assert_eq!(
            conn.normalize_cluster_seed_urls(&seeds),
            vec![
                "rediss://svc-user:svc-pass@node1.example.com:7000",
                "redis://svc-user:svc-pass@node2.example.com:7001",
                "rediss://svc-user:svc-pass@node3.example.com:6379",
            ]
        );
    }
}

#[cfg(test)]
mod cluster_node_tests {
    use super::ClusterNode;

    #[test]
    fn test_to_url_basic_host() {
        let node = ClusterNode {
            host: "localhost".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "redis://localhost:6379");
    }

    #[test]
    fn test_to_url_ip_address() {
        let node = ClusterNode {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "redis://127.0.0.1:6379");
    }

    #[test]
    fn test_to_url_with_redis_protocol() {
        let node = ClusterNode {
            host: "redis://example.com".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "redis://example.com:6379");
    }

    #[test]
    fn test_to_url_with_rediss_protocol() {
        let node = ClusterNode {
            host: "rediss://secure.example.com".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://secure.example.com:6379");
    }

    #[test]
    fn test_to_url_with_rediss_protocol_and_port_in_url() {
        let node = ClusterNode {
            host: "rediss://secure.example.com:7000".to_string(),
            port: 6379, // This should be ignored since port is in URL
        };
        assert_eq!(node.to_url(), "rediss://secure.example.com:7000");
    }

    #[test]
    fn test_to_url_with_redis_protocol_and_port_in_url() {
        let node = ClusterNode {
            host: "redis://example.com:7001".to_string(),
            port: 6379, // This should be ignored since port is in URL
        };
        assert_eq!(node.to_url(), "redis://example.com:7001");
    }

    #[test]
    fn test_to_url_with_trailing_whitespace() {
        let node = ClusterNode {
            host: "  rediss://secure.example.com  ".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://secure.example.com:6379");
    }

    #[test]
    fn test_to_url_custom_port() {
        let node = ClusterNode {
            host: "redis-cluster.example.com".to_string(),
            port: 7000,
        };
        assert_eq!(node.to_url(), "redis://redis-cluster.example.com:7000");
    }

    #[test]
    fn test_to_url_plain_host_with_port_in_host_field() {
        let node = ClusterNode {
            host: "redis-cluster.example.com:7010".to_string(),
            port: 7000,
        };
        assert_eq!(node.to_url(), "redis://redis-cluster.example.com:7010");
    }

    #[test]
    fn test_to_url_with_options_adds_auth_and_tls() {
        let node = ClusterNode {
            host: "node.example.com".to_string(),
            port: 7000,
        };
        assert_eq!(
            node.to_url_with_options(true, Some("svc-user"), Some("secret")),
            "rediss://svc-user:secret@node.example.com:7000"
        );
    }

    #[test]
    fn test_to_url_with_options_keeps_embedded_auth() {
        let node = ClusterNode {
            host: "rediss://:node-secret@node.example.com:7000".to_string(),
            port: 7000,
        };
        assert_eq!(
            node.to_url_with_options(true, Some("global-user"), Some("global-secret")),
            "rediss://:node-secret@node.example.com:7000"
        );
    }

    #[test]
    fn test_from_seed_parses_plain_host_port() {
        let node = ClusterNode::from_seed("cluster-node-1:7005").expect("node should parse");
        assert_eq!(node.host, "cluster-node-1");
        assert_eq!(node.port, 7005);
    }

    #[test]
    fn test_from_seed_keeps_scheme_urls() {
        let node =
            ClusterNode::from_seed("rediss://secure.example.com:7005").expect("node should parse");
        assert_eq!(node.host, "rediss://secure.example.com:7005");
        assert_eq!(node.port, 7005);
    }

    #[test]
    fn test_to_url_aws_elasticache_hostname() {
        let node = ClusterNode {
            host: "rediss://my-cluster.use1.cache.amazonaws.com".to_string(),
            port: 6379,
        };
        assert_eq!(
            node.to_url(),
            "rediss://my-cluster.use1.cache.amazonaws.com:6379"
        );
    }

    #[test]
    fn test_to_url_with_ipv6_no_port() {
        let node = ClusterNode {
            host: "rediss://[::1]".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://[::1]:6379");
    }

    #[test]
    fn test_to_url_with_ipv6_and_port_in_url() {
        let node = ClusterNode {
            host: "rediss://[::1]:7000".to_string(),
            port: 6379, // This should be ignored since port is in URL
        };
        assert_eq!(node.to_url(), "rediss://[::1]:7000");
    }

    #[test]
    fn test_to_url_with_ipv6_full_address_no_port() {
        let node = ClusterNode {
            host: "rediss://[2001:db8::1]".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "rediss://[2001:db8::1]:6379");
    }

    #[test]
    fn test_to_url_with_ipv6_full_address_with_port() {
        let node = ClusterNode {
            host: "rediss://[2001:db8::1]:7000".to_string(),
            port: 6379, // This should be ignored
        };
        assert_eq!(node.to_url(), "rediss://[2001:db8::1]:7000");
    }

    #[test]
    fn test_to_url_with_redis_protocol_ipv6() {
        let node = ClusterNode {
            host: "redis://[::1]".to_string(),
            port: 6379,
        };
        assert_eq!(node.to_url(), "redis://[::1]:6379");
    }
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
pub struct LoggingConfig {
    pub colors_enabled: bool,
    pub include_target: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PresenceConfig {
    pub max_members_per_channel: u32,
    pub max_member_size_in_kb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct QueueConfig {
    pub driver: QueueDriver,
    pub redis: RedisQueueConfig,
    pub redis_cluster: RedisClusterQueueConfig, // Add this field
    pub sqs: SqsQueueConfig,
}

// Updated RedisQueueConfig for type safety
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisQueueConfig {
    pub concurrency: u32,
    pub prefix: Option<String>, // Optional prefix for this specific queue
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
    pub driver: CacheDriver, // Rate limiter backend often uses a cache driver
    pub api_rate_limit: RateLimit,
    pub websocket_rate_limit: RateLimit,
    pub redis: RedisConfig, // Specific Redis settings if Redis is chosen as backend
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterHealthConfig {
    pub enabled: bool,              // Enable cluster health monitoring
    pub heartbeat_interval_ms: u64, // Heartbeat broadcast interval
    pub node_timeout_ms: u64,       // Node considered dead after this timeout
    pub cleanup_interval_ms: u64,   // How often to check for dead nodes
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UnixSocketConfig {
    pub enabled: bool,
    pub path: String,
    #[serde(deserialize_with = "deserialize_octal_permission")]
    pub permission_mode: u32, // Octal file permissions (e.g., "755" string, 755 number, or 0o755 literal)
}

// --- Default Implementations ---

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            adapter: AdapterConfig::default(),
            app_manager: AppManagerConfig::default(),
            cache: CacheConfig::default(),
            channel_limits: ChannelLimits::default(),
            cors: CorsConfig::default(),
            database: DatabaseConfig::default(),
            database_pooling: DatabasePooling::default(),
            debug: false,
            event_limits: EventLimits::default(),
            host: "0.0.0.0".to_string(),
            http_api: HttpApiConfig::default(),
            instance: InstanceConfig::default(),
            logging: None, // Optional - maintains backward compatibility
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
            cleanup: crate::cleanup::CleanupConfig::default(),
            activity_timeout: 120,
            cluster_health: ClusterHealthConfig::default(),
            unix_socket: UnixSocketConfig::default(),
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
            #[cfg(feature = "redis-cluster")]
            prefix: REDIS_CLUSTER_DEFAULT_PREFIX.to_string(),
            #[cfg(not(feature = "redis-cluster"))]
            prefix: "sockudo_adapter:".to_string(),
            request_timeout_ms: 5000,
            use_connection_manager: true,
        }
    }
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

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl: 300,
        }
    }
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

impl Default for ChannelLimits {
    fn default() -> Self {
        Self {
            max_name_length: 200,
            cache_ttl: 3600,
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
            pool_min: None,
            pool_max: None,
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
            cluster: RedisClusterConnection::default(),
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
            host: "127.0.0.1".to_string(),
            port: 7000,
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

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            colors_enabled: true, // Current default behavior
            include_target: true, // Current default behavior
        }
    }
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
    fn default() -> Self {
        Self {
            prefix: "sockudo_".to_string(),
        }
    }
}

impl Default for PresenceConfig {
    fn default() -> Self {
        Self {
            max_members_per_channel: 100,
            max_member_size_in_kb: 2,
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
            driver: CacheDriver::Memory, // Default Rate Limiter backend to Memory
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
                // Specific Redis settings if Redis is chosen as backend for rate limiting
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

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            duration: 50,
        }
    }
}

impl Default for ClusterHealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,                // Enable by default for horizontal adapters
            heartbeat_interval_ms: 10000, // 10 seconds (= 30000/3 = 10000)
            node_timeout_ms: 30000,       // 30 seconds
            cleanup_interval_ms: 10000,   // 10 seconds
        }
    }
}

impl Default for UnixSocketConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "/var/run/sockudo/sockudo.sock".to_string(),
            permission_mode: 0o660, // rw-rw---- (most restrictive reasonable default)
        }
    }
}

impl ClusterHealthConfig {
    /// Validate cluster health configuration and return helpful error messages
    pub fn validate(&self) -> Result<(), String> {
        // All intervals must be positive
        if self.heartbeat_interval_ms == 0 {
            return Err("heartbeat_interval_ms must be greater than 0".to_string());
        }
        if self.node_timeout_ms == 0 {
            return Err("node_timeout_ms must be greater than 0".to_string());
        }
        if self.cleanup_interval_ms == 0 {
            return Err("cleanup_interval_ms must be greater than 0".to_string());
        }

        // Heartbeat interval should be much smaller than node timeout to avoid false positives
        if self.heartbeat_interval_ms > self.node_timeout_ms / 3 {
            return Err(format!(
                "heartbeat_interval_ms ({}) should be at least 3x smaller than node_timeout_ms ({}) to avoid false positive dead node detection. Recommended: heartbeat_interval_ms <= {}",
                self.heartbeat_interval_ms,
                self.node_timeout_ms,
                self.node_timeout_ms / 3
            ));
        }

        // Cleanup interval should be reasonable compared to node timeout
        if self.cleanup_interval_ms > self.node_timeout_ms {
            return Err(format!(
                "cleanup_interval_ms ({}) should not be larger than node_timeout_ms ({}) to ensure timely dead node detection",
                self.cleanup_interval_ms, self.node_timeout_ms
            ));
        }

        Ok(())
    }
}

impl ServerOptions {
    pub async fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = tokio::fs::read_to_string(path).await?;
        let options: Self = serde_json::from_str(&content)?;
        Ok(options)
    }

    pub async fn override_from_env(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // --- General Settings ---
        if let Ok(mode) = std::env::var("ENVIRONMENT") {
            self.mode = mode;
        }
        // Handle DEBUG_MODE first
        self.debug = parse_bool_env("DEBUG_MODE", self.debug);
        // Special handling for DEBUG env var (takes precedence over DEBUG_MODE)
        if parse_bool_env("DEBUG", false) {
            self.debug = true;
            info!("DEBUG environment variable forces debug mode ON");
        }

        self.activity_timeout = parse_env::<u64>("ACTIVITY_TIMEOUT", self.activity_timeout);

        if let Ok(host) = std::env::var("HOST") {
            self.host = host;
        }
        self.port = parse_env::<u16>("PORT", self.port);
        self.shutdown_grace_period =
            parse_env::<u64>("SHUTDOWN_GRACE_PERIOD", self.shutdown_grace_period);
        self.user_authentication_timeout = parse_env::<u64>(
            "USER_AUTHENTICATION_TIMEOUT",
            self.user_authentication_timeout,
        );
        self.websocket_max_payload_kb =
            parse_env::<u32>("WEBSOCKET_MAX_PAYLOAD_KB", self.websocket_max_payload_kb);
        if let Ok(id) = std::env::var("INSTANCE_PROCESS_ID") {
            self.instance.process_id = id;
        }

        // --- Driver Configuration ---
        if let Ok(driver_str) = std::env::var("ADAPTER_DRIVER") {
            self.adapter.driver =
                parse_driver_enum(driver_str, self.adapter.driver.clone(), "Adapter");
        }
        self.adapter.buffer_multiplier_per_cpu = parse_env::<usize>(
            "ADAPTER_BUFFER_MULTIPLIER_PER_CPU",
            self.adapter.buffer_multiplier_per_cpu,
        );
        if let Ok(driver_str) = std::env::var("CACHE_DRIVER") {
            self.cache.driver = parse_driver_enum(driver_str, self.cache.driver.clone(), "Cache");
        }
        if let Ok(driver_str) = std::env::var("QUEUE_DRIVER") {
            self.queue.driver = parse_driver_enum(driver_str, self.queue.driver.clone(), "Queue");
        }
        if let Ok(driver_str) = std::env::var("APP_MANAGER_DRIVER") {
            self.app_manager.driver =
                parse_driver_enum(driver_str, self.app_manager.driver.clone(), "AppManager");
        }
        if let Ok(driver_str) = std::env::var("RATE_LIMITER_DRIVER") {
            self.rate_limiter.driver = parse_driver_enum(
                driver_str,
                self.rate_limiter.driver.clone(),
                "RateLimiter Backend",
            );
        }

        // --- Database: Redis ---
        if let Ok(host) = std::env::var("DATABASE_REDIS_HOST") {
            self.database.redis.host = host;
        }
        self.database.redis.port =
            parse_env::<u16>("DATABASE_REDIS_PORT", self.database.redis.port);
        if let Ok(username) = std::env::var("DATABASE_REDIS_USERNAME") {
            self.database.redis.username = if username.is_empty() {
                None
            } else {
                Some(username)
            };
        }
        if let Ok(password) = std::env::var("DATABASE_REDIS_PASSWORD") {
            self.database.redis.password = Some(password);
        }
        self.database.redis.db = parse_env::<u32>("DATABASE_REDIS_DB", self.database.redis.db);
        if let Ok(prefix) = std::env::var("DATABASE_REDIS_KEY_PREFIX") {
            self.database.redis.key_prefix = prefix;
        }
        if let Ok(cluster_username) = std::env::var("DATABASE_REDIS_CLUSTER_USERNAME") {
            self.database.redis.cluster.username = if cluster_username.is_empty() {
                None
            } else {
                Some(cluster_username)
            };
        }
        if let Ok(cluster_password) = std::env::var("DATABASE_REDIS_CLUSTER_PASSWORD") {
            self.database.redis.cluster.password = Some(cluster_password);
        }
        self.database.redis.cluster.use_tls = parse_bool_env(
            "DATABASE_REDIS_CLUSTER_USE_TLS",
            self.database.redis.cluster.use_tls,
        );

        // --- Database: MySQL ---
        if let Ok(host) = std::env::var("DATABASE_MYSQL_HOST") {
            self.database.mysql.host = host;
        }
        self.database.mysql.port =
            parse_env::<u16>("DATABASE_MYSQL_PORT", self.database.mysql.port);
        if let Ok(user) = std::env::var("DATABASE_MYSQL_USERNAME") {
            self.database.mysql.username = user;
        }
        if let Ok(pass) = std::env::var("DATABASE_MYSQL_PASSWORD") {
            self.database.mysql.password = pass;
        }
        if let Ok(db) = std::env::var("DATABASE_MYSQL_DATABASE") {
            self.database.mysql.database = db;
        }
        if let Ok(table) = std::env::var("DATABASE_MYSQL_TABLE_NAME") {
            self.database.mysql.table_name = table;
        }
        override_db_pool_settings(&mut self.database.mysql, "DATABASE_MYSQL");

        // --- Database: PostgreSQL ---
        if let Ok(host) = std::env::var("DATABASE_POSTGRES_HOST") {
            self.database.postgres.host = host;
        }
        self.database.postgres.port =
            parse_env::<u16>("DATABASE_POSTGRES_PORT", self.database.postgres.port);
        if let Ok(user) = std::env::var("DATABASE_POSTGRES_USERNAME") {
            self.database.postgres.username = user;
        }
        if let Ok(pass) = std::env::var("DATABASE_POSTGRES_PASSWORD") {
            self.database.postgres.password = pass;
        }
        if let Ok(db) = std::env::var("DATABASE_POSTGRES_DATABASE") {
            self.database.postgres.database = db;
        }
        override_db_pool_settings(&mut self.database.postgres, "DATABASE_POSTGRES");

        // --- Database: DynamoDB ---
        if let Ok(region) = std::env::var("DATABASE_DYNAMODB_REGION") {
            self.database.dynamodb.region = region;
        }
        if let Ok(table) = std::env::var("DATABASE_DYNAMODB_TABLE_NAME") {
            self.database.dynamodb.table_name = table;
        }
        if let Ok(endpoint) = std::env::var("DATABASE_DYNAMODB_ENDPOINT_URL") {
            self.database.dynamodb.endpoint_url = Some(endpoint);
        }
        if let Ok(key_id) = std::env::var("AWS_ACCESS_KEY_ID") {
            self.database.dynamodb.aws_access_key_id = Some(key_id);
        }
        if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            self.database.dynamodb.aws_secret_access_key = Some(secret);
        }

        // --- Redis Cluster ---
        let apply_redis_cluster_nodes = |options: &mut Self, nodes: &str| {
            let node_list: Vec<String> = nodes
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .collect();

            options.adapter.cluster.nodes = node_list.clone();
            options.queue.redis_cluster.nodes = node_list.clone();

            let parsed_nodes: Vec<ClusterNode> = node_list
                .iter()
                .filter_map(|seed| ClusterNode::from_seed(seed))
                .collect();
            options.database.redis.cluster.nodes = parsed_nodes.clone();
            options.database.redis.cluster_nodes = parsed_nodes;
        };

        if let Ok(nodes) = std::env::var("REDIS_CLUSTER_NODES") {
            apply_redis_cluster_nodes(self, &nodes);
        }
        if let Ok(nodes) = std::env::var("DATABASE_REDIS_CLUSTER_NODES") {
            apply_redis_cluster_nodes(self, &nodes);
        }
        self.queue.redis_cluster.concurrency = parse_env::<u32>(
            "REDIS_CLUSTER_QUEUE_CONCURRENCY",
            self.queue.redis_cluster.concurrency,
        );
        if let Ok(prefix) = std::env::var("REDIS_CLUSTER_QUEUE_PREFIX") {
            self.queue.redis_cluster.prefix = Some(prefix);
        }

        // --- SSL Configuration ---
        self.ssl.enabled = parse_bool_env("SSL_ENABLED", self.ssl.enabled);
        if let Ok(val) = std::env::var("SSL_CERT_PATH") {
            self.ssl.cert_path = val;
        }
        if let Ok(val) = std::env::var("SSL_KEY_PATH") {
            self.ssl.key_path = val;
        }
        self.ssl.redirect_http = parse_bool_env("SSL_REDIRECT_HTTP", self.ssl.redirect_http);
        if let Some(port) = parse_env_optional::<u16>("SSL_HTTP_PORT") {
            self.ssl.http_port = Some(port);
        }

        // --- Unix Socket Configuration ---
        self.unix_socket.enabled = parse_bool_env("UNIX_SOCKET_ENABLED", self.unix_socket.enabled);
        if let Ok(path) = std::env::var("UNIX_SOCKET_PATH") {
            self.unix_socket.path = path;
        }
        if let Ok(mode_str) = std::env::var("UNIX_SOCKET_PERMISSION_MODE") {
            // Only accept octal string format (like chmod)
            if mode_str.chars().all(|c| c.is_digit(8)) {
                if let Ok(mode) = u32::from_str_radix(&mode_str, 8) {
                    if mode <= 0o777 {
                        self.unix_socket.permission_mode = mode;
                    } else {
                        warn!(
                            "UNIX_SOCKET_PERMISSION_MODE '{}' exceeds maximum value 777. Using default: {:o}",
                            mode_str, self.unix_socket.permission_mode
                        );
                    }
                } else {
                    warn!(
                        "Failed to parse UNIX_SOCKET_PERMISSION_MODE '{}' as octal. Using default: {:o}",
                        mode_str, self.unix_socket.permission_mode
                    );
                }
            } else {
                warn!(
                    "UNIX_SOCKET_PERMISSION_MODE '{}' must contain only octal digits (0-7). Using default: {:o}",
                    mode_str, self.unix_socket.permission_mode
                );
            }
        }

        // --- Metrics ---
        if let Ok(driver_str) = std::env::var("METRICS_DRIVER") {
            self.metrics.driver =
                parse_driver_enum(driver_str, self.metrics.driver.clone(), "Metrics");
        }
        self.metrics.enabled = parse_bool_env("METRICS_ENABLED", self.metrics.enabled);
        if let Ok(val) = std::env::var("METRICS_HOST") {
            self.metrics.host = val;
        }
        self.metrics.port = parse_env::<u16>("METRICS_PORT", self.metrics.port);
        if let Ok(val) = std::env::var("METRICS_PROMETHEUS_PREFIX") {
            self.metrics.prometheus.prefix = val;
        }

        // --- Rate Limiter ---
        self.rate_limiter.enabled =
            parse_bool_env("RATE_LIMITER_ENABLED", self.rate_limiter.enabled);
        self.rate_limiter.api_rate_limit.max_requests = parse_env::<u32>(
            "RATE_LIMITER_API_MAX_REQUESTS",
            self.rate_limiter.api_rate_limit.max_requests,
        );
        self.rate_limiter.api_rate_limit.window_seconds = parse_env::<u64>(
            "RATE_LIMITER_API_WINDOW_SECONDS",
            self.rate_limiter.api_rate_limit.window_seconds,
        );
        if let Some(hops) = parse_env_optional::<u32>("RATE_LIMITER_API_TRUST_HOPS") {
            self.rate_limiter.api_rate_limit.trust_hops = Some(hops);
        }
        self.rate_limiter.websocket_rate_limit.max_requests = parse_env::<u32>(
            "RATE_LIMITER_WS_MAX_REQUESTS",
            self.rate_limiter.websocket_rate_limit.max_requests,
        );
        self.rate_limiter.websocket_rate_limit.window_seconds = parse_env::<u64>(
            "RATE_LIMITER_WS_WINDOW_SECONDS",
            self.rate_limiter.websocket_rate_limit.window_seconds,
        );
        if let Ok(prefix) = std::env::var("RATE_LIMITER_REDIS_PREFIX") {
            self.rate_limiter.redis.prefix = Some(prefix);
        }

        // --- Queue: Redis ---
        self.queue.redis.concurrency =
            parse_env::<u32>("QUEUE_REDIS_CONCURRENCY", self.queue.redis.concurrency);
        if let Ok(prefix) = std::env::var("QUEUE_REDIS_PREFIX") {
            self.queue.redis.prefix = Some(prefix);
        }

        // --- Queue: SQS ---
        if let Ok(region) = std::env::var("QUEUE_SQS_REGION") {
            self.queue.sqs.region = region;
        }
        self.queue.sqs.visibility_timeout = parse_env::<i32>(
            "QUEUE_SQS_VISIBILITY_TIMEOUT",
            self.queue.sqs.visibility_timeout,
        );
        self.queue.sqs.max_messages =
            parse_env::<i32>("QUEUE_SQS_MAX_MESSAGES", self.queue.sqs.max_messages);
        self.queue.sqs.wait_time_seconds = parse_env::<i32>(
            "QUEUE_SQS_WAIT_TIME_SECONDS",
            self.queue.sqs.wait_time_seconds,
        );
        self.queue.sqs.concurrency =
            parse_env::<u32>("QUEUE_SQS_CONCURRENCY", self.queue.sqs.concurrency);
        self.queue.sqs.fifo = parse_bool_env("QUEUE_SQS_FIFO", self.queue.sqs.fifo);
        if let Ok(endpoint) = std::env::var("QUEUE_SQS_ENDPOINT_URL") {
            self.queue.sqs.endpoint_url = Some(endpoint);
        }

        // --- Webhooks ---
        self.webhooks.batching.enabled =
            parse_bool_env("WEBHOOK_BATCHING_ENABLED", self.webhooks.batching.enabled);
        self.webhooks.batching.duration =
            parse_env::<u64>("WEBHOOK_BATCHING_DURATION", self.webhooks.batching.duration);

        // --- NATS Adapter ---
        if let Ok(servers) = std::env::var("NATS_SERVERS") {
            self.adapter.nats.servers = servers.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(user) = std::env::var("NATS_USERNAME") {
            self.adapter.nats.username = Some(user);
        }
        if let Ok(pass) = std::env::var("NATS_PASSWORD") {
            self.adapter.nats.password = Some(pass);
        }
        if let Ok(token) = std::env::var("NATS_TOKEN") {
            self.adapter.nats.token = Some(token);
        }
        self.adapter.nats.connection_timeout_ms = parse_env::<u64>(
            "NATS_CONNECTION_TIMEOUT_MS",
            self.adapter.nats.connection_timeout_ms,
        );
        self.adapter.nats.request_timeout_ms = parse_env::<u64>(
            "NATS_REQUEST_TIMEOUT_MS",
            self.adapter.nats.request_timeout_ms,
        );

        // --- CORS ---
        if let Ok(origins) = std::env::var("CORS_ORIGINS") {
            self.cors.origin = origins.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(methods) = std::env::var("CORS_METHODS") {
            self.cors.methods = methods.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(headers) = std::env::var("CORS_HEADERS") {
            self.cors.allowed_headers = headers.split(',').map(|s| s.trim().to_string()).collect();
        }
        self.cors.credentials = parse_bool_env("CORS_CREDENTIALS", self.cors.credentials);

        // --- Performance Tuning ---
        // Global DB pooling controls
        self.database_pooling.enabled =
            parse_bool_env("DATABASE_POOLING_ENABLED", self.database_pooling.enabled);
        if let Some(min) = parse_env_optional::<u32>("DATABASE_POOL_MIN") {
            self.database_pooling.min = min;
        }
        if let Some(max) = parse_env_optional::<u32>("DATABASE_POOL_MAX") {
            self.database_pooling.max = max;
        }

        if let Some(pool_size) = parse_env_optional::<u32>("DATABASE_CONNECTION_POOL_SIZE") {
            self.database.mysql.connection_pool_size = pool_size;
            self.database.postgres.connection_pool_size = pool_size;
        }
        self.app_manager.cache.enabled =
            parse_bool_env("APP_MANAGER_CACHE_ENABLED", self.app_manager.cache.enabled);
        self.app_manager.cache.ttl =
            parse_env::<u64>("APP_MANAGER_CACHE_TTL", self.app_manager.cache.ttl);

        if let Some(cache_ttl) = parse_env_optional::<u64>("CACHE_TTL_SECONDS") {
            self.app_manager.cache.ttl = cache_ttl;
            self.cache.memory.ttl = cache_ttl;
        }
        if let Some(cleanup_interval) = parse_env_optional::<u64>("CACHE_CLEANUP_INTERVAL") {
            self.cache.memory.cleanup_interval = cleanup_interval;
        }
        if let Some(max_capacity) = parse_env_optional::<u64>("CACHE_MAX_CAPACITY") {
            self.cache.memory.max_capacity = max_capacity;
        }

        let default_app_id = std::env::var("SOCKUDO_DEFAULT_APP_ID");
        let default_app_key = std::env::var("SOCKUDO_DEFAULT_APP_KEY");
        let default_app_secret = std::env::var("SOCKUDO_DEFAULT_APP_SECRET");
        let default_app_enabled = parse_bool_env("SOCKUDO_DEFAULT_APP_ENABLED", true);

        if let (Ok(app_id), Ok(app_key), Ok(app_secret)) =
            (default_app_id, default_app_key, default_app_secret)
            && default_app_enabled
        {
            let default_app = App {
                id: app_id,
                key: app_key,
                secret: app_secret,
                enable_client_messages: parse_bool_env("SOCKUDO_ENABLE_CLIENT_MESSAGES", false),
                enabled: default_app_enabled,
                max_connections: parse_env::<u32>("SOCKUDO_DEFAULT_APP_MAX_CONNECTIONS", 100),
                max_client_events_per_second: parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_CLIENT_EVENTS_PER_SECOND",
                    100,
                ),
                max_read_requests_per_second: Some(parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_READ_REQUESTS_PER_SECOND",
                    100,
                )),
                max_presence_members_per_channel: Some(parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_PRESENCE_MEMBERS_PER_CHANNEL",
                    100,
                )),
                max_presence_member_size_in_kb: Some(parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_PRESENCE_MEMBER_SIZE_IN_KB",
                    100,
                )),
                max_channel_name_length: Some(parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_CHANNEL_NAME_LENGTH",
                    100,
                )),
                max_event_channels_at_once: Some(parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_EVENT_CHANNELS_AT_ONCE",
                    100,
                )),
                max_event_name_length: Some(parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_EVENT_NAME_LENGTH",
                    100,
                )),
                max_event_payload_in_kb: Some(parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_EVENT_PAYLOAD_IN_KB",
                    100,
                )),
                max_event_batch_size: Some(parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_EVENT_BATCH_SIZE",
                    100,
                )),
                enable_user_authentication: Some(parse_bool_env(
                    "SOCKUDO_DEFAULT_APP_ENABLE_USER_AUTHENTICATION",
                    false,
                )),
                webhooks: None,
                max_backend_events_per_second: Some(parse_env::<u32>(
                    "SOCKUDO_DEFAULT_APP_MAX_BACKEND_EVENTS_PER_SECOND",
                    100,
                )),
                enable_watchlist_events: Some(parse_bool_env(
                    "SOCKUDO_DEFAULT_APP_ENABLE_WATCHLIST_EVENTS",
                    false,
                )),
                allowed_origins: {
                    if let Ok(origins_str) = std::env::var("SOCKUDO_DEFAULT_APP_ALLOWED_ORIGINS") {
                        if !origins_str.is_empty() {
                            Some(
                                origins_str
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .collect(),
                            )
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                },
            };

            self.app_manager.array.apps.push(default_app);
            info!("Successfully registered default app from env");
        }

        // Special handling for REDIS_URL - overrides all Redis-related configs
        if let Ok(redis_url_env) = std::env::var("REDIS_URL") {
            info!("Applying REDIS_URL environment variable override");

            let redis_url_json = serde_json::json!(redis_url_env);

            // Adapter uses HashMap approach (for flexible configuration)
            self.adapter
                .redis
                .redis_pub_options
                .insert("url".to_string(), redis_url_json.clone());
            self.adapter
                .redis
                .redis_sub_options
                .insert("url".to_string(), redis_url_json);

            // Other components use direct url_override approach (simpler, more direct)
            self.cache.redis.url_override = Some(redis_url_env.clone());
            self.queue.redis.url_override = Some(redis_url_env.clone());
            self.rate_limiter.redis.url_override = Some(redis_url_env);
        }

        // --- Logging Configuration ---
        // Override existing config values or create new ones if env vars are set
        let has_colors_env = std::env::var("LOG_COLORS_ENABLED").is_ok();
        let has_target_env = std::env::var("LOG_INCLUDE_TARGET").is_ok();
        if has_colors_env || has_target_env {
            let logging_config = self.logging.get_or_insert_with(Default::default);
            if has_colors_env {
                logging_config.colors_enabled =
                    parse_bool_env("LOG_COLORS_ENABLED", logging_config.colors_enabled);
            }
            if has_target_env {
                logging_config.include_target =
                    parse_bool_env("LOG_INCLUDE_TARGET", logging_config.include_target);
            }
        }

        // --- Cleanup Configuration ---
        self.cleanup.async_enabled =
            parse_bool_env("CLEANUP_ASYNC_ENABLED", self.cleanup.async_enabled);
        self.cleanup.fallback_to_sync =
            parse_bool_env("CLEANUP_FALLBACK_TO_SYNC", self.cleanup.fallback_to_sync);
        self.cleanup.queue_buffer_size =
            parse_env::<usize>("CLEANUP_QUEUE_BUFFER_SIZE", self.cleanup.queue_buffer_size);
        self.cleanup.batch_size = parse_env::<usize>("CLEANUP_BATCH_SIZE", self.cleanup.batch_size);
        self.cleanup.batch_timeout_ms =
            parse_env::<u64>("CLEANUP_BATCH_TIMEOUT_MS", self.cleanup.batch_timeout_ms);
        // Handle worker_threads which can be "auto" or a number
        if let Ok(worker_threads_str) = std::env::var("CLEANUP_WORKER_THREADS") {
            self.cleanup.worker_threads = if worker_threads_str.to_lowercase() == "auto" {
                crate::cleanup::WorkerThreadsConfig::Auto
            } else if let Ok(n) = worker_threads_str.parse::<usize>() {
                crate::cleanup::WorkerThreadsConfig::Fixed(n)
            } else {
                warn!(
                    "Invalid CLEANUP_WORKER_THREADS value '{}', keeping current setting",
                    worker_threads_str
                );
                self.cleanup.worker_threads.clone()
            };
        }
        self.cleanup.max_retry_attempts = parse_env::<u32>(
            "CLEANUP_MAX_RETRY_ATTEMPTS",
            self.cleanup.max_retry_attempts,
        );

        // Cluster health configuration
        self.cluster_health.enabled =
            parse_bool_env("CLUSTER_HEALTH_ENABLED", self.cluster_health.enabled);
        self.cluster_health.heartbeat_interval_ms = parse_env::<u64>(
            "CLUSTER_HEALTH_HEARTBEAT_INTERVAL",
            self.cluster_health.heartbeat_interval_ms,
        );
        self.cluster_health.node_timeout_ms = parse_env::<u64>(
            "CLUSTER_HEALTH_NODE_TIMEOUT",
            self.cluster_health.node_timeout_ms,
        );
        self.cluster_health.cleanup_interval_ms = parse_env::<u64>(
            "CLUSTER_HEALTH_CLEANUP_INTERVAL",
            self.cluster_health.cleanup_interval_ms,
        );

        Ok(())
    }

    /// Validate configuration for logical consistency
    pub fn validate(&self) -> Result<(), String> {
        // Unix socket validation
        if self.unix_socket.enabled {
            // Unix socket path validation
            if self.unix_socket.path.is_empty() {
                return Err(
                    "Unix socket path cannot be empty when Unix socket is enabled".to_string(),
                );
            }

            // Security validation: Check for dangerous socket paths
            self.validate_unix_socket_security()?;

            // Warn about potential conflicts with SSL when Unix socket is enabled
            if self.ssl.enabled {
                // This is just a warning - technically both can be enabled but it's unusual
                tracing::warn!(
                    "Both Unix socket and SSL are enabled. This is unusual as Unix sockets are typically used behind reverse proxies that handle SSL termination."
                );
            }

            // Validate permission mode (should be valid octal)
            if self.unix_socket.permission_mode > 0o777 {
                return Err(format!(
                    "Unix socket permission_mode ({:o}) is invalid. Must be a valid octal mode (0o000 to 0o777)",
                    self.unix_socket.permission_mode
                ));
            }
        }

        // Validate cleanup configuration if present
        if let Err(e) = self.cleanup.validate() {
            return Err(format!("Invalid cleanup configuration: {}", e));
        }

        Ok(())
    }

    /// Validate Unix socket security settings
    fn validate_unix_socket_security(&self) -> Result<(), String> {
        let path = &self.unix_socket.path;

        // Prevent directory traversal attacks
        if path.contains("../") || path.contains("..\\") {
            return Err(
                "Unix socket path contains directory traversal sequences (../). This is not allowed for security reasons.".to_string()
            );
        }

        // Warn about world-writable permissions
        if self.unix_socket.permission_mode & 0o002 != 0 {
            warn!(
                "Unix socket permission mode ({:o}) allows world write access. This may be a security risk. Consider using more restrictive permissions like 0o660 or 0o750.",
                self.unix_socket.permission_mode
            );
        }

        // Warn about overly permissive permissions for others
        if self.unix_socket.permission_mode & 0o007 > 0o005 {
            warn!(
                "Unix socket permission mode ({:o}) grants write permissions to others. Consider using more restrictive permissions.",
                self.unix_socket.permission_mode
            );
        }

        // Warn about /tmp usage in production
        if self.mode == "production" && path.starts_with("/tmp/") {
            warn!(
                "Unix socket path '{}' is in /tmp directory. In production, consider using a more permanent location like /var/run/sockudo/ for better security and persistence.",
                path
            );
        }

        // Validate path is absolute
        if !path.starts_with('/') {
            return Err(
                "Unix socket path must be absolute (start with /) for security and reliability."
                    .to_string(),
            );
        }

        Ok(())
    }
}
