use serde::{Deserialize, Serialize};
use sockudo_protocol::constants::PONG_TIMEOUT;
use std::collections::BTreeMap;

use super::{CacheDriver, MetricsDriver, RedisConfig};

const PONG_TIMEOUT_CLOSE_REASON: &str = "Pong reply not received in time";

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpApiConfig {
    pub request_limit_in_mb: u32,
    pub accept_traffic: AcceptTraffic,
    pub usage_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AcceptTraffic {
    /// Enable memory-pressure admission control for new WebSocket connections.
    pub enabled: bool,
    /// Fraction of the effective memory limit at which admission is closed.
    pub memory_threshold: f64,
    /// Optional explicit memory limit. When unset, Linux cgroup limits are discovered.
    pub memory_limit_bytes: Option<u64>,
    /// Interval used by the admission sampler, independent of metrics scraping.
    pub sample_interval_ms: u64,
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
    pub tcp_exporter: MetricsTcpExporterConfig,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrometheusConfig {
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsTcpExporterConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub buffer_size: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub colors_enabled: bool,
    pub include_target: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct OpenTelemetryConfig {
    pub enabled: bool,
    pub traces_enabled: bool,
    pub metrics_enabled: bool,
    pub logs_enabled: bool,
    pub service_name: String,
    pub service_namespace: Option<String>,
    pub deployment_environment: Option<String>,
    pub resource_attributes: BTreeMap<String, String>,
    pub endpoint: Option<String>,
    pub export_timeout_ms: u64,
    pub batch_scheduled_delay_ms: u64,
    pub batch_max_queue_size: usize,
    pub batch_max_export_batch_size: usize,
    pub metric_export_interval_ms: u64,
    pub propagation_trace_context: bool,
    pub propagation_baggage: bool,
}

/// WebSocket connection buffer configuration
/// Controls backpressure handling for slow consumers
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebSocketConfig {
    pub max_messages: Option<usize>,
    pub max_bytes: Option<usize>,
    pub disconnect_on_buffer_full: bool,
    pub max_message_size: usize,
    pub max_frame_size: usize,
    pub write_buffer_size: usize,
    pub max_backpressure: usize,
    pub auto_ping: bool,
    pub ping_interval: u32,
    pub idle_timeout: u32,
    pub compression: String,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            max_messages: Some(1000),
            max_bytes: None,
            disconnect_on_buffer_full: true,
            max_message_size: 64 * 1024 * 1024,
            max_frame_size: 16 * 1024 * 1024,
            write_buffer_size: 16 * 1024,
            max_backpressure: 1024 * 1024,
            auto_ping: true,
            ping_interval: 30,
            idle_timeout: 120,
            compression: "disabled".to_string(),
        }
    }
}

impl WebSocketConfig {
    /// Convert to WebSocketBufferConfig for runtime use
    pub fn to_buffer_config(&self) -> crate::websocket::WebSocketBufferConfig {
        use crate::websocket::{BufferLimit, WebSocketBufferConfig};

        let limit = match (self.max_messages, self.max_bytes) {
            (Some(messages), Some(bytes)) => BufferLimit::Both { messages, bytes },
            (Some(messages), None) => BufferLimit::Messages(messages),
            (None, Some(bytes)) => BufferLimit::Bytes(bytes),
            (None, None) => BufferLimit::Messages(1000),
        };

        WebSocketBufferConfig {
            limit,
            disconnect_on_full: self.disconnect_on_buffer_full,
        }
    }

    /// Convert to native sockudo-ws runtime configuration with native
    /// heartbeat settings enabled.
    ///
    /// This keeps the existing two-argument API available. Protocol-aware
    /// server call sites should use
    /// [`Self::to_sockudo_ws_config_with_native_heartbeat`] instead.
    pub fn to_sockudo_ws_config(
        &self,
        websocket_max_payload_kb: u32,
        activity_timeout: u64,
    ) -> sockudo_ws::Config {
        self.to_sockudo_ws_config_with_native_heartbeat(
            websocket_max_payload_kb,
            activity_timeout,
            true,
        )
    }

    /// Convert to native sockudo-ws runtime configuration with explicit
    /// heartbeat ownership.
    ///
    /// `use_native_heartbeat` is enabled for Protocol V2. Protocol V1 and
    /// compatibility protocols keep their application-level heartbeat loops.
    pub fn to_sockudo_ws_config_with_native_heartbeat(
        &self,
        websocket_max_payload_kb: u32,
        activity_timeout: u64,
        use_native_heartbeat: bool,
    ) -> sockudo_ws::Config {
        use sockudo_ws::Compression;

        let compression = match self.compression.to_lowercase().as_str() {
            "dedicated" => Compression::Dedicated,
            "shared" => Compression::Shared,
            "window256b" => Compression::Window256B,
            "window1kb" => Compression::Window1KB,
            "window2kb" => Compression::Window2KB,
            "window4kb" => Compression::Window4KB,
            "window8kb" => Compression::Window8KB,
            "window16kb" => Compression::Window16KB,
            "window32kb" => Compression::Window32KB,
            _ => Compression::Disabled,
        };
        let minimum_ping_interval =
            u32::try_from((activity_timeout / 2).max(5)).unwrap_or(u32::MAX);
        let ping_interval = if self.ping_interval == 0 {
            0
        } else {
            self.ping_interval.max(minimum_ping_interval)
        };

        sockudo_ws::Config::builder()
            .max_payload_length(
                self.max_bytes
                    .unwrap_or(websocket_max_payload_kb as usize * 1024),
            )
            .max_message_size(self.max_message_size)
            .max_frame_size(self.max_frame_size)
            .write_buffer_size(self.write_buffer_size)
            .max_backpressure(self.max_backpressure)
            .idle_timeout(if use_native_heartbeat {
                self.idle_timeout
            } else {
                0
            })
            .auto_ping(use_native_heartbeat && self.auto_ping)
            .ping_interval(ping_interval)
            .pong_timeout(PONG_TIMEOUT as u32)
            .pong_timeout_close(
                crate::error::Error::PongNotReceived.close_code(),
                PONG_TIMEOUT_CLOSE_REASON,
            )
            .compression(compression)
            .build()
    }
}

#[cfg(test)]
mod websocket_config_tests {
    use super::*;

    #[test]
    fn native_heartbeat_uses_sockudo_timeout_contract() {
        let config = WebSocketConfig::default().to_sockudo_ws_config(64, 120);

        assert!(config.auto_ping);
        assert_eq!(config.ping_interval, 60);
        assert_eq!(config.idle_timeout, 120);
        assert_eq!(config.pong_timeout, PONG_TIMEOUT as u32);
        assert_eq!(
            config.pong_timeout_close_code,
            crate::error::Error::PongNotReceived.close_code()
        );
        assert_eq!(config.pong_timeout_close_reason, PONG_TIMEOUT_CLOSE_REASON);
    }

    #[test]
    fn application_heartbeat_disables_native_deadlines() {
        let config =
            WebSocketConfig::default().to_sockudo_ws_config_with_native_heartbeat(64, 120, false);

        assert!(!config.auto_ping);
        assert_eq!(config.idle_timeout, 0);
    }

    #[test]
    fn zero_ping_interval_remains_disabled() {
        let websocket = WebSocketConfig {
            ping_interval: 0,
            ..WebSocketConfig::default()
        };
        let config = websocket.to_sockudo_ws_config(64, 120);

        assert_eq!(config.ping_interval, 0);
    }
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
pub struct ClusterHealthConfig {
    pub enabled: bool,
    pub heartbeat_interval_ms: u64,
    pub node_timeout_ms: u64,
    pub cleanup_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UnixSocketConfig {
    pub enabled: bool,
    pub path: String,
    #[serde(deserialize_with = "deserialize_octal_permission")]
    pub permission_mode: u32,
}

/// Cleanup system configuration (minimal version for options; full impl lives in sockudo crate)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CleanupConfig {
    pub queue_buffer_size: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub worker_threads: WorkerThreadsConfig,
    pub max_retry_attempts: u32,
    pub async_enabled: bool,
    pub fallback_to_sync: bool,
}

/// Worker threads configuration for the cleanup system
#[derive(Debug, Clone)]
pub enum WorkerThreadsConfig {
    Auto,
    Fixed(usize),
}

impl serde::Serialize for WorkerThreadsConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            WorkerThreadsConfig::Auto => serializer.serialize_str("auto"),
            WorkerThreadsConfig::Fixed(n) => serializer.serialize_u64(*n as u64),
        }
    }
}

impl<'de> serde::Deserialize<'de> for WorkerThreadsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de;
        struct WorkerThreadsVisitor;
        impl<'de> de::Visitor<'de> for WorkerThreadsVisitor {
            type Value = WorkerThreadsConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(r#""auto" or a positive integer"#)
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value.eq_ignore_ascii_case("auto") {
                    Ok(WorkerThreadsConfig::Auto)
                } else if let Ok(n) = value.parse::<usize>() {
                    Ok(WorkerThreadsConfig::Fixed(n))
                } else {
                    Err(E::custom(format!(
                        "expected 'auto' or a number, got '{value}'"
                    )))
                }
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(WorkerThreadsConfig::Fixed(value as usize))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                if value >= 0 {
                    Ok(WorkerThreadsConfig::Fixed(value as usize))
                } else {
                    Err(E::custom("worker_threads must be non-negative"))
                }
            }
        }
        deserializer.deserialize_any(WorkerThreadsVisitor)
    }
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            queue_buffer_size: 1024,
            batch_size: 64,
            batch_timeout_ms: 100,
            worker_threads: WorkerThreadsConfig::Auto,
            max_retry_attempts: 3,
            async_enabled: true,
            fallback_to_sync: true,
        }
    }
}

impl CleanupConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.queue_buffer_size == 0 {
            return Err("queue_buffer_size must be greater than 0".to_string());
        }
        if self.batch_size == 0 {
            return Err("batch_size must be greater than 0".to_string());
        }
        if self.batch_timeout_ms == 0 {
            return Err("batch_timeout_ms must be greater than 0".to_string());
        }
        if let WorkerThreadsConfig::Fixed(n) = self.worker_threads
            && n == 0
        {
            return Err("worker_threads must be greater than 0 when using fixed count".to_string());
        }
        Ok(())
    }
}

// --- Default Implementations ---

impl Default for HttpApiConfig {
    fn default() -> Self {
        Self {
            request_limit_in_mb: 10,
            accept_traffic: AcceptTraffic::default(),
            usage_enabled: true,
        }
    }
}

impl Default for AcceptTraffic {
    fn default() -> Self {
        Self {
            enabled: false,
            memory_threshold: 0.90,
            memory_limit_bytes: None,
            sample_interval_ms: 500,
        }
    }
}

impl AcceptTraffic {
    pub fn validate(&self) -> Result<(), String> {
        if !self.memory_threshold.is_finite()
            || self.memory_threshold <= 0.0
            || self.memory_threshold > 1.0
        {
            return Err("memory_threshold must be greater than 0 and at most 1".to_string());
        }
        if self.memory_limit_bytes == Some(0) {
            return Err("memory_limit_bytes must be greater than 0 when set".to_string());
        }
        if self.sample_interval_ms == 0 {
            return Err("sample_interval_ms must be greater than 0".to_string());
        }
        Ok(())
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
            colors_enabled: true,
            include_target: true,
        }
    }
}

impl Default for OpenTelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            traces_enabled: true,
            metrics_enabled: true,
            logs_enabled: true,
            service_name: "sockudo".to_string(),
            service_namespace: None,
            deployment_environment: None,
            resource_attributes: BTreeMap::new(),
            endpoint: None,
            export_timeout_ms: 10_000,
            batch_scheduled_delay_ms: 5_000,
            batch_max_queue_size: 2_048,
            batch_max_export_batch_size: 512,
            metric_export_interval_ms: 60_000,
            propagation_trace_context: true,
            propagation_baggage: true,
        }
    }
}

impl OpenTelemetryConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.service_name.trim().is_empty() {
            return Err("service_name must not be empty when enabled".to_string());
        }
        if self.export_timeout_ms == 0 {
            return Err("export_timeout_ms must be greater than 0".to_string());
        }
        if self.batch_scheduled_delay_ms == 0 {
            return Err("batch_scheduled_delay_ms must be greater than 0".to_string());
        }
        if self.batch_max_queue_size == 0 {
            return Err("batch_max_queue_size must be greater than 0".to_string());
        }
        if self.batch_max_export_batch_size == 0 {
            return Err("batch_max_export_batch_size must be greater than 0".to_string());
        }
        if self.metric_export_interval_ms == 0 {
            return Err("metric_export_interval_ms must be greater than 0".to_string());
        }
        if self.batch_max_export_batch_size > self.batch_max_queue_size {
            return Err(
                "batch_max_export_batch_size must not exceed batch_max_queue_size".to_string(),
            );
        }

        Ok(())
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            driver: MetricsDriver::default(),
            host: "0.0.0.0".to_string(),
            prometheus: PrometheusConfig::default(),
            tcp_exporter: MetricsTcpExporterConfig::default(),
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

impl Default for MetricsTcpExporterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: "127.0.0.1".to_string(),
            port: 5000,
            buffer_size: Some(1024),
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

impl Default for UnixSocketConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "/var/run/sockudo/sockudo.sock".to_string(),
            permission_mode: 0o660,
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
