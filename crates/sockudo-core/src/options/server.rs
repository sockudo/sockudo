use super::*;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Deployment topology role for this process.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ServerRole {
    /// WebSocket + HTTP + cluster participation.
    #[default]
    Default,
    /// HTTP publish only — no WebSocket listeners or cluster participation.
    Api,
}

impl ServerRole {
    pub fn is_api(&self) -> bool {
        matches!(self, ServerRole::Api)
    }
}

impl std::fmt::Display for ServerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerRole::Default => write!(f, "default"),
            ServerRole::Api => write!(f, "api"),
        }
    }
}

impl std::str::FromStr for ServerRole {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "default" => Ok(ServerRole::Default),
            "api" => Ok(ServerRole::Api),
            _ => Err(format!(
                "Unknown server role: {s} (expected \"default\" or \"api\")"
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerOptions {
    pub ably_compat: AblyCompatConfig,
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
    pub max_connections: u32,
    pub metrics: MetricsConfig,
    pub opentelemetry: OpenTelemetryConfig,
    pub mode: String,
    pub server_role: ServerRole,
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
    pub cleanup: CleanupConfig,
    pub activity_timeout: u64,
    pub cluster_health: ClusterHealthConfig,
    pub unix_socket: UnixSocketConfig,
    pub delta_compression: DeltaCompressionOptionsConfig,
    pub tag_filtering: TagFilteringConfig,
    pub websocket: WebSocketConfig,
    pub connection_recovery: ConnectionRecoveryConfig,
    pub history: HistoryConfig,
    pub presence_history: PresenceHistoryConfig,
    pub idempotency: IdempotencyConfig,
    pub ephemeral: EphemeralConfig,
    pub echo_control: EchoControlConfig,
    pub event_name_filtering: EventNameFilteringConfig,
    pub versioned_messages: VersionedMessagesConfig,
    pub annotations: AnnotationsConfig,
    pub ai_transport: AiTransportConfig,
    pub push: PushConfig,
    pub push_rules: Vec<PushRuleConfig>,
    /// Embedded Model Context Protocol server (requires the `mcp` Cargo feature).
    pub mcp: McpConfig,
    /// Timeout in milliseconds for each subsystem check in the `/up` health endpoint.
    /// Applies to adapter, cache, queue, and app manager checks independently.
    pub health_check_timeout_ms: u64,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            ably_compat: AblyCompatConfig::default(),
            adapter: AdapterConfig::default(),
            app_manager: AppManagerConfig::default(),
            cache: CacheConfig::default(),
            channel_limits: ChannelLimits::default(),
            cors: CorsConfig::default(),
            database: DatabaseConfig::default(),
            database_pooling: DatabasePooling::default(),
            debug: false,
            tag_filtering: TagFilteringConfig::default(),
            event_limits: EventLimits::default(),
            host: "0.0.0.0".to_string(),
            http_api: HttpApiConfig::default(),
            instance: InstanceConfig::default(),
            logging: None,
            max_connections: 0,
            metrics: MetricsConfig::default(),
            opentelemetry: OpenTelemetryConfig::default(),
            mode: "production".to_string(),
            server_role: ServerRole::default(),
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
            cleanup: CleanupConfig::default(),
            activity_timeout: 120,
            cluster_health: ClusterHealthConfig::default(),
            unix_socket: UnixSocketConfig::default(),
            delta_compression: DeltaCompressionOptionsConfig::default(),
            websocket: WebSocketConfig::default(),
            connection_recovery: ConnectionRecoveryConfig::default(),
            history: HistoryConfig::default(),
            presence_history: PresenceHistoryConfig::default(),
            idempotency: IdempotencyConfig::default(),
            ephemeral: EphemeralConfig::default(),
            echo_control: EchoControlConfig::default(),
            event_name_filtering: EventNameFilteringConfig::default(),
            versioned_messages: VersionedMessagesConfig::default(),
            annotations: AnnotationsConfig::default(),
            ai_transport: AiTransportConfig::default(),
            push: PushConfig::default(),
            push_rules: Vec::new(),
            mcp: McpConfig::default(),
            health_check_timeout_ms: 2000,
        }
    }
}

impl ServerOptions {
    pub async fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = tokio::fs::read_to_string(path).await?;
        let content =
            crate::utils::substitute_env_vars(&content).map_err(crate::error::Error::ConfigFile)?;
        let options: Self = if path.ends_with(".toml") {
            toml::from_str(&content)?
        } else {
            // Legacy JSON support
            sonic_rs::from_str(&content)?
        };
        Ok(options)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.ably_compat.validate()?;
        self.http_api
            .accept_traffic
            .validate()
            .map_err(|error| format!("http_api.accept_traffic: {error}"))?;
        self.opentelemetry
            .validate()
            .map_err(|error| format!("opentelemetry: {error}"))?;
        if self.ai_transport.enabled {
            self.ai_transport.validate_deployment_matrix(
                &self.adapter,
                &self.cache,
                &self.history,
                &self.versioned_messages,
            )?;
        }
        let clustered = self.adapter.driver != AdapterDriver::Local;
        let shared_cache = matches!(
            self.cache.driver,
            CacheDriver::Redis | CacheDriver::RedisCluster
        );
        if self.server_role.is_api() {
            if self.adapter.driver == AdapterDriver::Local {
                return Err(
                    "server_role=api requires a horizontal adapter; adapter.driver=local is not supported"
                        .to_string(),
                );
            }
            if self.adapter.fallback_to_local {
                return Err("server_role=api cannot use adapter.fallback_to_local".to_string());
            }
        }
        if clustered && (self.ably_compat.enabled || self.idempotency.enabled) && !shared_cache {
            return Err(
                "clustered Ably compatibility and idempotency require cache.driver=redis or redis-cluster; node-local cache cannot authorize or deduplicate across nodes"
                    .to_string(),
            );
        }
        if clustered && self.ably_compat.enabled && !self.adapter.cluster_health.enabled {
            return Err(
                "clustered Ably compatibility requires adapter.cluster_health.enabled so dead-node presence can be removed"
                    .to_string(),
            );
        }
        if clustered && self.history.enabled && self.history.backend == HistoryBackend::Memory {
            return Err(
                "clustered history requires a durable non-memory history backend".to_string(),
            );
        }
        if clustered && self.presence_history.enabled && !self.history.enabled {
            return Err(
                "clustered presence history requires history.enabled with a durable backend"
                    .to_string(),
            );
        }
        if clustered
            && self.versioned_messages.enabled
            && self.versioned_messages.driver == VersionStoreDriver::Memory
        {
            return Err(
                "clustered mutable messages and annotations require a durable non-memory version store"
                    .to_string(),
            );
        }
        if self.unix_socket.enabled {
            if self.unix_socket.path.is_empty() {
                return Err(
                    "Unix socket path cannot be empty when Unix socket is enabled".to_string(),
                );
            }

            self.validate_unix_socket_security()?;

            if self.ssl.enabled {
                tracing::warn!(
                    reason = "unusual_combination",
                    "unix socket and ssl both enabled; unix sockets are typically used behind ssl-terminating proxies"
                );
            }

            if self.unix_socket.permission_mode > 0o777 {
                return Err(format!(
                    "Unix socket permission_mode ({:o}) is invalid. Must be a valid octal mode (0o000 to 0o777)",
                    self.unix_socket.permission_mode
                ));
            }
        }

        if let Err(e) = self.cleanup.validate() {
            return Err(format!("Invalid cleanup configuration: {}", e));
        }

        if self.idempotency.enabled {
            if self.idempotency.ttl_seconds == 0 {
                return Err("idempotency.ttl_seconds must be greater than 0".to_string());
            }
            if self.idempotency.max_key_length == 0 {
                return Err("idempotency.max_key_length must be greater than 0".to_string());
            }
        }

        if self.connection_recovery.enabled {
            if self.connection_recovery.buffer_ttl_seconds == 0 {
                return Err(
                    "connection_recovery.buffer_ttl_seconds must be greater than 0".to_string(),
                );
            }
            if self.connection_recovery.max_buffer_size == 0 {
                return Err(
                    "connection_recovery.max_buffer_size must be greater than 0".to_string()
                );
            }
        }

        if self.history.enabled {
            if self.history.max_page_size == 0 {
                return Err("history.max_page_size must be greater than 0".to_string());
            }
            if self.history.writer_shards == 0 {
                return Err("history.writer_shards must be greater than 0".to_string());
            }
            if self.history.writer_queue_capacity == 0 {
                return Err("history.writer_queue_capacity must be greater than 0".to_string());
            }
            if self.history.retention_window_seconds == 0 {
                return Err("history.retention_window_seconds must be greater than 0".to_string());
            }
            let (driver, table_prefix) = match self.history.backend {
                HistoryBackend::Postgres => ("postgres", &self.history.postgres.table_prefix),
                HistoryBackend::Mysql => ("mysql", &self.history.mysql.table_prefix),
                HistoryBackend::DynamoDb => ("dynamodb", &self.history.dynamodb.table_prefix),
                HistoryBackend::SurrealDb => ("surrealdb", &self.history.surrealdb.table_prefix),
                HistoryBackend::ScyllaDb => ("scylladb", &self.history.scylladb.table_prefix),
                HistoryBackend::Memory => ("memory", &self.history.postgres.table_prefix),
            };
            if self.history.backend != HistoryBackend::Memory && table_prefix.trim().is_empty() {
                return Err(format!("history.{driver}.table_prefix must not be empty"));
            }
        }

        if self.presence_history.enabled {
            if self.presence_history.max_page_size == 0 {
                return Err("presence_history.max_page_size must be greater than 0".to_string());
            }
            if self.presence_history.retention_window_seconds == 0 {
                return Err(
                    "presence_history.retention_window_seconds must be greater than 0".to_string(),
                );
            }
        }

        if self.versioned_messages.enabled {
            if self.versioned_messages.max_page_size == 0 {
                return Err("versioned_messages.max_page_size must be greater than 0".to_string());
            }
            let durable_prefix = match self.versioned_messages.driver {
                VersionStoreDriver::Memory => None,
                VersionStoreDriver::Postgres => {
                    Some(("postgres", self.history.postgres.table_prefix.as_str()))
                }
                VersionStoreDriver::Mysql => {
                    Some(("mysql", self.history.mysql.table_prefix.as_str()))
                }
                VersionStoreDriver::DynamoDb => {
                    Some(("dynamodb", self.history.dynamodb.table_prefix.as_str()))
                }
                VersionStoreDriver::ScyllaDb => {
                    Some(("scylladb", self.history.scylladb.table_prefix.as_str()))
                }
                VersionStoreDriver::SurrealDb => {
                    Some(("surrealdb", self.history.surrealdb.table_prefix.as_str()))
                }
            };
            if let Some((driver, prefix)) = durable_prefix
                && prefix.trim().is_empty()
            {
                return Err(format!(
                    "versioned_messages.driver={driver} requires history.{driver}.table_prefix"
                ));
            }
        }
        if self.presence.update_rate_limit_per_member_per_second == 0 {
            return Err(
                "presence.update_rate_limit_per_member_per_second must be greater than 0"
                    .to_string(),
            );
        }
        if self.annotations.enabled && !self.versioned_messages.enabled {
            return Err("annotations require versioned_messages.enabled".to_string());
        }
        if self.ai_transport.enabled {
            if self.ai_transport.max_accumulated_message_bytes == 0 {
                return Err(
                    "ai_transport.max_accumulated_message_bytes must be greater than 0".to_string(),
                );
            }
            if self.ai_transport.max_appends_per_message == 0 {
                return Err(
                    "ai_transport.max_appends_per_message must be greater than 0".to_string(),
                );
            }
            if self.ai_transport.max_open_streaming_messages_per_channel == 0 {
                return Err(
                    "ai_transport.max_open_streaming_messages_per_channel must be greater than 0"
                        .to_string(),
                );
            }
            if !self
                .ai_transport
                .rollup
                .allows_window(self.ai_transport.rollup.default_window_ms)
            {
                return Err(
                    "ai_transport.rollup.default_window_ms must be one of 0, 20, 40, 100, 500 and within min/max".to_string(),
                );
            }
            if self.ai_transport.rollup.min_window_ms > self.ai_transport.rollup.max_window_ms {
                return Err(
                    "ai_transport.rollup.min_window_ms must be less than or equal to max_window_ms"
                        .to_string(),
                );
            }
            if self.ai_transport.rollup.orphan_ttl_ms == 0 {
                return Err("ai_transport.rollup.orphan_ttl_ms must be greater than 0".to_string());
            }
            if self.ai_transport.rollup.wheel_tick_ms == 0 {
                return Err("ai_transport.rollup.wheel_tick_ms must be greater than 0".to_string());
            }
            if self.ai_transport.rollup.shards == 0 {
                return Err("ai_transport.rollup.shards must be greater than 0".to_string());
            }
        }

        for (index, rule) in self.push_rules.iter().enumerate() {
            rule.validate(index)?;
        }
        self.mcp
            .validate(self.port, self.metrics.enabled, self.metrics.port)?;
        if self.adapter.nats.presence_sync_chunk_size == Some(0) {
            return Err("nats.presence_sync_chunk_size must be > 0 when set".to_string());
        }

        Ok(())
    }

    fn validate_unix_socket_security(&self) -> Result<(), String> {
        let path = &self.unix_socket.path;

        if path.contains("../") || path.contains("..\\") {
            return Err(
                "Unix socket path contains directory traversal sequences (../). This is not allowed for security reasons.".to_string()
            );
        }

        if self.unix_socket.permission_mode & 0o002 != 0 {
            warn!(
                reason = "world_write_access",
                "unix socket permission mode allows world write; consider more restrictive permissions"
            );
        }

        if self.unix_socket.permission_mode & 0o007 > 0o005 {
            warn!(
                reason = "other_write_access",
                "unix socket permission mode grants write to others; consider more restrictive permissions"
            );
        }

        if self.mode == "production" && path.starts_with("/tmp/") {
            warn!(
                reason = "tmp_path_in_production",
                "unix socket path is in /tmp; consider a permanent location for production"
            );
        }

        if !path.starts_with('/') {
            return Err(
                "Unix socket path must be absolute (start with /) for security and reliability."
                    .to_string(),
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_health_check_timeout_leaves_probe_headroom() {
        assert_eq!(ServerOptions::default().health_check_timeout_ms, 2000);
    }

    #[test]
    fn opentelemetry_defaults_are_backward_compatible() {
        let options: ServerOptions = toml::from_str("").unwrap();
        let legacy_json: ServerOptions = sonic_rs::from_str("{}").unwrap();
        let config = options.opentelemetry;

        assert_eq!(legacy_json.opentelemetry, config);

        assert!(!config.enabled);
        assert!(config.traces_enabled);
        assert!(config.metrics_enabled);
        assert!(config.logs_enabled);
        assert_eq!(config.service_name, "sockudo");
        assert_eq!(config.service_namespace, None);
        assert_eq!(config.deployment_environment, None);
        assert!(config.resource_attributes.is_empty());
        assert_eq!(config.endpoint, None);
        assert_eq!(config.export_timeout_ms, 10_000);
        assert_eq!(config.batch_scheduled_delay_ms, 5_000);
        assert_eq!(config.batch_max_queue_size, 2_048);
        assert_eq!(config.batch_max_export_batch_size, 512);
        assert_eq!(config.metric_export_interval_ms, 60_000);
        assert!(config.propagation_trace_context);
        assert!(config.propagation_baggage);
    }

    #[test]
    fn opentelemetry_partial_configuration_uses_field_defaults() {
        let options: ServerOptions = toml::from_str(
            r#"
            [opentelemetry]
            enabled = true
            service_namespace = "realtime"
            "#,
        )
        .unwrap();

        assert!(options.opentelemetry.enabled);
        assert_eq!(
            options.opentelemetry.service_namespace.as_deref(),
            Some("realtime")
        );
        assert_eq!(options.opentelemetry.service_name, "sockudo");
        assert_eq!(options.opentelemetry.batch_max_queue_size, 2_048);
    }

    #[test]
    fn opentelemetry_configuration_is_validated() {
        let mut options = ServerOptions::default();
        options.opentelemetry.service_name = "  ".to_string();
        options.validate().unwrap();

        options.opentelemetry.enabled = true;
        assert_eq!(
            options.validate().unwrap_err(),
            "opentelemetry: service_name must not be empty when enabled"
        );

        options.opentelemetry = OpenTelemetryConfig::default();
        options.opentelemetry.export_timeout_ms = 0;
        assert!(
            options
                .validate()
                .unwrap_err()
                .contains("export_timeout_ms")
        );

        options.opentelemetry = OpenTelemetryConfig::default();
        options.opentelemetry.batch_scheduled_delay_ms = 0;
        assert!(
            options
                .validate()
                .unwrap_err()
                .contains("batch_scheduled_delay_ms")
        );

        options.opentelemetry = OpenTelemetryConfig::default();
        options.opentelemetry.batch_max_queue_size = 0;
        assert!(
            options
                .validate()
                .unwrap_err()
                .contains("batch_max_queue_size")
        );

        options.opentelemetry = OpenTelemetryConfig::default();
        options.opentelemetry.batch_max_export_batch_size = 0;
        assert!(
            options
                .validate()
                .unwrap_err()
                .contains("batch_max_export_batch_size")
        );

        options.opentelemetry = OpenTelemetryConfig::default();
        options.opentelemetry.metric_export_interval_ms = 0;
        assert!(
            options
                .validate()
                .unwrap_err()
                .contains("metric_export_interval_ms")
        );

        options.opentelemetry = OpenTelemetryConfig::default();
        options.opentelemetry.batch_max_queue_size = 100;
        options.opentelemetry.batch_max_export_batch_size = 101;
        assert!(
            options
                .validate()
                .unwrap_err()
                .contains("must not exceed batch_max_queue_size")
        );
    }

    #[test]
    fn memory_pressure_admission_configuration_is_validated() {
        let mut options = ServerOptions::default();
        options.http_api.accept_traffic.memory_threshold = 1.01;
        assert!(
            options
                .validate()
                .unwrap_err()
                .contains("http_api.accept_traffic")
        );

        options.http_api.accept_traffic.memory_threshold = 0.9;
        options.http_api.accept_traffic.sample_interval_ms = 0;
        assert!(
            options
                .validate()
                .unwrap_err()
                .contains("sample_interval_ms")
        );

        options.http_api.accept_traffic.sample_interval_ms = 500;
        options.http_api.accept_traffic.memory_limit_bytes = Some(0);
        assert!(
            options
                .validate()
                .unwrap_err()
                .contains("memory_limit_bytes")
        );
    }

    #[test]
    fn clustered_compatibility_rejects_node_local_coordination() {
        let mut options = ServerOptions::default();
        options.ably_compat.enabled = true;
        options.adapter.driver = AdapterDriver::Redis;
        options.cache.driver = CacheDriver::Memory;

        let error = options.validate().unwrap_err();
        assert!(error.contains("cache.driver=redis or redis-cluster"));
    }

    #[test]
    fn clustered_state_accepts_shared_coordination_and_durable_stores() {
        let mut options = ServerOptions::default();
        options.ably_compat.enabled = true;
        options.adapter.driver = AdapterDriver::Redis;
        options.cache.driver = CacheDriver::Redis;
        options.history.enabled = true;
        options.history.backend = HistoryBackend::Postgres;
        options.versioned_messages.enabled = true;
        options.versioned_messages.driver = VersionStoreDriver::Postgres;

        options.validate().unwrap();
    }

    #[test]
    fn clustered_annotations_accept_every_durable_version_driver() {
        for driver in [
            VersionStoreDriver::Postgres,
            VersionStoreDriver::Mysql,
            VersionStoreDriver::DynamoDb,
            VersionStoreDriver::ScyllaDb,
            VersionStoreDriver::SurrealDb,
        ] {
            let mut options = ServerOptions::default();
            options.adapter.driver = AdapterDriver::Redis;
            options.cache.driver = CacheDriver::Redis;
            options.annotations.enabled = true;
            options.versioned_messages.enabled = true;
            options.versioned_messages.driver = driver.clone();
            options
                .validate()
                .unwrap_or_else(|error| panic!("{driver:?} was rejected: {error}"));
        }
    }

    #[test]
    fn enabled_idempotency_requires_nonzero_limits() {
        let mut options = ServerOptions::default();
        options.idempotency.ttl_seconds = 0;
        assert_eq!(
            options.validate().unwrap_err(),
            "idempotency.ttl_seconds must be greater than 0"
        );

        options.idempotency.ttl_seconds = 120;
        options.idempotency.max_key_length = 0;
        assert_eq!(
            options.validate().unwrap_err(),
            "idempotency.max_key_length must be greater than 0"
        );
    }

    #[test]
    fn enabled_connection_recovery_requires_a_nonzero_window() {
        let mut options = ServerOptions::default();
        options.connection_recovery.enabled = true;
        options.connection_recovery.buffer_ttl_seconds = 0;
        assert_eq!(
            options.validate().unwrap_err(),
            "connection_recovery.buffer_ttl_seconds must be greater than 0"
        );

        options.connection_recovery.buffer_ttl_seconds = 120;
        options.connection_recovery.max_buffer_size = 0;
        assert_eq!(
            options.validate().unwrap_err(),
            "connection_recovery.max_buffer_size must be greater than 0"
        );
    }

    #[test]
    fn server_role_defaults_to_default() {
        assert_eq!(ServerOptions::default().server_role, ServerRole::Default);
        assert!(!ServerOptions::default().server_role.is_api());
    }

    #[test]
    fn server_role_parses_case_insensitive() {
        for raw in ["api", "Api", "API"] {
            assert_eq!(raw.parse::<ServerRole>().unwrap(), ServerRole::Api);
        }
        assert_eq!(
            "default".parse::<ServerRole>().unwrap(),
            ServerRole::Default
        );
    }

    #[test]
    fn server_role_rejects_unknown() {
        assert!("ws".parse::<ServerRole>().is_err());
        assert!("".parse::<ServerRole>().is_err());
    }

    #[test]
    fn server_role_displays_lowercase() {
        assert_eq!(ServerRole::Api.to_string(), "api");
        assert_eq!(ServerRole::Default.to_string(), "default");
    }

    #[test]
    fn api_role_rejects_local_adapter() {
        let mut options = ServerOptions {
            server_role: ServerRole::Api,
            ..Default::default()
        };
        options.adapter.driver = AdapterDriver::Local;
        let error = options.validate().unwrap_err();
        assert!(error.contains("horizontal adapter"), "got: {error}");
    }

    #[test]
    fn api_role_rejects_fallback_to_local() {
        let mut options = ServerOptions {
            server_role: ServerRole::Api,
            ..Default::default()
        };
        options.adapter.driver = AdapterDriver::Redis;
        options.adapter.fallback_to_local = true;
        let error = options.validate().unwrap_err();
        assert!(error.contains("fallback_to_local"), "got: {error}");
    }

    #[test]
    fn api_role_accepts_redis_adapter() {
        let mut options = ServerOptions {
            server_role: ServerRole::Api,
            ..Default::default()
        };
        options.adapter.driver = AdapterDriver::Redis;
        options.adapter.fallback_to_local = false;
        options.cache.driver = CacheDriver::Redis;
        options.validate().unwrap();
    }

    #[test]
    fn default_role_accepts_local_adapter() {
        let options = ServerOptions::default();
        options.validate().unwrap();
    }
}
