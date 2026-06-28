use super::*;
use serde::{Deserialize, Serialize};
use tracing::warn;

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
    /// Timeout in milliseconds for each subsystem check in the `/up` health endpoint.
    /// Applies to adapter, cache, queue, and app manager checks independently.
    pub health_check_timeout_ms: u64,
}

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
            tag_filtering: TagFilteringConfig::default(),
            event_limits: EventLimits::default(),
            host: "0.0.0.0".to_string(),
            http_api: HttpApiConfig::default(),
            instance: InstanceConfig::default(),
            logging: None,
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
            health_check_timeout_ms: 2000,
        }
    }
}

impl ServerOptions {
    pub async fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = tokio::fs::read_to_string(path).await?;
        let options: Self = if path.ends_with(".toml") {
            toml::from_str(&content)?
        } else {
            // Legacy JSON support
            sonic_rs::from_str(&content)?
        };
        Ok(options)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.unix_socket.enabled {
            if self.unix_socket.path.is_empty() {
                return Err(
                    "Unix socket path cannot be empty when Unix socket is enabled".to_string(),
                );
            }

            self.validate_unix_socket_security()?;

            if self.ssl.enabled {
                tracing::warn!(
                    "Both Unix socket and SSL are enabled. This is unusual as Unix sockets are typically used behind reverse proxies that handle SSL termination."
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
            if self.history.postgres.table_prefix.trim().is_empty() {
                return Err("history.postgres.table_prefix must not be empty".to_string());
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

        if self.versioned_messages.enabled && self.versioned_messages.max_page_size == 0 {
            return Err("versioned_messages.max_page_size must be greater than 0".to_string());
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
            self.ai_transport.validate_deployment_matrix(
                &self.adapter,
                &self.cache,
                &self.history,
                &self.versioned_messages,
            )?;
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
                "Unix socket permission mode ({:o}) allows world write access. This may be a security risk. Consider using more restrictive permissions like 0o660 or 0o750.",
                self.unix_socket.permission_mode
            );
        }

        if self.unix_socket.permission_mode & 0o007 > 0o005 {
            warn!(
                "Unix socket permission mode ({:o}) grants write permissions to others. Consider using more restrictive permissions.",
                self.unix_socket.permission_mode
            );
        }

        if self.mode == "production" && path.starts_with("/tmp/") {
            warn!(
                "Unix socket path '{}' is in /tmp directory. In production, consider using a more permanent location like /var/run/sockudo/ for better security and persistence.",
                path
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
}
