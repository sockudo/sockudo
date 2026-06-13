use super::*;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IdempotencyConfig {
    /// Whether idempotency key support is enabled
    pub enabled: bool,
    /// TTL in seconds for idempotency keys (default: 120s, like Ably's 2-minute window)
    pub ttl_seconds: u64,
    /// Maximum length of an idempotency key
    pub max_key_length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EphemeralConfig {
    /// Whether ephemeral message handling is enabled.
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EchoControlConfig {
    /// Whether connection-level and per-message echo control is enabled.
    pub enabled: bool,
    /// Default echo behavior for new V2 connections when the query param is omitted.
    pub default_echo_messages: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EventNameFilteringConfig {
    /// Whether per-subscription event name filtering is enabled.
    pub enabled: bool,
    /// Maximum event names allowed in a single filter.
    pub max_events_per_filter: usize,
    /// Maximum length for each event name in a filter.
    pub max_event_name_length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectionRecoveryConfig {
    /// Whether connection recovery (resume) is enabled.
    /// When enabled, the server keeps a bounded replay buffer per channel so that
    /// reconnecting clients can receive missed messages. Disabled by default for
    /// Pusher protocol compatibility.
    pub enabled: bool,
    /// How long messages stay in the replay buffer (seconds). Default: 120 (2 min).
    pub buffer_ttl_seconds: u64,
    /// Maximum number of messages kept per channel. Default: 100.
    pub max_buffer_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum VersionStoreDriver {
    #[default]
    Memory,
    Postgres,
    Mysql,
    DynamoDb,
    ScyllaDb,
    SurrealDb,
}

impl FromStr for VersionStoreDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" | "pgsql" => Ok(Self::Postgres),
            "mysql" => Ok(Self::Mysql),
            "dynamodb" => Ok(Self::DynamoDb),
            "scylladb" | "scylla" => Ok(Self::ScyllaDb),
            "surrealdb" | "surreal" => Ok(Self::SurrealDb),
            "memory" => Ok(Self::Memory),
            _ => Err(format!("Unknown version store driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VersionedMessagesConfig {
    /// Whether V2 mutable message HTTP/retrieval surfaces are enabled.
    pub enabled: bool,
    /// Storage driver for versioned messages. Defaults to memory.
    pub driver: VersionStoreDriver,
    /// Maximum page size for version-history retrieval.
    pub max_page_size: usize,
    /// Retention window for version entries, in seconds. `0` disables expiry
    /// (entries are kept forever).
    ///
    /// Backends with native row TTL (ScyllaDB `USING TTL`, DynamoDB TTL
    /// attribute) apply this at the storage layer. Other backends (MySQL,
    /// PostgreSQL, SurrealDB, Memory) enforce it via a periodic background
    /// purge worker controlled by `purge_interval_seconds`.
    pub retention_window_seconds: u64,
    /// Interval between purge worker runs, in seconds. Only consulted for
    /// backends without native TTL. Clamped to a minimum of 10 seconds.
    pub purge_interval_seconds: u64,
    /// Maximum rows deleted per purge query. Bounds lock/transaction sizes
    /// for SQL backends. The worker loops until no more expired rows remain
    /// or `max_purge_per_tick` is reached.
    pub purge_batch_size: usize,
    /// Hard cap on rows deleted per purge tick across all loop iterations.
    /// Prevents a backlog of expired rows from monopolising a worker run.
    pub max_purge_per_tick: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AnnotationsConfig {
    /// Whether Sockudo-native annotation APIs and realtime annotation protocol
    /// surfaces are enabled. Disabled by default while the feature is opt-in.
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum HistoryBackend {
    #[default]
    Postgres,
    Mysql,
    DynamoDb,
    SurrealDb,
    ScyllaDb,
    Memory,
}

impl FromStr for HistoryBackend {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" | "pgsql" => Ok(Self::Postgres),
            "mysql" => Ok(Self::Mysql),
            "dynamodb" => Ok(Self::DynamoDb),
            "surrealdb" | "surreal" => Ok(Self::SurrealDb),
            "scylladb" | "scylla" => Ok(Self::ScyllaDb),
            "memory" => Ok(Self::Memory),
            _ => Err(format!("Unknown history backend: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PostgresHistoryConfig {
    pub table_prefix: String,
    pub write_timeout_ms: u64,
}

impl Default for PostgresHistoryConfig {
    fn default() -> Self {
        Self {
            table_prefix: "sockudo_history".to_string(),
            write_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MySqlHistoryConfig {
    pub table_prefix: String,
    pub write_timeout_ms: u64,
}

impl Default for MySqlHistoryConfig {
    fn default() -> Self {
        Self {
            table_prefix: "sockudo_history".to_string(),
            write_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DynamoDbHistoryConfig {
    pub table_prefix: String,
    pub write_timeout_ms: u64,
}

impl Default for DynamoDbHistoryConfig {
    fn default() -> Self {
        Self {
            table_prefix: "sockudo_history".to_string(),
            write_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SurrealDbHistoryConfig {
    pub table_prefix: String,
    pub write_timeout_ms: u64,
}

impl Default for SurrealDbHistoryConfig {
    fn default() -> Self {
        Self {
            table_prefix: "sockudo_history".to_string(),
            write_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScyllaDbHistoryConfig {
    pub table_prefix: String,
    pub write_timeout_ms: u64,
}

impl Default for ScyllaDbHistoryConfig {
    fn default() -> Self {
        Self {
            table_prefix: "sockudo_history".to_string(),
            write_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryConfig {
    pub enabled: bool,
    pub rewind_enabled: bool,
    pub backend: HistoryBackend,
    pub retention_window_seconds: u64,
    pub max_page_size: usize,
    pub max_messages_per_channel: Option<usize>,
    pub max_bytes_per_channel: Option<u64>,
    pub writer_shards: usize,
    pub writer_queue_capacity: usize,
    pub purge_interval_seconds: u64,
    pub purge_batch_size: usize,
    pub max_purge_per_tick: usize,
    pub postgres: PostgresHistoryConfig,
    pub mysql: MySqlHistoryConfig,
    pub dynamodb: DynamoDbHistoryConfig,
    pub surrealdb: SurrealDbHistoryConfig,
    pub scylladb: ScyllaDbHistoryConfig,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rewind_enabled: true,
            backend: HistoryBackend::Postgres,
            retention_window_seconds: 86400,
            max_page_size: 100,
            max_messages_per_channel: None,
            max_bytes_per_channel: None,
            writer_shards: 16,
            writer_queue_capacity: 4096,
            purge_interval_seconds: 300,
            purge_batch_size: 1000,
            max_purge_per_tick: 100_000,
            postgres: PostgresHistoryConfig::default(),
            mysql: MySqlHistoryConfig::default(),
            dynamodb: DynamoDbHistoryConfig::default(),
            surrealdb: SurrealDbHistoryConfig::default(),
            scylladb: ScyllaDbHistoryConfig::default(),
        }
    }
}

impl Default for VersionedMessagesConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            driver: VersionStoreDriver::Memory,
            max_page_size: 100,
            retention_window_seconds: 0,
            purge_interval_seconds: 300,
            purge_batch_size: 1000,
            max_purge_per_tick: 100_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PresenceHistoryConfig {
    pub enabled: bool,
    pub retention_window_seconds: u64,
    pub max_page_size: usize,
    pub max_events_per_channel: Option<usize>,
    pub max_bytes_per_channel: Option<u64>,
}

impl Default for PresenceHistoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            retention_window_seconds: 86400,
            max_page_size: 100,
            max_events_per_channel: None,
            max_bytes_per_channel: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DeltaCompressionOptionsConfig {
    pub enabled: bool,
    pub algorithm: String,
    pub full_message_interval: u32,
    pub min_message_size: usize,
    pub max_state_age_secs: u64,
    pub max_channel_states_per_socket: usize,
    pub max_conflation_states_per_channel: Option<usize>,
    pub conflation_key_path: Option<String>,
    pub cluster_coordination: bool,
    pub coordination_backend: DeltaCoordinationBackend,
    pub omit_delta_algorithm: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TagFilteringConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub enable_tags: bool,
}

fn default_true() -> bool {
    true
}

impl Default for IdempotencyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_seconds: 120,
            max_key_length: 128,
        }
    }
}

impl Default for EphemeralConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl Default for EchoControlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_echo_messages: true,
        }
    }
}

impl Default for EventNameFilteringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_events_per_filter: 50,
            max_event_name_length: 200,
        }
    }
}

impl Default for ConnectionRecoveryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            buffer_ttl_seconds: 120,
            max_buffer_size: 100,
        }
    }
}

impl Default for DeltaCompressionOptionsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: "fossil".to_string(),
            full_message_interval: 10,
            min_message_size: 100,
            max_state_age_secs: 300,
            max_channel_states_per_socket: 100,
            max_conflation_states_per_channel: Some(100),
            conflation_key_path: None,
            cluster_coordination: false,
            coordination_backend: DeltaCoordinationBackend::Auto,
            omit_delta_algorithm: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ServerOptions, VersionStoreDriver};

    #[tokio::test]
    async fn versioned_messages_driver_overrides_from_env() {
        let previous = std::env::var("VERSIONED_MESSAGES_DRIVER").ok();
        // SAFETY: This test controls the environment variable lifecycle for a
        // single key and restores the prior value before it returns.
        unsafe { std::env::set_var("VERSIONED_MESSAGES_DRIVER", "postgres") };

        let mut options = ServerOptions::default();
        options.override_from_env().await.unwrap();

        if let Some(previous) = previous {
            // SAFETY: Restoring the pre-test value for the same key.
            unsafe { std::env::set_var("VERSIONED_MESSAGES_DRIVER", previous) };
        } else {
            // SAFETY: Removing the test-only environment variable before exit.
            unsafe { std::env::remove_var("VERSIONED_MESSAGES_DRIVER") };
        }

        assert_eq!(
            options.versioned_messages.driver,
            VersionStoreDriver::Postgres
        );
    }
}
