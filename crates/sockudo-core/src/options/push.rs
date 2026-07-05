use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PushStorageDriver {
    #[default]
    Memory,
    Postgres,
    Mysql,
    DynamoDb,
    SurrealDb,
    ScyllaDb,
}

impl FromStr for PushStorageDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            "postgres" | "postgresql" | "pgsql" => Ok(Self::Postgres),
            "mysql" => Ok(Self::Mysql),
            "dynamodb" => Ok(Self::DynamoDb),
            "surrealdb" | "surreal" => Ok(Self::SurrealDb),
            "scylladb" | "scylla" => Ok(Self::ScyllaDb),
            _ => Err(format!("Unknown push storage driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PushQueueDriver {
    #[default]
    Memory,
    Redis,
    #[serde(rename = "redis-cluster")]
    RedisCluster,
    Nats,
    Pulsar,
    RabbitMq,
    #[serde(rename = "google-pubsub")]
    GooglePubsub,
    Kafka,
    Iggy,
    Sqs,
    Sns,
}

impl FromStr for PushQueueDriver {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            "redis" => Ok(Self::Redis),
            "redis-cluster" | "redis_cluster" => Ok(Self::RedisCluster),
            "nats" => Ok(Self::Nats),
            "pulsar" => Ok(Self::Pulsar),
            "rabbitmq" | "rabbit-mq" => Ok(Self::RabbitMq),
            "google-pubsub" | "google_pubsub" | "gcp-pubsub" | "pubsub" => Ok(Self::GooglePubsub),
            "kafka" => Ok(Self::Kafka),
            "iggy" | "apache-iggy" | "apache_iggy" => Ok(Self::Iggy),
            "sqs" => Ok(Self::Sqs),
            "sns" => Ok(Self::Sns),
            _ => Err(format!("Unknown push queue driver: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PushConfig {
    pub storage_driver: PushStorageDriver,
    pub queue_driver: PushQueueDriver,
    pub allow_memory_drivers: bool,
    pub fcm_enabled: bool,
    pub apns_enabled: bool,
    pub webpush_enabled: bool,
    pub hms_enabled: bool,
    pub wns_enabled: bool,
    pub fcm_credential_ref: Option<String>,
    pub apns_credential_ref: Option<String>,
    pub webpush_credential_ref: Option<String>,
    pub hms_credential_ref: Option<String>,
    pub wns_credential_ref: Option<String>,
    pub accept_worker_count: u32,
    pub planner_worker_count: u32,
    pub shard_worker_count: u32,
    pub dispatch_worker_count: u32,
    pub dispatch_max_outbound_requests: usize,
    pub feedback_worker_count: u32,
    pub retry_worker_count: u32,
    pub queue_partition_count: u32,
    pub channel_shard_count: u32,
    pub fanout_fast_threshold: u64,
    pub fanout_shard_size: u64,
    pub fanout_sync_threshold: u64,
    pub backpressure_lag_threshold_secs: u64,
    pub publish_status_ttl_days: u64,
    pub stale_device_max_age_days: u64,
    pub retry: PushRetryConfig,
    pub circuit_breaker: PushCircuitBreakerConfig,
    pub default_quotas: PushDefaultQuotas,
    pub credential_encryption_key: Option<String>,
    pub kms_key_ref: Option<String>,
    pub vault_secret_ref: Option<String>,
    pub dry_run: bool,
    pub analytics_enabled: bool,
    pub analytics_retention_days: u64,
    pub payload_redaction: PushPayloadRedactionConfig,
    pub scheduler_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PushRulePayloadMappingConfig {
    pub title_field: String,
    pub body_field: String,
    pub template_data_field: String,
    pub include_remaining_fields: bool,
}

impl Default for PushRulePayloadMappingConfig {
    fn default() -> Self {
        Self {
            title_field: "title".to_string(),
            body_field: "body".to_string(),
            template_data_field: "data".to_string(),
            include_remaining_fields: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PushRuleConfig {
    pub enabled: bool,
    pub channel_pattern: String,
    pub event_filter: Vec<String>,
    pub payload_mapping: PushRulePayloadMappingConfig,
    pub rate_limit_per_second: u64,
}

impl Default for PushRuleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            channel_pattern: String::new(),
            event_filter: Vec::new(),
            payload_mapping: PushRulePayloadMappingConfig::default(),
            rate_limit_per_second: 100,
        }
    }
}

impl PushRuleConfig {
    pub(super) fn validate(&self, index: usize) -> Result<(), String> {
        if self.channel_pattern.is_empty() {
            return Err(format!(
                "push_rules[{index}].channel_pattern must not be empty"
            ));
        }
        if self.channel_pattern.len() > 256 {
            return Err(format!(
                "push_rules[{index}].channel_pattern must be at most 256 bytes"
            ));
        }
        if self.channel_pattern != "*" {
            if let Some(prefix) = self.channel_pattern.strip_suffix('*') {
                if prefix.is_empty() || prefix.contains('*') {
                    return Err(format!(
                        "push_rules[{index}].channel_pattern supports only exact, *, or trailing wildcard patterns"
                    ));
                }
            } else if self.channel_pattern.contains('*') {
                return Err(format!(
                    "push_rules[{index}].channel_pattern supports only exact, *, or trailing wildcard patterns"
                ));
            }
        }
        if self.event_filter.is_empty() {
            return Err(format!(
                "push_rules[{index}].event_filter must contain at least one event name"
            ));
        }
        if self.event_filter.len() > 32 {
            return Err(format!(
                "push_rules[{index}].event_filter must contain at most 32 event names"
            ));
        }
        for event in &self.event_filter {
            if event.is_empty() {
                return Err(format!(
                    "push_rules[{index}].event_filter must not contain empty event names"
                ));
            }
            if event.len() > 200 {
                return Err(format!(
                    "push_rules[{index}].event_filter event names must be at most 200 bytes"
                ));
            }
        }
        if self.rate_limit_per_second == 0 {
            return Err(format!(
                "push_rules[{index}].rate_limit_per_second must be greater than 0"
            ));
        }
        if self.payload_mapping.title_field.is_empty()
            || self.payload_mapping.body_field.is_empty()
            || self.payload_mapping.template_data_field.is_empty()
        {
            return Err(format!(
                "push_rules[{index}].payload_mapping fields must not be empty"
            ));
        }
        Ok(())
    }
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            storage_driver: PushStorageDriver::Memory,
            queue_driver: PushQueueDriver::Memory,
            allow_memory_drivers: false,
            fcm_enabled: false,
            apns_enabled: false,
            webpush_enabled: false,
            hms_enabled: false,
            wns_enabled: false,
            fcm_credential_ref: None,
            apns_credential_ref: None,
            webpush_credential_ref: None,
            hms_credential_ref: None,
            wns_credential_ref: None,
            accept_worker_count: 1,
            planner_worker_count: 1,
            shard_worker_count: 1,
            dispatch_worker_count: 1,
            dispatch_max_outbound_requests: 32,
            feedback_worker_count: 1,
            retry_worker_count: 1,
            queue_partition_count: 1,
            channel_shard_count: 1,
            fanout_fast_threshold: 10_000,
            fanout_shard_size: 100_000,
            fanout_sync_threshold: 0,
            backpressure_lag_threshold_secs: 60,
            publish_status_ttl_days: 30,
            stale_device_max_age_days: 90,
            retry: PushRetryConfig::default(),
            circuit_breaker: PushCircuitBreakerConfig::default(),
            default_quotas: PushDefaultQuotas::default(),
            credential_encryption_key: None,
            kms_key_ref: None,
            vault_secret_ref: None,
            dry_run: false,
            analytics_enabled: false,
            analytics_retention_days: 30,
            payload_redaction: PushPayloadRedactionConfig::default(),
            scheduler_interval_secs: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PushRetryConfig {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub max_elapsed_secs: u64,
    pub jitter: bool,
    pub jitter_ratio_percent: u8,
    pub respect_retry_after: bool,
}

impl Default for PushRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
            max_elapsed_secs: 86_400,
            jitter: true,
            jitter_ratio_percent: 20,
            respect_retry_after: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PushCircuitBreakerConfig {
    pub failure_threshold: u32,
    pub cooldown_secs: u64,
    pub half_open_max_inflight: u32,
}

impl Default for PushCircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            cooldown_secs: 60,
            half_open_max_inflight: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PushDefaultQuotas {
    pub acceptance_rps: u64,
    pub delivery_quota_daily: u64,
    pub fanout_max: u64,
    pub inflight_max: u64,
}

impl Default for PushDefaultQuotas {
    fn default() -> Self {
        Self {
            acceptance_rps: 100,
            delivery_quota_daily: 0,
            fanout_max: 0,
            inflight_max: 1_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PushPayloadRedactionConfig {
    pub redact_payload: bool,
    pub redact_template_data: bool,
    pub redact_provider_overrides: bool,
    pub allow_debug_payload_logging: bool,
}

impl Default for PushPayloadRedactionConfig {
    fn default() -> Self {
        Self {
            redact_payload: true,
            redact_template_data: true,
            redact_provider_overrides: true,
            allow_debug_payload_logging: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PushQueueDriver, PushRuleConfig, PushStorageDriver};
    use crate::options::ServerOptions;

    #[tokio::test]
    async fn push_storage_driver_overrides_from_env() {
        let previous = std::env::var("PUSH_STORAGE_DRIVER").ok();
        // SAFETY: This test controls the environment variable lifecycle for a
        // single key and restores the prior value before it returns.
        unsafe { std::env::set_var("PUSH_STORAGE_DRIVER", "mysql") };

        let mut options = ServerOptions::default();
        options.override_from_env().await.unwrap();

        if let Some(previous) = previous {
            // SAFETY: Restoring the pre-test value for the same key.
            unsafe { std::env::set_var("PUSH_STORAGE_DRIVER", previous) };
        } else {
            // SAFETY: Removing the test-only environment variable before exit.
            unsafe { std::env::remove_var("PUSH_STORAGE_DRIVER") };
        }

        assert_eq!(options.push.storage_driver, PushStorageDriver::Mysql);
    }

    #[tokio::test]
    async fn push_queue_driver_overrides_from_env() {
        let previous = std::env::var("PUSH_QUEUE_DRIVER").ok();
        // SAFETY: This test controls the environment variable lifecycle for a
        // single key and restores the prior value before it returns.
        unsafe { std::env::set_var("PUSH_QUEUE_DRIVER", "redis-cluster") };

        let mut options = ServerOptions::default();
        options.override_from_env().await.unwrap();

        if let Some(previous) = previous {
            // SAFETY: Restoring the pre-test value for the same key.
            unsafe { std::env::set_var("PUSH_QUEUE_DRIVER", previous) };
        } else {
            // SAFETY: Removing the test-only environment variable before exit.
            unsafe { std::env::remove_var("PUSH_QUEUE_DRIVER") };
        }

        assert_eq!(options.push.queue_driver, PushQueueDriver::RedisCluster);
    }

    #[test]
    fn push_config_defaults_follow_capacity_model() {
        let options = ServerOptions::default();

        assert_eq!(options.push.storage_driver, PushStorageDriver::Memory);
        assert_eq!(options.push.queue_driver, PushQueueDriver::Memory);
        assert!(!options.push.allow_memory_drivers);
        assert!(!options.push.fcm_enabled);
        assert!(!options.push.apns_enabled);
        assert!(!options.push.webpush_enabled);
        assert!(!options.push.hms_enabled);
        assert!(!options.push.wns_enabled);
        assert_eq!(options.push.fanout_fast_threshold, 10_000);
        assert_eq!(options.push.fanout_shard_size, 100_000);
        assert_eq!(options.push.publish_status_ttl_days, 30);
        assert_eq!(options.push.default_quotas.acceptance_rps, 100);
        assert_eq!(options.push.circuit_breaker.failure_threshold, 5);
        assert!(options.push.payload_redaction.redact_payload);
    }

    #[test]
    fn push_rules_default_off_and_validate_startup_shape() {
        let mut options = ServerOptions::default();
        options.push.allow_memory_drivers = true;
        assert!(options.push_rules.is_empty());

        options.push_rules.push(PushRuleConfig {
            channel_pattern: "notifications:*".to_string(),
            event_filter: vec!["agent-complete".to_string()],
            ..PushRuleConfig::default()
        });
        assert!(options.validate().is_ok());

        options.push_rules[0].event_filter.clear();
        let error = options.validate().unwrap_err();
        assert!(error.contains("push_rules[0].event_filter"));
    }

    #[tokio::test]
    async fn push_release_env_overrides_are_parsed() {
        let keys = [
            "PUSH_FCM_ENABLED",
            "PUSH_APNS_ENABLED",
            "PUSH_WEBPUSH_ENABLED",
            "PUSH_HMS_ENABLED",
            "PUSH_WNS_ENABLED",
            "PUSH_ALLOW_MEMORY_DRIVERS",
            "PUSH_CREDENTIAL_ENCRYPTION_KEY",
            "PUSH_FANOUT_FAST_THRESHOLD",
            "PUSH_FANOUT_SHARD_SIZE",
            "PUSH_FANOUT_SYNC_THRESHOLD",
            "PUSH_BACKPRESSURE_LAG_THRESHOLD_SECS",
            "PUSH_PUBLISH_STATUS_TTL_DAYS",
            "PUSH_DISPATCH_MAX_OUTBOUND_REQUESTS",
            "PUSH_FAILURE_THRESHOLD",
            "PUSH_SCHEDULER_INTERVAL_SECS",
            "PUSH_STALE_DEVICE_MAX_AGE_DAYS",
            "PUSH_ANALYTICS_ENABLED",
            "PUSH_DEFAULT_ACCEPTANCE_RPS",
            "PUSH_DEFAULT_DELIVERY_QUOTA_DAILY",
            "PUSH_DEFAULT_FANOUT_MAX",
            "PUSH_DEFAULT_INFLIGHT_MAX",
        ];
        let previous: Vec<_> = keys
            .iter()
            .map(|key| (*key, std::env::var(key).ok()))
            .collect();

        // SAFETY: This test owns the listed environment variables and restores
        // their previous values before returning.
        unsafe {
            std::env::set_var("PUSH_FCM_ENABLED", "true");
            std::env::set_var("PUSH_APNS_ENABLED", "true");
            std::env::set_var("PUSH_WEBPUSH_ENABLED", "true");
            std::env::set_var("PUSH_HMS_ENABLED", "true");
            std::env::set_var("PUSH_WNS_ENABLED", "true");
            std::env::set_var("PUSH_ALLOW_MEMORY_DRIVERS", "true");
            std::env::set_var("PUSH_CREDENTIAL_ENCRYPTION_KEY", "env:key:v1");
            std::env::set_var("PUSH_FANOUT_FAST_THRESHOLD", "12345");
            std::env::set_var("PUSH_FANOUT_SHARD_SIZE", "54321");
            std::env::set_var("PUSH_FANOUT_SYNC_THRESHOLD", "250");
            std::env::set_var("PUSH_BACKPRESSURE_LAG_THRESHOLD_SECS", "42");
            std::env::set_var("PUSH_PUBLISH_STATUS_TTL_DAYS", "14");
            std::env::set_var("PUSH_DISPATCH_MAX_OUTBOUND_REQUESTS", "17");
            std::env::set_var("PUSH_FAILURE_THRESHOLD", "9");
            std::env::set_var("PUSH_SCHEDULER_INTERVAL_SECS", "11");
            std::env::set_var("PUSH_STALE_DEVICE_MAX_AGE_DAYS", "120");
            std::env::set_var("PUSH_ANALYTICS_ENABLED", "true");
            std::env::set_var("PUSH_DEFAULT_ACCEPTANCE_RPS", "700");
            std::env::set_var("PUSH_DEFAULT_DELIVERY_QUOTA_DAILY", "8000");
            std::env::set_var("PUSH_DEFAULT_FANOUT_MAX", "9000");
            std::env::set_var("PUSH_DEFAULT_INFLIGHT_MAX", "1000");
        }

        let mut options = ServerOptions::default();
        options.override_from_env().await.unwrap();

        for (key, value) in previous {
            // SAFETY: Restoring each pre-test value or removing the test-only
            // variable for the same key.
            unsafe {
                if let Some(value) = value {
                    std::env::set_var(key, value);
                } else {
                    std::env::remove_var(key);
                }
            }
        }

        assert!(options.push.fcm_enabled);
        assert!(options.push.apns_enabled);
        assert!(options.push.webpush_enabled);
        assert!(options.push.hms_enabled);
        assert!(options.push.wns_enabled);
        assert!(options.push.allow_memory_drivers);
        assert_eq!(
            options.push.credential_encryption_key.as_deref(),
            Some("env:key:v1")
        );
        assert_eq!(options.push.fanout_fast_threshold, 12_345);
        assert_eq!(options.push.fanout_shard_size, 54_321);
        assert_eq!(options.push.fanout_sync_threshold, 250);
        assert_eq!(options.push.backpressure_lag_threshold_secs, 42);
        assert_eq!(options.push.publish_status_ttl_days, 14);
        assert_eq!(options.push.dispatch_max_outbound_requests, 17);
        assert_eq!(options.push.circuit_breaker.failure_threshold, 9);
        assert_eq!(options.push.scheduler_interval_secs, 11);
        assert_eq!(options.push.stale_device_max_age_days, 120);
        assert!(options.push.analytics_enabled);
        assert_eq!(options.push.default_quotas.acceptance_rps, 700);
        assert_eq!(options.push.default_quotas.delivery_quota_daily, 8_000);
        assert_eq!(options.push.default_quotas.fanout_max, 9_000);
        assert_eq!(options.push.default_quotas.inflight_max, 1_000);
    }
}
