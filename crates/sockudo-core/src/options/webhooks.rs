use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhooksConfig {
    pub batching: BatchingConfig,
    pub retry: WebhookRetryConfig,
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchingConfig {
    pub enabled: bool,
    pub duration: u64,
    pub size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhookRetryConfig {
    pub enabled: bool,
    pub max_attempts: Option<u32>,
    pub max_elapsed_time_ms: u64,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for WebhooksConfig {
    fn default() -> Self {
        Self {
            batching: BatchingConfig::default(),
            retry: WebhookRetryConfig::default(),
            request_timeout_ms: 10_000,
        }
    }
}

impl Default for WebhookRetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: None,
            max_elapsed_time_ms: 300_000,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
        }
    }
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            duration: 50,
            size: 100,
        }
    }
}
