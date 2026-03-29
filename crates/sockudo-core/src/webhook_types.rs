use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Webhook {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<url::Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lambda_function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lambda: Option<LambdaConfig>,
    pub event_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<WebhookFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<WebhookHeaders>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<WebhookRetryPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEventType {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebhookFilter {
    pub channel_prefix: Option<String>,
    pub channel_suffix: Option<String>,
    pub channel_pattern: Option<String>,
    pub channel_namespace: Option<String>,
    pub channel_namespaces: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct WebhookRetryPolicy {
    pub enabled: Option<bool>,
    pub max_attempts: Option<u32>,
    pub max_elapsed_time_ms: Option<u64>,
    pub initial_backoff_ms: Option<u64>,
    pub max_backoff_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct WebhookHeaders {
    pub headers: AHashMap<String, String>,
}

impl<'de> Deserialize<'de> for WebhookHeaders {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Flatten workaround for sonic-rs issue #114.
        let headers = AHashMap::<String, String>::deserialize(deserializer)?;
        Ok(Self { headers })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct LambdaConfig {
    pub function_name: String,
    pub region: String,
}

// This is the JobData structure that Sockudo uses internally for its queue.
// The `payload` field will be structured to produce the Pusher-compatible format when sent.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct JobData {
    pub app_key: String,
    pub app_id: String,
    pub app_secret: String,
    pub payload: JobPayload,
    pub original_signature: String,
}

// This is the JobPayload structure.
// The `events` field will now hold a vector of fully formed Pusher event objects.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct JobPayload {
    pub time_ms: i64,
    pub events: Vec<Value>,
}

// This struct represents the final payload sent to the webhook receiver,
// aligning with Pusher's format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PusherWebhookPayload {
    pub time_ms: i64,
    pub events: Vec<Value>,
}

/// Type alias for async job processor callback
pub type JobProcessorFnAsync = Box<
    dyn Fn(JobData) -> Pin<Box<dyn Future<Output = crate::error::Result<()>> + Send>>
        + Send
        + Sync
        + 'static,
>;
