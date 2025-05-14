use aws_config::SdkConfig;
// src/webhook/types.rs
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEventType {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookFilter {
    pub channel_prefix: Option<String>,
    pub channel_suffix: Option<String>,
    pub channel_pattern: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookHeaders {
    #[serde(flatten)]
    pub headers: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaConfig {
    pub function_name: String,
    pub region: String,
}

// This is the JobData structure based on your definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobData {
    pub app_key: String,
    pub app_id: String,
    pub payload: JobPayload,
    pub original_signature: String,
}

// And this is the JobPayload structure that's part of JobData
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPayload {
    pub time_ms: i64,
    pub events: Vec<String>,
    pub data: Value,
}
