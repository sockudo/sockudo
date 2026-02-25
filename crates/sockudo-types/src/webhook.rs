use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;

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
        let headers = AHashMap::<String, String>::deserialize(deserializer)?;
        Ok(Self { headers })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct LambdaConfig {
    pub function_name: String,
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct JobData {
    pub app_key: String,
    pub app_id: String,
    pub app_secret: String,
    pub payload: JobPayload,
    pub original_signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct JobPayload {
    pub time_ms: i64,
    pub events: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PusherWebhookPayload {
    pub time_ms: i64,
    pub events: Vec<Value>,
}
