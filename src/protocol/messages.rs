use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageData {
    String(String),
    Structured {
        #[serde(skip_serializing_if = "Option::is_none")]
        channel_data: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        channel: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        user_data: Option<String>,
        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    Json(Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorData {
    pub code: Option<u16>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PusherMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<MessageData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PusherApiMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<ApiMessageData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchPusherApiMessage {
    pub batch: Vec<PusherApiMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ApiMessageData {
    String(String),
    Json(Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentPusherMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<MessageData>,
}

// Helper implementations
impl MessageData {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            MessageData::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<String> {
        match self {
            MessageData::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<&Value> {
        match self {
            MessageData::Structured { extra, .. } => extra.values().next(),
            _ => None,
        }
    }
}

impl From<String> for MessageData {
    fn from(s: String) -> Self {
        MessageData::String(s)
    }
}

impl From<Value> for MessageData {
    fn from(v: Value) -> Self {
        MessageData::Json(v)
    }
}

impl PusherMessage {
    pub fn connection_established(socket_id: String) -> Self {
        Self {
            event: Some("pusher:connection_established".to_string()),
            data: Some(MessageData::from(
                json!({
                    "socket_id": socket_id,
                    "activity_timeout": 120
                })
                .to_string(),
            )),
            channel: None,
            name: None,
        }
    }

    pub fn subscription_succeeded(channel: String, presence_data: Option<Value>) -> Self {
        Self {
            event: Some("pusher_internal:subscription_succeeded".to_string()),
            channel: Some(channel),
            data: Some(MessageData::Json(
                presence_data.unwrap_or_else(|| json!({})),
            )),
            name: None,
        }
    }

    pub fn error(code: u16, message: String, channel: Option<String>) -> Self {
        Self {
            event: Some("pusher:error".to_string()),
            data: Some(MessageData::Json(json!({
                "code": code,
                "message": message
            }))),
            channel,
            name: None,
        }
    }

    pub fn channel_event<S: Into<String>>(event: S, channel: S, data: Value) -> Self {
        Self {
            event: Some(event.into()),
            channel: Some(channel.into()),
            data: Some(MessageData::Json(data)),
            name: None,
        }
    }

    pub fn member_added(channel: String, user_id: String, user_info: Option<Value>) -> Self {
        Self {
            event: Some("pusher_internal:member_added".to_string()),
            channel: Some(channel),
            data: Some(MessageData::Json(json!(
                {
                    "user_id": user_id,
                    "user_info": user_info.unwrap_or_else(|| json!({}))
                }
            ))),
            name: None,
        }
    }

    pub fn member_removed(channel: String, user_id: String) -> Self {
        Self {
            event: Some("pusher_internal:member_removed".to_string()),
            channel: Some(channel),
            data: Some(MessageData::Json(json!({ "user_id": user_id }))),
            name: None,
        }
    }

    // New helper method for pong response
    pub fn pong() -> Self {
        Self {
            event: Some("pusher:pong".to_string()),
            data: None,
            channel: None,
            name: None,
        }
    }

    // Helper for creating channel info response
    pub fn channel_info(
        occupied: bool,
        subscription_count: Option<u64>,
        user_count: Option<u64>,
        cache_data: Option<(String, Duration)>,
    ) -> Value {
        let mut response = json!({
            "occupied": occupied
        });

        if let Some(count) = subscription_count {
            response["subscription_count"] = json!(count);
        }

        if let Some(count) = user_count {
            response["user_count"] = json!(count);
        }

        if let Some((data, ttl)) = cache_data {
            response["cache"] = json!({
                "data": data,
                "ttl": ttl
            });
        }

        response
    }

    // Helper for creating channels list response
    pub fn channels_list(channels_info: HashMap<String, Value>) -> Value {
        json!({
            "channels": channels_info
        })
    }

    // Helper for creating user list response
    pub fn user_list(user_ids: Vec<String>) -> Value {
        let users = user_ids
            .into_iter()
            .map(|id| json!({ "id": id }))
            .collect::<Vec<_>>();

        json!({ "users": users })
    }

    // Helper for batch events response
    pub fn batch_response(batch_info: Vec<Value>) -> Value {
        json!({ "batch": batch_info })
    }

    // Helper for simple success response
    pub fn success_response() -> Value {
        json!({ "ok": true })
    }
}

// Add a helper extension trait for working with info parameters
pub trait InfoQueryParser {
    fn parse_info(&self) -> Vec<&str>;
    fn wants_user_count(&self) -> bool;
    fn wants_subscription_count(&self) -> bool;
    fn wants_cache(&self) -> bool;
}

impl InfoQueryParser for Option<&String> {
    fn parse_info(&self) -> Vec<&str> {
        self.map(|s| s.split(',').collect::<Vec<_>>())
            .unwrap_or_default()
    }

    fn wants_user_count(&self) -> bool {
        self.parse_info().contains(&"user_count")
    }

    fn wants_subscription_count(&self) -> bool {
        self.parse_info().contains(&"subscription_count")
    }

    fn wants_cache(&self) -> bool {
        self.parse_info().contains(&"cache")
    }
}
