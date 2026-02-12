use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceData {
    pub ids: Vec<String>,
    pub hash: AHashMap<String, Option<Value>>,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
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
        extra: AHashMap<String, Value>,
    },
    Json(Value),
}

impl<'de> Deserialize<'de> for MessageData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = Value::deserialize(deserializer)?;
        if let Some(s) = v.as_str() {
            return Ok(MessageData::String(s.to_string()));
        }
        if let Some(obj) = v.as_object() {
            // Flatten workaround for sonic-rs issue #114:
            // manually split known structured keys and keep remaining keys in `extra`.
            let channel_data = obj
                .get(&"channel_data")
                .and_then(|x| x.as_str())
                .map(ToString::to_string);
            let channel = obj
                .get(&"channel")
                .and_then(|x| x.as_str())
                .map(ToString::to_string);
            let user_data = obj
                .get(&"user_data")
                .and_then(|x| x.as_str())
                .map(ToString::to_string);

            if channel_data.is_some() || channel.is_some() || user_data.is_some() {
                let mut extra = AHashMap::new();
                for (k, val) in obj.iter() {
                    if k != "channel_data" && k != "channel" && k != "user_data" {
                        extra.insert(k.to_string(), val.clone());
                    }
                }
                return Ok(MessageData::Structured {
                    channel_data,
                    channel,
                    user_data,
                    extra,
                });
            }
        }
        Ok(MessageData::Json(v))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorData {
    pub code: Option<u16>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PusherMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<MessageData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    /// Tags for filtering - uses BTreeMap for deterministic serialization order
    /// which is required for delta compression to work correctly
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<BTreeMap<String, String>>,
    /// Delta compression sequence number for full messages
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// Delta compression conflation key for message grouping
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflation_key: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<AHashMap<String, String>>,
    /// Per-publish delta compression control.
    /// - `Some(true)`: Force delta compression for this message (if client supports it)
    /// - `Some(false)`: Force full message (skip delta compression)
    /// - `None`: Use default behavior based on channel/global configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<bool>,
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
    pub fn connection_established(socket_id: String, activity_timeout: u64) -> Self {
        Self {
            event: Some("pusher:connection_established".to_string()),
            data: Some(MessageData::from(
                json!({
                    "socket_id": socket_id,
                    "activity_timeout": activity_timeout  // Now configurable
                })
                .to_string(),
            )),
            channel: None,
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }
    pub fn subscription_succeeded(channel: String, presence_data: Option<PresenceData>) -> Self {
        let data_obj = if let Some(data) = presence_data {
            json!({
                "presence": {
                    "ids": data.ids,
                    "hash": data.hash,
                    "count": data.count
                }
            })
        } else {
            json!({})
        };

        Self {
            event: Some("pusher_internal:subscription_succeeded".to_string()),
            channel: Some(channel),
            data: Some(MessageData::String(data_obj.to_string())),
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
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
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }

    pub fn ping() -> Self {
        Self {
            event: Some("pusher:ping".to_string()),
            data: None,
            channel: None,
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }
    pub fn channel_event<S: Into<String>>(event: S, channel: S, data: Value) -> Self {
        Self {
            event: Some(event.into()),
            channel: Some(channel.into()),
            data: Some(MessageData::String(data.to_string())),
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }

    pub fn member_added(channel: String, user_id: String, user_info: Option<Value>) -> Self {
        Self {
            event: Some("pusher_internal:member_added".to_string()),
            channel: Some(channel),
            // FIX: Use MessageData::String with JSON-encoded string instead of MessageData::Json
            data: Some(MessageData::String(
                json!({
                    "user_id": user_id,
                    "user_info": user_info.unwrap_or_else(|| json!({}))
                })
                .to_string(),
            )),
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }

    pub fn member_removed(channel: String, user_id: String) -> Self {
        Self {
            event: Some("pusher_internal:member_removed".to_string()),
            channel: Some(channel),
            // FIX: Also apply same fix to member_removed for consistency
            data: Some(MessageData::String(
                json!({
                    "user_id": user_id
                })
                .to_string(),
            )),
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }

    // New helper method for pong response
    pub fn pong() -> Self {
        Self {
            event: Some("pusher:pong".to_string()),
            data: None,
            channel: None,
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
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
                "ttl": ttl.as_secs()
            });
        }

        response
    }

    // Helper for creating channels list response
    pub fn channels_list(channels_info: AHashMap<String, Value>) -> Value {
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

    pub fn watchlist_online_event(user_ids: Vec<String>) -> Self {
        Self {
            event: Some("online".to_string()),
            channel: None, // Watchlist events don't use channels
            name: None,
            data: Some(MessageData::Json(json!({
                "user_ids": user_ids
            }))),
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }

    pub fn watchlist_offline_event(user_ids: Vec<String>) -> Self {
        Self {
            event: Some("offline".to_string()),
            channel: None,
            name: None,
            data: Some(MessageData::Json(json!({
                "user_ids": user_ids
            }))),
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }

    pub fn cache_miss_event(channel: String) -> Self {
        Self {
            event: Some("pusher:cache_miss".to_string()),
            channel: Some(channel),
            data: Some(MessageData::String("{}".to_string())),
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }

    pub fn signin_success(user_data: String) -> Self {
        Self {
            event: Some("pusher:signin_success".to_string()),
            data: Some(MessageData::Json(json!({
                "user_data": user_data
            }))),
            channel: None,
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }

    /// Create a delta-compressed message
    pub fn delta_message(
        channel: String,
        event: String,
        delta_base64: String,
        base_sequence: u32,
        target_sequence: u32,
        algorithm: &str,
    ) -> Self {
        Self {
            event: Some("pusher:delta".to_string()),
            channel: Some(channel.clone()),
            data: Some(MessageData::String(
                json!({
                    "channel": channel,
                    "event": event,
                    "delta": delta_base64,
                    "base_seq": base_sequence,
                    "target_seq": target_sequence,
                    "algorithm": algorithm,
                })
                .to_string(),
            )),
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
    }

    /// Add base sequence marker to a full message for delta tracking
    pub fn add_base_sequence(mut self, base_sequence: u32) -> Self {
        if let Some(MessageData::String(ref data_str)) = self.data
            && let Ok(mut data_obj) = sonic_rs::from_str::<Value>(data_str)
            && let Some(obj) = data_obj.as_object_mut()
        {
            obj.insert("__delta_base_seq", json!(base_sequence));
            self.data = Some(MessageData::String(data_obj.to_string()));
        }
        self
    }

    /// Create delta compression enabled confirmation
    pub fn delta_compression_enabled(default_algorithm: &str) -> Self {
        Self {
            event: Some("pusher:delta_compression_enabled".to_string()),
            data: Some(MessageData::Json(json!({
                "enabled": true,
                "default_algorithm": default_algorithm,
            }))),
            channel: None,
            name: None,
            user_id: None,
            sequence: None,
            conflation_key: None,
            tags: None,
        }
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
