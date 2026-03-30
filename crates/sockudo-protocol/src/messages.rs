use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::protocol_version::ProtocolVersion;

/// Allowed value types for extras.headers.
/// Flat only — no Object or Array variant so nesting is structurally impossible.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ExtrasValue {
    String(String),
    Number(f64),
    Bool(bool),
}

/// Structured metadata envelope for V2-specific message features.
///
/// Present on the wire for V2 connections only. V1 connections receive messages
/// with extras stripped entirely. Pusher SDKs ignore unknown fields so the
/// field is safe to carry through internal pipelines even when the publisher
/// is V1.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MessageExtras {
    /// Flat metadata for server-side event name filtering.
    /// Must be a flat object — no nested objects, no arrays.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, ExtrasValue>>,

    /// If true: skip connection recovery buffer and webhook forwarding.
    /// Deliver to currently connected V2 subscribers only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,

    /// Server-side deduplication key. If the same key arrives again within
    /// the app's idempotency TTL window, the message is silently dropped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,

    /// Per-message echo control. Overrides the connection-level echo setting
    /// when explicitly set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub echo: Option<bool>,
}

impl MessageExtras {
    /// Validate that headers (if present) contain only flat scalar values.
    /// This is structurally guaranteed by `ExtrasValue` having no Object/Array
    /// variants, but this method provides an explicit check with a clear error
    /// when validating raw JSON before deserialization.
    pub fn validate_headers_from_json(raw: &Value) -> Result<(), String> {
        if let Some(extras) = raw.get("extras")
            && let Some(headers) = extras.get("headers")
            && let Some(obj) = headers.as_object()
        {
            for (key, val) in obj.iter() {
                if val.is_object() || val.is_array() {
                    return Err(format!(
                        "extras.headers must be a flat object — nested objects and arrays are not allowed (key: '{key}')"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Generate a unique message ID (UUIDv4) for client-side deduplication.
pub fn generate_message_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

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
    /// Unique message ID for client-side deduplication
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// Monotonically increasing serial for connection recovery.
    /// Assigned per-channel at broadcast time when connection recovery is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial: Option<u64>,
    /// Idempotency key for cross-region deduplication.
    /// Threaded from the HTTP publish request through the broadcast pipeline
    /// so that receiving nodes can register it in their local cache.
    /// Never sent to WebSocket clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// V2 message extras envelope. Carries ephemeral flag, per-message echo
    /// control, header-based filtering metadata, and extras-level idempotency.
    /// Stripped from V1 deliveries; included in V2 wire format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extras: Option<MessageExtras>,
    /// Delta sequence marker for full messages in V2 delta streams.
    #[serde(rename = "__delta_seq", skip_serializing_if = "Option::is_none")]
    pub delta_sequence: Option<u64>,
    /// Delta conflation key marker for full messages in V2 delta streams.
    #[serde(rename = "__conflation_key", skip_serializing_if = "Option::is_none")]
    pub delta_conflation_key: Option<String>,
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
    /// Idempotency key for deduplicating publish requests.
    /// If the same key is seen within the TTL window, the server returns the
    /// cached response without re-broadcasting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// V2 extras envelope. Passed through to PusherMessage for V2 delivery.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extras: Option<MessageExtras>,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        }
    }

    /// Rewrite the event name prefix to match the given protocol version.
    /// This is the single translation point between V1 (`pusher:`) and V2 (`sockudo:`) wire formats.
    pub fn rewrite_prefix(&mut self, version: ProtocolVersion) {
        if let Some(ref event) = self.event {
            self.event = Some(version.rewrite_event_prefix(event));
        }
    }

    /// Returns true if this message is ephemeral (skip recovery buffer and webhooks).
    pub fn is_ephemeral(&self) -> bool {
        self.extras
            .as_ref()
            .and_then(|e| e.ephemeral)
            .unwrap_or(false)
    }

    /// Returns the extras-level idempotency key, if set.
    pub fn extras_idempotency_key(&self) -> Option<&str> {
        self.extras
            .as_ref()
            .and_then(|e| e.idempotency_key.as_deref())
    }

    /// Resolve whether this message should be echoed back to the publishing socket.
    /// Message-level `extras.echo` takes precedence over the connection default.
    pub fn should_echo(&self, connection_default: bool) -> bool {
        self.extras
            .as_ref()
            .and_then(|e| e.echo)
            .unwrap_or(connection_default)
    }

    /// Returns the extras headers for server-side filtering, if present.
    pub fn filter_headers(&self) -> Option<&HashMap<String, ExtrasValue>> {
        self.extras.as_ref().and_then(|e| e.headers.as_ref())
    }

    /// Returns true if the given protocol version should receive extras in delivered messages.
    pub fn should_include_extras(protocol: &ProtocolVersion) -> bool {
        matches!(protocol, ProtocolVersion::V2)
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
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
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
