// Delta message and cached-message types plus protocol message builders.

use serde::{Deserialize, Serialize};
use sockudo_core::delta_types::DeltaAlgorithm;
use sonic_rs::Value;
use std::sync::Arc;
use std::time::Instant;

/// Delta-compressed message format sent to clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaMessage {
    /// The delta payload (base64 encoded for JSON transport)
    pub delta: String,
    /// Sequence number for ordering
    pub seq: u32,
    /// Original event name
    pub event: String,
    /// Channel name
    pub channel: String,
    /// Algorithm used for delta compression
    pub algorithm: String,
}

/// Cached message for delta compression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedMessage {
    /// Message content (full message) - Arc to avoid cloning large messages
    #[serde(
        serialize_with = "serialize_arc_vec",
        deserialize_with = "deserialize_arc_vec"
    )]
    pub content: Arc<Vec<u8>>,
    /// Sequence number for this message
    pub sequence: u32,
    /// Timestamp when message was cached (not serialized)
    #[serde(skip, default = "Instant::now")]
    pub timestamp: Instant,
}

fn serialize_arc_vec<S>(arc: &Arc<Vec<u8>>, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_bytes(arc.as_ref())
}

fn deserialize_arc_vec<'de, D>(deserializer: D) -> std::result::Result<Arc<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let vec = Vec::<u8>::deserialize(deserializer)?;
    Ok(Arc::new(vec))
}

impl CachedMessage {
    pub(crate) fn new(content: Vec<u8>, sequence: u32) -> Self {
        Self {
            content: Arc::new(content),
            sequence,
            timestamp: Instant::now(),
        }
    }
}

/// Create a delta-compressed Pusher protocol message with conflation support
pub fn create_delta_message(
    channel: &str,
    event: &str,
    delta: Vec<u8>,
    sequence: u32,
    algorithm: Option<DeltaAlgorithm>,
    conflation_key: Option<&str>,
    base_index: Option<usize>,
) -> Value {
    let delta_base64 = base64_encode(&delta);

    let mut data = sonic_rs::json!({
        "event": event,
        "delta": delta_base64,
        "seq": sequence,
    });

    if let Some(algo) = algorithm {
        let algo_str = match algo {
            DeltaAlgorithm::Fossil => "fossil",
            DeltaAlgorithm::Xdelta3 => "xdelta3",
        };
        data["algorithm"] = sonic_rs::json!(algo_str);
    }

    if let Some(key) = conflation_key {
        data["conflation_key"] = sonic_rs::json!(key);
    }
    if let Some(idx) = base_index {
        data["base_index"] = sonic_rs::json!(idx);
    }

    sonic_rs::json!({
        "event": "pusher:delta",
        "channel": channel,
        "data": data.to_string()
    })
}

/// Create a cache sync message to send to clients on subscription
pub fn create_cache_sync_message(
    channel: &str,
    conflation_key: Option<&str>,
    max_messages_per_key: usize,
    caches: Vec<(String, Vec<CachedMessage>)>,
) -> Value {
    let mut states = sonic_rs::Object::new();

    for (key, messages) in caches {
        let messages_json: Vec<Value> = messages
            .iter()
            .rev()
            .take(1)
            .map(|msg| {
                sonic_rs::json!({
                    "content": String::from_utf8_lossy(&msg.content),
                    "seq": msg.sequence
                })
            })
            .collect();

        if !messages_json.is_empty() {
            let client_key = if let Some(colon_pos) = key.find(':') {
                key[colon_pos + 1..].to_string()
            } else {
                key
            };
            states.insert(&client_key, sonic_rs::json!(messages_json));
        }
    }

    let mut data = sonic_rs::json!({
        "max_messages_per_key": max_messages_per_key,
        "states": states,
    });

    if let Some(key) = conflation_key {
        data["conflation_key"] = sonic_rs::json!(key);
    }

    sonic_rs::json!({
        "event": "pusher:delta_cache_sync",
        "channel": channel,
        "data": data.to_string()
    })
}

/// Create a full message with sequence number for delta tracking
pub fn create_full_message_with_seq(
    channel: &str,
    event: &str,
    data: &str,
    sequence: u32,
) -> Value {
    sonic_rs::json!({
        "event": event,
        "channel": channel,
        "data": data,
        "__delta_seq": sequence,
    })
}

/// Base64 encode delta for JSON transport
fn base64_encode(data: &[u8]) -> String {
    use base64::{Engine as _, engine::general_purpose};
    general_purpose::STANDARD.encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_base64_encode() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b"world"), "d29ybGQ=");
        assert_eq!(base64_encode(b""), "");
    }
}
