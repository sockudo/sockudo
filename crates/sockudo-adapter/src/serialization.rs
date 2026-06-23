// crates/sockudo-adapter/src/serialization.rs

//! Serialization helpers for horizontal adapter pub/sub payloads.
//!
//! Publish-side format is controlled by the `serialization` config field.
//! Receive-side auto-detects JSON vs MessagePack from the first byte so that
//! mixed-version clusters work safely during rolling deploys.

use serde::Serialize;
use serde::de::DeserializeOwned;
use sockudo_core::error::{Error, Result};

/// Deserialize an `Option<sonic_rs::Value>` from any serde format.
///
/// `sonic_rs::Value` only deserializes from JSON tokens. For MessagePack
/// compatibility, we deserialize through `serde_json::Value` (which accepts
/// any format) and convert via JSON bytes. The cost is negligible on the
/// small `user_info` payloads this is used for.
pub(crate) fn compat_value<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<sonic_rs::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let fallback: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match fallback {
        None => Ok(None),
        Some(v) => {
            let bytes = serde_json::to_vec(&v).map_err(serde::de::Error::custom)?;
            let sonic = sonic_rs::from_slice(&bytes).map_err(serde::de::Error::custom)?;
            Ok(Some(sonic))
        }
    }
}

/// Same as [`compat_value`] but wraps the result in `Arc`.
pub(crate) fn compat_arc_value<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<std::sync::Arc<sonic_rs::Value>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    compat_value(deserializer).map(|opt| opt.map(std::sync::Arc::new))
}

/// Adapter serialization format for inter-node pub/sub payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Serialization {
    #[default]
    Json,
    MessagePack,
}

impl Serialization {
    /// Parse from config string. Unrecognized values fall back to JSON.
    pub fn from_config(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "msgpack" | "messagepack" => Self::MessagePack,
            _ => Self::Json,
        }
    }
}

/// Serialize a value using the configured format.
pub(crate) fn serialize<T: Serialize>(value: &T, format: Serialization) -> Result<Vec<u8>> {
    match format {
        Serialization::Json => sonic_rs::to_vec(value)
            .map_err(|e| Error::Other(format!("JSON serialization failed: {e}"))),
        Serialization::MessagePack => rmp_serde::to_vec_named(value)
            .map_err(|e| Error::Other(format!("MessagePack serialization failed: {e}"))),
    }
}

/// Deserialize a value, auto-detecting JSON vs MessagePack from the first byte.
///
/// JSON objects start with `{` (0x7B). MessagePack maps start with fixmap
/// (0x80–0x8F), map16 (0xDE), or map32 (0xDF). All adapter message types
/// serialize as maps, so the first byte is always unambiguous.
pub(crate) fn deserialize<T: DeserializeOwned>(data: &[u8]) -> Result<T> {
    if data.is_empty() {
        return Err(Error::Other("Empty payload".to_string()));
    }

    match data[0] {
        // MessagePack: fixmap (0x80-0x8F), map16 (0xDE), map32 (0xDF)
        0x80..=0x8F | 0xDE | 0xDF => rmp_serde::from_slice(data)
            .map_err(|e| Error::Other(format!("MessagePack deserialization failed: {e}"))),
        // Everything else: JSON (0x7B = '{', 0x5B = '[', or edge cases)
        _ => sonic_rs::from_slice(data)
            .map_err(|e| Error::Other(format!("JSON deserialization failed: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestMessage {
        request_id: String,
        node_id: String,
        app_id: String,
        count: u64,
    }

    fn sample() -> TestMessage {
        TestMessage {
            request_id: "req-123".to_string(),
            node_id: "node-1".to_string(),
            app_id: "app-1".to_string(),
            count: 42,
        }
    }

    #[test]
    fn json_roundtrip() {
        let msg = sample();
        let bytes = serialize(&msg, Serialization::Json).unwrap();
        assert_eq!(bytes[0], b'{'); // JSON object
        let decoded: TestMessage = deserialize(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn msgpack_roundtrip() {
        let msg = sample();
        let bytes = serialize(&msg, Serialization::MessagePack).unwrap();
        assert!(matches!(bytes[0], 0x80..=0x8F | 0xDE | 0xDF)); // MessagePack map
        let decoded: TestMessage = deserialize(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn cross_format_msgpack_to_auto() {
        // Serialize as MessagePack, deserialize with auto-detection
        let msg = sample();
        let bytes = serialize(&msg, Serialization::MessagePack).unwrap();
        let decoded: TestMessage = deserialize(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn cross_format_json_to_auto() {
        // Serialize as JSON, deserialize with auto-detection
        let msg = sample();
        let bytes = serialize(&msg, Serialization::Json).unwrap();
        let decoded: TestMessage = deserialize(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn msgpack_smaller_than_json() {
        let msg = sample();
        let json_bytes = serialize(&msg, Serialization::Json).unwrap();
        let msgpack_bytes = serialize(&msg, Serialization::MessagePack).unwrap();
        assert!(
            msgpack_bytes.len() < json_bytes.len(),
            "MessagePack ({} bytes) should be smaller than JSON ({} bytes)",
            msgpack_bytes.len(),
            json_bytes.len()
        );
    }

    #[test]
    fn empty_payload_error() {
        let result = deserialize::<TestMessage>(b"");
        assert!(result.is_err());
    }

    #[test]
    fn msgpack_roundtrip_request_body_with_user_info() {
        use crate::horizontal_adapter::{RequestBody, RequestType};

        let request = RequestBody {
            request_id: "req-abc".to_string(),
            node_id: "node-1".to_string(),
            app_id: "app-1".to_string(),
            request_type: RequestType::PresenceMemberJoined,
            channel: Some("presence-chat".to_string()),
            socket_id: Some("sock-1".to_string()),
            user_id: Some("user-42".to_string()),
            user_info: Some(sonic_rs::json!({
                "name": "Alice",
                "avatar": "https://example.com/alice.png",
                "status": "online",
                "nested": {"level": 2, "tags": ["admin", "vip"]}
            })),
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
            reply_to: None,
            channels: None,
        };

        let bytes = serialize(&request, Serialization::MessagePack).unwrap();
        assert!(matches!(bytes[0], 0x80..=0x8F | 0xDE | 0xDF));
        let decoded: RequestBody = deserialize(&bytes).unwrap();
        assert_eq!(decoded.request_id, "req-abc");
        assert_eq!(decoded.user_info, request.user_info);

        let json_bytes = serialize(&request, Serialization::Json).unwrap();
        let decoded_json: RequestBody = deserialize(&json_bytes).unwrap();
        assert_eq!(decoded_json.user_info, request.user_info);
    }

    #[test]
    fn msgpack_roundtrip_request_body_user_info_none() {
        use crate::horizontal_adapter::{RequestBody, RequestType};

        let request = RequestBody {
            request_id: "req-none".to_string(),
            node_id: "node-1".to_string(),
            app_id: "app-1".to_string(),
            request_type: RequestType::Heartbeat,
            channel: None,
            socket_id: None,
            user_id: None,
            user_info: None,
            timestamp: Some(1234567890),
            dead_node_id: None,
            target_node_id: None,
            reply_to: None,
            channels: None,
        };

        let bytes = serialize(&request, Serialization::MessagePack).unwrap();
        let decoded: RequestBody = deserialize(&bytes).unwrap();
        assert!(decoded.user_info.is_none());
        assert_eq!(decoded.timestamp, Some(1234567890));
    }

    #[test]
    fn msgpack_roundtrip_response_body_with_presence_members() {
        use crate::horizontal_adapter::ResponseBody;
        use sockudo_core::channel::PresenceMemberInfo;

        let mut members = ahash::AHashMap::new();
        members.insert(
            "user-1".to_string(),
            PresenceMemberInfo {
                user_id: "user-1".to_string(),
                user_info: Some(sonic_rs::json!({
                    "name": "Bob",
                    "role": "moderator"
                })),
            },
        );
        members.insert(
            "user-2".to_string(),
            PresenceMemberInfo {
                user_id: "user-2".to_string(),
                user_info: None,
            },
        );

        let response = ResponseBody {
            request_id: "req-resp".to_string(),
            node_id: "node-2".to_string(),
            app_id: "app-1".to_string(),
            members,
            channels_with_sockets_count: ahash::AHashMap::new(),
            socket_ids: vec!["sock-1".to_string()],
            sockets_count: 1,
            exists: true,
            channels: std::collections::HashSet::new(),
            members_count: 2,
            responses_received: 1,
            expected_responses: 3,
            complete: false,
        };

        let bytes = serialize(&response, Serialization::MessagePack).unwrap();
        assert!(matches!(bytes[0], 0x80..=0x8F | 0xDE | 0xDF));
        let decoded: ResponseBody = deserialize(&bytes).unwrap();
        assert_eq!(decoded.members.len(), 2);
        let bob = decoded.members.get("user-1").unwrap();
        assert_eq!(bob.user_id, "user-1");
        assert!(bob.user_info.is_some());

        let json_bytes = serialize(&response, Serialization::Json).unwrap();
        let decoded_json: ResponseBody = deserialize(&json_bytes).unwrap();
        assert_eq!(decoded_json.members.len(), 2);
    }

    #[test]
    fn msgpack_roundtrip_channel_count_gossip() {
        use crate::horizontal_adapter::{RequestBody, RequestType};

        let counts = sonic_rs::json!({
            "app-1": {"presence-lobby": 5, "private-chat": 3},
            "app-2": {"public-feed": 12}
        });

        let request = RequestBody {
            request_id: "gossip-1".to_string(),
            node_id: "node-3".to_string(),
            app_id: "app-1".to_string(),
            request_type: RequestType::ChannelCountUpdate,
            channel: None,
            socket_id: None,
            user_id: None,
            user_info: Some(counts.clone()),
            timestamp: Some(9999),
            dead_node_id: None,
            target_node_id: None,
            reply_to: None,
            channels: None,
        };

        let bytes = serialize(&request, Serialization::MessagePack).unwrap();
        let decoded: RequestBody = deserialize(&bytes).unwrap();
        assert_eq!(decoded.user_info.unwrap(), counts);
    }

    #[test]
    fn from_config_parsing() {
        assert_eq!(
            Serialization::from_config("msgpack"),
            Serialization::MessagePack
        );
        assert_eq!(
            Serialization::from_config("messagepack"),
            Serialization::MessagePack
        );
        assert_eq!(
            Serialization::from_config("MsgPack"),
            Serialization::MessagePack
        );
        assert_eq!(
            Serialization::from_config("json"),
            Serialization::Json
        );
        assert_eq!(
            Serialization::from_config("unknown"),
            Serialization::Json
        );
    }
}
