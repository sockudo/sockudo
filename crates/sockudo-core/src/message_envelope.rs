//! Typed facts captured when a message is committed.
//!
//! The envelope is deliberately protocol-neutral.  Wire projections may omit
//! fields, but durable storage and egress adapters must not reconstruct these
//! facts from a serialized `PusherMessage`.

use crate::versioned_messages::{MessageAction, MessageSerial, VersionSerial};
use serde::{Deserialize, Serialize};
use sockudo_protocol::messages::{MessageData, MessageExtras};
use sonic_rs::Value;
use std::collections::BTreeMap;

/// The complete encoding chain supplied with a message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(transparent)]
pub struct EncodingChain(pub String);

impl EncodingChain {
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty()).then_some(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Content retained independently of the wire codec used for delivery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MessageContent {
    Text(String),
    Structured(Value),
    Binary(Vec<u8>),
}

impl MessageContent {
    pub fn from_message_data(data: &MessageData) -> Result<Self, String> {
        match data {
            MessageData::String(value) => Ok(Self::Text(value.clone())),
            MessageData::Structured { .. } => sonic_rs::to_value(data)
                .map(Self::Structured)
                .map_err(|error| format!("failed to encode structured message data: {error}")),
            MessageData::Json(value) => Ok(Self::Structured(value.clone())),
        }
    }
}

/// Projection of a committed version for compatibility consumers.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VersionProjection {
    AppendFragment,
    Aggregate,
}

/// Operation metadata committed with one version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VersionOperationMetadata {
    pub serial: VersionSerial,
    pub timestamp_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    pub projection: VersionProjection,
}

/// Canonical publish/delivery facts shared by native and compatibility paths.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct MessageEnvelope {
    pub message_id: Option<String>,
    pub acknowledgement_id: Option<String>,
    pub name: Option<String>,
    pub data: Option<MessageContent>,
    pub encoding: Option<EncodingChain>,
    pub publisher_client_id: Option<String>,
    pub publisher_socket_id: Option<String>,
    pub publisher_connection_id: Option<String>,
    pub published_at_ms: Option<i64>,
    pub extras: Option<MessageExtras>,
    pub stream_id: Option<String>,
    pub history_serial: Option<u64>,
    pub delivery_serial: Option<u64>,
    pub action: Option<MessageAction>,
    pub message_serial: Option<MessageSerial>,
    pub version: Option<VersionOperationMetadata>,
}

impl MessageEnvelope {
    /// Build the protocol-neutral portion available before positions are assigned.
    pub fn from_message(
        message: &sockudo_protocol::messages::PusherMessage,
        publisher_socket_id: Option<String>,
        publisher_connection_id: Option<String>,
        published_at_ms: i64,
    ) -> Result<Self, String> {
        Ok(Self {
            message_id: message.message_id.clone(),
            acknowledgement_id: None,
            name: message.name.clone().or_else(|| message.event.clone()),
            data: message
                .data
                .as_ref()
                .map(Self::content_from_message_data)
                .transpose()?,
            encoding: None,
            publisher_client_id: message.user_id.clone(),
            publisher_socket_id,
            publisher_connection_id,
            published_at_ms: Some(published_at_ms),
            extras: message.extras.clone(),
            stream_id: message.stream_id.clone(),
            history_serial: None,
            delivery_serial: message.serial,
            action: None,
            message_serial: None,
            version: None,
        })
    }

    fn content_from_message_data(data: &MessageData) -> Result<MessageContent, String> {
        MessageContent::from_message_data(data)
    }

    pub fn set_commit_positions(
        &mut self,
        stream_id: Option<String>,
        history_serial: Option<u64>,
        delivery_serial: Option<u64>,
    ) {
        if stream_id.is_some() {
            self.stream_id = stream_id;
        }
        self.history_serial = history_serial;
        self.delivery_serial = delivery_serial;
    }
}

/// Envelope wrapper used for new history payloads.  `envelope` is optional so
/// payloads written before the envelope migration remain readable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StoredMessagePayload {
    pub message: sockudo_protocol::messages::PusherMessage,
    pub envelope: Option<MessageEnvelope>,
}

/// Decode a new envelope-bearing history payload while retaining compatibility
/// with payloads written before the envelope migration.
pub fn decode_stored_message_payload(bytes: &[u8]) -> Result<StoredMessagePayload, String> {
    match sonic_rs::from_slice::<StoredMessagePayload>(bytes) {
        Ok(payload) if payload.message.channel.is_some() || payload.message.event.is_some() => {
            Ok(payload)
        }
        _ => sonic_rs::from_slice::<sockudo_protocol::messages::PusherMessage>(bytes)
            .map(|message| StoredMessagePayload {
                message,
                envelope: None,
            })
            .map_err(|error| error.to_string()),
    }
}

impl StoredMessagePayload {
    pub fn new(
        message: sockudo_protocol::messages::PusherMessage,
        envelope: MessageEnvelope,
    ) -> Self {
        Self {
            message,
            envelope: Some(envelope),
        }
    }
}

impl Default for StoredMessagePayload {
    fn default() -> Self {
        Self {
            message: sockudo_protocol::messages::PusherMessage {
                event: None,
                channel: None,
                data: None,
                name: None,
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: None,
                serial: None,
                idempotency_key: None,
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
            envelope: None,
        }
    }
}

/// Opaque compatibility facts that are intentionally not native fields.
pub type OpaqueMessageExtensions = BTreeMap<String, serde_json::Value>;

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_protocol::messages::{MessageData, PusherMessage};

    fn message() -> PusherMessage {
        PusherMessage {
            event: Some("message.create".to_string()),
            channel: Some("golden".to_string()),
            data: Some(MessageData::String("ciphertext".to_string())),
            name: Some("event".to_string()),
            user_id: Some("publisher".to_string()),
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: Some("client-message-id".to_string()),
            stream_id: Some("stream-1".to_string()),
            serial: Some(7),
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        }
    }

    #[test]
    fn envelope_round_trip_preserves_typed_positions_and_binary_content() {
        let mut extras = MessageExtras::default();
        extras
            .insert_opaque("ref", serde_json::json!("client-ref"))
            .expect("supported extension");
        let mut envelope = MessageEnvelope::from_message(
            &message(),
            Some("socket-1".to_string()),
            Some("connection-1".to_string()),
            123,
        )
        .expect("envelope");
        envelope.encoding = EncodingChain::new("cipher/base64");
        envelope.extras = Some(extras);
        envelope.data = Some(MessageContent::Binary(vec![0, 1, 255]));
        envelope.delivery_serial = Some(7);
        envelope.history_serial = Some(6);
        envelope.message_serial = Some(MessageSerial::new("message-serial").expect("serial"));
        envelope.version = Some(VersionOperationMetadata {
            serial: VersionSerial::new("version-serial").expect("serial"),
            timestamp_ms: 123,
            client_id: Some("publisher".to_string()),
            description: Some("append fragment".to_string()),
            metadata: Some(sonic_rs::json!({"source": "test"})),
            projection: VersionProjection::AppendFragment,
        });

        let bytes = sonic_rs::to_vec(&envelope).expect("serialize");
        let decoded: MessageEnvelope = sonic_rs::from_slice(&bytes).expect("deserialize");
        assert_eq!(decoded, envelope);
        assert_eq!(decoded.delivery_serial, Some(7));
        assert_eq!(
            decoded.extras.as_ref().and_then(|e| e.opaque_value("ref")),
            Some(&serde_json::json!("client-ref"))
        );
    }

    #[test]
    fn history_payload_reads_legacy_and_new_shapes() {
        let legacy = sonic_rs::to_vec(&message()).expect("serialize legacy");
        let decoded = decode_stored_message_payload(&legacy).expect("legacy decode");
        assert!(decoded.envelope.is_none());
        assert_eq!(decoded.message, message());

        let envelope = MessageEnvelope::from_message(&message(), None, None, 1).expect("envelope");
        let payload = StoredMessagePayload::new(message(), envelope);
        let bytes = sonic_rs::to_vec(&payload).expect("serialize new");
        assert!(
            decode_stored_message_payload(&bytes)
                .expect("new decode")
                .envelope
                .is_some()
        );
    }
}
