//! Opaque fixtures that let Criterion exercise production compatibility paths.
//!
//! This module is deliberately feature-gated. It exposes no protocol DTOs and
//! does not provide an alternate implementation: every operation delegates to
//! the codec, projection, subscriber registry, egress tap, and bounded outbound
//! queues used by the server.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::Serialize;
use sockudo_adapter::handler::RealtimeEgressTap;
use sockudo_core::{
    message_envelope::{EncodingChain, MessageContent, MessageEnvelope},
    options::AblyCompatConfig,
    presence_registry::PresenceRegistry,
    versioned_messages::{MessageAction, MessageSerial},
};
use sockudo_protocol::messages::{MessageData, PusherMessage};
use std::{collections::HashSet, sync::Arc};

use crate::{
    AblyCompatDependencies, AblyCompatRuntime,
    codec::{decode_protocol_bytes, encode_protocol_bytes},
    outbound::AblyOutboundReceiver,
    protocol::{
        ACTION_ERROR, ACTION_MESSAGE, AblyErrorInfo, AblyFormat, AblyMessageProjection,
        AblyProtocolMessage, empty_protocol_message,
    },
    runtime::benchmark_project_envelope,
};

const APP_ID: &str = "benchmark-app";
const CHANNEL: &str = "benchmark:channel";

/// Wire formats supported by the compatibility facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BenchFormat {
    Json,
    MsgPack,
}

impl BenchFormat {
    fn wire(self) -> AblyFormat {
        match self {
            Self::Json => AblyFormat::Json,
            Self::MsgPack => AblyFormat::MsgPack,
        }
    }

    /// Stable lowercase name used in benchmark identifiers.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::MsgPack => "msgpack",
        }
    }
}

/// An opaque production protocol frame.
pub struct BenchFrame(AblyProtocolMessage);

impl BenchFrame {
    /// Encode through the production JSON or MessagePack codec.
    pub fn encode(&self, format: BenchFormat) -> Result<Bytes, String> {
        encode_protocol_bytes(&self.0, format.wire())
    }

    /// Touch the projected payload without serializing it again.
    #[must_use]
    pub fn projected_payload_len(&self) -> usize {
        self.0
            .messages
            .as_ref()
            .and_then(|messages| messages.first())
            .and_then(|message| message.data.as_ref())
            .map_or(0, |data| data.to_string().len())
    }
}

#[derive(Serialize)]
struct BinaryFrame<'a> {
    action: u8,
    messages: [BinaryMessage<'a>; 1],
}

#[derive(Serialize)]
struct BinaryMessage<'a> {
    name: &'static str,
    data: &'a [u8],
    encoding: &'static str,
}

#[derive(Serialize)]
struct JsonFrame<'a> {
    action: u8,
    messages: [JsonMessage<'a>; 1],
}

#[derive(Serialize)]
struct JsonMessage<'a> {
    name: &'static str,
    data: &'a str,
    encoding: &'static str,
}

/// Reusable JSON and binary MessagePack decode inputs.
pub struct CodecFixture {
    json: Bytes,
    msgpack: Bytes,
}

impl CodecFixture {
    /// Build both wire-format inputs from the same-sized encrypted value.
    pub fn encrypted(payload_bytes: usize) -> Result<Self, String> {
        let payload = vec![0xa5; payload_bytes];
        let encoded_payload = STANDARD.encode(&payload);
        let json_frame = JsonFrame {
            action: ACTION_MESSAGE,
            messages: [JsonMessage {
                name: "encrypted",
                data: &encoded_payload,
                encoding: "cipher+aes-256-cbc/base64",
            }],
        };
        let json = sonic_rs::to_vec(&json_frame)
            .map(Bytes::from)
            .map_err(|error| error.to_string())?;
        let msgpack_frame = BinaryFrame {
            action: ACTION_MESSAGE,
            messages: [BinaryMessage {
                name: "encrypted",
                data: &payload,
                encoding: "cipher+aes-256-cbc",
            }],
        };
        let mut msgpack = Vec::with_capacity(payload_bytes.saturating_add(128));
        let mut serializer = rmp_serde::Serializer::new(&mut msgpack)
            .with_struct_map()
            .with_bytes(rmp_serde::config::BytesMode::ForceAll);
        msgpack_frame
            .serialize(&mut serializer)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            json,
            msgpack: Bytes::from(msgpack),
        })
    }

    /// Raw encoded input for throughput accounting.
    #[must_use]
    pub fn encoded(&self, format: BenchFormat) -> &[u8] {
        match format {
            BenchFormat::Json => &self.json,
            BenchFormat::MsgPack => &self.msgpack,
        }
    }

    /// Decode through the exact production codec.
    pub fn decode(&self, format: BenchFormat) -> Result<BenchFrame, String> {
        decode_protocol_bytes(self.encoded(format), format.wire()).map(BenchFrame)
    }
}

/// Commit-time facts used by realtime and history projections.
pub struct ProjectionFixture {
    message: PusherMessage,
    envelope: MessageEnvelope,
}

impl ProjectionFixture {
    /// Build an encrypted binary payload with the compatibility encoding chain.
    #[must_use]
    pub fn encrypted(payload: Vec<u8>) -> Self {
        let message = pusher_message(MessageData::Binary(payload.clone()), 1);
        let envelope = MessageEnvelope {
            message_id: Some("benchmark-message".to_string()),
            name: Some("encrypted".to_string()),
            data: Some(MessageContent::Binary(payload)),
            encoding: EncodingChain::new("cipher+aes-256-cbc"),
            publisher_client_id: Some("benchmark-client".to_string()),
            publisher_connection_id: Some("benchmark-connection".to_string()),
            published_at_ms: Some(1_700_000_000_000),
            stream_id: Some("benchmark-stream".to_string()),
            history_serial: Some(1),
            delivery_serial: Some(1),
            action: Some(MessageAction::Create),
            message_serial: MessageSerial::new("benchmark-message").ok(),
            ..MessageEnvelope::default()
        };
        Self { message, envelope }
    }

    /// Project through the deterministic commit-envelope conversion.
    pub fn project(&self) -> Result<BenchFrame, String> {
        benchmark_project_envelope(
            CHANNEL,
            &self.message,
            &self.envelope,
            AblyMessageProjection::Mutation,
            None,
        )
        .map(BenchFrame)
    }
}

/// A production-shaped Ably error frame.
#[must_use]
pub fn error_frame() -> BenchFrame {
    BenchFrame(AblyProtocolMessage {
        action: ACTION_ERROR,
        error: Some(AblyErrorInfo {
            message: "benchmark compatibility error".to_string(),
            code: 50000,
            status_code: 500,
        }),
        ..empty_protocol_message(ACTION_ERROR)
    })
}

/// Bounded history/recovery fixture using the production envelope projection.
pub struct HistoryFixture {
    records: Vec<(PusherMessage, MessageEnvelope)>,
}

impl HistoryFixture {
    /// Build a retained sequence with stable stream and delivery serials.
    #[must_use]
    pub fn new(message_count: usize, payload_bytes: usize) -> Self {
        let payload = "x".repeat(payload_bytes);
        let records = (1..=message_count)
            .map(|serial| {
                let message = pusher_message(MessageData::String(payload.clone()), serial as u64);
                let message_serial = format!("benchmark-message-{serial}");
                let envelope = MessageEnvelope {
                    message_id: Some(message_serial.clone()),
                    name: Some("history".to_string()),
                    data: Some(MessageContent::Text(payload.clone())),
                    publisher_client_id: Some("benchmark-client".to_string()),
                    published_at_ms: Some(1_700_000_000_000 + serial as i64),
                    stream_id: Some("benchmark-stream".to_string()),
                    history_serial: Some(serial as u64),
                    delivery_serial: Some(serial as u64),
                    action: Some(MessageAction::Create),
                    message_serial: MessageSerial::new(message_serial).ok(),
                    ..MessageEnvelope::default()
                };
                (message, envelope)
            })
            .collect();
        Self { records }
    }

    /// Project all retained records as REST history aggregates.
    pub fn project_history(&self) -> Result<Vec<BenchFrame>, String> {
        self.records
            .iter()
            .map(|(message, envelope)| {
                benchmark_project_envelope(
                    CHANNEL,
                    message,
                    envelope,
                    AblyMessageProjection::Aggregate,
                    None,
                )
                .map(BenchFrame)
            })
            .collect()
    }

    /// Project all retained records as a realtime recovery replay.
    pub fn project_replay(&self) -> Result<Vec<BenchFrame>, String> {
        self.records
            .iter()
            .enumerate()
            .map(|(index, (message, envelope))| {
                benchmark_project_envelope(
                    CHANNEL,
                    message,
                    envelope,
                    AblyMessageProjection::Mutation,
                    Some(format!("benchmark-stream:{}", index + 1)),
                )
                .map(BenchFrame)
            })
            .collect()
    }
}

/// Compatibility presence at the adapter egress boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityCase {
    Disabled,
    EnabledNoSubscribers,
    JsonOnly,
    MsgPackOnly,
    Mixed,
}

impl CompatibilityCase {
    /// Stable benchmark identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::EnabledNoSubscribers => "enabled_no_subscriber",
            Self::JsonOnly => "json_only",
            Self::MsgPackOnly => "msgpack_only",
            Self::Mixed => "mixed",
        }
    }
}

/// Counters observed across one real registry/fanout/socket delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanoutObservation {
    pub encoded: usize,
    pub fanout: usize,
    pub drained_sockets: usize,
    pub shared_payload_allocations: usize,
}

/// Reusable actual compatibility hub, subscriber registry, and socket queues.
pub struct FanoutFixture {
    tap: Option<Arc<dyn RealtimeEgressTap>>,
    compatibility: Arc<AblyCompatRuntime>,
    receivers: Vec<(BenchFormat, AblyOutboundReceiver)>,
    message: PusherMessage,
    next_serial: u64,
}

impl FanoutFixture {
    /// Register the requested number of real compatibility subscribers.
    pub fn new(case: CompatibilityCase, subscribers: usize) -> Result<Self, String> {
        let config = AblyCompatConfig {
            enabled: case != CompatibilityCase::Disabled,
            ..AblyCompatConfig::default()
        };
        let compatibility = Arc::new(AblyCompatRuntime::new(AblyCompatDependencies {
            config,
            presence_registry: Arc::new(PresenceRegistry::default()),
            ..AblyCompatDependencies::default()
        }));
        let tap = (case != CompatibilityCase::Disabled).then(|| compatibility.egress_tap());
        let mut receivers = Vec::with_capacity(subscribers);
        if matches!(
            case,
            CompatibilityCase::JsonOnly | CompatibilityCase::MsgPackOnly | CompatibilityCase::Mixed
        ) {
            for index in 0..subscribers {
                let format = match case {
                    CompatibilityCase::JsonOnly => BenchFormat::Json,
                    CompatibilityCase::MsgPackOnly => BenchFormat::MsgPack,
                    CompatibilityCase::Mixed if index % 2 == 0 => BenchFormat::Json,
                    CompatibilityCase::Mixed => BenchFormat::MsgPack,
                    CompatibilityCase::Disabled | CompatibilityCase::EnabledNoSubscribers => {
                        unreachable!("subscriber case was checked")
                    }
                };
                let session_id = format!("benchmark-session-{index}");
                let receiver = compatibility.register_benchmark_subscriber(
                    format.wire(),
                    &session_id,
                    CHANNEL,
                );
                receivers.push((format, receiver));
            }
        }

        Ok(Self {
            tap,
            compatibility,
            receivers,
            message: pusher_message(MessageData::String("x".repeat(256)), 1),
            next_serial: 1,
        })
    }

    /// Exercise the same optional-tap/has-subscriber/deliver boundary as the adapter.
    pub fn deliver_and_drain(&mut self) -> Result<FanoutObservation, String> {
        let before = self.compatibility.metrics();
        self.deliver(false)?;
        Ok(self.drain(before))
    }

    /// Deliver while retaining queued frames, modelling a stalled consumer.
    pub fn deliver_without_drain(&mut self) -> Result<FanoutObservation, String> {
        self.deliver(false)?;
        let snapshot = self.compatibility.metrics();
        Ok(FanoutObservation {
            encoded: snapshot.encoded,
            fanout: snapshot.fanout,
            drained_sockets: 0,
            shared_payload_allocations: 0,
        })
    }

    fn deliver(&mut self, _force_full_message: bool) -> Result<(), String> {
        let Some(tap) = self.tap.as_ref() else {
            return Ok(());
        };
        if !tap.has_subscribers(APP_ID, CHANNEL) {
            return Ok(());
        }
        let serial = self.next_serial;
        self.next_serial = self.next_serial.saturating_add(1);
        self.message.serial = Some(serial);
        let envelope = MessageEnvelope {
            message_id: Some(format!("benchmark-{serial}")),
            name: Some("benchmark".to_string()),
            data: Some(MessageContent::Text("x".repeat(256))),
            published_at_ms: Some(1_700_000_000_000 + serial as i64),
            stream_id: Some("benchmark-stream".to_string()),
            history_serial: Some(serial),
            delivery_serial: Some(serial),
            action: Some(MessageAction::Create),
            message_serial: MessageSerial::new(format!("benchmark-{serial}")).ok(),
            ..MessageEnvelope::default()
        };
        tap.deliver(APP_ID, CHANNEL, &self.message, &envelope)
            .map_err(|error| error.to_string())
    }

    fn drain(&mut self, before: crate::OutboundMetricsSnapshot) -> FanoutObservation {
        let mut allocations = HashSet::new();
        let mut drained = 0;
        for (_, receiver) in &mut self.receivers {
            if let Some(frame) = receiver.try_recv() {
                allocations.insert(Arc::as_ptr(&frame.bytes) as usize);
                drained += 1;
            }
        }
        let after = self.compatibility.metrics();
        FanoutObservation {
            encoded: after.encoded.saturating_sub(before.encoded),
            fanout: after.fanout.saturating_sub(before.fanout),
            drained_sockets: drained,
            shared_payload_allocations: allocations.len(),
        }
    }

    /// Current cumulative delivery metrics, including bounded-queue overflow.
    #[must_use]
    pub fn metrics(&self) -> crate::OutboundMetricsSnapshot {
        self.compatibility.metrics()
    }
}

fn pusher_message(data: MessageData, serial: u64) -> PusherMessage {
    PusherMessage {
        event: Some("benchmark".to_string()),
        channel: Some(CHANNEL.to_string()),
        data: Some(data),
        name: Some("benchmark".to_string()),
        user_id: Some("benchmark-client".to_string()),
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: Some(format!("benchmark-{serial}")),
        stream_id: Some("benchmark-stream".to_string()),
        serial: Some(serial),
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_decode_payload_sizes_are_valid_in_both_formats() {
        for payload_bytes in [64, 1_024, 64 * 1_024] {
            let fixture = CodecFixture::encrypted(payload_bytes).unwrap();
            for format in [BenchFormat::Json, BenchFormat::MsgPack] {
                fixture.decode(format).unwrap();
            }
        }
    }

    #[test]
    fn fanout_encodes_once_per_active_format_and_shares_bytes() {
        let mut fixture = FanoutFixture::new(CompatibilityCase::Mixed, 100).unwrap();
        let before = fixture.metrics();
        let observation = fixture.deliver_and_drain().unwrap();
        let after = fixture.metrics();

        assert_eq!(after.encoded - before.encoded, 2);
        assert_eq!(after.fanout - before.fanout, 100);
        assert_eq!(observation.drained_sockets, 100);
        assert_eq!(observation.shared_payload_allocations, 2);
    }

    #[test]
    fn stalled_socket_is_bounded_and_marks_continuity_loss() {
        let mut fixture = FanoutFixture::new(CompatibilityCase::JsonOnly, 1).unwrap();
        fixture.deliver_without_drain().unwrap();
        fixture.deliver_without_drain().unwrap();
        let metrics = fixture.metrics();
        assert_eq!(metrics.continuity_lost, 1);
        assert_eq!(metrics.reset_required, 1);
    }

    #[test]
    fn maximum_replay_fixture_projects_every_message() {
        let fixture = HistoryFixture::new(4_096, 64);
        assert_eq!(fixture.project_replay().unwrap().len(), 4_096);
    }

    #[test]
    fn disabled_and_no_subscriber_cases_do_not_encode() {
        for case in [
            CompatibilityCase::Disabled,
            CompatibilityCase::EnabledNoSubscribers,
        ] {
            let mut fixture = FanoutFixture::new(case, 0).unwrap();
            fixture.deliver_and_drain().unwrap();
            assert_eq!(fixture.metrics().encoded, 0);
        }
    }
}
