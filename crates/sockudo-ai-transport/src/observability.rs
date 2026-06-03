use ahash::AHashMap;
use sockudo_protocol::messages::{
    AI_EVENT_CANCEL, AI_EVENT_TURN_END, AI_EVENT_TURN_START, MessageData, PusherMessage,
};
use sockudo_protocol::versioned_messages::extract_runtime_message_serial;
use std::sync::Mutex;

const DEFAULT_TRACKER_SHARDS: usize = 64;

/// Low-cardinality reason labels accepted for AI turn end metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnEndReason {
    Complete,
    Cancelled,
    Error,
    Suspended,
    Unknown,
}

impl TurnEndReason {
    #[must_use]
    pub fn from_header(value: Option<&str>) -> Self {
        match value {
            Some("complete") => Self::Complete,
            Some("cancelled") => Self::Cancelled,
            Some("error") => Self::Error,
            Some("suspended") => Self::Suspended,
            _ => Self::Unknown,
        }
    }

    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Cancelled => "cancelled",
            Self::Error => "error",
            Self::Suspended => "suspended",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnStarted {
    pub turn_id: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnEnded {
    pub turn_id: Option<String>,
    pub reason: TurnEndReason,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelRequested {
    pub turn_id: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamMetricUpdate {
    pub active_streams: usize,
    pub bytes: Option<usize>,
    pub ended_duration_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AiObservabilityUpdate {
    pub unparseable: bool,
    pub turn_started: Option<TurnStarted>,
    pub turn_ended: Option<TurnEnded>,
    pub cancel_requested: Option<CancelRequested>,
    pub stream: Option<StreamMetricUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    app_id: String,
    channel: String,
    message_serial: String,
}

#[derive(Debug, Clone)]
struct StreamState {
    started_ms: i64,
}

struct StreamShard {
    streams: Mutex<AHashMap<StreamKey, StreamState>>,
}

/// Tracks AI Transport observability state without interpreting codec payloads.
pub struct AiObservabilityTracker {
    shards: Vec<StreamShard>,
}

impl Default for AiObservabilityTracker {
    fn default() -> Self {
        Self::new(DEFAULT_TRACKER_SHARDS)
    }
}

impl AiObservabilityTracker {
    #[must_use]
    pub fn new(shards: usize) -> Self {
        let shard_count = shards.max(1);
        let shards = (0..shard_count)
            .map(|_| StreamShard {
                streams: Mutex::new(AHashMap::new()),
            })
            .collect();
        Self { shards }
    }

    #[must_use]
    pub fn observe(
        &self,
        app_id: &str,
        channel: &str,
        message: &PusherMessage,
        now_ms: i64,
    ) -> AiObservabilityUpdate {
        let mut update = classify_turn_event(message);

        if message.validate_ai_headers().is_err() {
            update.unparseable = true;
        }

        if let Some(stream) = self.observe_stream(app_id, channel, message, now_ms) {
            update.stream = Some(stream);
        }

        update
    }

    #[must_use]
    pub fn active_streams(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .streams
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .len()
            })
            .sum()
    }

    fn observe_stream(
        &self,
        app_id: &str,
        channel: &str,
        message: &PusherMessage,
        now_ms: i64,
    ) -> Option<StreamMetricUpdate> {
        let headers = message.ai_transport_headers()?;
        let status = headers.status()?;
        let message_serial = extract_runtime_message_serial(message)
            .or(headers.codec_message_id())
            .or(message.message_id.as_deref())?;
        let key = StreamKey {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message_serial: message_serial.to_string(),
        };
        let shard = self.shard(&key);
        let mut streams = shard.streams.lock().unwrap_or_else(|err| err.into_inner());
        let bytes = message_payload_bytes(message);

        match status {
            "streaming" => {
                streams
                    .entry(key)
                    .or_insert(StreamState { started_ms: now_ms });
                Some(StreamMetricUpdate {
                    active_streams: self.active_streams_locked_delta(&streams, 0),
                    bytes,
                    ended_duration_seconds: None,
                })
            }
            "complete" | "cancelled" => {
                let ended = streams
                    .remove(&key)
                    .map(|state| now_ms.saturating_sub(state.started_ms).max(0) as f64 / 1_000.0);
                Some(StreamMetricUpdate {
                    active_streams: self.active_streams_locked_delta(&streams, 0),
                    bytes,
                    ended_duration_seconds: ended,
                })
            }
            _ => None,
        }
    }

    fn active_streams_locked_delta(
        &self,
        locked_shard: &AHashMap<StreamKey, StreamState>,
        delta: isize,
    ) -> usize {
        let locked_len = locked_shard.len().saturating_add_signed(delta);
        let other_len: usize = self
            .shards
            .iter()
            .map(|shard| match shard.streams.try_lock() {
                Ok(guard) => {
                    if std::ptr::eq(&*guard, locked_shard) {
                        0
                    } else {
                        guard.len()
                    }
                }
                Err(_) => 0,
            })
            .sum();
        locked_len + other_len
    }

    #[inline]
    fn shard(&self, key: &StreamKey) -> &StreamShard {
        &self.shards[fast_stream_shard(key, self.shards.len())]
    }
}

fn classify_turn_event(message: &PusherMessage) -> AiObservabilityUpdate {
    let event = message.event.as_deref();
    let headers = message.ai_transport_headers();
    match event {
        Some(AI_EVENT_TURN_START) => AiObservabilityUpdate {
            turn_started: Some(TurnStarted {
                turn_id: headers
                    .as_ref()
                    .and_then(|h| h.turn_id())
                    .map(str::to_owned),
                client_id: headers
                    .as_ref()
                    .and_then(|h| h.turn_client_id())
                    .map(str::to_owned),
            }),
            ..AiObservabilityUpdate::default()
        },
        Some(AI_EVENT_TURN_END) => AiObservabilityUpdate {
            turn_ended: Some(TurnEnded {
                turn_id: headers
                    .as_ref()
                    .and_then(|h| h.turn_id())
                    .map(str::to_owned),
                reason: TurnEndReason::from_header(headers.as_ref().and_then(|h| h.turn_reason())),
                error_code: headers
                    .as_ref()
                    .and_then(|h| h.error_code())
                    .map(str::to_owned),
            }),
            ..AiObservabilityUpdate::default()
        },
        Some(AI_EVENT_CANCEL) => AiObservabilityUpdate {
            cancel_requested: Some(CancelRequested {
                turn_id: headers
                    .as_ref()
                    .and_then(|h| h.turn_id())
                    .map(str::to_owned),
                client_id: headers
                    .as_ref()
                    .and_then(|h| h.turn_client_id())
                    .map(str::to_owned),
            }),
            ..AiObservabilityUpdate::default()
        },
        _ => AiObservabilityUpdate::default(),
    }
}

fn message_payload_bytes(message: &PusherMessage) -> Option<usize> {
    match message.data.as_ref()? {
        MessageData::String(value) => Some(value.len()),
        MessageData::Json(value) => sonic_rs::to_vec(value).ok().map(|bytes| bytes.len()),
        MessageData::Structured { .. } => sonic_rs::to_vec(message.data.as_ref()?)
            .ok()
            .map(|bytes| bytes.len()),
    }
}

#[inline]
fn fast_stream_shard(key: &StreamKey, shards: usize) -> usize {
    let mut hash = 0xcbf29ce484222325_u64;
    for bytes in [
        key.app_id.as_bytes(),
        key.channel.as_bytes(),
        key.message_serial.as_bytes(),
    ] {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    (hash as usize) % shards
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_protocol::messages::{AiExtras, MessageExtras};
    use std::collections::HashMap;

    fn ai_message(event: &str, headers: &[(&str, &str)], data: &str) -> PusherMessage {
        let mut transport = HashMap::new();
        for (key, value) in headers {
            transport.insert((*key).to_string(), (*value).to_string());
        }
        PusherMessage {
            event: Some(event.to_string()),
            channel: Some("ai-chat".to_string()),
            data: Some(MessageData::String(data.to_string())),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: Some(MessageExtras {
                ai: Some(AiExtras {
                    transport: Some(transport),
                    codec: None,
                }),
                ..MessageExtras::default()
            }),
            delta_sequence: None,
            delta_conflation_key: None,
        }
    }

    #[test]
    fn turn_end_reason_is_bounded() {
        assert_eq!(
            TurnEndReason::from_header(Some("complete")).as_label(),
            "complete"
        );
        assert_eq!(
            TurnEndReason::from_header(Some("anything")).as_label(),
            "unknown"
        );
        assert_eq!(TurnEndReason::from_header(None).as_label(), "unknown");
    }

    #[test]
    fn classifies_turn_and_cancel_events_without_turn_labels_for_metrics() {
        let tracker = AiObservabilityTracker::new(2);
        let start = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(
                AI_EVENT_TURN_START,
                &[("turn-id", "turn-1"), ("turn-client-id", "client-1")],
                "{}",
            ),
            1,
        );
        assert_eq!(
            start.turn_started.unwrap().turn_id.as_deref(),
            Some("turn-1")
        );

        let cancel = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(AI_EVENT_CANCEL, &[("turn-id", "turn-1")], "{}"),
            2,
        );
        assert_eq!(
            cancel.cancel_requested.unwrap().turn_id.as_deref(),
            Some("turn-1")
        );
    }

    #[test]
    fn malformed_headers_are_counted_but_do_not_block_observation() {
        let tracker = AiObservabilityTracker::new(2);
        let update = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(
                AI_EVENT_TURN_END,
                &[("turn-id", "turn-1"), ("turn-reason", "bad")],
                "{}",
            ),
            1,
        );

        assert!(update.unparseable);
        assert_eq!(update.turn_ended.unwrap().reason, TurnEndReason::Unknown);
    }

    #[test]
    fn tracks_stream_duration_and_bytes() {
        let tracker = AiObservabilityTracker::new(2);
        let streaming = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(
                "sockudo:message.append",
                &[
                    ("codec-message-id", "msg-1"),
                    ("status", "streaming"),
                    ("stream", "true"),
                ],
                "abc",
            ),
            1_000,
        );
        assert_eq!(streaming.stream.as_ref().unwrap().active_streams, 1);
        assert_eq!(streaming.stream.as_ref().unwrap().bytes, Some(3));

        let complete = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(
                "sockudo:message.append",
                &[("codec-message-id", "msg-1"), ("status", "complete")],
                "abcd",
            ),
            2_500,
        );
        let stream = complete.stream.unwrap();
        assert_eq!(stream.active_streams, 0);
        assert_eq!(stream.bytes, Some(4));
        assert_eq!(stream.ended_duration_seconds, Some(1.5));
    }
}
