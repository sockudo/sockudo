use ahash::AHashMap;
use parking_lot::Mutex;
use sockudo_protocol::messages::{
    AI_EVENT_CANCEL, AI_EVENT_LEGACY_TURN_END, AI_EVENT_LEGACY_TURN_START, AI_EVENT_RUN_END,
    AI_EVENT_RUN_RESUME, AI_EVENT_RUN_START, AI_EVENT_RUN_SUSPEND, AiTransportHeaders, MessageData,
    PusherMessage,
};
use sockudo_protocol::versioned_messages::extract_runtime_message_serial;
use std::sync::atomic::{AtomicUsize, Ordering};

const DEFAULT_TRACKER_SHARDS: usize = 64;

/// Low-cardinality reason labels accepted for AI run end metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEndReason {
    Complete,
    Cancelled,
    Error,
    /// Legacy `turn-reason=suspended`; native Ably runs use `ai-run-suspend`.
    Suspended,
    Unknown,
}

impl RunEndReason {
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
pub struct RunStarted {
    pub run_id: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEnded {
    pub run_id: Option<String>,
    pub reason: RunEndReason,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLifecycleSignal {
    pub run_id: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelRequested {
    pub run_id: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamMetricUpdate {
    pub active_streams: usize,
    pub active_stream_delta: isize,
    pub bytes: Option<usize>,
    pub ended_duration_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AiObservabilityUpdate {
    pub unparseable: bool,
    pub run_started: Option<RunStarted>,
    pub run_suspended: Option<RunLifecycleSignal>,
    pub run_resumed: Option<RunLifecycleSignal>,
    pub run_ended: Option<RunEnded>,
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
    last_seen_ms: i64,
}

struct StreamShard {
    streams: Mutex<AHashMap<StreamKey, StreamState>>,
}

/// Tracks AI Transport observability state without interpreting codec payloads.
pub struct AiObservabilityTracker {
    shards: Vec<StreamShard>,
    active_streams: AtomicUsize,
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
        Self {
            shards,
            active_streams: AtomicUsize::new(0),
        }
    }

    #[must_use]
    pub fn observe(
        &self,
        app_id: &str,
        channel: &str,
        message: &PusherMessage,
        now_ms: i64,
    ) -> AiObservabilityUpdate {
        let headers = match message.validated_ai_transport_headers() {
            Ok(headers) => headers,
            Err(_) => {
                let mut update = classify_run_event(message, None);
                update.unparseable = true;
                return update;
            }
        };
        let mut update = classify_run_event(message, headers.as_ref());

        if let Some(stream) =
            self.observe_stream(app_id, channel, message, headers.as_ref(), now_ms)
        {
            update.stream = Some(stream);
        }

        update
    }

    #[must_use]
    pub fn active_streams(&self) -> usize {
        self.active_streams.load(Ordering::Acquire)
    }

    /// Expire streams whose terminal event never arrived.
    #[must_use]
    pub fn expire_stale(&self, now_ms: i64, max_idle_ms: i64) -> Vec<String> {
        let mut expired_apps = Vec::new();
        for shard in &self.shards {
            let mut streams = shard.streams.lock();
            let expired = streams
                .iter()
                .filter_map(|(key, state)| {
                    (now_ms.saturating_sub(state.last_seen_ms) >= max_idle_ms)
                        .then_some(key.clone())
                })
                .collect::<Vec<_>>();
            for key in expired {
                if streams.remove(&key).is_some() {
                    let previous = self.active_streams.fetch_sub(1, Ordering::AcqRel);
                    debug_assert!(previous > 0, "AI observability active stream underflow");
                    expired_apps.push(key.app_id);
                }
            }
        }
        expired_apps
    }

    fn observe_stream(
        &self,
        app_id: &str,
        channel: &str,
        message: &PusherMessage,
        headers: Option<&AiTransportHeaders<'_>>,
        now_ms: i64,
    ) -> Option<StreamMetricUpdate> {
        let headers = headers?;
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
        let mut streams = shard.streams.lock();
        let bytes = message_payload_bytes(message);

        match status {
            "streaming" => {
                let active_stream_delta = match streams.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut occupied) => {
                        occupied.get_mut().last_seen_ms = now_ms;
                        0
                    }
                    std::collections::hash_map::Entry::Vacant(vacant) => {
                        vacant.insert(StreamState {
                            started_ms: now_ms,
                            last_seen_ms: now_ms,
                        });
                        self.active_streams.fetch_add(1, Ordering::AcqRel);
                        1
                    }
                };
                Some(StreamMetricUpdate {
                    active_streams: self.active_streams(),
                    active_stream_delta,
                    bytes,
                    ended_duration_seconds: None,
                })
            }
            "complete" | "cancelled" => {
                let ended = streams
                    .remove(&key)
                    .map(|state| now_ms.saturating_sub(state.started_ms).max(0) as f64 / 1_000.0);
                let active_stream_delta = if ended.is_some() {
                    let previous = self.active_streams.fetch_sub(1, Ordering::AcqRel);
                    debug_assert!(previous > 0, "AI observability active stream underflow");
                    -1
                } else {
                    0
                };
                Some(StreamMetricUpdate {
                    active_streams: self.active_streams(),
                    active_stream_delta,
                    bytes,
                    ended_duration_seconds: ended,
                })
            }
            _ => None,
        }
    }

    #[inline]
    fn shard(&self, key: &StreamKey) -> &StreamShard {
        &self.shards[fast_stream_shard(key, self.shards.len())]
    }
}

fn classify_run_event(
    message: &PusherMessage,
    headers: Option<&AiTransportHeaders<'_>>,
) -> AiObservabilityUpdate {
    let event = message.event.as_deref();
    match event {
        Some(AI_EVENT_RUN_START | AI_EVENT_LEGACY_TURN_START) => AiObservabilityUpdate {
            run_started: Some(RunStarted {
                run_id: headers.and_then(|h| h.run_id()).map(str::to_owned),
                client_id: headers
                    .and_then(|h| h.run_client_identity())
                    .map(str::to_owned),
            }),
            ..AiObservabilityUpdate::default()
        },
        Some(AI_EVENT_RUN_SUSPEND) => AiObservabilityUpdate {
            run_suspended: Some(RunLifecycleSignal {
                run_id: headers.and_then(|h| h.run_id()).map(str::to_owned),
                client_id: headers
                    .and_then(|h| h.run_client_identity())
                    .map(str::to_owned),
            }),
            ..AiObservabilityUpdate::default()
        },
        Some(AI_EVENT_RUN_RESUME) => AiObservabilityUpdate {
            run_resumed: Some(RunLifecycleSignal {
                run_id: headers.and_then(|h| h.run_id()).map(str::to_owned),
                client_id: headers
                    .and_then(|h| h.run_client_identity())
                    .map(str::to_owned),
            }),
            ..AiObservabilityUpdate::default()
        },
        Some(AI_EVENT_RUN_END | AI_EVENT_LEGACY_TURN_END) => {
            let reason = RunEndReason::from_header(headers.and_then(|h| h.run_reason()));
            if reason == RunEndReason::Suspended {
                AiObservabilityUpdate {
                    run_suspended: Some(RunLifecycleSignal {
                        run_id: headers.and_then(|h| h.run_id()).map(str::to_owned),
                        client_id: headers
                            .and_then(|h| h.run_client_identity())
                            .map(str::to_owned),
                    }),
                    ..AiObservabilityUpdate::default()
                }
            } else {
                AiObservabilityUpdate {
                    run_ended: Some(RunEnded {
                        run_id: headers.and_then(|h| h.run_id()).map(str::to_owned),
                        reason,
                        error_code: headers.and_then(|h| h.error_code()).map(str::to_owned),
                    }),
                    ..AiObservabilityUpdate::default()
                }
            }
        }
        Some(AI_EVENT_CANCEL) => AiObservabilityUpdate {
            cancel_requested: Some(CancelRequested {
                run_id: headers.and_then(|h| h.run_id()).map(str::to_owned),
                client_id: headers
                    .and_then(|h| h.run_client_identity())
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
        MessageData::Binary(value) => Some(value.len()),
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
    fn run_end_reason_is_bounded() {
        assert_eq!(
            RunEndReason::from_header(Some("complete")).as_label(),
            "complete"
        );
        assert_eq!(
            RunEndReason::from_header(Some("anything")).as_label(),
            "unknown"
        );
        assert_eq!(RunEndReason::from_header(None).as_label(), "unknown");
    }

    #[test]
    fn classifies_run_and_cancel_events_without_legacy_turn_labels_for_metrics() {
        let tracker = AiObservabilityTracker::new(2);
        let start = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(
                AI_EVENT_RUN_START,
                &[("run-id", "run-1"), ("run-client-id", "client-1")],
                "{}",
            ),
            1,
        );
        assert_eq!(start.run_started.unwrap().run_id.as_deref(), Some("run-1"));

        let cancel = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(AI_EVENT_CANCEL, &[("run-id", "run-1")], "{}"),
            2,
        );
        assert_eq!(
            cancel.cancel_requested.unwrap().run_id.as_deref(),
            Some("run-1")
        );
    }

    #[test]
    fn treats_empty_run_client_id_as_absent_for_derived_observability() {
        let tracker = AiObservabilityTracker::new(1);
        let update = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(
                AI_EVENT_RUN_START,
                &[("run-id", "run-unknown"), ("run-client-id", "")],
                "{}",
            ),
            1,
        );

        let started = update.run_started.expect("run-start observation");
        assert_eq!(started.run_id.as_deref(), Some("run-unknown"));
        assert_eq!(started.client_id, None);
        assert!(!update.unparseable);
    }

    #[test]
    fn legacy_turn_suspend_maps_to_run_suspended_signal() {
        let tracker = AiObservabilityTracker::new(2);
        let update = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(
                AI_EVENT_LEGACY_TURN_END,
                &[("turn-id", "legacy-1"), ("turn-reason", "suspended")],
                "{}",
            ),
            1,
        );

        assert_eq!(
            update.run_suspended.unwrap().run_id.as_deref(),
            Some("legacy-1")
        );
        assert!(update.run_ended.is_none());
    }

    #[test]
    fn malformed_headers_are_counted_but_do_not_block_observation() {
        let tracker = AiObservabilityTracker::new(2);
        let update = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(
                AI_EVENT_RUN_END,
                &[("run-id", "run-1"), ("run-reason", "bad")],
                "{}",
            ),
            1,
        );

        assert!(update.unparseable);
        assert_eq!(update.run_ended.unwrap().reason, RunEndReason::Unknown);
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

    #[test]
    fn expires_streams_that_never_receive_a_terminal_event() {
        let tracker = AiObservabilityTracker::new(4);
        let update = tracker.observe(
            "app",
            "ai-chat",
            &ai_message(
                "sockudo:message.append",
                &[
                    ("codec-message-id", "msg-expired"),
                    ("status", "streaming"),
                    ("stream", "true"),
                ],
                "abc",
            ),
            1_000,
        );
        assert_eq!(update.stream.unwrap().active_stream_delta, 1);

        assert!(tracker.expire_stale(1_999, 1_000).is_empty());
        assert_eq!(tracker.expire_stale(2_000, 1_000), vec!["app"]);
        assert_eq!(tracker.active_streams(), 0);
        assert!(tracker.expire_stale(3_000, 1_000).is_empty());
    }
}
