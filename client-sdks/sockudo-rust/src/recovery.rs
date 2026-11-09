//! Connection recovery and rewind (Protocol V2).

use serde::{Deserialize, Serialize};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};

use crate::event::{Event, value_to_u64};

/// A channel position the server can resume from.
///
/// Positions are `Serialize`/`Deserialize` so you can persist them across
/// process restarts and restore them with
/// [`crate::SockudoClient::set_recovery_positions`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryPosition {
    /// Last delivered serial.
    pub serial: u64,
    /// Continuity token; resume fails closed when it changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    /// Last delivered message id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_id: Option<String>,
}

impl RecoveryPosition {
    fn from_value(value: &Value) -> Option<Self> {
        Some(Self {
            serial: value.get("serial").and_then(value_to_u64)?,
            stream_id: value
                .get("stream_id")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
            last_message_id: value
                .get("last_message_id")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
        })
    }
}

/// One channel recovered by `resume`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResumeRecovered {
    /// Channel name.
    pub channel: String,
    /// Recovery source reported by the server (e.g. hot buffer, durable history).
    pub source: String,
    /// Number of replayed messages.
    pub replayed: u64,
    /// The position after replay.
    pub position: Option<RecoveryPosition>,
}

/// One channel that could not be resumed. Discard derived state for it and
/// reload from an authoritative snapshot; the client resubscribes it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResumeFailed {
    /// Channel name.
    pub channel: String,
    /// Failure code.
    pub code: String,
    /// Human-readable reason.
    pub reason: String,
    /// Stream id the client asked for.
    pub expected_stream_id: Option<String>,
    /// Stream id the server has now.
    pub current_stream_id: Option<String>,
    /// Oldest serial still retained.
    pub oldest_available_serial: Option<u64>,
    /// Newest serial retained.
    pub newest_available_serial: Option<u64>,
}

impl ResumeFailed {
    pub(crate) fn from_value(value: &Value) -> Self {
        let text = |key: &str| value.get(key).and_then(|v| v.as_str()).map(str::to_owned);
        let code = value
            .get("code")
            .map(|code| {
                code.as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| code.to_string())
            })
            .unwrap_or_default();
        Self {
            channel: text("channel").unwrap_or_default(),
            code,
            reason: text("reason").unwrap_or_default(),
            expected_stream_id: text("expected_stream_id"),
            current_stream_id: text("current_stream_id"),
            oldest_available_serial: value.get("oldest_available_serial").and_then(value_to_u64),
            newest_available_serial: value.get("newest_available_serial").and_then(value_to_u64),
        }
    }

    /// Parse a `resume_failed` event.
    pub fn from_event(event: &Event) -> Self {
        Self::from_value(&event.value())
    }
}

/// The `resume_success` summary.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ResumeSuccess {
    /// Channels recovered.
    pub recovered: Vec<ResumeRecovered>,
    /// Channels that failed within the same resume.
    pub failed: Vec<ResumeFailed>,
}

impl ResumeSuccess {
    pub(crate) fn from_value(value: &Value) -> Self {
        let recovered = value
            .get("recovered")
            .and_then(|items| items.as_array())
            .map(|items| {
                items
                    .iter()
                    .map(|item| ResumeRecovered {
                        channel: item
                            .get("channel")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_owned(),
                        source: item
                            .get("source")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_owned(),
                        replayed: item.get("replayed").and_then(value_to_u64).unwrap_or(0),
                        position: item.get("position").and_then(RecoveryPosition::from_value),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let failed = value
            .get("failed")
            .and_then(|items| items.as_array())
            .map(|items| items.iter().map(ResumeFailed::from_value).collect())
            .unwrap_or_default();
        Self { recovered, failed }
    }

    /// Parse a `resume_success` event.
    pub fn from_event(event: &Event) -> Self {
        Self::from_value(&event.value())
    }
}

/// The `rewind_complete` summary for a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct RewindComplete {
    /// Historical messages replayed.
    pub historical_count: u64,
    /// Live messages buffered during the rewind.
    pub live_count: u64,
    /// Whether the full requested window was replayed.
    pub complete: bool,
    /// Retention cut the window short.
    pub truncated_by_retention: bool,
    /// The server's rewind limit cut the window short.
    pub truncated_by_limit: bool,
}

impl RewindComplete {
    /// Parse a `rewind_complete` event.
    pub fn from_event(event: &Event) -> Self {
        let value = event.value();
        let flag = |key: &str| value.get(key).and_then(|v| v.as_bool()).unwrap_or(false);
        Self {
            historical_count: value
                .get("historical_count")
                .and_then(value_to_u64)
                .unwrap_or(0),
            live_count: value.get("live_count").and_then(value_to_u64).unwrap_or(0),
            complete: flag("complete"),
            truncated_by_retention: flag("truncated_by_retention"),
            truncated_by_limit: flag("truncated_by_limit"),
        }
    }
}

/// How much history to replay when subscribing (Protocol V2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rewind {
    /// Replay the last `n` messages.
    Count(u64),
    /// Replay messages from the last `n` seconds.
    Seconds(u64),
}

impl Rewind {
    pub(crate) fn subscription_value(self) -> Value {
        match self {
            Self::Count(count) => Value::from(count),
            Self::Seconds(seconds) => sonic_rs::json!({ "seconds": seconds }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Payload;

    #[test]
    fn parses_resume_payloads() {
        let event = Event::new("sockudo:resume_success").with_data(Payload::Text(
            r#"{"recovered":[{"channel":"a","source":"hot","replayed":3,"position":{"serial":"9","stream_id":"s"}}],"failed":[{"channel":"b","code":"stream_reset","reason":"gone","oldest_available_serial":4}]}"#.into(),
        ));
        let success = ResumeSuccess::from_event(&event);
        assert_eq!(success.recovered[0].replayed, 3);
        assert_eq!(
            success.recovered[0].position,
            Some(RecoveryPosition {
                serial: 9,
                stream_id: Some("s".into()),
                last_message_id: None
            })
        );
        assert_eq!(success.failed[0].code, "stream_reset");
        assert_eq!(success.failed[0].oldest_available_serial, Some(4));
    }

    #[test]
    fn numeric_failure_codes_are_stringified() {
        let failed = ResumeFailed::from_value(&sonic_rs::json!({"channel": "x", "code": 4004}));
        assert_eq!(failed.code, "4004");
    }

    #[test]
    fn parses_rewind_complete_and_values() {
        let event = Event::new("sockudo:rewind_complete").with_data(Payload::Text(
            r#"{"historical_count":5,"live_count":1,"complete":true}"#.into(),
        ));
        let done = RewindComplete::from_event(&event);
        assert_eq!(done.historical_count, 5);
        assert!(done.complete && !done.truncated_by_limit);
        assert_eq!(Rewind::Count(10).subscription_value(), Value::from(10u64));
        assert_eq!(
            Rewind::Seconds(30).subscription_value(),
            sonic_rs::json!({"seconds": 30})
        );
    }

    #[test]
    fn positions_round_trip_through_json() {
        let position = RecoveryPosition {
            serial: 3,
            stream_id: Some("s".into()),
            last_message_id: None,
        };
        let json = sonic_rs::to_string(&position).unwrap();
        assert_eq!(json, r#"{"serial":3,"stream_id":"s"}"#);
        assert_eq!(
            sonic_rs::from_str::<RecoveryPosition>(&json).unwrap(),
            position
        );
    }
}
