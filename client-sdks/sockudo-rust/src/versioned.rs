//! Client-side folding of versioned (mutable) message events.
//!
//! Mutations are delivered as ordinary events carrying `sockudo_action`,
//! `sockudo_message_serial` and related headers in `extras`. The reducer
//! folds a sequence of them into the latest state of one message.

use sonic_rs::{JsonValueTrait, Value};

use crate::error::{Error, Result};
use crate::event::{Event, HeaderValue, float_to_u64};

/// A mutation action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MutableMessageAction {
    /// `message.create`
    Create,
    /// `message.update`
    Update,
    /// `message.delete`
    Delete,
    /// `message.append`
    Append,
}

impl MutableMessageAction {
    /// Wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "message.create",
            Self::Update => "message.update",
            Self::Delete => "message.delete",
            Self::Append => "message.append",
        }
    }

    /// Parse a wire name.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "message.create" => Some(Self::Create),
            "message.update" => Some(Self::Update),
            "message.delete" => Some(Self::Delete),
            "message.append" => Some(Self::Append),
            _ => None,
        }
    }
}

/// Version metadata read from an event's headers.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MutableMessageInfo {
    /// The mutation.
    pub action: MutableMessageAction,
    /// Event name.
    pub event: String,
    /// Stable id of the logical message.
    pub message_serial: String,
    /// Id of this version.
    pub version_serial: Option<String>,
    /// History serial of this version.
    pub history_serial: Option<u64>,
    /// Version timestamp (ms).
    pub version_timestamp_ms: Option<u64>,
}

fn header_u64(value: Option<&HeaderValue>) -> Option<u64> {
    match value? {
        HeaderValue::Number(number) => float_to_u64(*number),
        HeaderValue::String(text) => text.trim().parse().ok(),
        HeaderValue::Bool(_) | HeaderValue::Null => None,
    }
}

impl MutableMessageInfo {
    /// Extract version metadata, or `None` if the event is not a mutation.
    pub fn from_event(event: &Event) -> Option<Self> {
        let extras = event.extras.as_ref()?;
        let action = MutableMessageAction::parse(extras.header_str("sockudo_action")?)?;
        let message_serial = extras.header_str("sockudo_message_serial")?.to_owned();
        Some(Self {
            action,
            event: event.event.clone(),
            message_serial,
            version_serial: extras
                .header_str("sockudo_version_serial")
                .map(str::to_owned),
            history_serial: header_u64(extras.header("sockudo_history_serial")),
            version_timestamp_ms: header_u64(extras.header("sockudo_version_timestamp_ms")),
        })
    }
}

/// Whether an event is a versioned-message mutation.
pub fn is_mutable_message_event(event: &Event) -> bool {
    MutableMessageInfo::from_event(event).is_some()
}

/// The folded state of one message.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MutableMessageState {
    /// Stable id of the logical message.
    pub message_serial: String,
    /// The last applied action.
    pub action: MutableMessageAction,
    /// Current data (`null` after a delete without tombstone data).
    pub data: Value,
    /// Event name of the last version.
    pub event: String,
    /// Delivery serial of the last version.
    pub serial: Option<u64>,
    /// Stream id of the last version.
    pub stream_id: Option<String>,
    /// Message id of the last version.
    pub message_id: Option<String>,
    /// Version serial of the last version.
    pub version_serial: Option<String>,
    /// History serial of the last version.
    pub history_serial: Option<u64>,
    /// Version timestamp of the last version (ms).
    pub version_timestamp_ms: Option<u64>,
}

/// Apply one mutation to `current`.
///
/// `message.append` concatenates string fragments, so seed the state from a
/// create/update event or a latest-view history item first.
pub fn reduce_mutable_message_event(
    current: Option<MutableMessageState>,
    event: &Event,
) -> Result<MutableMessageState> {
    let info = MutableMessageInfo::from_event(event)
        .ok_or_else(|| Error::MutableMessage("event is not a mutable-message event".into()))?;
    if let Some(state) = current.as_ref()
        && state.message_serial != info.message_serial
    {
        return Err(Error::MutableMessage(format!(
            "reducer expected message_serial '{}' but received '{}'",
            state.message_serial, info.message_serial
        )));
    }
    let incoming = event.value();
    let data = match info.action {
        MutableMessageAction::Append => {
            let base = current
                .as_ref()
                .and_then(|state| state.data.as_str())
                .ok_or_else(|| {
                    Error::MutableMessage(
                        "message.append requires an existing string base; seed state from a create/update payload or latest-view history first".into(),
                    )
                })?;
            let fragment = incoming.as_str().ok_or_else(|| {
                Error::MutableMessage("message.append payload must be a string fragment".into())
            })?;
            let mut joined = String::with_capacity(base.len() + fragment.len());
            joined.push_str(base);
            joined.push_str(fragment);
            Value::from(joined.as_str())
        }
        MutableMessageAction::Create
        | MutableMessageAction::Update
        | MutableMessageAction::Delete => incoming,
    };
    Ok(MutableMessageState {
        message_serial: info.message_serial,
        action: info.action,
        data,
        event: info.event,
        serial: event.serial,
        stream_id: event.stream_id.clone(),
        message_id: event.message_id.clone(),
        version_serial: info.version_serial,
        history_serial: info.history_serial,
        version_timestamp_ms: info.version_timestamp_ms,
    })
}

/// Fold a sequence of mutations for one message.
pub fn reduce_mutable_message_events<'a>(
    events: impl IntoIterator<Item = &'a Event>,
) -> Result<Option<MutableMessageState>> {
    let mut state = None;
    for event in events {
        state = Some(reduce_mutable_message_event(state, event)?);
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Extras, Payload};

    fn mutation(action: &str, serial: &str, data: &str, history: f64) -> Event {
        Event::new("chat.message")
            .with_channel("room")
            .with_data(Payload::Text(data.into()))
            .with_extras(
                Extras::new()
                    .with_header("sockudo_action", action)
                    .with_header("sockudo_message_serial", serial)
                    .with_header("sockudo_version_serial", format!("{serial}:v{history}"))
                    .with_header("sockudo_history_serial", history),
            )
    }

    #[test]
    fn folds_create_append_update_delete() {
        let events = [
            mutation("message.create", "m1", "hello", 1.0),
            mutation("message.append", "m1", " world", 2.0),
            mutation("message.append", "m1", "!", 3.0),
        ];
        let state = reduce_mutable_message_events(&events).unwrap().unwrap();
        assert_eq!(state.data.as_str(), Some("hello world!"));
        assert_eq!(state.action, MutableMessageAction::Append);
        assert_eq!(state.history_serial, Some(3));

        let state = reduce_mutable_message_event(
            Some(state),
            &mutation("message.update", "m1", r#"{"text":"edited"}"#, 4.0),
        )
        .unwrap();
        assert_eq!(state.data, sonic_rs::json!({"text": "edited"}));
        let state = reduce_mutable_message_event(
            Some(state),
            &mutation("message.delete", "m1", "null", 5.0),
        )
        .unwrap();
        assert!(state.data.is_null());
    }

    #[test]
    fn rejects_mismatched_and_unseeded_events() {
        let created =
            reduce_mutable_message_event(None, &mutation("message.create", "m1", "a", 1.0))
                .unwrap();
        assert!(
            reduce_mutable_message_event(
                Some(created),
                &mutation("message.update", "m2", "b", 2.0)
            )
            .is_err()
        );
        assert!(
            reduce_mutable_message_event(None, &mutation("message.append", "m1", "x", 1.0))
                .is_err()
        );
        assert!(reduce_mutable_message_event(None, &Event::new("plain")).is_err());
        assert!(!is_mutable_message_event(&Event::new("plain")));
        assert!(is_mutable_message_event(&mutation(
            "message.create",
            "m",
            "",
            1.0
        )));
    }

    #[test]
    fn string_history_serials_parse() {
        let mut event = mutation("message.create", "m1", "a", 0.0);
        event.extras = Some(
            Extras::new()
                .with_header("sockudo_action", "message.create")
                .with_header("sockudo_message_serial", "m1")
                .with_header("sockudo_history_serial", "18446744073709551615"),
        );
        let info = MutableMessageInfo::from_event(&event).unwrap();
        assert_eq!(info.history_serial, Some(u64::MAX));
    }
}
