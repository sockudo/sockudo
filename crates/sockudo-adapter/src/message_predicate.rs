//! Stable Protocol V2 message projection used by subscription predicates.

use serde::{Serialize, Serializer, ser::SerializeMap};
use sockudo_protocol::{
    messages::{ExtrasValue, MessageData, PusherMessage},
    versioned_messages::{MessageAction, extract_runtime_action},
};
use std::collections::{BTreeMap, HashMap};

const INTERNAL_HEADER_PREFIX: &str = "sockudo_";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NativeMessageProjection<'a> {
    event: Option<&'a str>,
    channel: Option<&'a str>,
    data: Option<&'a MessageData>,
    name: Option<&'a str>,
    user_id: Option<&'a str>,
    tags: Option<&'a BTreeMap<String, String>>,
    headers: Option<SafeHeaders<'a>>,
    message_id: Option<&'a str>,
    stream_id: Option<&'a str>,
    serial: Option<u64>,
    action: Option<&'static str>,
}

impl<'a> NativeMessageProjection<'a> {
    #[must_use]
    pub(crate) fn from_message(message: &'a PusherMessage) -> Self {
        Self {
            event: message.event.as_deref(),
            channel: message.channel.as_deref(),
            data: message.data.as_ref(),
            name: message.name.as_deref(),
            user_id: message.user_id.as_deref(),
            tags: message.tags.as_ref(),
            headers: message
                .extras
                .as_ref()
                .and_then(|extras| extras.headers.as_ref())
                .map(SafeHeaders),
            message_id: message.message_id.as_deref(),
            stream_id: message.stream_id.as_deref(),
            serial: message.serial,
            action: extract_runtime_action(message).map(action_name),
        }
    }
}

#[derive(Clone, Copy)]
struct SafeHeaders<'a>(&'a HashMap<String, ExtrasValue>);

impl Serialize for SafeHeaders<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let visible = self
            .0
            .iter()
            .filter(|(key, _)| !key.starts_with(INTERNAL_HEADER_PREFIX));
        let mut map = serializer.serialize_map(None)?;
        for (key, value) in visible {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

const fn action_name(action: MessageAction) -> &'static str {
    match action {
        MessageAction::Create => "create",
        MessageAction::Update => "update",
        MessageAction::Delete => "delete",
        MessageAction::Append => "append",
        MessageAction::Summary => "summary",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_protocol::messages::MessageExtras;
    use sonic_rs::JsonValueTrait;

    #[test]
    fn projection_exposes_public_filter_fields_and_hides_internal_headers() {
        let mut headers = HashMap::new();
        headers.insert("priority".to_string(), ExtrasValue::String("high".into()));
        headers.insert(
            "sockudo_append_fragment".to_string(),
            ExtrasValue::String("private".into()),
        );
        let mut message = PusherMessage::channel_event(
            "order.updated",
            "orders",
            sonic_rs::json!({ "total": 125 }),
        );
        message.data = Some(MessageData::Json(sonic_rs::json!({ "total": 125 })));
        message.message_id = Some("message-1".into());
        message.extras = Some(MessageExtras {
            headers: Some(headers),
            idempotency_key: Some("must-not-project".into()),
            ..MessageExtras::default()
        });

        let projected = sonic_rs::to_value(&NativeMessageProjection::from_message(&message))
            .expect("projection serializes");
        assert_eq!(projected["event"].as_str(), Some("order.updated"));
        assert_eq!(projected["data"]["total"].as_u64(), Some(125));
        assert_eq!(projected["headers"]["priority"].as_str(), Some("high"));
        assert!(
            projected["headers"]
                .get("sockudo_append_fragment")
                .is_none()
        );
        assert!(projected.get("idempotencyKey").is_none());
        assert!(projected.get("extras").is_none());
    }
}
