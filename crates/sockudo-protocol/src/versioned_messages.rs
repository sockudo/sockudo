use crate::messages::{ExtrasValue, MessageData, MessageExtras, PusherMessage};
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::HashMap;

pub const HEADER_ACTION: &str = "sockudo_action";
pub const HEADER_MESSAGE_SERIAL: &str = "sockudo_message_serial";
pub const HEADER_VERSION_SERIAL: &str = "sockudo_version_serial";
pub const HEADER_HISTORY_SERIAL: &str = "sockudo_history_serial";
pub const HEADER_VERSION_TIMESTAMP_MS: &str = "sockudo_version_timestamp_ms";
const HEADER_APPEND_FRAGMENT: &str = "sockudo_append_fragment";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppendMode {
    Delta,
    Full,
}

impl AppendMode {
    pub fn parse_query_param(value: Option<&str>) -> Result<Self, String> {
        match value {
            None | Some("") | Some("delta") => Ok(Self::Delta),
            Some("full") => Ok(Self::Full),
            Some(other) => Err(format!("unsupported append_mode '{other}'")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delta => "delta",
            Self::Full => "full",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageAction {
    Create,
    Update,
    Delete,
    Append,
    Summary,
}

impl MessageAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "message.create",
            Self::Update => "message.update",
            Self::Delete => "message.delete",
            Self::Append => "message.append",
            Self::Summary => "message.summary",
        }
    }

    pub fn v2_event_name(self) -> String {
        format!("sockudo:{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VersionDirection {
    NewestFirst,
    OldestFirst,
}

impl VersionDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewestFirst => "newest_first",
            Self::OldestFirst => "oldest_first",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ClearField {
    Name,
    Data,
    Extras,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageVersionMetadata {
    pub serial: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    pub timestamp_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

impl MessageVersionMetadata {
    pub fn validate(&self) -> Result<(), String> {
        if self.serial.trim().is_empty() {
            return Err("version.serial must not be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VersionedRealtimeMessage {
    #[serde(flatten)]
    pub message: PusherMessage,
    pub action: MessageAction,
    pub message_serial: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_serial: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_serial: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<MessageVersionMetadata>,
}

impl VersionedRealtimeMessage {
    pub fn validate_v2(&self) -> Result<(), String> {
        if self.message_serial.trim().is_empty() {
            return Err("message_serial must not be empty".to_string());
        }

        let expected_event = self.action.v2_event_name();
        match self.message.event.as_deref() {
            Some(event) if event == expected_event => {}
            Some(event) => {
                return Err(format!(
                    "event '{}' does not match action '{}'",
                    event,
                    self.action.as_str()
                ));
            }
            None => {
                return Err(format!(
                    "event must be present for versioned action '{}'",
                    self.action.as_str()
                ));
            }
        }

        match self.message.channel.as_deref() {
            Some(channel) if !channel.trim().is_empty() => {}
            _ => return Err("channel must be present for versioned messages".to_string()),
        }

        let version = self
            .version
            .as_ref()
            .ok_or_else(|| "version metadata must be present for versioned messages".to_string())?;
        version.validate()?;

        if self.history_serial.is_none() {
            return Err("history_serial must be present for versioned messages".to_string());
        }

        let delivery_serial = self
            .delivery_serial
            .ok_or_else(|| "delivery_serial must be present for versioned messages".to_string())?;

        if self.message.serial != Some(delivery_serial) {
            return Err(
                "message.serial must match delivery_serial for versioned messages".to_string(),
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct UpdateMessageRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<MessageData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extras: Option<MessageExtras>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub clear_fields: Vec<ClearField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op_id: Option<String>,
}

impl UpdateMessageRequest {
    pub fn validate(&self) -> Result<(), String> {
        let has_patch = self.name.is_some()
            || self.data.is_some()
            || self.extras.is_some()
            || !self.clear_fields.is_empty()
            || self.client_id.is_some()
            || self.description.is_some()
            || self.metadata.is_some()
            || self.op_id.is_some();

        if !has_patch {
            return Err(
                "update request must include at least one patch field or operation metadata"
                    .to_string(),
            );
        }

        validate_unique_clear_fields(&self.clear_fields)?;
        validate_clear_field_conflicts(
            self.name.is_some(),
            self.data.is_some(),
            self.extras.is_some(),
            &self.clear_fields,
            "update",
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct DeleteMessageRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<MessageData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extras: Option<MessageExtras>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub clear_fields: Vec<ClearField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op_id: Option<String>,
}

impl DeleteMessageRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_unique_clear_fields(&self.clear_fields)?;
        validate_clear_field_conflicts(
            false,
            self.data.is_some(),
            self.extras.is_some(),
            &self.clear_fields,
            "delete",
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppendMessageRequest {
    pub data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extras: Option<MessageExtras>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op_id: Option<String>,
}

impl AppendMessageRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.data.is_empty() {
            return Err("append request data must not be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct MessageVersionsQuery {
    pub limit: Option<usize>,
    pub direction: Option<VersionDirection>,
    pub cursor: Option<String>,
}

impl MessageVersionsQuery {
    pub fn validate(&self) -> Result<(), String> {
        if self.limit == Some(0) {
            return Err("versions limit must be greater than 0".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MutationResponse {
    pub channel: String,
    pub message_serial: String,
    pub action: MessageAction,
    pub accepted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_serial: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_serial: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_serial: Option<u64>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GetMessageResponse {
    pub channel: String,
    pub item: VersionedRealtimeMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ListMessageVersionsResponse {
    pub channel: String,
    pub direction: VersionDirection,
    pub limit: usize,
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub items: Vec<VersionedRealtimeMessage>,
}

fn validate_unique_clear_fields(fields: &[ClearField]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for field in fields {
        if !seen.insert(*field) {
            return Err("clear_fields must not contain duplicates".to_string());
        }
    }
    Ok(())
}

fn validate_clear_field_conflicts(
    has_name: bool,
    has_data: bool,
    has_extras: bool,
    fields: &[ClearField],
    request_label: &str,
) -> Result<(), String> {
    for field in fields {
        let conflicts = match field {
            ClearField::Name => has_name,
            ClearField::Data => has_data,
            ClearField::Extras => has_extras,
        };
        if conflicts {
            return Err(format!(
                "{request_label} request cannot both replace and clear the same field"
            ));
        }
    }
    Ok(())
}

pub fn apply_runtime_metadata(
    message: &mut PusherMessage,
    action: MessageAction,
    message_serial: &str,
    version: &MessageVersionMetadata,
    history_serial: Option<u64>,
) {
    let extras = message.extras.get_or_insert_with(MessageExtras::default);
    let headers = extras.headers.get_or_insert_with(HashMap::new);
    headers.insert(
        HEADER_ACTION.to_string(),
        ExtrasValue::String(action.as_str().to_string()),
    );
    headers.insert(
        HEADER_MESSAGE_SERIAL.to_string(),
        ExtrasValue::String(message_serial.to_string()),
    );
    headers.insert(
        HEADER_VERSION_SERIAL.to_string(),
        ExtrasValue::String(version.serial.clone()),
    );
    headers.insert(
        HEADER_VERSION_TIMESTAMP_MS.to_string(),
        ExtrasValue::Number(version.timestamp_ms as f64),
    );
    if let Some(history_serial) = history_serial {
        headers.insert(
            HEADER_HISTORY_SERIAL.to_string(),
            ExtrasValue::Number(history_serial as f64),
        );
    }
}

pub fn extract_runtime_message_serial(message: &PusherMessage) -> Option<&str> {
    match message
        .extras
        .as_ref()
        .and_then(|extras| extras.headers.as_ref())
        .and_then(|headers| headers.get(HEADER_MESSAGE_SERIAL))
    {
        Some(ExtrasValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

pub fn extract_runtime_action(message: &PusherMessage) -> Option<MessageAction> {
    match message
        .extras
        .as_ref()
        .and_then(|extras| extras.headers.as_ref())
        .and_then(|headers| headers.get(HEADER_ACTION))
    {
        Some(ExtrasValue::String(value)) => match value.as_str() {
            "message.create" => Some(MessageAction::Create),
            "message.update" => Some(MessageAction::Update),
            "message.delete" => Some(MessageAction::Delete),
            "message.append" => Some(MessageAction::Append),
            "message.summary" => Some(MessageAction::Summary),
            _ => None,
        },
        _ => None,
    }
}

pub fn set_runtime_append_fragment(message: &mut PusherMessage, fragment: impl Into<String>) {
    let extras = message.extras.get_or_insert_with(MessageExtras::default);
    let headers = extras.headers.get_or_insert_with(HashMap::new);
    headers.insert(
        HEADER_APPEND_FRAGMENT.to_string(),
        ExtrasValue::String(fragment.into()),
    );
}

pub fn extract_runtime_append_fragment(message: &PusherMessage) -> Option<&str> {
    match message
        .extras
        .as_ref()
        .and_then(|extras| extras.headers.as_ref())
        .and_then(|headers| headers.get(HEADER_APPEND_FRAGMENT))
    {
        Some(ExtrasValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

pub fn clear_runtime_append_fragment(message: &mut PusherMessage) {
    let Some(extras) = message.extras.as_mut() else {
        return;
    };
    let Some(headers) = extras.headers.as_mut() else {
        return;
    };
    headers.remove(HEADER_APPEND_FRAGMENT);
    if headers.is_empty() {
        extras.headers = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::MessageData;
    use sonic_rs::json;

    #[test]
    fn update_request_rejects_empty_body() {
        let error = UpdateMessageRequest::default().validate().unwrap_err();
        assert!(error.contains("at least one patch field"));
    }

    #[test]
    fn append_request_rejects_empty_data() {
        let error = AppendMessageRequest {
            data: String::new(),
            extras: None,
            client_id: None,
            socket_id: None,
            description: None,
            metadata: None,
            op_id: None,
        }
        .validate()
        .unwrap_err();
        assert!(error.contains("must not be empty"));
    }

    #[test]
    fn versioned_realtime_message_validates_v2_event_name() {
        let message = VersionedRealtimeMessage {
            message: PusherMessage {
                event: Some("sockudo:message.update".to_string()),
                channel: Some("chat".to_string()),
                data: Some(MessageData::Json(json!({"hello": "world"}))),
                name: Some("chat.message".to_string()),
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: None,
                serial: Some(3),
                idempotency_key: None,
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
            action: MessageAction::Update,
            message_serial: "msg:1".to_string(),
            history_serial: Some(1),
            delivery_serial: Some(3),
            version: Some(MessageVersionMetadata {
                serial: "ver:2".to_string(),
                client_id: None,
                timestamp_ms: 1,
                description: None,
                metadata: None,
            }),
        };

        message.validate_v2().unwrap();
    }

    #[test]
    fn versioned_realtime_message_rejects_mismatched_event() {
        let message = VersionedRealtimeMessage {
            message: PusherMessage {
                event: Some("sockudo:message.delete".to_string()),
                channel: Some("chat".to_string()),
                data: Some(MessageData::String("hello".to_string())),
                name: Some("chat.message".to_string()),
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: None,
                serial: Some(3),
                idempotency_key: None,
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
            action: MessageAction::Update,
            message_serial: "msg:1".to_string(),
            history_serial: Some(1),
            delivery_serial: Some(3),
            version: Some(MessageVersionMetadata {
                serial: "ver:2".to_string(),
                client_id: None,
                timestamp_ms: 1,
                description: None,
                metadata: None,
            }),
        };

        let error = message.validate_v2().unwrap_err();
        assert!(error.contains("does not match action"));
    }

    #[test]
    fn versioned_realtime_message_requires_version_metadata() {
        let message = VersionedRealtimeMessage {
            message: PusherMessage {
                event: Some("sockudo:message.update".to_string()),
                channel: Some("chat".to_string()),
                data: Some(MessageData::String("hello".to_string())),
                name: Some("chat.message".to_string()),
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: None,
                serial: Some(3),
                idempotency_key: None,
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
            action: MessageAction::Update,
            message_serial: "msg:1".to_string(),
            history_serial: Some(1),
            delivery_serial: Some(3),
            version: None,
        };

        let error = message.validate_v2().unwrap_err();
        assert!(error.contains("version metadata"));
    }

    #[test]
    fn update_request_rejects_replace_and_clear_same_field() {
        let error = UpdateMessageRequest {
            name: Some("chat.message".to_string()),
            data: None,
            extras: None,
            clear_fields: vec![ClearField::Name],
            client_id: None,
            socket_id: None,
            description: None,
            metadata: None,
            op_id: None,
        }
        .validate()
        .unwrap_err();

        assert!(error.contains("cannot both replace and clear"));
    }

    #[test]
    fn delete_request_rejects_replace_and_clear_same_field() {
        let error = DeleteMessageRequest {
            data: Some(MessageData::String("gone".to_string())),
            extras: None,
            clear_fields: vec![ClearField::Data],
            client_id: None,
            socket_id: None,
            description: None,
            metadata: None,
            op_id: None,
        }
        .validate()
        .unwrap_err();

        assert!(error.contains("cannot both replace and clear"));
    }

    #[test]
    fn update_request_deserializes_string_data_via_sonic() {
        let request: UpdateMessageRequest =
            sonic_rs::from_str(r#"{"data":"hello brave","description":"replace base"}"#).unwrap();

        assert_eq!(
            request.data,
            Some(MessageData::String("hello brave".to_string()))
        );
        assert_eq!(request.description.as_deref(), Some("replace base"));
    }

    #[test]
    fn update_request_deserializes_string_data_via_serde_json() {
        let request: UpdateMessageRequest =
            serde_json::from_str(r#"{"data":"hello brave","description":"replace base"}"#).unwrap();

        assert_eq!(
            request.data,
            Some(MessageData::String("hello brave".to_string()))
        );
        assert_eq!(request.description.as_deref(), Some("replace base"));
    }

    #[test]
    fn delete_request_deserializes_string_data_via_serde_json() {
        let request: DeleteMessageRequest =
            serde_json::from_str(r#"{"data":"gone","description":"soft delete"}"#).unwrap();

        assert_eq!(request.data, Some(MessageData::String("gone".to_string())));
        assert_eq!(request.description.as_deref(), Some("soft delete"));
    }
}
