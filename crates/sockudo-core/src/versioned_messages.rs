use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use sockudo_protocol::messages::{MessageData, MessageExtras};
use sonic_rs::Value;
use std::collections::HashSet;

pub const MAX_VERSIONED_SERIAL_LENGTH: usize = 128;

/// Stable logical identity for a mutable message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct MessageSerial(String);

impl MessageSerial {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_serial("message_serial", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Identity for one specific version of a logical message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct VersionSerial(String);

impl VersionSerial {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_serial("version_serial", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_serial(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::InvalidMessageFormat(format!(
            "{label} must not be empty"
        )));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(Error::InvalidMessageFormat(format!(
            "{label} must not contain whitespace"
        )));
    }
    if value.len() > MAX_VERSIONED_SERIAL_LENGTH {
        return Err(Error::InvalidMessageFormat(format!(
            "{label} must be at most {MAX_VERSIONED_SERIAL_LENGTH} bytes"
        )));
    }
    Ok(())
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageIdentity {
    pub message_serial: MessageSerial,
    pub history_serial: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayPosition {
    pub delivery_serial: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionMetadata {
    pub serial: VersionSerial,
    pub client_id: Option<String>,
    pub timestamp_ms: i64,
    pub description: Option<String>,
    pub metadata: Option<Value>,
}

/// Tri-state update operator for mutable message fields.
///
/// `Keep` preserves the previous value, `Clear` sets the field to `None`,
/// and `Replace` writes a new value.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "op", content = "value", rename_all = "snake_case")]
pub enum FieldPatch<T> {
    #[default]
    Keep,
    Clear,
    Replace(T),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MessageFieldDelta {
    #[serde(default)]
    pub name: FieldPatch<String>,
    #[serde(default)]
    pub data: FieldPatch<MessageData>,
    #[serde(default)]
    pub extras: FieldPatch<MessageExtras>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageAppend {
    pub data_fragment: String,
    pub extras: Option<MessageExtras>,
}

/// Domain model for one concrete version of a mutable V2 message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedMessage {
    pub action: MessageAction,
    pub identity: MessageIdentity,
    pub replay_position: ReplayPosition,
    pub version: VersionMetadata,
    pub name: Option<String>,
    pub data: Option<MessageData>,
    pub extras: Option<MessageExtras>,
}

impl VersionedMessage {
    pub fn new_create(
        message_serial: MessageSerial,
        version: VersionMetadata,
        history_serial: u64,
        delivery_serial: u64,
        name: Option<String>,
        data: Option<MessageData>,
        extras: Option<MessageExtras>,
    ) -> Self {
        Self {
            action: MessageAction::Create,
            identity: MessageIdentity {
                message_serial,
                history_serial,
            },
            replay_position: ReplayPosition { delivery_serial },
            version,
            name,
            data,
            extras,
        }
    }

    pub fn apply_mutation(
        &self,
        action: MessageAction,
        version: VersionMetadata,
        delivery_serial: u64,
        delta: MessageFieldDelta,
    ) -> Result<Self> {
        match action {
            MessageAction::Update | MessageAction::Delete => {}
            _ => {
                return Err(Error::InvalidMessageFormat(format!(
                    "{} does not use shallow-mixin mutation semantics",
                    action.as_str()
                )));
            }
        }

        self.validate_next_version(&version, delivery_serial)?;

        Ok(Self {
            action,
            identity: self.identity.clone(),
            replay_position: ReplayPosition { delivery_serial },
            version,
            name: apply_field_patch(&self.name, &delta.name),
            data: apply_field_patch(&self.data, &delta.data),
            extras: apply_field_patch(&self.extras, &delta.extras),
        })
    }

    pub fn apply_append(
        &self,
        version: VersionMetadata,
        delivery_serial: u64,
        append: MessageAppend,
    ) -> Result<Self> {
        self.validate_next_version(&version, delivery_serial)?;

        let data = Some(append_message_data(
            self.data.as_ref(),
            &append.data_fragment,
        )?);

        Ok(Self {
            action: MessageAction::Append,
            identity: self.identity.clone(),
            replay_position: ReplayPosition { delivery_serial },
            version,
            name: self.name.clone(),
            data,
            extras: append.extras.or_else(|| self.extras.clone()),
        })
    }

    pub fn select_latest_visible<'a, I>(versions: I) -> Result<&'a Self>
    where
        I: IntoIterator<Item = &'a Self>,
    {
        let mut iter = versions.into_iter();
        let first = iter
            .next()
            .ok_or_else(|| Error::InvalidMessageFormat("version chain must not be empty".into()))?;

        let mut winner = first;
        for candidate in iter {
            ensure_same_chain(first, candidate)?;
            if candidate.version.serial > winner.version.serial {
                winner = candidate;
            }
        }

        Ok(winner)
    }

    fn validate_next_version(&self, version: &VersionMetadata, delivery_serial: u64) -> Result<()> {
        if version.serial <= self.version.serial {
            return Err(Error::InvalidMessageFormat(format!(
                "next version_serial {} must be greater than current {}",
                version.serial.as_str(),
                self.version.serial.as_str()
            )));
        }
        if delivery_serial <= self.replay_position.delivery_serial {
            return Err(Error::InvalidMessageFormat(format!(
                "next delivery_serial {delivery_serial} must be greater than current {}",
                self.replay_position.delivery_serial
            )));
        }
        Ok(())
    }
}

pub fn validate_version_chain(versions: &[VersionedMessage]) -> Result<()> {
    let Some(first) = versions.first() else {
        return Err(Error::InvalidMessageFormat(
            "version chain must not be empty".to_string(),
        ));
    };

    let mut version_serials = HashSet::new();
    let mut delivery_serials = HashSet::new();

    for version in versions {
        ensure_same_chain(first, version)?;

        if !version_serials.insert(version.version.serial.as_str().to_string()) {
            return Err(Error::InvalidMessageFormat(format!(
                "duplicate version_serial {} in version chain",
                version.version.serial.as_str()
            )));
        }

        if !delivery_serials.insert(version.replay_position.delivery_serial) {
            return Err(Error::InvalidMessageFormat(format!(
                "duplicate delivery_serial {} in version chain",
                version.replay_position.delivery_serial
            )));
        }
    }

    Ok(())
}

pub fn validate_replay_continuity(
    replay: &[VersionedMessage],
    requested_after_serial: u64,
) -> Result<()> {
    let mut expected = requested_after_serial.saturating_add(1);

    for version in replay {
        if version.replay_position.delivery_serial != expected {
            return Err(Error::InvalidMessageFormat(format!(
                "replay gap detected: expected delivery_serial {expected}, got {}",
                version.replay_position.delivery_serial
            )));
        }
        expected = expected.saturating_add(1);
    }

    Ok(())
}

fn ensure_same_chain(expected: &VersionedMessage, actual: &VersionedMessage) -> Result<()> {
    if expected.identity.message_serial != actual.identity.message_serial {
        return Err(Error::InvalidMessageFormat(format!(
            "mixed message_serial values in one version chain: {} vs {}",
            expected.identity.message_serial.as_str(),
            actual.identity.message_serial.as_str()
        )));
    }

    if expected.identity.history_serial != actual.identity.history_serial {
        return Err(Error::InvalidMessageFormat(format!(
            "mixed history_serial values in one version chain: {} vs {}",
            expected.identity.history_serial, actual.identity.history_serial
        )));
    }

    Ok(())
}

fn apply_field_patch<T: Clone>(current: &Option<T>, patch: &FieldPatch<T>) -> Option<T> {
    match patch {
        FieldPatch::Keep => current.clone(),
        FieldPatch::Clear => None,
        FieldPatch::Replace(value) => Some(value.clone()),
    }
}

fn append_message_data(current: Option<&MessageData>, fragment: &str) -> Result<MessageData> {
    match current {
        None => Ok(MessageData::String(fragment.to_string())),
        Some(MessageData::String(existing)) => {
            Ok(MessageData::String(format!("{existing}{fragment}")))
        }
        Some(_) => Err(Error::InvalidMessageFormat(
            "append requires string message data in the release 4.3 core model".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::json;

    fn version(serial: &str, timestamp_ms: i64) -> VersionMetadata {
        VersionMetadata {
            serial: VersionSerial::new(serial).unwrap(),
            client_id: Some("user-1".to_string()),
            timestamp_ms,
            description: None,
            metadata: None,
        }
    }

    fn base_message() -> VersionedMessage {
        VersionedMessage::new_create(
            MessageSerial::new("msg:1").unwrap(),
            version("ver:1", 1),
            10,
            20,
            Some("event-created".to_string()),
            Some(MessageData::String("hello".to_string())),
            Some(MessageExtras {
                headers: None,
                ephemeral: Some(false),
                idempotency_key: None,
                push: None,
                echo: Some(true),
                ai: None,
            }),
        )
    }

    #[test]
    fn update_keeps_omitted_fields() {
        let current = base_message();

        let updated = current
            .apply_mutation(
                MessageAction::Update,
                version("ver:2", 2),
                21,
                MessageFieldDelta {
                    data: FieldPatch::Replace(MessageData::String("patched".to_string())),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(updated.action, MessageAction::Update);
        assert_eq!(updated.name.as_deref(), Some("event-created"));
        assert_eq!(
            updated.data.unwrap().into_string().as_deref(),
            Some("patched")
        );
        assert_eq!(updated.extras.and_then(|extras| extras.echo), Some(true));
        assert_eq!(
            updated.identity.message_serial.as_str(),
            current.identity.message_serial.as_str()
        );
        assert_eq!(
            updated.identity.history_serial,
            current.identity.history_serial
        );
        assert_eq!(updated.replay_position.delivery_serial, 21);
    }

    #[test]
    fn delete_can_clear_visible_fields() {
        let current = base_message();

        let deleted = current
            .apply_mutation(
                MessageAction::Delete,
                version("ver:2", 2),
                21,
                MessageFieldDelta {
                    data: FieldPatch::Clear,
                    extras: FieldPatch::Clear,
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(deleted.action, MessageAction::Delete);
        assert!(deleted.data.is_none());
        assert!(deleted.extras.is_none());
        assert_eq!(deleted.name.as_deref(), Some("event-created"));
    }

    #[test]
    fn update_distinguishes_explicit_empty_from_clear() {
        let current = base_message();

        let updated = current
            .apply_mutation(
                MessageAction::Update,
                version("ver:2", 2),
                21,
                MessageFieldDelta {
                    name: FieldPatch::Replace(String::new()),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(updated.name.as_deref(), Some(""));
    }

    #[test]
    fn append_rolls_up_full_string_state() {
        let current = base_message();

        let appended = current
            .apply_append(
                version("ver:2", 2),
                21,
                MessageAppend {
                    data_fragment: " world".to_string(),
                    extras: None,
                },
            )
            .unwrap();

        assert_eq!(appended.action, MessageAction::Append);
        assert_eq!(
            appended.data.unwrap().into_string().as_deref(),
            Some("hello world")
        );
    }

    #[test]
    fn append_rejects_non_string_payloads() {
        let current = VersionedMessage::new_create(
            MessageSerial::new("msg:1").unwrap(),
            version("ver:1", 1),
            10,
            20,
            Some("event-created".to_string()),
            Some(MessageData::Json(json!({"hello": "world"}))),
            None,
        );

        let error = current
            .apply_append(
                version("ver:2", 2),
                21,
                MessageAppend {
                    data_fragment: "!".to_string(),
                    extras: None,
                },
            )
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("append requires string message data"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn latest_visible_winner_is_highest_version_serial() {
        let current = base_message();
        let older = current
            .apply_mutation(
                MessageAction::Update,
                version("ver:2", 2),
                21,
                MessageFieldDelta {
                    data: FieldPatch::Replace(MessageData::String("older".to_string())),
                    ..Default::default()
                },
            )
            .unwrap();
        let winner = older
            .apply_mutation(
                MessageAction::Update,
                version("ver:9", 3),
                22,
                MessageFieldDelta {
                    data: FieldPatch::Replace(MessageData::String("winner".to_string())),
                    ..Default::default()
                },
            )
            .unwrap();

        let selected =
            VersionedMessage::select_latest_visible([&winner, &current, &older]).unwrap();

        assert_eq!(selected.version.serial.as_str(), "ver:9");
        assert_eq!(
            selected.data.clone().unwrap().into_string().as_deref(),
            Some("winner")
        );
    }

    #[test]
    fn replay_continuity_requires_contiguous_delivery_serials() {
        let current = base_message();
        let next = current
            .apply_mutation(
                MessageAction::Update,
                version("ver:2", 2),
                22,
                MessageFieldDelta::default(),
            )
            .unwrap();

        let error = validate_replay_continuity(&[next], 20).unwrap_err();
        assert!(
            error.to_string().contains("replay gap detected"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn version_chain_rejects_mixed_message_identity() {
        let left = base_message();
        let right = VersionedMessage::new_create(
            MessageSerial::new("msg:2").unwrap(),
            version("ver:1", 1),
            10,
            20,
            Some("event-created".to_string()),
            Some(MessageData::String("hello".to_string())),
            None,
        );

        let error = validate_version_chain(&[left, right]).unwrap_err();
        assert!(
            error.to_string().contains("mixed message_serial"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn version_chain_rejects_mixed_history_identity() {
        let left = base_message();
        let mut right = left
            .apply_mutation(
                MessageAction::Update,
                version("ver:2", 2),
                21,
                MessageFieldDelta::default(),
            )
            .unwrap();
        right.identity.history_serial = 999;

        let error = validate_version_chain(&[left, right]).unwrap_err();
        assert!(
            error.to_string().contains("mixed history_serial"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn version_chain_rejects_duplicate_version_serial() {
        let left = base_message();
        let mut right = left.clone();
        right.replay_position.delivery_serial = 21;

        let error = validate_version_chain(&[left, right]).unwrap_err();
        assert!(
            error.to_string().contains("duplicate version_serial"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn serials_reject_values_longer_than_storage_limit() {
        let over_limit = "x".repeat(MAX_VERSIONED_SERIAL_LENGTH + 1);

        let message_error = MessageSerial::new(over_limit.clone()).unwrap_err();
        assert!(
            message_error.to_string().contains("at most"),
            "unexpected error: {message_error}"
        );

        let version_error = VersionSerial::new(over_limit).unwrap_err();
        assert!(
            version_error.to_string().contains("at most"),
            "unexpected error: {version_error}"
        );
    }
}
