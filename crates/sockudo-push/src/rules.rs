use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::domain::PushPayload;

const MAX_CHANNEL_PATTERN_BYTES: usize = 256;
const MAX_EVENT_FILTERS: usize = 32;
const MAX_EVENT_NAME_BYTES: usize = 200;
const DEFAULT_TITLE_FIELD: &str = "title";
const DEFAULT_BODY_FIELD: &str = "body";
const DEFAULT_TEMPLATE_DATA_FIELD: &str = "data";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct PushRulePayloadMapping {
    pub title_field: String,
    pub body_field: String,
    pub template_data_field: String,
    pub include_remaining_fields: bool,
}

impl Default for PushRulePayloadMapping {
    fn default() -> Self {
        Self {
            title_field: DEFAULT_TITLE_FIELD.to_owned(),
            body_field: DEFAULT_BODY_FIELD.to_owned(),
            template_data_field: DEFAULT_TEMPLATE_DATA_FIELD.to_owned(),
            include_remaining_fields: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelPushRule {
    pub enabled: bool,
    pub channel_pattern: String,
    pub event_filter: Vec<String>,
    pub payload_mapping: PushRulePayloadMapping,
    pub rate_limit_per_second: u64,
}

impl Default for ChannelPushRule {
    fn default() -> Self {
        Self {
            enabled: true,
            channel_pattern: String::new(),
            event_filter: Vec::new(),
            payload_mapping: PushRulePayloadMapping::default(),
            rate_limit_per_second: 100,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PushRuleError {
    #[error("channel_pattern must not be empty")]
    EmptyChannelPattern,
    #[error("channel_pattern is too long")]
    ChannelPatternTooLong,
    #[error("channel_pattern supports only exact, *, or trailing wildcard patterns")]
    InvalidChannelPattern,
    #[error("event_filter must contain at least one event name")]
    EmptyEventFilter,
    #[error("event_filter contains too many event names")]
    TooManyEventFilters,
    #[error("event_filter contains an empty event name")]
    EmptyEventName,
    #[error("event_filter contains an event name that is too long")]
    EventNameTooLong,
    #[error("rate_limit_per_second must be greater than zero")]
    InvalidRateLimit,
    #[error("payload_mapping field names must not be empty")]
    EmptyMappingField,
    #[error("message data must be a JSON object for channel push rules")]
    MessageDataNotObject,
    #[error("message data is missing required title field")]
    MissingTitle,
    #[error("message data title must be a string")]
    InvalidTitle,
    #[error("message data is missing required body field")]
    MissingBody,
    #[error("message data body must be a string")]
    InvalidBody,
    #[error("push payload is invalid: {0}")]
    InvalidPayload(String),
}

impl ChannelPushRule {
    pub fn validate(&self) -> Result<(), PushRuleError> {
        validate_channel_pattern(&self.channel_pattern)?;
        if self.event_filter.is_empty() {
            return Err(PushRuleError::EmptyEventFilter);
        }
        if self.event_filter.len() > MAX_EVENT_FILTERS {
            return Err(PushRuleError::TooManyEventFilters);
        }
        for event in &self.event_filter {
            if event.is_empty() {
                return Err(PushRuleError::EmptyEventName);
            }
            if event.len() > MAX_EVENT_NAME_BYTES {
                return Err(PushRuleError::EventNameTooLong);
            }
        }
        if self.rate_limit_per_second == 0 {
            return Err(PushRuleError::InvalidRateLimit);
        }
        self.payload_mapping.validate()
    }

    #[inline]
    pub fn matches(&self, channel: &str, event: &str) -> bool {
        self.enabled
            && channel_pattern_matches(&self.channel_pattern, channel)
            && self
                .event_filter
                .iter()
                .any(|candidate| candidate.as_str() == event)
    }

    pub fn map_payload(&self, message_data: &Value) -> Result<PushPayload, PushRuleError> {
        self.payload_mapping.map_payload(message_data)
    }
}

impl PushRulePayloadMapping {
    fn validate(&self) -> Result<(), PushRuleError> {
        if self.title_field.is_empty()
            || self.body_field.is_empty()
            || self.template_data_field.is_empty()
        {
            return Err(PushRuleError::EmptyMappingField);
        }
        Ok(())
    }

    fn map_payload(&self, message_data: &Value) -> Result<PushPayload, PushRuleError> {
        let object = message_data
            .as_object()
            .ok_or(PushRuleError::MessageDataNotObject)?;
        let title = required_string(object, &self.title_field, true)?;
        let body = required_string(object, &self.body_field, false)?;
        let template_data = if self.include_remaining_fields {
            let mut data = Map::with_capacity(object.len().saturating_sub(2));
            for (key, value) in object {
                if key != &self.title_field && key != &self.body_field {
                    data.insert(key.clone(), value.clone());
                }
            }
            json!({ self.template_data_field.clone(): Value::Object(data) })
        } else {
            Value::Object(Map::new())
        };
        let payload = PushPayload {
            template_id: None,
            template_data,
            title: Some(title),
            body: Some(body),
            icon: None,
            sound: None,
            collapse_key: None,
        };
        payload
            .validate()
            .map_err(|error| PushRuleError::InvalidPayload(error.to_string()))?;
        Ok(payload)
    }
}

#[inline]
pub fn matching_rule_indices(rules: &[ChannelPushRule], channel: &str, event: &str) -> Vec<usize> {
    rules
        .iter()
        .enumerate()
        .filter_map(|(index, rule)| rule.matches(channel, event).then_some(index))
        .collect()
}

#[inline]
pub fn any_rule_matches(rules: &[ChannelPushRule], channel: &str, event: &str) -> bool {
    rules.iter().any(|rule| rule.matches(channel, event))
}

fn required_string(
    object: &Map<String, Value>,
    field: &str,
    is_title: bool,
) -> Result<String, PushRuleError> {
    let value = object.get(field).ok_or(if is_title {
        PushRuleError::MissingTitle
    } else {
        PushRuleError::MissingBody
    })?;
    value.as_str().map(str::to_owned).ok_or(if is_title {
        PushRuleError::InvalidTitle
    } else {
        PushRuleError::InvalidBody
    })
}

fn validate_channel_pattern(pattern: &str) -> Result<(), PushRuleError> {
    if pattern.is_empty() {
        return Err(PushRuleError::EmptyChannelPattern);
    }
    if pattern.len() > MAX_CHANNEL_PATTERN_BYTES {
        return Err(PushRuleError::ChannelPatternTooLong);
    }
    if pattern == "*" {
        return Ok(());
    }
    if let Some(stem) = pattern.strip_suffix('*') {
        if stem.is_empty() || stem.contains('*') {
            return Err(PushRuleError::InvalidChannelPattern);
        }
        return Ok(());
    }
    if pattern.contains('*') {
        return Err(PushRuleError::InvalidChannelPattern);
    }
    Ok(())
}

#[inline]
fn channel_pattern_matches(pattern: &str, channel: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return channel.starts_with(prefix);
    }
    pattern == channel
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_and_wildcard_rules_match_expected_channels() {
        let exact = rule("notifications:user-1", &["agent-complete"]);
        assert!(exact.matches("notifications:user-1", "agent-complete"));
        assert!(!exact.matches("notifications:user-2", "agent-complete"));

        let wildcard = rule("notifications:*", &["agent-complete"]);
        assert!(wildcard.matches("notifications:user-1", "agent-complete"));
        assert!(!wildcard.matches("private:user-1", "agent-complete"));
        assert!(!wildcard.matches("notifications:user-1", "agent-start"));
    }

    #[test]
    fn validation_rejects_ambiguous_or_unbounded_rules() {
        assert_eq!(
            rule("", &["agent-complete"]).validate(),
            Err(PushRuleError::EmptyChannelPattern)
        );
        assert_eq!(
            rule("notifications:*:bad", &["agent-complete"]).validate(),
            Err(PushRuleError::InvalidChannelPattern)
        );
        assert_eq!(
            rule("notifications:*", &[]).validate(),
            Err(PushRuleError::EmptyEventFilter)
        );
        let mut invalid = rule("notifications:*", &["agent-complete"]);
        invalid.rate_limit_per_second = 0;
        assert_eq!(invalid.validate(), Err(PushRuleError::InvalidRateLimit));
    }

    #[test]
    fn payload_mapping_extracts_title_body_and_remaining_data() {
        let rule = rule("notifications:*", &["agent-complete"]);
        let payload = rule
            .map_payload(&json!({
                "title": "Done",
                "body": "Agent finished",
                "sessionId": "sess-1"
            }))
            .unwrap();

        assert_eq!(payload.title.as_deref(), Some("Done"));
        assert_eq!(payload.body.as_deref(), Some("Agent finished"));
        assert_eq!(payload.template_data["data"]["sessionId"], "sess-1");
        assert!(payload.template_data["data"].get("title").is_none());
    }

    #[test]
    fn payload_mapping_rejects_malformed_data() {
        let rule = rule("notifications:*", &["agent-complete"]);
        assert_eq!(
            rule.map_payload(&json!("not-object")),
            Err(PushRuleError::MessageDataNotObject)
        );
        assert_eq!(
            rule.map_payload(&json!({"body": "Body"})),
            Err(PushRuleError::MissingTitle)
        );
        assert_eq!(
            rule.map_payload(&json!({"title": "Title", "body": 1})),
            Err(PushRuleError::InvalidBody)
        );
    }

    fn rule(pattern: &str, events: &[&str]) -> ChannelPushRule {
        ChannelPushRule {
            channel_pattern: pattern.to_owned(),
            event_filter: events.iter().map(|event| (*event).to_owned()).collect(),
            ..ChannelPushRule::default()
        }
    }
}
