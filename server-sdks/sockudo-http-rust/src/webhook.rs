use crate::{Result, SockudoError, Token, WebhookError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Webhook for validating and accessing Sockudo webhook data
#[derive(Debug)]
pub struct Webhook {
    token: Token,
    key: Option<String>,
    signature: Option<String>,
    content_type: Option<String>,
    body: String,
    data: Option<WebhookData>,
}

/// Webhook data structure matching Sockudo's format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookData {
    /// The timestamp of the webhook in milliseconds
    pub time_ms: i64,
    /// The events received with the webhook
    pub events: Vec<HashMap<String, String>>,
}

/// Strongly typed webhook event
#[derive(Debug, Clone, PartialEq)]
pub enum WebhookEvent {
    ChannelOccupied {
        channel: String,
    },
    ChannelVacated {
        channel: String,
    },
    MemberAdded {
        channel: String,
        user_id: String,
    },
    MemberRemoved {
        channel: String,
        user_id: String,
    },
    ClientEvent {
        channel: String,
        event: String,
        data: String,
        socket_id: String,
        user_id: Option<String>,
    },
    CacheMiss {
        channel: String,
        event: String,
    },
    Unknown(HashMap<String, String>),
}

impl Webhook {
    /// Creates a new webhook from request data
    pub fn new(token: &Token, headers: &BTreeMap<String, String>, body: &str) -> Self {
        // Normalize header names to lowercase for case-insensitive lookup
        let normalized_headers: BTreeMap<String, String> = headers
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v.clone()))
            .collect();

        let key = normalized_headers.get("x-sockudo-key").cloned();
        let signature = normalized_headers.get("x-sockudo-signature").cloned();
        let content_type = normalized_headers.get("content-type").cloned();

        let data = if Self::validate_content_type(&content_type) {
            sonic_rs::from_str::<WebhookData>(body).ok()
        } else {
            None
        };

        Self {
            token: token.clone(),
            key,
            signature,
            content_type,
            body: body.to_string(),
            data,
        }
    }

    /// Validates the webhook signature and content
    pub fn is_valid(&self, extra_tokens: Option<&[Token]>) -> bool {
        if !self.is_body_valid() {
            return false;
        }

        let tokens_to_check = if let Some(extra) = extra_tokens {
            let mut tokens = vec![&self.token];
            tokens.extend(extra.iter());
            tokens
        } else {
            vec![&self.token]
        };

        if let (Some(key), Some(signature)) = (&self.key, &self.signature) {
            for token in tokens_to_check {
                if key == &token.key && token.verify(&self.body, signature) {
                    return true;
                }
            }
        }

        false
    }

    /// Checks if the content type is valid (application/json)
    pub fn is_content_type_valid(&self) -> bool {
        Self::validate_content_type(&self.content_type)
    }

    /// Private helper method to validate content type
    fn validate_content_type(content_type: &Option<String>) -> bool {
        match content_type {
            Some(ct) => ct.starts_with("application/json"),
            None => false,
        }
    }

    /// Checks if the body is valid JSON
    pub fn is_body_valid(&self) -> bool {
        self.data.is_some()
    }

    /// Gets the parsed webhook data
    pub fn get_data(&self) -> Result<&WebhookData> {
        self.data.as_ref().ok_or_else(|| {
            SockudoError::Webhook(WebhookError::new(
                "Invalid webhook body",
                self.content_type.clone(),
                &self.body,
                self.signature.clone(),
            ))
        })
    }

    /// Gets the raw events from webhook data
    pub fn get_raw_events(&self) -> Result<&Vec<HashMap<String, String>>> {
        Ok(&self.get_data()?.events)
    }

    /// Gets the events as strongly typed enums
    pub fn get_events(&self) -> Result<Vec<WebhookEvent>> {
        let raw_events = self.get_raw_events()?;
        Ok(raw_events.iter().map(parse_webhook_event).collect())
    }

    /// Gets the timestamp from webhook data
    pub fn get_time(&self) -> Result<std::time::SystemTime> {
        let time_ms = self.get_data()?.time_ms;
        if time_ms < 0 {
            return Err(SockudoError::Webhook(WebhookError::new(
                "Invalid negative timestamp",
                self.content_type.clone(),
                &self.body,
                self.signature.clone(),
            )));
        }
        let duration = std::time::Duration::from_millis(time_ms as u64);
        Ok(std::time::UNIX_EPOCH + duration)
    }

    /// Gets the raw body
    pub fn body(&self) -> &str {
        &self.body
    }

    /// Gets the signature
    pub fn signature(&self) -> Option<&str> {
        self.signature.as_deref()
    }

    /// Gets the key from headers
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    /// Finds events by type
    pub fn find_events_by_type(&self, event_type: &str) -> Result<Vec<WebhookEvent>> {
        let events = self.get_events()?;
        Ok(events
            .into_iter()
            .filter(|e| e.event_name() == event_type)
            .collect())
    }

    /// Finds events by channel
    pub fn find_events_by_channel(&self, channel: &str) -> Result<Vec<WebhookEvent>> {
        let events = self.get_events()?;
        Ok(events
            .into_iter()
            .filter(|e| e.channel() == Some(channel))
            .collect())
    }
}

/// Parses a raw webhook event into a strongly typed event
fn parse_webhook_event(raw: &HashMap<String, String>) -> WebhookEvent {
    match raw.get("name").map(|s| s.as_str()) {
        Some("channel_occupied") => {
            if let Some(channel) = raw.get("channel") {
                WebhookEvent::ChannelOccupied {
                    channel: channel.clone(),
                }
            } else {
                WebhookEvent::Unknown(raw.clone())
            }
        }
        Some("channel_vacated") => {
            if let Some(channel) = raw.get("channel") {
                WebhookEvent::ChannelVacated {
                    channel: channel.clone(),
                }
            } else {
                WebhookEvent::Unknown(raw.clone())
            }
        }
        Some("member_added") => {
            if let (Some(channel), Some(user_id)) = (raw.get("channel"), raw.get("user_id")) {
                WebhookEvent::MemberAdded {
                    channel: channel.clone(),
                    user_id: user_id.clone(),
                }
            } else {
                WebhookEvent::Unknown(raw.clone())
            }
        }
        Some("member_removed") => {
            if let (Some(channel), Some(user_id)) = (raw.get("channel"), raw.get("user_id")) {
                WebhookEvent::MemberRemoved {
                    channel: channel.clone(),
                    user_id: user_id.clone(),
                }
            } else {
                WebhookEvent::Unknown(raw.clone())
            }
        }
        Some("client_event") => {
            if let (Some(channel), Some(event), Some(data), Some(socket_id)) = (
                raw.get("channel"),
                raw.get("event"),
                raw.get("data"),
                raw.get("socket_id"),
            ) {
                WebhookEvent::ClientEvent {
                    channel: channel.clone(),
                    event: event.clone(),
                    data: data.clone(),
                    socket_id: socket_id.clone(),
                    user_id: raw.get("user_id").cloned(),
                }
            } else {
                WebhookEvent::Unknown(raw.clone())
            }
        }
        Some("cache_miss") => {
            if let (Some(channel), Some(event)) = (raw.get("channel"), raw.get("event")) {
                WebhookEvent::CacheMiss {
                    channel: channel.clone(),
                    event: event.clone(),
                }
            } else {
                WebhookEvent::Unknown(raw.clone())
            }
        }
        _ => WebhookEvent::Unknown(raw.clone()),
    }
}

impl WebhookEvent {
    /// Gets the event name
    pub fn event_name(&self) -> &str {
        match self {
            WebhookEvent::ChannelOccupied { .. } => "channel_occupied",
            WebhookEvent::ChannelVacated { .. } => "channel_vacated",
            WebhookEvent::MemberAdded { .. } => "member_added",
            WebhookEvent::MemberRemoved { .. } => "member_removed",
            WebhookEvent::ClientEvent { .. } => "client_event",
            WebhookEvent::CacheMiss { .. } => "cache_miss",
            WebhookEvent::Unknown(map) => map.get("name").map(|s| s.as_str()).unwrap_or("unknown"),
        }
    }

    /// Gets the channel name if applicable
    pub fn channel(&self) -> Option<&str> {
        match self {
            WebhookEvent::ChannelOccupied { channel }
            | WebhookEvent::ChannelVacated { channel }
            | WebhookEvent::MemberAdded { channel, .. }
            | WebhookEvent::MemberRemoved { channel, .. }
            | WebhookEvent::ClientEvent { channel, .. }
            | WebhookEvent::CacheMiss { channel, .. } => Some(channel),
            WebhookEvent::Unknown(map) => map.get("channel").map(|s| s.as_str()),
        }
    }

    /// Gets the user ID if applicable
    pub fn user_id(&self) -> Option<&str> {
        match self {
            WebhookEvent::MemberAdded { user_id, .. }
            | WebhookEvent::MemberRemoved { user_id, .. } => Some(user_id),
            WebhookEvent::ClientEvent { user_id, .. } => user_id.as_deref(),
            WebhookEvent::Unknown(map) => map.get("user_id").map(|s| s.as_str()),
            _ => None,
        }
    }

    /// Converts the event back to a HashMap
    pub fn to_hashmap(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();

        match self {
            WebhookEvent::ChannelOccupied { channel } => {
                map.insert("name".to_string(), "channel_occupied".to_string());
                map.insert("channel".to_string(), channel.clone());
            }
            WebhookEvent::ChannelVacated { channel } => {
                map.insert("name".to_string(), "channel_vacated".to_string());
                map.insert("channel".to_string(), channel.clone());
            }
            WebhookEvent::MemberAdded { channel, user_id } => {
                map.insert("name".to_string(), "member_added".to_string());
                map.insert("channel".to_string(), channel.clone());
                map.insert("user_id".to_string(), user_id.clone());
            }
            WebhookEvent::MemberRemoved { channel, user_id } => {
                map.insert("name".to_string(), "member_removed".to_string());
                map.insert("channel".to_string(), channel.clone());
                map.insert("user_id".to_string(), user_id.clone());
            }
            WebhookEvent::ClientEvent {
                channel,
                event,
                data,
                socket_id,
                user_id,
            } => {
                map.insert("name".to_string(), "client_event".to_string());
                map.insert("channel".to_string(), channel.clone());
                map.insert("event".to_string(), event.clone());
                map.insert("data".to_string(), data.clone());
                map.insert("socket_id".to_string(), socket_id.clone());
                if let Some(uid) = user_id {
                    map.insert("user_id".to_string(), uid.clone());
                }
            }
            WebhookEvent::CacheMiss { channel, event } => {
                map.insert("name".to_string(), "cache_miss".to_string());
                map.insert("channel".to_string(), channel.clone());
                map.insert("event".to_string(), event.clone());
            }
            WebhookEvent::Unknown(original) => {
                return original.clone();
            }
        }

        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_webhook_data_parsing() {
        let json_str = r#"{
            "time_ms": 1234567890,
            "events": [
                {"name": "channel_occupied", "channel": "test-channel"},
                {"name": "member_added", "channel": "presence-channel", "user_id": "user123"}
            ]
        }"#;

        let data: WebhookData = sonic_rs::from_str(json_str).unwrap();
        assert_eq!(data.time_ms, 1234567890);
        assert_eq!(data.events.len(), 2);
        assert_eq!(
            data.events[0].get("name"),
            Some(&"channel_occupied".to_string())
        );
    }

    #[test]
    fn test_webhook_validation() {
        let token = Token::new("test_key", "test_secret");
        let body = r#"{"time_ms": 1234567890, "events": []}"#;
        let signature = token.sign(body);

        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        headers.insert("x-sockudo-key".to_string(), "test_key".to_string());
        headers.insert("x-sockudo-signature".to_string(), signature);

        let webhook = Webhook::new(&token, &headers, body);
        assert!(webhook.is_valid(None));
    }

    #[test]
    fn test_event_parsing() {
        let mut event_map = HashMap::new();
        event_map.insert("name".to_string(), "channel_occupied".to_string());
        event_map.insert("channel".to_string(), "test-channel".to_string());

        let event = parse_webhook_event(&event_map);
        assert!(matches!(event, WebhookEvent::ChannelOccupied { .. }));
        assert_eq!(event.channel(), Some("test-channel"));
    }

    #[test]
    fn test_event_round_trip() {
        let event = WebhookEvent::MemberAdded {
            channel: "presence-test".to_string(),
            user_id: "user123".to_string(),
        };

        let map = event.to_hashmap();
        let parsed = parse_webhook_event(&map);

        assert_eq!(event, parsed);
    }
}
