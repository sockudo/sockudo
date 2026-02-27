use crate::webhook::types::Webhook;
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use serde_aux::field_attributes::{
    deserialize_number_from_string, deserialize_option_number_from_string,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct App {
    pub id: String,
    pub key: String,
    pub secret: String,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub max_connections: u32,
    pub enable_client_messages: bool,
    pub enabled: bool,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_backend_events_per_second: Option<u32>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub max_client_events_per_second: u32,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_read_requests_per_second: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_presence_members_per_channel: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_presence_member_size_in_kb: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_channel_name_length: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_event_channels_at_once: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_event_name_length: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_event_payload_in_kb: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_number_from_string")]
    pub max_event_batch_size: Option<u32>,
    #[serde(default)]
    pub enable_user_authentication: Option<bool>,
    #[serde(default)]
    pub webhooks: Option<Vec<Webhook>>,
    #[serde(default)]
    pub enable_watchlist_events: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_and_validate_origins")]
    pub allowed_origins: Option<Vec<String>>,
    #[serde(default)]
    pub channel_delta_compression:
        Option<AHashMap<String, crate::delta_compression::ChannelDeltaConfig>>,
}

fn deserialize_and_validate_origins<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let value = Option::<Vec<String>>::deserialize(deserializer)?;

    if let Some(ref origins) = value {
        // Validate origin patterns at configuration load time
        if let Err(validation_error) =
            crate::adapter::handler::origin_validation::OriginValidator::validate_patterns(origins)
        {
            return Err(D::Error::custom(format!(
                "Origin pattern validation failed: {}",
                validation_error
            )));
        }
    }

    Ok(value)
}

// Implementation with default values

#[cfg(test)]
mod tests {
    use super::*;

    fn test_app_json(overrides: &str) -> String {
        format!(
            r#"{{"id":"test","key":"key","secret":"secret","max_connections":100,"enable_client_messages":false,"enabled":true,"max_client_events_per_second":100{overrides}}}"#
        )
    }

    #[test]
    fn deserialize_optional_numbers_from_integers() {
        let json = test_app_json(
            r#","max_presence_members_per_channel":100,"max_event_payload_in_kb":64"#,
        );
        let app: App = sonic_rs::from_str(&json).unwrap();
        assert_eq!(app.max_presence_members_per_channel, Some(100));
        assert_eq!(app.max_event_payload_in_kb, Some(64));
    }

    #[test]
    fn deserialize_optional_numbers_from_strings() {
        let json = test_app_json(
            r#","max_presence_members_per_channel":"100","max_event_payload_in_kb":"64""#,
        );
        let app: App = sonic_rs::from_str(&json).unwrap();
        assert_eq!(app.max_presence_members_per_channel, Some(100));
        assert_eq!(app.max_event_payload_in_kb, Some(64));
    }

    #[test]
    fn deserialize_optional_numbers_from_null() {
        let json = test_app_json(r#","max_presence_members_per_channel":null"#);
        let app: App = sonic_rs::from_str(&json).unwrap();
        assert_eq!(app.max_presence_members_per_channel, None);
    }

    #[test]
    fn deserialize_optional_numbers_missing_fields() {
        let json = test_app_json("");
        let app: App = sonic_rs::from_str(&json).unwrap();
        assert_eq!(app.max_presence_members_per_channel, None);
        assert_eq!(app.max_event_payload_in_kb, None);
    }

    #[test]
    fn cache_round_trip_preserves_optional_numbers() {
        let json = test_app_json(
            r#","max_presence_members_per_channel":100,"max_backend_events_per_second":50,"max_channel_name_length":200"#,
        );
        let app: App = sonic_rs::from_str(&json).unwrap();

        // Simulate cache write/read (sonic_rs::to_string → sonic_rs::from_str)
        let cached = sonic_rs::to_string(&app).unwrap();
        let restored: App = sonic_rs::from_str(&cached).unwrap();

        assert_eq!(restored.max_presence_members_per_channel, Some(100));
        assert_eq!(restored.max_backend_events_per_second, Some(50));
        assert_eq!(restored.max_channel_name_length, Some(200));
    }
}
