use std::env;
use std::sync::LazyLock;

use crate::app::config::App;
use crate::error::Error;
use regex::Regex;
use tracing::warn;

// Compile regexes once using lazy_static
static CACHING_CHANNEL_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    let patterns = vec![
        "cache-*",
        "private-cache-*",
        "private-encrypted-cache-*",
        "presence-cache-*",
    ];
    patterns
        .into_iter()
        .map(|pattern| Regex::new(&pattern.replace("*", ".*")).unwrap())
        .collect()
});

pub fn is_cache_channel(channel: &str) -> bool {
    // Use the pre-compiled regexes
    for regex in CACHING_CHANNEL_REGEXES.iter() {
        if regex.is_match(channel) {
            return true;
        }
    }
    false
}

pub fn data_to_bytes<T: AsRef<str> + serde::Serialize>(data: &[T]) -> usize {
    data.iter()
        .map(|element| {
            // Convert element to string representation
            let string_data = element.as_ref().to_string();

            // Calculate byte length of string
            string_data.len()
        })
        .sum()
}

pub fn data_to_bytes_flexible(data: Vec<serde_json::Value>) -> usize {
    data.iter().fold(0, |total_bytes, element| {
        let element_str = if element.is_string() {
            // Use string value directly if it's a string
            element.as_str().unwrap_or_default().to_string()
        } else {
            // Convert to JSON string for other types
            serde_json::to_string(element).unwrap_or_default()
        };

        // Add byte length, handling potential encoding errors
        // No specific error handling for as_bytes().len() as it's inherent to string length.
        total_bytes + element_str.len()
    })
}

pub async fn validate_channel_name(app: &App, channel: &str) -> crate::error::Result<()> {
    if channel.len() > app.max_channel_name_length.unwrap_or(200) as usize {
        return Err(Error::Channel(format!(
            "Channel name too long. Max length is {}",
            app.max_channel_name_length.unwrap_or(200)
        )));
    }
    if !channel.chars().all(|c| {
        c.is_ascii_alphanumeric()
            || c == '-'
            || c == '_'
            || c == '='
            || c == '@'
            || c == '.'
            || c == ':'
    }) {
        return Err(Error::Channel(
            "Channel name contains invalid characters".to_string(),
        ));
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_data_to_bytes_flexible() {
        let test_data = vec![
            json!("simple string"),
            json!(123),
            json!({"key": "value"}),
            json!([1, 2, 3]),
        ];

        let bytes = data_to_bytes_flexible(test_data);
        assert!(bytes > 0);
    }

    #[tokio::test]
    async fn test_validate_channel_name() {
        let app: App = App {
            id: "test-app".to_string(),
            key: "test-key".to_string(),
            secret: "test-secret".to_string(),
            max_connections: 100,
            enable_client_messages: true,
            enabled: true,
            max_client_events_per_second: 100,
            max_channel_name_length: Some(50),
            ..Default::default()
        };

        // Test valid channel names
        assert!(validate_channel_name(&app, "test-123").await.is_ok());
        assert!(validate_channel_name(&app, "user@domain").await.is_ok());
        assert!(validate_channel_name(&app, "channel.name").await.is_ok());

        // Test invalid channel names
        assert!(
            validate_channel_name(&app, "invalid#channel")
                .await
                .is_err()
        );
        assert!(
            validate_channel_name(
                &app,
                "too_long_channel_name_that_exceeds_fifty_characters_and_should_fail"
            )
            .await
            .is_err()
        );
        assert!(validate_channel_name(&app, "space in name").await.is_err());
    }

}

/// Parse a boolean value from an environment variable with flexible format support.
/// 
/// Supports the following formats (case-insensitive):
/// - true: "true", "1", "yes", "on" 
/// - false: "false", "0", "no", "off"
/// 
/// Returns the default value if the environment variable is not set.
/// Logs a warning and returns the default value for unrecognized formats.
pub fn parse_bool_env(var_name: &str, default: bool) -> bool {
    match env::var(var_name) {
        Ok(s) => {
            let lowercased_s = s.to_lowercase();
            match lowercased_s.as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" => false,
                _ => {
                    warn!(
                        "Unrecognized value for {}: '{}'. Defaulting to {}.",
                        var_name, s, default
                    );
                    default
                }
            }
        }
        Err(_) => default, // Variable not set, use default
    }
}
