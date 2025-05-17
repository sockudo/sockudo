use crate::app::config::App;
use crate::error::Error;
use lazy_static::lazy_static; // Added for one-time regex compilation
use regex::Regex;

// Compile regexes once using lazy_static
lazy_static! {
    static ref CACHING_CHANNEL_REGEXES: Vec<Regex> = {
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
    };
}

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
            let string_data = if let Ok(s) = element.as_ref().to_string().parse::<String>() {
                // Element is already a string-like value
                s
            } else {
                // Convert to JSON string representation
                serde_json::to_string(element).unwrap_or_else(|_| String::new())
            };

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
        return Err(Error::ChannelError(format!(
            "Channel name too long. Max length is {}",
            app.max_channel_name_length.unwrap_or(200)
        )));
    }
    if !channel.chars().all(|c| {
        c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '=' || c == '@' || c == '.'
    }) {
        return Err(Error::ChannelError(
            "Channel name contains invalid characters".to_string(),
        ));
    }

    Ok(())
}
