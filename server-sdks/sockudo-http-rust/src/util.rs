use regex::Regex;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::LazyLock;
use subtle::ConstantTimeEq;

// Pre-compiled regex patterns
static SOCKET_ID_PATTERN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d+\.\d+$").unwrap());

static USER_ID_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_\-=@,.;]+$").unwrap());

/// Converts a map to an ordered array of key=value pairs
pub fn to_ordered_array(map: &BTreeMap<String, String>) -> Vec<String> {
    map.iter()
        .map(|(key, value)| {
            let mut result = String::with_capacity(key.len() + value.len() + 1);
            write!(&mut result, "{}={}", key, value).unwrap();
            result
        })
        .collect()
}

/// Calculates MD5 hash of the input
/// Note: MD5 is used here for compatibility with the protocol, not for security
pub fn get_md5(body: &str) -> String {
    let digest = md5::compute(body.as_bytes());
    hex::encode(digest.as_ref())
}

/// Constant-time string comparison to prevent timing attacks
pub fn secure_compare(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();

    // Use the subtle crate for constant-time comparison
    a_bytes.ct_eq(b_bytes).into()
}

/// Checks if a channel is encrypted (moved from channel.rs for backward compatibility)
pub fn is_encrypted_channel(channel: &str) -> bool {
    channel.starts_with("private-encrypted-")
}

/// Validates a channel name (moved to channel.rs but kept for backward compatibility)
pub fn validate_channel(channel: &str) -> crate::Result<()> {
    use crate::channel::Channel;
    Channel::from_string(channel)?;
    Ok(())
}

/// Validates that a channel name starts with `presence-`.
pub fn validate_presence_channel(channel: &str) -> crate::Result<()> {
    validate_channel(channel)?;
    if !channel.starts_with("presence-") {
        return Err(crate::SockudoError::Validation {
            message: format!(
                "Presence history is only available for presence channels: '{}'",
                channel
            ),
        });
    }
    Ok(())
}

/// Validates a socket ID
pub fn validate_socket_id(socket_id: &str) -> crate::Result<()> {
    if !SOCKET_ID_PATTERN.is_match(socket_id) {
        return Err(crate::SockudoError::Validation {
            message: format!(
                "Invalid socket id: '{}'. Must be in format: \\d+.\\d+",
                socket_id
            ),
        });
    }
    Ok(())
}

/// Validates a user ID
pub fn validate_user_id(user_id: &str) -> crate::Result<()> {
    if user_id.is_empty() {
        return Err(crate::SockudoError::Validation {
            message: "User ID cannot be empty".to_string(),
        });
    }

    if user_id.len() > 200 {
        return Err(crate::SockudoError::Validation {
            message: format!("User ID too long: '{}' (max 200 characters)", user_id),
        });
    }

    if !USER_ID_PATTERN.is_match(user_id) {
        return Err(crate::SockudoError::Validation {
            message: format!(
                "Invalid user ID: '{}'. Must match pattern: [a-zA-Z0-9_\\-=@,.;]+",
                user_id
            ),
        });
    }

    Ok(())
}

/// Efficiently joins strings with a separator
pub fn join_strings<'a, I>(items: I, separator: &str) -> String
where
    I: IntoIterator<Item = &'a str>,
    I::IntoIter: ExactSizeIterator,
{
    let iter = items.into_iter();
    let (lower_bound, _) = iter.size_hint();

    // Pre-allocate with estimated size
    let mut result = String::with_capacity(lower_bound * 20); // Rough estimate

    for (i, item) in iter.enumerate() {
        if i > 0 {
            result.push_str(separator);
        }
        result.push_str(item);
    }

    result
}

/// Creates a timestamp string for the current time
pub fn current_timestamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string()
}

/// Formats a duration in a human-readable way
pub fn format_duration(duration: std::time::Duration) -> String {
    let secs = duration.as_secs();
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secure_compare() {
        assert!(secure_compare("hello", "hello"));
        assert!(!secure_compare("hello", "world"));
        assert!(!secure_compare("hello", "hello!"));
        assert!(!secure_compare("hello", "hell"));
    }

    #[test]
    fn test_is_encrypted_channel() {
        assert!(is_encrypted_channel("private-encrypted-test"));
        assert!(!is_encrypted_channel("private-test"));
        assert!(!is_encrypted_channel("public-test"));
    }

    #[test]
    fn test_validate_socket_id() {
        assert!(validate_socket_id("123.456").is_ok());
        assert!(validate_socket_id("0.0").is_ok());
        assert!(validate_socket_id("123").is_err());
        assert!(validate_socket_id("123.456.789").is_err());
        assert!(validate_socket_id("abc.def").is_err());
    }

    #[test]
    fn test_validate_user_id() {
        assert!(validate_user_id("user123").is_ok());
        assert!(validate_user_id("user-123_test@example.com").is_ok());
        assert!(validate_user_id("").is_err());
        assert!(validate_user_id(&"a".repeat(201)).is_err());
        assert!(validate_user_id("user with spaces").is_err());
    }

    #[test]
    fn test_join_strings() {
        let items = ["a", "b", "c"];
        assert_eq!(join_strings(items.iter().copied(), ","), "a,b,c");
        assert_eq!(join_strings(["single"].iter().copied(), ","), "single");
        assert_eq!(join_strings(Vec::<&str>::new().iter().copied(), ","), "");
    }

    #[test]
    fn test_format_duration() {
        use std::time::Duration;

        assert_eq!(format_duration(Duration::from_secs(30)), "30s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m 30s");
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h 1m");
    }

    #[test]
    fn test_to_ordered_array() {
        let mut map = BTreeMap::new();
        map.insert("key1".to_string(), "value1".to_string());
        map.insert("key2".to_string(), "value2".to_string());

        let result = to_ordered_array(&map);
        assert_eq!(result, vec!["key1=value1", "key2=value2"]);
    }

    #[test]
    fn test_get_md5() {
        let hash = get_md5("hello");
        assert_eq!(hash, "5d41402abc4b2a76b9719d911017c592");
    }
}
