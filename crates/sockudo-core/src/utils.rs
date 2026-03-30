use std::env;
use std::sync::LazyLock;

use crate::app::{App, ChannelNamespace};
use crate::error::Error;
use regex::Regex;
use sonic_rs::prelude::*;
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

pub fn data_to_bytes_flexible(data: Vec<sonic_rs::Value>) -> usize {
    data.iter().fold(0, |total_bytes, element| {
        let element_str = if element.is_str() {
            // Use string value directly if it's a string
            element.as_str().unwrap_or_default().to_string()
        } else {
            // Convert to JSON string for other types
            sonic_rs::to_string(element).unwrap_or_default()
        };

        // Add byte length, handling potential encoding errors
        // No specific error handling for as_bytes().len() as it's inherent to string length.
        total_bytes + element_str.len()
    })
}

pub fn strip_channel_type_prefix(channel: &str) -> &str {
    for prefix in [
        "private-encrypted-cache-",
        "private-cache-",
        "presence-cache-",
        "cache-",
        "private-encrypted-",
        "presence-",
        "private-",
    ] {
        if let Some(stripped) = channel.strip_prefix(prefix) {
            return stripped;
        }
    }

    channel
}

pub fn channel_namespace_name(channel: &str) -> Option<&str> {
    let logical = strip_channel_type_prefix(channel);
    let (namespace, _) = logical.split_once(':')?;
    (!namespace.is_empty()).then_some(namespace)
}

pub fn strip_channel_user_limit_suffix(channel: &str) -> &str {
    channel.split_once('#').map_or(channel, |(base, _)| base)
}

pub fn is_wildcard_subscription_pattern(channel: &str) -> bool {
    !channel.starts_with("#server-to-user-") && channel.contains('*')
}

pub fn is_meta_channel(channel: &str) -> bool {
    channel.starts_with("[meta]") && channel.len() > "[meta]".len()
}

pub fn meta_channel_for(channel: &str) -> Option<String> {
    if is_meta_channel(channel) {
        None
    } else {
        Some(format!("[meta]{channel}"))
    }
}

pub fn wildcard_pattern_matches(channel: &str, pattern: &str) -> bool {
    if channel == pattern {
        return true;
    }

    if pattern.contains('*') {
        if let Some(prefix) = pattern.strip_suffix('*') {
            return channel.starts_with(prefix);
        } else if let Some(suffix) = pattern.strip_prefix('*') {
            return channel.ends_with(suffix);
        } else {
            let parts: Vec<&str> = pattern.split('*').collect();
            if parts.len() == 2 {
                return channel.starts_with(parts[0]) && channel.ends_with(parts[1]);
            }
        }
    }

    false
}

pub fn validate_wildcard_subscription_pattern(channel: &str) -> crate::error::Result<()> {
    if !is_wildcard_subscription_pattern(channel) {
        return Ok(());
    }

    if channel.matches('*').count() != 1 {
        return Err(Error::Channel(format!(
            "Wildcard subscription '{}' must contain exactly one '*'",
            channel
        )));
    }

    if channel.contains('#') {
        return Err(Error::Channel(format!(
            "Wildcard subscription '{}' cannot use user-limited syntax",
            channel
        )));
    }

    if channel.starts_with("private-")
        || channel.starts_with("presence-")
        || channel.starts_with("private-encrypted-")
    {
        return Err(Error::Channel(format!(
            "Wildcard subscription '{}' is only supported for public channels",
            channel
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
            || c == '*'
    }) {
        return Err(Error::Channel(format!(
            "Wildcard subscription '{}' contains invalid characters",
            channel
        )));
    }

    Ok(())
}

pub fn channel_user_limit_ids(channel: &str) -> crate::error::Result<Option<Vec<&str>>> {
    let logical = strip_channel_type_prefix(channel);
    let Some((base_channel, raw_users)) = logical.split_once('#') else {
        return Ok(None);
    };

    if base_channel.is_empty() {
        return Ok(None);
    }

    if logical.matches('#').count() > 1 {
        return Err(Error::Channel(format!(
            "Channel '{}' contains multiple user limit separators",
            channel
        )));
    }

    if raw_users.is_empty() {
        return Err(Error::Channel(format!(
            "Channel '{}' has an empty user limit segment",
            channel
        )));
    }

    let users = raw_users.split(',').map(str::trim).collect::<Vec<_>>();

    if users.iter().any(|user| user.is_empty()) {
        return Err(Error::Channel(format!(
            "Channel '{}' contains an empty user id in its user limit segment",
            channel
        )));
    }

    if users.iter().any(|user| {
        !user.chars().all(|c| {
            c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '=' || c == '@' || c == '.'
        })
    }) {
        return Err(Error::Channel(format!(
            "Channel '{}' contains an invalid user id in its user limit segment",
            channel
        )));
    }

    Ok(Some(users))
}

pub fn resolve_channel_namespace<'a>(
    app: &'a App,
    channel: &str,
) -> crate::error::Result<Option<&'a ChannelNamespace>> {
    let Some(namespace_name) = channel_namespace_name(channel) else {
        return Ok(None);
    };

    let Some(namespaces) = app.namespaces() else {
        return Ok(None);
    };

    let namespace = namespaces.iter().find(|ns| ns.name == namespace_name);
    if let Some(ns) = namespace {
        ns.validate().map_err(Error::Config)?;
    }

    Ok(namespace)
}

pub async fn validate_channel_name(app: &App, channel: &str) -> crate::error::Result<()> {
    let namespace = resolve_channel_namespace(app, channel)?;
    let logical_channel = strip_channel_type_prefix(channel);
    let logical_channel_without_user_limit = strip_channel_user_limit_suffix(logical_channel);

    if logical_channel_without_user_limit.contains(':') && namespace.is_none() {
        let namespace_name = channel_namespace_name(channel).unwrap_or_default();
        return Err(Error::Channel(format!(
            "Unknown channel namespace '{}'",
            namespace_name
        )));
    }

    let max_channel_length = namespace
        .and_then(|ns| ns.max_channel_name_length)
        .or(app.max_channel_name_limit())
        .unwrap_or(200) as usize;

    if channel.len() > max_channel_length {
        return Err(Error::Channel(format!(
            "Channel name too long. Max length is {}",
            max_channel_length
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
            || c == '#'
    }) {
        let invalid_chars: Vec<char> = channel
            .chars()
            .filter(|c| {
                !c.is_ascii_alphanumeric()
                    && *c != '-'
                    && *c != '_'
                    && *c != '='
                    && *c != '@'
                    && *c != '.'
                    && *c != ':'
                    && *c != '#'
            })
            .collect();
        tracing::warn!(
            channel_name = %channel,
            invalid_chars = ?invalid_chars,
            "Channel name contains invalid characters"
        );
        return Err(Error::Channel(format!(
            "Channel name contains invalid characters: '{}' (invalid chars: {:?})",
            channel, invalid_chars
        )));
    }

    if let Some(user_limit_ids) = channel_user_limit_ids(channel)? {
        if namespace
            .and_then(|ns| ns.allow_user_limited_channels)
            .unwrap_or(false)
        {
            if user_limit_ids.is_empty() {
                return Err(Error::Channel(format!(
                    "Channel '{}' must specify at least one user id",
                    channel
                )));
            }
        } else {
            return Err(Error::Channel(format!(
                "Channel '{}' uses user-limited syntax but the namespace does not allow it",
                channel
            )));
        }
    }

    if let Some(namespace) = namespace
        && let Some(pattern) = &namespace.channel_name_pattern
    {
        let regex = Regex::new(pattern).map_err(|e| {
            Error::Config(format!(
                "Invalid channel namespace pattern for '{}': {e}",
                namespace.name
            ))
        })?;

        if !regex.is_match(logical_channel_without_user_limit) {
            return Err(Error::Channel(format!(
                "Channel '{}' does not match namespace '{}' pattern",
                channel, namespace.name
            )));
        }
    }

    Ok(())
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

/// Parse an environment variable as any numeric type that implements FromStr + Display.
///
/// Returns the default value if the environment variable is not set.
/// Logs a warning and returns the default value if parsing fails.
pub fn parse_env<T>(var_name: &str, default: T) -> T
where
    T: std::str::FromStr + std::fmt::Display + Clone,
    T::Err: std::fmt::Display,
{
    match env::var(var_name) {
        Ok(s) => match s.parse::<T>() {
            Ok(value) => value,
            Err(e) => {
                warn!(
                    "Failed to parse {} as {}: '{}'. Error: {}. Defaulting to {}.",
                    var_name,
                    std::any::type_name::<T>(),
                    s,
                    e,
                    default
                );
                default
            }
        },
        Err(_) => default, // Variable not set, use default
    }
}

/// Parse an environment variable as any numeric type, returning None if not set.
///
/// Returns Some(value) if the environment variable is set and parsing succeeds.
/// Returns None if the environment variable is not set.
/// Logs a warning and returns None if parsing fails.
pub fn parse_env_optional<T>(var_name: &str) -> Option<T>
where
    T: std::str::FromStr + std::fmt::Display + Clone,
    T::Err: std::fmt::Display,
{
    match env::var(var_name) {
        Ok(s) => match s.parse::<T>() {
            Ok(value) => Some(value),
            Err(e) => {
                warn!(
                    "Failed to parse {} as {}: '{}'. Error: {}.",
                    var_name,
                    std::any::type_name::<T>(),
                    s,
                    e
                );
                None
            }
        },
        Err(_) => None, // Variable not set
    }
}

/// Resolve a hostname and port to a SocketAddr with proper DNS resolution.
///
/// Features:
/// - Prefers IPv4 addresses over IPv6 for better compatibility
/// - Handles hostname resolution (localhost, domain names, etc.)
/// - Falls back to 127.0.0.1 with the specified port on resolution failure
/// - Logs warnings for IPv6-only resolution or failures with context
///
/// # Parameters
/// - `host`: The hostname or IP address to resolve
/// - `port`: The port number
/// - `context`: A description of what this address is for (e.g., "HTTP server", "metrics server")
pub async fn resolve_socket_addr(host: &str, port: u16, context: &str) -> std::net::SocketAddr {
    use std::net::SocketAddr;
    use tokio::net::lookup_host;

    let host_port = format!("{host}:{port}");

    match lookup_host(&host_port).await {
        Ok(addrs) => {
            // Prefer IPv4 addresses to avoid potential IPv6 issues
            let mut ipv4_addr = None;
            let mut ipv6_addr = None;

            for addr in addrs {
                if addr.is_ipv4() && ipv4_addr.is_none() {
                    ipv4_addr = Some(addr);
                } else if addr.is_ipv6() && ipv6_addr.is_none() {
                    ipv6_addr = Some(addr);
                }
            }

            if let Some(addr) = ipv4_addr {
                addr
            } else if let Some(addr) = ipv6_addr {
                warn!(
                    "[{}] Only IPv6 address found for {}, using: {}",
                    context, host_port, addr
                );
                addr
            } else {
                let fallback = SocketAddr::new("127.0.0.1".parse().unwrap(), port);
                warn!(
                    "[{}] No addresses found for {}. Using fallback {}",
                    context, host_port, fallback
                );
                fallback
            }
        }
        Err(e) => {
            let fallback = SocketAddr::new("127.0.0.1".parse().unwrap(), port);
            warn!(
                "[{}] Failed to resolve {}: {}. Using fallback {}",
                context, host_port, e, fallback
            );
            fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::ChannelNamespace;
    use sonic_rs::json;

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
        let app = App::from_policy(
            "test-app".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            true,
            crate::app::AppPolicy {
                limits: crate::app::AppLimitsPolicy {
                    max_connections: 100,
                    max_client_events_per_second: 100,
                    max_channel_name_length: Some(50),
                    ..Default::default()
                },
                features: crate::app::AppFeaturesPolicy {
                    enable_client_messages: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        // Test valid channel names
        assert!(validate_channel_name(&app, "test-123").await.is_ok());
        assert!(validate_channel_name(&app, "user@domain").await.is_ok());
        assert!(validate_channel_name(&app, "channel.name").await.is_ok());
        assert!(
            validate_channel_name(&app, "#server-to-user-123")
                .await
                .is_ok()
        );

        // Test invalid channel names
        assert!(
            validate_channel_name(&app, "invalid$channel")
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

    #[tokio::test]
    async fn test_channel_namespace_name() {
        assert_eq!(channel_namespace_name("chat:room-1"), Some("chat"));
        assert_eq!(channel_namespace_name("private-chat:room-1"), Some("chat"));
        assert_eq!(channel_namespace_name("presence-chat:room-1"), Some("chat"));
        assert_eq!(channel_namespace_name("plain-channel"), None);
    }

    #[tokio::test]
    async fn test_validate_channel_name_with_namespace_override() {
        let app = App::from_policy(
            "test-app".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            true,
            crate::app::AppPolicy {
                limits: crate::app::AppLimitsPolicy {
                    max_connections: 100,
                    max_client_events_per_second: 100,
                    max_channel_name_length: Some(200),
                    ..Default::default()
                },
                features: crate::app::AppFeaturesPolicy {
                    enable_client_messages: true,
                    ..Default::default()
                },
                channels: crate::app::AppChannelsPolicy {
                    channel_namespaces: Some(vec![ChannelNamespace {
                        name: "chat".to_string(),
                        channel_name_pattern: Some("^chat:[a-z0-9-]+$".to_string()),
                        max_channel_name_length: Some(20),
                        allow_user_limited_channels: Some(true),
                        allow_subscribe_for_client: None,
                        allow_publish_for_client: None,
                        allow_presence_for_client: None,
                    }]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        assert!(validate_channel_name(&app, "chat:room-1").await.is_ok());
        assert!(validate_channel_name(&app, "chat:ROOM-1").await.is_err());
        assert!(
            validate_channel_name(&app, "chat:room-123456789012345")
                .await
                .is_err()
        );
        assert!(
            validate_channel_name(&app, "chat:room-1#user-1")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_validate_channel_name_rejects_unknown_namespace() {
        let app = App::from_policy(
            "test-app".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            true,
            crate::app::AppPolicy {
                limits: crate::app::AppLimitsPolicy {
                    max_connections: 100,
                    max_client_events_per_second: 100,
                    ..Default::default()
                },
                features: crate::app::AppFeaturesPolicy {
                    enable_client_messages: true,
                    ..Default::default()
                },
                channels: crate::app::AppChannelsPolicy {
                    channel_namespaces: Some(vec![ChannelNamespace {
                        name: "chat".to_string(),
                        channel_name_pattern: Some("^chat:[a-z0-9-]+$".to_string()),
                        max_channel_name_length: Some(20),
                        allow_user_limited_channels: None,
                        allow_subscribe_for_client: None,
                        allow_publish_for_client: None,
                        allow_presence_for_client: None,
                    }]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        assert!(validate_channel_name(&app, "unknown:room-1").await.is_err());
    }

    #[tokio::test]
    async fn test_validate_channel_name_rejects_user_limited_without_namespace_support() {
        let app = App::from_policy(
            "test-app".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
            true,
            crate::app::AppPolicy {
                limits: crate::app::AppLimitsPolicy {
                    max_connections: 100,
                    max_client_events_per_second: 100,
                    ..Default::default()
                },
                features: crate::app::AppFeaturesPolicy {
                    enable_client_messages: true,
                    ..Default::default()
                },
                channels: crate::app::AppChannelsPolicy {
                    channel_namespaces: Some(vec![ChannelNamespace {
                        name: "chat".to_string(),
                        channel_name_pattern: Some("^chat:[a-z0-9-]+$".to_string()),
                        max_channel_name_length: Some(20),
                        allow_user_limited_channels: Some(false),
                        allow_subscribe_for_client: None,
                        allow_publish_for_client: None,
                        allow_presence_for_client: None,
                    }]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        assert!(
            validate_channel_name(&app, "chat:room-1#user-1")
                .await
                .is_err()
        );
    }

    #[test]
    fn test_wildcard_pattern_matches() {
        assert!(wildcard_pattern_matches("news.btc", "news.*"));
        assert!(wildcard_pattern_matches("price-data", "*-data"));
        assert!(wildcard_pattern_matches("market-btc-data", "market-*-data"));
        assert!(!wildcard_pattern_matches("news.btc", "alerts.*"));
    }

    #[test]
    fn test_validate_wildcard_subscription_pattern() {
        assert!(validate_wildcard_subscription_pattern("news.*").is_ok());
        assert!(validate_wildcard_subscription_pattern("news.*.btc").is_ok());
        assert!(validate_wildcard_subscription_pattern("private-news.*").is_err());
        assert!(validate_wildcard_subscription_pattern("news.*.*").is_err());
        assert!(validate_wildcard_subscription_pattern("news.*#user-1").is_err());
    }
}
