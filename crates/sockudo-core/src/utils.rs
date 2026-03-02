use std::env;
use std::sync::LazyLock;

use crate::error::Error;
use regex::Regex;
use sonic_rs::prelude::*;
use tracing::warn;

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
            let string_data = element.as_ref().to_string();
            string_data.len()
        })
        .sum()
}

pub fn data_to_bytes_flexible(data: Vec<sonic_rs::Value>) -> usize {
    data.iter().fold(0, |total_bytes, element| {
        let element_str = if element.is_str() {
            element.as_str().unwrap_or_default().to_string()
        } else {
            sonic_rs::to_string(element).unwrap_or_default()
        };
        total_bytes + element_str.len()
    })
}

pub async fn validate_channel_name(
    max_channel_name_length: Option<u32>,
    channel: &str,
) -> crate::error::Result<()> {
    if channel.len() > max_channel_name_length.unwrap_or(200) as usize {
        return Err(Error::Channel(format!(
            "Channel name too long. Max length is {}",
            max_channel_name_length.unwrap_or(200)
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

    Ok(())
}

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
        Err(_) => default,
    }
}

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
        Err(_) => default,
    }
}

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
        Err(_) => None,
    }
}

pub async fn resolve_socket_addr(host: &str, port: u16, context: &str) -> std::net::SocketAddr {
    use std::net::SocketAddr;
    use tokio::net::lookup_host;

    let host_port = format!("{host}:{port}");

    match lookup_host(&host_port).await {
        Ok(addrs) => {
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
                    "{} resolved to IPv6 only for {}. This may cause binding issues on some systems.",
                    context, host_port
                );
                addr
            } else {
                warn!(
                    "{}: no addresses found for {}. Falling back to 127.0.0.1:{}",
                    context, host_port, port
                );
                SocketAddr::from(([127, 0, 0, 1], port))
            }
        }
        Err(e) => {
            warn!(
                "{}: failed to resolve {}: {}. Falling back to 127.0.0.1:{}",
                context, host_port, e, port
            );
            SocketAddr::from(([127, 0, 0, 1], port))
        }
    }
}
