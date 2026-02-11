use crate::channel::ChannelType;
use crate::delta_compression::DeltaAlgorithm;
use crate::filter::FilterNode;
use crate::protocol::messages::{MessageData, PusherMessage};
use sonic_rs::Value;
use sonic_rs::prelude::*;
use std::option::Option;

/// Per-subscription delta compression settings
/// Allows clients to negotiate delta compression on a per-channel basis
#[derive(Debug, Clone, Default)]
pub struct SubscriptionDeltaSettings {
    /// Enable delta compression for this subscription
    /// - `Some(true)`: Enable delta compression
    /// - `Some(false)`: Disable delta compression
    /// - `None`: Use server default (global enable_delta_compression event)
    pub enabled: Option<bool>,
    /// Preferred algorithm for this subscription
    /// - `Some(algorithm)`: Use specific algorithm
    /// - `None`: Use server default algorithm
    pub algorithm: Option<DeltaAlgorithm>,
}

impl SubscriptionDeltaSettings {
    /// Parse delta settings from subscription data
    pub fn from_value(value: &Value) -> Option<Self> {
        // Handle both object format and simple string format
        if let Some(s) = value.as_str() {
            // Simple string format: "delta": "fossil" or "delta": "xdelta3"
            let algorithm = match s.to_lowercase().as_str() {
                "fossil" => Some(DeltaAlgorithm::Fossil),
                "xdelta3" | "vcdiff" => Some(DeltaAlgorithm::Xdelta3),
                "disabled" | "false" | "none" => {
                    return Some(Self {
                        enabled: Some(false),
                        algorithm: None,
                    });
                }
                _ => None,
            };
            Some(Self {
                enabled: Some(true),
                algorithm,
            })
        } else if let Some(b) = value.as_bool() {
            // Boolean format: "delta": true or "delta": false
            Some(Self {
                enabled: Some(b),
                algorithm: None,
            })
        } else if let Some(obj) = value.as_object() {
            // Object format: "delta": { "enabled": true, "algorithm": "fossil" }
            let enabled = obj.get(&"enabled").and_then(|v| v.as_bool());
            let algorithm = obj
                .get(&"algorithm")
                .and_then(|v| v.as_str())
                .and_then(|s| match s.to_lowercase().as_str() {
                    "fossil" => Some(DeltaAlgorithm::Fossil),
                    "xdelta3" | "vcdiff" => Some(DeltaAlgorithm::Xdelta3),
                    _ => None,
                });
            Some(Self { enabled, algorithm })
        } else {
            None
        }
    }

    /// Check if delta compression should be enabled for this subscription
    /// Returns None if the subscription didn't specify (use global setting)
    pub fn should_enable(&self) -> Option<bool> {
        self.enabled
    }

    /// Get the preferred algorithm, if specified
    pub fn preferred_algorithm(&self) -> Option<DeltaAlgorithm> {
        self.algorithm
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionRequest {
    pub channel: String,
    pub auth: Option<String>,
    pub channel_data: Option<String>,
    pub tags_filter: Option<FilterNode>,
    /// Per-subscription delta compression settings
    /// Allows clients to negotiate delta compression per-channel
    pub delta: Option<SubscriptionDeltaSettings>,
}

#[derive(Debug, Clone)]
pub struct ClientEventRequest {
    pub event: String,
    pub channel: String,
    pub data: Value,
}

#[derive(Debug)]
pub struct SignInRequest {
    pub user_data: String,
    pub auth: String,
}

impl SubscriptionRequest {
    pub fn from_message(message: &PusherMessage) -> crate::error::Result<Self> {
        let (channel, auth, channel_data, tags_filter, delta) = match &message.data {
            Some(MessageData::Structured {
                channel,
                extra,
                channel_data,
                ..
            }) => {
                let ch = channel.as_ref().ok_or_else(|| {
                    crate::error::Error::InvalidMessageFormat("Missing channel field".into())
                })?;
                let channel_data = if ChannelType::from_name(ch) == ChannelType::Presence {
                    Some(channel_data.as_ref().unwrap().clone())
                } else {
                    None
                };
                let auth = extra.get("auth").and_then(Value::as_str).map(String::from);

                // Accept both "filter" (client-side) and "tags_filter" (server-side) for compatibility
                let tags_filter: Option<FilterNode> = extra
                    .get("filter")
                    .or_else(|| extra.get("tags_filter"))
                    .and_then(|v| sonic_rs::from_value::<FilterNode>(&v.clone()).ok())
                    .map(|mut filter| {
                        // PERFORMANCE: Pre-build HashSet caches for large IN/NIN operators
                        filter.optimize();
                        filter
                    });

                // Parse per-subscription delta settings
                let delta = extra
                    .get("delta")
                    .and_then(SubscriptionDeltaSettings::from_value);

                (ch.clone(), auth, channel_data, tags_filter, delta)
            }
            Some(MessageData::Json(data)) => {
                let ch = data.get("channel").and_then(Value::as_str).ok_or_else(|| {
                    crate::error::Error::InvalidMessageFormat("Missing channel field".into())
                })?;
                let auth = data.get("auth").and_then(Value::as_str).map(String::from);
                let channel_data = data
                    .get("channel_data")
                    .and_then(Value::as_str)
                    .map(String::from);

                // Accept both "filter" (client-side) and "tags_filter" (server-side) for compatibility
                let tags_filter = data
                    .get("filter")
                    .or_else(|| data.get("tags_filter"))
                    .and_then(|v| sonic_rs::from_value::<FilterNode>(&v.clone()).ok())
                    .map(|mut filter| {
                        // PERFORMANCE: Pre-build HashSet caches for large IN/NIN operators
                        filter.optimize();
                        filter
                    });

                // Parse per-subscription delta settings
                let delta = data
                    .get("delta")
                    .and_then(SubscriptionDeltaSettings::from_value);

                return Ok(Self {
                    channel: ch.to_string(),
                    auth,
                    channel_data,
                    tags_filter,
                    delta,
                });
            }
            Some(MessageData::String(s)) => {
                let data: Value = sonic_rs::from_str(s).map_err(|_| {
                    crate::error::Error::InvalidMessageFormat(
                        "Failed to parse subscription data".into(),
                    )
                })?;
                let ch = data.get("channel").and_then(Value::as_str).ok_or_else(|| {
                    crate::error::Error::InvalidMessageFormat(
                        "Missing channel field in string data".into(),
                    )
                })?;
                let auth = data.get("auth").and_then(Value::as_str).map(String::from);
                let channel_data = data
                    .get("channel_data")
                    .and_then(Value::as_str)
                    .map(String::from);

                // Accept both "filter" (client-side) and "tags_filter" (server-side) for compatibility
                let tags_filter = data
                    .get("filter")
                    .or_else(|| data.get("tags_filter"))
                    .and_then(|v| sonic_rs::from_value::<FilterNode>(&v.clone()).ok())
                    .map(|mut filter| {
                        // PERFORMANCE: Pre-build HashSet caches for large IN/NIN operators
                        filter.optimize();
                        filter
                    });

                // Parse per-subscription delta settings
                let delta = data
                    .get("delta")
                    .and_then(SubscriptionDeltaSettings::from_value);

                (ch.to_string(), auth, channel_data, tags_filter, delta)
            }
            _ => {
                return Err(crate::error::Error::InvalidMessageFormat(
                    "Invalid subscription data format".into(),
                ));
            }
        };

        // Validate the filter if present
        if let Some(ref filter) = tags_filter
            && let Some(err) = filter.validate()
        {
            return Err(crate::error::Error::InvalidMessageFormat(format!(
                "Invalid tags filter: {}",
                err
            )));
        }

        Ok(Self {
            channel,
            auth,
            channel_data,
            tags_filter,
            delta,
        })
    }
}

impl SignInRequest {
    pub fn from_message(message: &PusherMessage) -> crate::error::Result<Self> {
        let extract_field = |data: &Value, field: &str| -> crate::error::Result<String> {
            data.get(field)
                .and_then(Value::as_str)
                .map(String::from)
                .ok_or_else(|| {
                    crate::error::Error::Auth(format!("Missing '{field}' field in signin data"))
                })
        };

        match &message.data {
            Some(MessageData::Json(data)) => Ok(Self {
                user_data: extract_field(data, "user_data")?,
                auth: extract_field(data, "auth")?,
            }),
            Some(MessageData::Structured {
                user_data, extra, ..
            }) => Ok(Self {
                user_data: user_data.as_ref().cloned().ok_or_else(|| {
                    crate::error::Error::Auth("Missing 'user_data' field in signin data".into())
                })?,
                auth: extra
                    .get("auth")
                    .and_then(Value::as_str)
                    .map(String::from)
                    .ok_or_else(|| {
                        crate::error::Error::Auth("Missing 'auth' field in signin data".into())
                    })?,
            }),
            _ => Err(crate::error::Error::InvalidMessageFormat(
                "Invalid signin data format".into(),
            )),
        }
    }
}
