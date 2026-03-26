use sockudo_core::channel::ChannelType;
#[cfg(feature = "delta")]
use sockudo_delta::DeltaAlgorithm;
#[cfg(feature = "tag-filtering")]
use sockudo_filter::FilterNode;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sonic_rs::Value;
use sonic_rs::prelude::*;
use std::option::Option;

/// Per-subscription delta compression settings
/// Allows clients to negotiate delta compression on a per-channel basis
#[cfg(feature = "delta")]
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

#[cfg(feature = "delta")]
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
    #[cfg(feature = "tag-filtering")]
    pub tags_filter: Option<FilterNode>,
    #[cfg(feature = "delta")]
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
    pub fn from_message(message: &PusherMessage) -> sockudo_core::error::Result<Self> {
        let (channel, auth, channel_data, _tags_filter_raw, _delta_raw) = match &message.data {
            Some(MessageData::Structured {
                channel,
                extra,
                channel_data,
                ..
            }) => {
                let ch = channel.as_ref().ok_or_else(|| {
                    sockudo_core::error::Error::InvalidMessageFormat("Missing channel field".into())
                })?;
                let channel_data = if ChannelType::from_name(ch) == ChannelType::Presence {
                    Some(channel_data.as_ref().unwrap().clone())
                } else {
                    None
                };
                let auth = extra.get("auth").and_then(Value::as_str).map(String::from);

                // Accept both "filter" (client-side) and "tags_filter" (server-side) for compatibility
                let tags_filter_raw: Option<&Value> = extra
                    .get("filter")
                    .or_else(|| extra.get("tags_filter"));

                // Parse per-subscription delta settings
                let delta_raw: Option<&Value> = extra.get("delta");

                (ch.clone(), auth, channel_data, tags_filter_raw.cloned(), delta_raw.cloned())
            }
            Some(MessageData::Json(data)) => {
                let ch = data.get("channel").and_then(Value::as_str).ok_or_else(|| {
                    sockudo_core::error::Error::InvalidMessageFormat("Missing channel field".into())
                })?;
                let auth = data.get("auth").and_then(Value::as_str).map(String::from);
                let channel_data = data
                    .get("channel_data")
                    .and_then(Value::as_str)
                    .map(String::from);

                let tags_filter_raw = data
                    .get("filter")
                    .or_else(|| data.get("tags_filter"))
                    .cloned();

                let delta_raw = data.get("delta").cloned();

                return Self::build(ch.to_string(), auth, channel_data, tags_filter_raw, delta_raw);
            }
            Some(MessageData::String(s)) => {
                let data: Value = sonic_rs::from_str(s).map_err(|_| {
                    sockudo_core::error::Error::InvalidMessageFormat(
                        "Failed to parse subscription data".into(),
                    )
                })?;
                let ch = data.get("channel").and_then(Value::as_str).ok_or_else(|| {
                    sockudo_core::error::Error::InvalidMessageFormat(
                        "Missing channel field in string data".into(),
                    )
                })?;
                let auth = data.get("auth").and_then(Value::as_str).map(String::from);
                let channel_data = data
                    .get("channel_data")
                    .and_then(Value::as_str)
                    .map(String::from);

                let tags_filter_raw = data
                    .get("filter")
                    .or_else(|| data.get("tags_filter"))
                    .cloned();

                let delta_raw = data.get("delta").cloned();

                (ch.to_string(), auth, channel_data, tags_filter_raw, delta_raw)
            }
            _ => {
                return Err(sockudo_core::error::Error::InvalidMessageFormat(
                    "Invalid subscription data format".into(),
                ));
            }
        };

        Self::build(channel, auth, channel_data, _tags_filter_raw, _delta_raw)
    }

    fn build(
        channel: String,
        auth: Option<String>,
        channel_data: Option<String>,
        #[allow(unused_variables)]
        tags_filter_raw: Option<Value>,
        #[allow(unused_variables)]
        delta_raw: Option<Value>,
    ) -> sockudo_core::error::Result<Self> {
        #[cfg(feature = "tag-filtering")]
        let tags_filter = tags_filter_raw
            .and_then(|v| sonic_rs::from_value::<FilterNode>(&v).ok())
            .map(|mut filter| {
                // PERFORMANCE: Pre-build HashSet caches for large IN/NIN operators
                filter.optimize();
                filter
            });

        #[cfg(feature = "tag-filtering")]
        if let Some(ref filter) = tags_filter
            && let Some(err) = filter.validate()
        {
            return Err(sockudo_core::error::Error::InvalidMessageFormat(format!(
                "Invalid tags filter: {}",
                err
            )));
        }

        #[cfg(feature = "delta")]
        let delta = delta_raw.and_then(|v| SubscriptionDeltaSettings::from_value(&v));

        Ok(Self {
            channel,
            auth,
            channel_data,
            #[cfg(feature = "tag-filtering")]
            tags_filter,
            #[cfg(feature = "delta")]
            delta,
        })
    }
}

impl SignInRequest {
    pub fn from_message(message: &PusherMessage) -> sockudo_core::error::Result<Self> {
        let extract_field = |data: &Value, field: &str| -> sockudo_core::error::Result<String> {
            data.get(field)
                .and_then(Value::as_str)
                .map(String::from)
                .ok_or_else(|| {
                    sockudo_core::error::Error::Auth(format!(
                        "Missing '{field}' field in signin data"
                    ))
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
                    sockudo_core::error::Error::Auth(
                        "Missing 'user_data' field in signin data".into(),
                    )
                })?,
                auth: extra
                    .get("auth")
                    .and_then(Value::as_str)
                    .map(String::from)
                    .ok_or_else(|| {
                        sockudo_core::error::Error::Auth(
                            "Missing 'auth' field in signin data".into(),
                        )
                    })?,
            }),
            _ => Err(sockudo_core::error::Error::InvalidMessageFormat(
                "Invalid signin data format".into(),
            )),
        }
    }
}
