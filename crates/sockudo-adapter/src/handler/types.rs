use sockudo_core::channel::ChannelType;
use sockudo_core::history::HistoryQueryBounds;
use sockudo_core::options::EventNameFilteringConfig;
#[cfg(feature = "delta")]
use sockudo_delta::DeltaAlgorithm;
#[cfg(feature = "tag-filtering")]
use sockudo_filter::{FilterNode, MessagePredicate, SubscriptionView};
use sockudo_protocol::messages::{ANNOTATION_SUBSCRIBE_MODE, MessageData, PusherMessage};
use sonic_rs::Value;
use sonic_rs::prelude::*;
use std::option::Option;
#[cfg(feature = "tag-filtering")]
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionRewind {
    Count(usize),
    Seconds(u64),
}

impl SubscriptionRewind {
    pub fn from_value(value: &Value) -> Option<Self> {
        if let Some(count) = value.as_u64() {
            return (count > 0).then_some(Self::Count(count as usize));
        }

        if let Some(s) = value.as_str()
            && let Ok(count) = s.parse::<usize>()
        {
            return (count > 0).then_some(Self::Count(count));
        }

        let obj = value.as_object()?;
        if let Some(count) = obj.get(&"count").and_then(Value::as_u64) {
            return (count > 0).then_some(Self::Count(count as usize));
        }
        if let Some(seconds) = obj.get(&"seconds").and_then(Value::as_u64) {
            return (seconds > 0).then_some(Self::Seconds(seconds));
        }
        if let Some(seconds) = obj.get(&"duration_seconds").and_then(Value::as_u64) {
            return (seconds > 0).then_some(Self::Seconds(seconds));
        }

        None
    }

    pub fn to_history_bounds(&self, now_ms: i64) -> HistoryQueryBounds {
        match self {
            Self::Count(_) => HistoryQueryBounds::default(),
            Self::Seconds(seconds) => HistoryQueryBounds {
                start_serial: None,
                end_serial: None,
                start_time_ms: Some(now_ms.saturating_sub((*seconds as i64) * 1000)),
                end_time_ms: None,
            },
        }
    }

    pub fn limit(&self) -> usize {
        match self {
            Self::Count(count) => *count,
            Self::Seconds(_) => usize::MAX,
        }
    }
}

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
    #[cfg(feature = "tag-filtering")]
    /// One immutable V2 delivery predicate. Event, tag, and expression parts use AND semantics.
    pub predicate: Option<Arc<MessagePredicate>>,
    #[cfg(feature = "delta")]
    /// Per-subscription delta compression settings
    /// Allows clients to negotiate delta compression per-channel
    pub delta: Option<SubscriptionDeltaSettings>,
    /// V2-only rewind option. Replays historical messages on first subscribe
    /// before live delivery resumes.
    pub rewind: Option<SubscriptionRewind>,
    /// V2 only. Event name filter — only events matching this list are delivered.
    /// None or empty = receive all events.
    pub event_name_filter: Option<Vec<String>>,
    /// V2 only. Enables raw annotation event delivery for this subscription.
    pub annotation_subscribe: bool,
}

type ExtractedSubscriptionFilter = (Option<Vec<String>>, Option<Value>, Option<String>);

#[derive(Debug, Clone)]
pub struct ClientEventRequest {
    pub event: String,
    pub channel: String,
    pub data: MessageData,
}

impl ClientEventRequest {
    /// Project native data into the JSON-shaped webhook contract. Realtime
    /// fanout retains `MessageData::Binary`; only the webhook edge uses a byte
    /// array because webhook payloads are JSON.
    pub(crate) fn webhook_data(&self) -> Value {
        match &self.data {
            MessageData::String(value) => Value::from(value.as_str()),
            MessageData::Binary(value) => {
                sonic_rs::to_value(value).unwrap_or_else(|_| Value::new_array())
            }
            MessageData::Structured { .. } => {
                sonic_rs::to_value(&self.data).unwrap_or_else(|_| Value::new_object())
            }
            MessageData::Json(value) => value.clone(),
        }
    }
}

#[derive(Debug)]
pub struct SignInRequest {
    pub user_data: String,
    pub auth: String,
}

impl SubscriptionRequest {
    pub fn from_message(
        message: &PusherMessage,
        event_name_filtering: &EventNameFilteringConfig,
    ) -> sockudo_core::error::Result<Self> {
        let (channel, auth, channel_data, _tags_filter_raw, _delta_raw, rewind_raw, modes_raw) =
            match &message.data {
                Some(MessageData::Structured {
                    channel,
                    extra,
                    channel_data,
                    ..
                }) => {
                    let ch = channel.as_ref().ok_or_else(|| {
                        sockudo_core::error::Error::InvalidMessageFormat(
                            "Missing channel field".into(),
                        )
                    })?;
                    let channel_data = if ChannelType::from_name(ch) == ChannelType::Presence {
                        Some(channel_data.as_ref().unwrap().clone())
                    } else {
                        None
                    };
                    let auth = extra.get("auth").and_then(Value::as_str).map(String::from);

                    // Accept both "filter" (client-side) and "tags_filter" (server-side) for compatibility
                    let tags_filter_raw: Option<&Value> =
                        extra.get("filter").or_else(|| extra.get("tags_filter"));

                    // Parse per-subscription delta settings
                    let delta_raw: Option<&Value> = extra.get("delta");
                    let rewind_raw: Option<&Value> = extra.get("rewind");
                    let modes_raw: Option<&Value> = extra.get("modes");

                    (
                        ch.clone(),
                        auth,
                        channel_data,
                        tags_filter_raw.cloned(),
                        delta_raw.cloned(),
                        rewind_raw.cloned(),
                        modes_raw.cloned(),
                    )
                }
                Some(MessageData::Json(data)) => {
                    let ch = data.get("channel").and_then(Value::as_str).ok_or_else(|| {
                        sockudo_core::error::Error::InvalidMessageFormat(
                            "Missing channel field".into(),
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
                    let rewind_raw = data.get("rewind").cloned();
                    let modes_raw = data.get("modes").cloned();

                    return Self::build(
                        ch.to_string(),
                        auth,
                        channel_data,
                        tags_filter_raw,
                        delta_raw,
                        rewind_raw,
                        modes_raw,
                        event_name_filtering,
                    );
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
                    let rewind_raw = data.get("rewind").cloned();
                    let modes_raw = data.get("modes").cloned();

                    (
                        ch.to_string(),
                        auth,
                        channel_data,
                        tags_filter_raw,
                        delta_raw,
                        rewind_raw,
                        modes_raw,
                    )
                }
                _ => {
                    return Err(sockudo_core::error::Error::InvalidMessageFormat(
                        "Invalid subscription data format".into(),
                    ));
                }
            };

        Self::build(
            channel,
            auth,
            channel_data,
            _tags_filter_raw,
            _delta_raw,
            rewind_raw,
            modes_raw,
            event_name_filtering,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        channel: String,
        auth: Option<String>,
        channel_data: Option<String>,
        #[allow(unused_variables)] tags_filter_raw: Option<Value>,
        #[allow(unused_variables)] delta_raw: Option<Value>,
        rewind_raw: Option<Value>,
        modes_raw: Option<Value>,
        event_name_filtering: &EventNameFilteringConfig,
    ) -> sockudo_core::error::Result<Self> {
        // Extract event name filter from filter.events (V2 only).
        // The filter field can be either:
        //   - An object with "events" and/or "tags" sub-fields (V2 compound filter)
        //   - A FilterNode directly (legacy tag filter format)
        let (event_name_filter, _effective_tags_filter_raw, expression) =
            if event_name_filtering.enabled {
                Self::extract_event_name_filter(tags_filter_raw)?
            } else {
                let (_, tags_only, expression) = Self::extract_event_name_filter(tags_filter_raw)?;
                (None, tags_only, expression)
            };
        #[cfg(not(feature = "tag-filtering"))]
        let _ = &expression;

        // Validate event name filter
        if let Some(ref events) = event_name_filter {
            if events.len() > event_name_filtering.max_events_per_filter {
                return Err(sockudo_core::error::Error::InvalidMessageFormat(format!(
                    "filter.events exceeds maximum of {} event names",
                    event_name_filtering.max_events_per_filter
                )));
            }
            for name in events {
                if name.len() > event_name_filtering.max_event_name_length {
                    return Err(sockudo_core::error::Error::InvalidMessageFormat(format!(
                        "Event name '{}...' exceeds maximum length of {} characters",
                        &name[..40.min(name.len())],
                        event_name_filtering.max_event_name_length
                    )));
                }
            }
        }

        #[cfg(feature = "tag-filtering")]
        let tags_filter = _effective_tags_filter_raw
            .map(|value| {
                let bytes = sonic_rs::to_vec(&value).map_err(|error| {
                    sockudo_core::error::Error::InvalidMessageFormat(format!(
                        "Invalid tags filter encoding: {error}"
                    ))
                })?;
                let mut filter = sonic_rs::from_slice::<FilterNode>(&bytes).map_err(|error| {
                    sockudo_core::error::Error::InvalidMessageFormat(format!(
                        "Invalid tags filter shape: {error}"
                    ))
                })?;
                filter.optimize();
                Ok::<FilterNode, sockudo_core::error::Error>(filter)
            })
            .transpose()?;

        #[cfg(feature = "tag-filtering")]
        if let Some(ref filter) = tags_filter
            && let Some(err) = filter.validate()
        {
            return Err(sockudo_core::error::Error::InvalidMessageFormat(format!(
                "Invalid tags filter: {}",
                err
            )));
        }

        #[cfg(feature = "tag-filtering")]
        let predicate =
            if event_name_filter.is_some() || tags_filter.is_some() || expression.is_some() {
                Some(Arc::new(
                    MessagePredicate::compile(SubscriptionView {
                        events: event_name_filter.clone().unwrap_or_default(),
                        tags: tags_filter.clone(),
                        expression,
                    })
                    .map_err(|error| {
                        sockudo_core::error::Error::InvalidMessageFormat(format!(
                            "Invalid subscription filter: {error}"
                        ))
                    })?,
                ))
            } else {
                None
            };

        #[cfg(feature = "delta")]
        let delta = delta_raw.and_then(|v| SubscriptionDeltaSettings::from_value(&v));

        let rewind = match rewind_raw.as_ref() {
            Some(raw) => Some(SubscriptionRewind::from_value(raw).ok_or_else(|| {
                sockudo_core::error::Error::InvalidMessageFormat(
                    "Invalid rewind option in subscription data".into(),
                )
            })?),
            None => None,
        };
        let annotation_subscribe = Self::parse_annotation_subscribe_mode(modes_raw.as_ref())?;

        Ok(Self {
            channel,
            auth,
            channel_data,
            #[cfg(feature = "tag-filtering")]
            tags_filter,
            #[cfg(feature = "tag-filtering")]
            predicate,
            #[cfg(feature = "delta")]
            delta,
            rewind,
            event_name_filter,
            annotation_subscribe,
        })
    }

    fn parse_annotation_subscribe_mode(
        modes_raw: Option<&Value>,
    ) -> sockudo_core::error::Result<bool> {
        let Some(modes) = modes_raw else {
            return Ok(false);
        };
        let Some(items) = modes.as_array() else {
            return Err(sockudo_core::error::Error::InvalidMessageFormat(
                "subscription modes must be an array".into(),
            ));
        };

        Ok(items
            .iter()
            .filter_map(Value::as_str)
            .any(|mode| mode == ANNOTATION_SUBSCRIBE_MODE))
    }

    /// Extract event name filter from the raw filter value.
    /// Returns (event_name_filter, effective_tags_filter_raw).
    /// If the filter is a compound object `{ events: [...], tags: ... }`,
    /// splits it into the event names and the tags sub-value.
    /// Otherwise, passes the whole value through as tags filter.
    fn extract_event_name_filter(
        filter_raw: Option<Value>,
    ) -> sockudo_core::error::Result<ExtractedSubscriptionFilter> {
        let Some(filter) = filter_raw else {
            return Ok((None, None, None));
        };

        let is_compound = filter.get("events").is_some()
            || filter.get("tags").is_some()
            || filter.get("expression").is_some();
        if is_compound {
            let event_names = match filter.get("events") {
                Some(events_val) => {
                    let events = events_val.as_array().ok_or_else(|| {
                        sockudo_core::error::Error::InvalidMessageFormat(
                            "filter.events must be an array of non-empty strings".into(),
                        )
                    })?;
                    let mut names = Vec::with_capacity(events.len());
                    for event in events {
                        let event = event
                            .as_str()
                            .filter(|event| !event.is_empty())
                            .ok_or_else(|| {
                                sockudo_core::error::Error::InvalidMessageFormat(
                                    "filter.events must contain only non-empty strings".into(),
                                )
                            })?;
                        names.push(event.to_string());
                    }
                    (!names.is_empty()).then_some(names)
                }
                None => None,
            };
            let expression = filter
                .get("expression")
                .map(Self::parse_expression_filter)
                .transpose()?;
            return Ok((event_names, filter.get("tags").cloned(), expression));
        }

        // Not a compound filter — treat entire value as tags filter (legacy)
        Ok((None, Some(filter), None))
    }

    fn parse_expression_filter(value: &Value) -> sockudo_core::error::Result<String> {
        if let Some(source) = value.as_str().filter(|source| !source.is_empty()) {
            return Ok(source.to_string());
        }
        let object = value.as_object().ok_or_else(|| {
            sockudo_core::error::Error::InvalidMessageFormat(
                "filter.expression must be a JMESPath string or object".into(),
            )
        })?;
        let language = object
            .get(&"language")
            .and_then(Value::as_str)
            .unwrap_or("jmespath");
        if language != "jmespath" {
            return Err(sockudo_core::error::Error::InvalidMessageFormat(format!(
                "Unsupported filter expression language '{language}'"
            )));
        }
        object
            .get(&"source")
            .and_then(Value::as_str)
            .filter(|source| !source.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                sockudo_core::error::Error::InvalidMessageFormat(
                    "filter.expression.source must be a non-empty string".into(),
                )
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

#[cfg(test)]
mod rewind_tests {
    use super::*;

    #[test]
    fn rewind_parser_accepts_count_and_seconds_shapes() {
        assert_eq!(
            SubscriptionRewind::from_value(&sonic_rs::json!(5)),
            Some(SubscriptionRewind::Count(5))
        );
        assert_eq!(
            SubscriptionRewind::from_value(&sonic_rs::json!({"count": 10})),
            Some(SubscriptionRewind::Count(10))
        );
        assert_eq!(
            SubscriptionRewind::from_value(&sonic_rs::json!({"seconds": 30})),
            Some(SubscriptionRewind::Seconds(30))
        );
    }

    #[test]
    fn rewind_parser_rejects_invalid_shapes() {
        assert_eq!(SubscriptionRewind::from_value(&sonic_rs::json!(0)), None);
        assert_eq!(
            SubscriptionRewind::from_value(&sonic_rs::json!({"count": 0})),
            None
        );
        assert_eq!(
            SubscriptionRewind::from_value(&sonic_rs::json!({"unknown": true})),
            None
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::options::EventNameFilteringConfig;
    use sonic_rs::json;

    fn event_name_filtering_config() -> EventNameFilteringConfig {
        EventNameFilteringConfig::default()
    }

    #[test]
    fn test_extract_event_name_filter_none_when_no_filter() {
        let (events, tags, expression) =
            SubscriptionRequest::extract_event_name_filter(None).unwrap();
        assert!(events.is_none());
        assert!(tags.is_none());
        assert!(expression.is_none());
    }

    #[test]
    fn test_extract_event_name_filter_compound_with_events() {
        let filter = json!({
            "events": ["price-update", "trade-executed"],
            "tags": {
                "cmp": "eq",
                "key": "headers.asset",
                "val": "BTC"
            }
        });
        let (events, tags, expression) =
            SubscriptionRequest::extract_event_name_filter(Some(filter)).unwrap();
        assert_eq!(events.unwrap(), vec!["price-update", "trade-executed"]);
        let tags = tags.unwrap();
        assert_eq!(tags["cmp"].as_str(), Some("eq"));
        assert_eq!(tags["key"].as_str(), Some("headers.asset"));
        assert_eq!(tags["val"].as_str(), Some("BTC"));
        assert!(expression.is_none());
    }

    #[test]
    fn test_extract_event_name_filter_compound_events_only() {
        let filter = json!({
            "events": ["my-event"]
        });
        let (events, tags, expression) =
            SubscriptionRequest::extract_event_name_filter(Some(filter)).unwrap();
        assert_eq!(events.unwrap(), vec!["my-event"]);
        assert!(tags.is_none());
        assert!(expression.is_none());
    }

    #[test]
    fn test_extract_event_name_filter_empty_events_array() {
        let filter = json!({
            "events": []
        });
        let (events, tags, expression) =
            SubscriptionRequest::extract_event_name_filter(Some(filter)).unwrap();
        // Empty array means no filter (receive all)
        assert!(events.is_none());
        assert!(tags.is_none());
        assert!(expression.is_none());
    }

    #[test]
    fn test_extract_event_name_filter_legacy_filter_passthrough() {
        // Legacy tag filter format (not compound)
        let filter = json!({
            "field": "headers.region",
            "op": "==",
            "value": "us-east"
        });
        let (events, tags, expression) =
            SubscriptionRequest::extract_event_name_filter(Some(filter.clone())).unwrap();
        assert!(events.is_none());
        assert_eq!(tags.unwrap(), filter);
        assert!(expression.is_none());
    }

    #[test]
    fn test_compound_filter_parses_tags_filter_node() {
        let filter = json!({
            "events": ["price-update"],
            "tags": {
                "cmp": "eq",
                "key": "symbol",
                "val": "BTC"
            }
        });

        let request = SubscriptionRequest::build(
            "ticker:*".to_string(),
            None,
            None,
            Some(filter),
            None,
            None,
            None,
            &event_name_filtering_config(),
        )
        .unwrap();

        assert_eq!(request.event_name_filter.unwrap(), vec!["price-update"]);
        #[cfg(feature = "tag-filtering")]
        {
            let tags_filter = request.tags_filter.expect("expected tags filter");
            assert_eq!(tags_filter.cmp.as_deref(), Some("eq"));
            assert_eq!(tags_filter.key.as_deref(), Some("symbol"));
            assert_eq!(tags_filter.val.as_deref(), Some("BTC"));
        }
    }

    #[cfg(feature = "tag-filtering")]
    #[test]
    fn malformed_tag_filter_fails_closed() {
        let filter = json!({
            "tags": {
                "cmp": ["eq"],
                "key": "symbol",
                "val": "BTC"
            }
        });

        let result = SubscriptionRequest::build(
            "ticker".to_string(),
            None,
            None,
            Some(filter),
            None,
            None,
            None,
            &event_name_filtering_config(),
        );
        assert!(result.is_err());
    }

    #[cfg(feature = "tag-filtering")]
    #[test]
    fn compound_expression_compiles_once_at_subscription_boundary() {
        let filter = json!({
            "events": ["order.updated"],
            "expression": {
                "language": "jmespath",
                "source": "headers.priority == `\"high\"` && data.total > `100`"
            }
        });
        let request = SubscriptionRequest::build(
            "orders".to_string(),
            None,
            None,
            Some(filter),
            None,
            None,
            None,
            &event_name_filtering_config(),
        )
        .expect("bounded expression should compile");

        assert_eq!(
            request.event_name_filter.as_deref(),
            Some([String::from("order.updated")].as_slice())
        );
        assert!(
            request
                .predicate
                .as_ref()
                .is_some_and(|predicate| predicate.requires_document())
        );
    }

    #[test]
    fn realtime_subscribe_message_builds_compound_predicate() {
        let data = json!({
            "channel": "orders",
            "filter": {
                "events": ["order.updated"],
                "expression": "data.total >= `100`"
            }
        });
        let mut message = PusherMessage::channel_event("sockudo:subscribe", "orders", data.clone());
        message.data = Some(MessageData::Json(data));

        let request = SubscriptionRequest::from_message(&message, &event_name_filtering_config())
            .expect("valid compound subscription");
        assert_eq!(
            request.event_name_filter.as_deref(),
            Some([String::from("order.updated")].as_slice())
        );
        #[cfg(feature = "tag-filtering")]
        assert!(request.predicate.is_some());
    }

    #[test]
    fn non_string_event_filter_entry_fails_closed() {
        let filter = json!({ "events": ["valid", 42] });
        let result = SubscriptionRequest::build(
            "ticker".to_string(),
            None,
            None,
            Some(filter),
            None,
            None,
            None,
            &event_name_filtering_config(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_max_50_event_names() {
        let many_events: Vec<String> = (0..51).map(|i| format!("event-{}", i)).collect();
        let filter = json!({
            "events": many_events
        });
        let result = SubscriptionRequest::build(
            "test-channel".to_string(),
            None,
            None,
            Some(filter),
            None,
            None,
            None,
            &event_name_filtering_config(),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("exceeds maximum of 50")
        );
    }

    #[test]
    fn test_validation_event_name_max_200_chars() {
        let long_name = "a".repeat(201);
        let filter = json!({
            "events": [long_name]
        });
        let result = SubscriptionRequest::build(
            "test-channel".to_string(),
            None,
            None,
            Some(filter),
            None,
            None,
            None,
            &event_name_filtering_config(),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("exceeds maximum length of 200")
        );
    }

    #[test]
    fn test_validation_event_name_exactly_200_chars_ok() {
        let name = "a".repeat(200);
        let filter = json!({
            "events": [name]
        });
        let result = SubscriptionRequest::build(
            "test-channel".to_string(),
            None,
            None,
            Some(filter),
            None,
            None,
            None,
            &event_name_filtering_config(),
        );
        assert!(result.is_ok());
        let req = result.unwrap();
        assert_eq!(req.event_name_filter.unwrap().len(), 1);
    }

    #[test]
    fn test_event_name_filtering_disabled_ignores_filter_events() {
        let filter = json!({
            "events": ["price-update"]
        });
        let config = EventNameFilteringConfig {
            enabled: false,
            ..Default::default()
        };
        let result = SubscriptionRequest::build(
            "test-channel".to_string(),
            None,
            None,
            Some(filter),
            None,
            None,
            None,
            &config,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().event_name_filter.is_none());
    }

    #[test]
    fn test_no_filter_results_in_none() {
        let result = SubscriptionRequest::build(
            "test-channel".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            &event_name_filtering_config(),
        );
        assert!(result.is_ok());
        assert!(result.unwrap().event_name_filter.is_none());
    }

    #[test]
    fn test_annotation_subscribe_mode_parses_from_modes_array() {
        let request = SubscriptionRequest::build(
            "test-channel".to_string(),
            None,
            None,
            None,
            None,
            None,
            Some(json!(["ANNOTATION_SUBSCRIBE"])),
            &event_name_filtering_config(),
        )
        .unwrap();

        assert!(request.annotation_subscribe);
    }

    #[test]
    fn test_annotation_subscribe_mode_rejects_non_array() {
        let result = SubscriptionRequest::build(
            "test-channel".to_string(),
            None,
            None,
            None,
            None,
            None,
            Some(json!("ANNOTATION_SUBSCRIBE")),
            &event_name_filtering_config(),
        );

        assert!(result.is_err());
    }
}
