use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Webhook {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<url::Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lambda_function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lambda: Option<LambdaConfig>,
    pub event_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<WebhookFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<WebhookHeaders>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<WebhookRetryPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEventType {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct WebhookFilter {
    pub channel_prefix: Option<String>,
    pub channel_suffix: Option<String>,
    pub channel_pattern: Option<String>,
    pub channel_namespace: Option<String>,
    pub channel_namespaces: Option<Vec<String>>,
}

impl WebhookFilter {
    /// Returns `true` if `channel` passes every constraint in this filter.
    /// Absent fields always pass. Invalid `channel_pattern` returns `false`.
    ///
    /// Compiles the pattern on every call; prefer
    /// [`Self::matches_channel_with_pattern`] when evaluating many channels.
    pub fn matches_channel(&self, channel: &str) -> bool {
        let pattern = match &self.channel_pattern {
            Some(pattern) => match regex::Regex::new(pattern) {
                Ok(re) => Some(re),
                Err(_) => return false,
            },
            None => None,
        };
        self.matches_channel_with_pattern(channel, pattern.as_ref())
    }

    /// Pre-compiled variant of [`Self::matches_channel`].
    /// `pattern` is the compiled `channel_pattern`, or `None` when absent.
    pub fn matches_channel_with_pattern(
        &self,
        channel: &str,
        pattern: Option<&regex::Regex>,
    ) -> bool {
        if let Some(prefix) = &self.channel_prefix
            && !channel.starts_with(prefix)
        {
            return false;
        }
        if let Some(suffix) = &self.channel_suffix
            && !channel.ends_with(suffix)
        {
            return false;
        }
        if let Some(regex) = pattern
            && !regex.is_match(channel)
        {
            return false;
        }
        let namespace = crate::utils::channel_namespace_name(channel);
        if let Some(expected) = &self.channel_namespace
            && namespace != Some(expected.as_str())
        {
            return false;
        }
        if let Some(expected) = &self.channel_namespaces
            && !expected.iter().any(|c| namespace == Some(c.as_str()))
        {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct WebhookRetryPolicy {
    pub enabled: Option<bool>,
    pub max_attempts: Option<u32>,
    pub max_elapsed_time_ms: Option<u64>,
    pub initial_backoff_ms: Option<u64>,
    pub max_backoff_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WebhookHeaders {
    pub headers: AHashMap<String, String>,
}

impl Serialize for WebhookHeaders {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.headers.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WebhookHeaders {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Flatten workaround for sonic-rs issue #114.
        let headers = AHashMap::<String, String>::deserialize(deserializer)?;
        Ok(Self { headers })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct LambdaConfig {
    pub function_name: String,
    pub region: String,
}

// This is the JobData structure that Sockudo uses internally for its queue.
// The `payload` field will be structured to produce the Pusher-compatible format when sent.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct JobData {
    /// Correlates one final queue envelope across webhook processing. Optional
    /// for rolling compatibility with records produced by older versions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    pub app_key: String,
    pub app_id: String,
    pub app_secret: String,
    /// W3C propagation fields carried through durable queues. These are never
    /// included in the user-facing webhook body.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub trace_context: BTreeMap<String, String>,
    pub payload: JobPayload,
    pub original_signature: String,
}

// This is the JobPayload structure.
// The `events` field will now hold a vector of fully formed Pusher event objects.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct JobPayload {
    pub time_ms: i64,
    pub events: Vec<Value>,
}

// This struct represents the final payload sent to the webhook receiver,
// aligning with Pusher's format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PusherWebhookPayload {
    pub time_ms: i64,
    pub events: Vec<Value>,
}

/// Type alias for async job processor callback
pub type JobProcessorFnAsync = Box<
    dyn Fn(JobData) -> Pin<Box<dyn Future<Output = crate::error::Result<()>> + Send>>
        + Send
        + Sync
        + 'static,
>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_job_data_without_job_id_is_accepted() {
        let legacy = r#"{"app_key":"key","app_id":"app","app_secret":"secret","payload":{"time_ms":1,"events":[]},"original_signature":"signature"}"#;
        let job: JobData = sonic_rs::from_str(legacy).expect("legacy job data should deserialize");
        assert_eq!(job.job_id, None);
        assert!(job.trace_context.is_empty());
    }

    fn empty_filter() -> WebhookFilter {
        WebhookFilter {
            channel_prefix: None,
            channel_suffix: None,
            channel_pattern: None,
            channel_namespace: None,
            channel_namespaces: None,
        }
    }

    #[test]
    fn matches_channel_all_none_always_true() {
        let f = empty_filter();
        assert!(f.matches_channel("anything"));
        assert!(f.matches_channel("private-foo:bar"));
        assert!(f.matches_channel(""));
    }

    #[test]
    fn matches_channel_prefix_match() {
        let f = WebhookFilter {
            channel_prefix: Some("orders-".into()),
            ..empty_filter()
        };
        assert!(f.matches_channel("orders-123"));
    }

    #[test]
    fn matches_channel_prefix_mismatch() {
        let f = WebhookFilter {
            channel_prefix: Some("orders-".into()),
            ..empty_filter()
        };
        assert!(!f.matches_channel("invoices-123"));
    }

    #[test]
    fn matches_channel_suffix_match() {
        let f = WebhookFilter {
            channel_suffix: Some("-live".into()),
            ..empty_filter()
        };
        assert!(f.matches_channel("chat-live"));
    }

    #[test]
    fn matches_channel_suffix_mismatch() {
        let f = WebhookFilter {
            channel_suffix: Some("-live".into()),
            ..empty_filter()
        };
        assert!(!f.matches_channel("chat-staging"));
    }

    #[test]
    fn matches_channel_valid_pattern_match() {
        let f = WebhookFilter {
            channel_pattern: Some(r"^orders-\d+$".into()),
            ..empty_filter()
        };
        assert!(f.matches_channel("orders-42"));
    }

    #[test]
    fn matches_channel_valid_pattern_no_match() {
        let f = WebhookFilter {
            channel_pattern: Some(r"^orders-\d+$".into()),
            ..empty_filter()
        };
        assert!(!f.matches_channel("orders-abc"));
    }

    #[test]
    fn matches_channel_invalid_regex_returns_false() {
        let f = WebhookFilter {
            channel_pattern: Some("[invalid".into()),
            ..empty_filter()
        };
        assert!(!f.matches_channel("any-channel"));
    }

    #[test]
    fn matches_channel_with_pattern_applies_supplied_pattern() {
        let f = WebhookFilter {
            channel_prefix: Some("presence-".into()),
            channel_pattern: Some("^presence-lobby$".into()),
            ..empty_filter()
        };
        let pattern = regex::Regex::new("^presence-lobby$").unwrap();

        // Prefix + supplied pattern both match.
        assert!(f.matches_channel_with_pattern("presence-lobby", Some(&pattern)));
        // Prefix matches but supplied pattern rejects.
        assert!(!f.matches_channel_with_pattern("presence-other", Some(&pattern)));
        // Supplied pattern would match but prefix rejects.
        assert!(!f.matches_channel_with_pattern("private-lobby", Some(&pattern)));
    }

    #[test]
    fn matches_channel_with_pattern_none_skips_pattern_check() {
        // Passing None skips the channel_pattern constraint entirely, even
        // though the filter declares one — the caller owns the pattern check.
        let f = WebhookFilter {
            channel_prefix: Some("presence-".into()),
            channel_pattern: Some("^never-matches$".into()),
            ..empty_filter()
        };
        assert!(f.matches_channel_with_pattern("presence-lobby", None));
        assert!(!f.matches_channel_with_pattern("private-lobby", None));
    }

    #[test]
    fn matches_channel_namespace_singular_match() {
        let f = WebhookFilter {
            channel_namespace: Some("chat".into()),
            ..empty_filter()
        };
        assert!(f.matches_channel("chat:room-1"));
    }

    #[test]
    fn matches_channel_namespace_singular_mismatch() {
        let f = WebhookFilter {
            channel_namespace: Some("chat".into()),
            ..empty_filter()
        };
        assert!(!f.matches_channel("orders:123"));
    }

    #[test]
    fn matches_channel_namespaces_plural_in_list() {
        let f = WebhookFilter {
            channel_namespaces: Some(vec!["chat".into(), "orders".into()]),
            ..empty_filter()
        };
        assert!(f.matches_channel("chat:room-1"));
        assert!(f.matches_channel("orders:456"));
    }

    #[test]
    fn matches_channel_namespaces_plural_not_in_list() {
        let f = WebhookFilter {
            channel_namespaces: Some(vec!["chat".into(), "orders".into()]),
            ..empty_filter()
        };
        assert!(!f.matches_channel("invoices:789"));
    }

    #[test]
    fn matches_channel_combined_prefix_and_namespace_both_pass() {
        let f = WebhookFilter {
            channel_prefix: Some("private-".into()),
            channel_namespace: Some("orders".into()),
            ..empty_filter()
        };
        assert!(f.matches_channel("private-orders:123"));
    }

    #[test]
    fn matches_channel_combined_prefix_and_namespace_prefix_fails() {
        let f = WebhookFilter {
            channel_prefix: Some("private-".into()),
            channel_namespace: Some("orders".into()),
            ..empty_filter()
        };
        assert!(!f.matches_channel("orders:123"));
    }

    #[test]
    fn matches_channel_combined_prefix_and_namespace_namespace_fails() {
        let f = WebhookFilter {
            channel_prefix: Some("private-".into()),
            channel_namespace: Some("orders".into()),
            ..empty_filter()
        };
        assert!(!f.matches_channel("private-chat:room-1"));
    }

    #[test]
    fn matches_channel_presence_prefix_namespace_extraction() {
        let f = WebhookFilter {
            channel_namespace: Some("orders".into()),
            ..empty_filter()
        };
        assert!(f.matches_channel("presence-orders:123"));
    }

    #[test]
    fn matches_channel_no_prefix_with_namespace() {
        let f = WebhookFilter {
            channel_namespace: Some("chat".into()),
            ..empty_filter()
        };
        assert!(f.matches_channel("chat:room-1"));
    }
}
