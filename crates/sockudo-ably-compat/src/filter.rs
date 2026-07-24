//! Bounded Ably derived-channel filter parsing and evaluation.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Serialize;
use sockudo_filter::{EmptyTagMap, MessagePredicate, ProjectedDocument, SubscriptionView};
use sonic_rs::{JsonValueTrait, Value};

use crate::{channel_name::AblyChannelName, protocol::AblyMessage};

const MAX_FILTER_EXPRESSION_BYTES: usize = 4096;

#[derive(Clone)]
pub(crate) struct AblyMessageFilter {
    predicate: MessagePredicate,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FilterInput<'a> {
    name: &'a Option<String>,
    data: &'a Option<Value>,
    client_id: &'a Option<String>,
    connection_id: &'a Option<String>,
    encoding: &'a Option<String>,
    extras: &'a Option<Value>,
    headers: Option<&'a Value>,
}

impl AblyMessageFilter {
    pub(crate) fn source_from_channel(channel: &AblyChannelName) -> Result<Option<String>, String> {
        let Some(qualifier) = channel.qualifier() else {
            return Ok(None);
        };
        let Some(encoded) = qualifier.strip_prefix("filter=") else {
            return Ok(None);
        };
        let encoded = encoded
            .split_once('?')
            .map_or(encoded, |(filter, _)| filter);
        if encoded.is_empty() || encoded.len() > MAX_FILTER_EXPRESSION_BYTES.saturating_mul(2) {
            return Err("derived channel filter is empty or oversized".to_string());
        }
        let decoded = STANDARD
            .decode(encoded)
            .map_err(|_| "derived channel filter is not valid base64".to_string())?;
        if decoded.is_empty() || decoded.len() > MAX_FILTER_EXPRESSION_BYTES {
            return Err("derived channel filter is empty or oversized".to_string());
        }
        let source = std::str::from_utf8(&decoded)
            .map_err(|_| "derived channel filter is not valid UTF-8".to_string())?;
        Ok(Some(source.to_owned()))
    }

    pub(crate) fn compile(source: &str) -> Result<Self, String> {
        let predicate = MessagePredicate::compile(SubscriptionView {
            expression: Some(source.to_owned()),
            ..SubscriptionView::default()
        })
        .map_err(|error| format!("derived channel filter is invalid: {error}"))?;
        Ok(Self { predicate })
    }

    #[cfg(test)]
    pub(crate) fn from_channel(channel: &AblyChannelName) -> Result<Option<Self>, String> {
        Self::source_from_channel(channel)?
            .map(|source| Self::compile(&source))
            .transpose()
    }

    pub(crate) fn matches(&self, message: &AblyMessage) -> Result<bool, String> {
        let headers = message
            .extras
            .as_ref()
            .and_then(|extras| extras.get("headers"));
        let projected = ProjectedDocument::new_bounded(
            &FilterInput {
                name: &message.name,
                data: &message.data,
                client_id: &message.client_id,
                connection_id: &message.connection_id,
                encoding: &message.encoding,
                extras: &message.extras,
                headers,
            },
            64 * 1024,
        )
        .map_err(|error| error.to_string())?;
        self.predicate
            .matches_projected(None, &EmptyTagMap, Some(&projected))
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::json;

    fn derived(expression: &str) -> AblyChannelName {
        let encoded = STANDARD.encode(expression);
        AblyChannelName::parse(format!("[filter={encoded}]chan")).expect("valid channel")
    }

    #[test]
    fn evaluates_message_name_and_headers() {
        let filter = AblyMessageFilter::from_channel(&derived(
            "name == `\"filtered\"` && headers.number == `26095`",
        ))
        .expect("valid filter")
        .expect("derived filter");
        let matching = AblyMessage {
            name: Some("filtered".to_string()),
            extras: Some(json!({ "headers": { "number": 26095 } })),
            ..AblyMessage::default()
        };
        assert!(filter.matches(&matching).expect("evaluation succeeds"));

        let rejected = AblyMessage {
            extras: Some(json!({ "headers": { "number": 12345 } })),
            ..matching
        };
        assert!(!filter.matches(&rejected).expect("evaluation succeeds"));
    }

    #[test]
    fn rejects_invalid_and_oversized_filters() {
        let invalid = AblyChannelName::parse("[filter=%%%]chan".to_string()).unwrap();
        assert!(AblyMessageFilter::from_channel(&invalid).is_err());

        let oversized = derived(&"x".repeat(MAX_FILTER_EXPRESSION_BYTES + 1));
        assert!(AblyMessageFilter::from_channel(&oversized).is_err());
    }
}
