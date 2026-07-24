//! V2 broadcast pipeline: tag filtering, delta compression, protocol rewriting.
//!
//! This module consolidates all feature-gated V2 logic behind clean function
//! boundaries, so that `local_adapter.rs` (and other adapters) can call into
//! these helpers without scattering `#[cfg]` attributes throughout method bodies.

use bytes::Bytes;
use sockudo_core::error::{Error, Result};
use sockudo_core::utils::{is_wildcard_subscription_pattern, wildcard_pattern_matches};
use sockudo_core::websocket::WebSocketRef;
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::versioned_messages::{
    AppendMode, MessageAction, clear_runtime_append_fragment, extract_runtime_action,
    extract_runtime_append_fragment,
};

/// Apply one socket's append delivery preference and remove internal metadata.
pub(crate) fn apply_append_mode(
    mut message: PusherMessage,
    append_mode: AppendMode,
) -> PusherMessage {
    if append_mode == AppendMode::Delta
        && extract_runtime_action(&message) == Some(MessageAction::Append)
        && let Some(fragment) = extract_runtime_append_fragment(&message)
    {
        message.data = Some(sockudo_protocol::messages::MessageData::String(
            fragment.to_string(),
        ));
    }
    clear_runtime_append_fragment(&mut message);
    message
}

/// Prepare and serialize a V2 message (rewrite prefix, keep serial/message_id).
pub(crate) fn prepare_v2_message(mut message: PusherMessage) -> Result<(PusherMessage, Bytes)> {
    clear_runtime_append_fragment(&mut message);
    message.rewrite_prefix(sockudo_protocol::ProtocolVersion::V2);
    message.idempotency_key = None;
    let v2_bytes = Bytes::from(
        sonic_rs::to_vec(&message)
            .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?,
    );
    Ok((message, v2_bytes))
}

// ---------------------------------------------------------------------------
// Subscription predicate helpers
// ---------------------------------------------------------------------------

fn subscription_view_allows(
    state: &sockudo_core::websocket::PerChannelState,
    message: &PusherMessage,
    filtering_enabled: bool,
    #[cfg(feature = "tag-filtering")] document: Option<&sockudo_filter::ProjectedDocument>,
) -> bool {
    #[cfg(not(feature = "tag-filtering"))]
    let _ = filtering_enabled;
    if message.event.as_deref() == Some(sockudo_protocol::messages::ANNOTATION_EVENT_NAME)
        && !state.annotation_subscribe
    {
        return false;
    }

    #[cfg(feature = "tag-filtering")]
    if filtering_enabled && let Some(predicate) = &state.predicate {
        return predicate
            .matches_projected(message.event.as_deref(), &message.tags, document)
            .unwrap_or_else(|error| {
                tracing::warn!(
                    predicate_id = predicate.id().as_u64(),
                    error = %error,
                    "subscription predicate evaluation failed closed"
                );
                false
            });
    }

    if state.event_name_filter.as_ref().is_some_and(|names| {
        !names.is_empty()
            && message
                .event
                .as_ref()
                .is_none_or(|event| !names.iter().any(|name| name == event))
    }) {
        return false;
    }

    #[cfg(feature = "tag-filtering")]
    if filtering_enabled && let Some(filter) = &state.filter {
        return message
            .tags
            .as_ref()
            .is_some_and(|tags| sockudo_filter::matches(filter, tags));
    }

    true
}

fn should_deliver_for_subscription(
    socket: &WebSocketRef,
    channel: &str,
    message: &PusherMessage,
    filtering_enabled: bool,
    #[cfg(feature = "tag-filtering")] document: Option<&sockudo_filter::ProjectedDocument>,
) -> bool {
    if let Some(state) = socket.channel_state.get(channel)
        && subscription_view_allows(
            state.value(),
            message,
            filtering_enabled,
            #[cfg(feature = "tag-filtering")]
            document,
        )
    {
        return true;
    }

    for entry in socket.channel_state.iter() {
        let subscribed_channel = entry.key();
        let subscribed_channel = subscribed_channel.as_ref();
        if subscribed_channel == channel
            || !is_wildcard_subscription_pattern(subscribed_channel)
            || !wildcard_pattern_matches(channel, subscribed_channel)
        {
            continue;
        }
        if subscription_view_allows(
            entry.value(),
            message,
            filtering_enabled,
            #[cfg(feature = "tag-filtering")]
            document,
        ) {
            return true;
        }
    }
    false
}

#[cfg(feature = "recovery")]
pub(crate) fn has_matching_subscription(socket: &WebSocketRef, channel: &str) -> bool {
    socket.channel_state.iter().any(|entry| {
        let subscribed = entry.key().as_ref();
        subscribed == channel
            || (is_wildcard_subscription_pattern(subscribed)
                && wildcard_pattern_matches(channel, subscribed))
    })
}

#[cfg(feature = "tag-filtering")]
fn matching_subscription_requires_document(socket: &WebSocketRef, channel: &str) -> bool {
    if socket.channel_state.get(channel).is_some_and(|state| {
        state
            .predicate
            .as_ref()
            .is_some_and(|predicate| predicate.requires_document())
    }) {
        return true;
    }
    socket.channel_state.iter().any(|state| {
        let subscribed = state.key().as_ref();
        is_wildcard_subscription_pattern(subscribed)
            && wildcard_pattern_matches(channel, subscribed)
            && state
                .predicate
                .as_ref()
                .is_some_and(|predicate| predicate.requires_document())
    })
}

pub(crate) fn socket_allows_message(
    socket: &WebSocketRef,
    channel: &str,
    message: &PusherMessage,
    filtering_enabled: bool,
) -> bool {
    #[cfg(feature = "tag-filtering")]
    let document = matching_subscription_requires_document(socket, channel)
        .then(|| {
            sockudo_filter::ProjectedDocument::new_bounded(
                &crate::message_predicate::NativeMessageProjection::from_message(message),
                64 * 1024,
            )
        })
        .transpose()
        .unwrap_or_else(|error| {
            tracing::warn!(error = %error, "message predicate projection failed closed");
            None
        });

    should_deliver_for_subscription(
        socket,
        channel,
        message,
        filtering_enabled,
        #[cfg(feature = "tag-filtering")]
        document.as_ref(),
    )
}

pub(crate) fn apply_subscription_predicates_in_place(
    filtering_enabled: bool,
    channel: &str,
    message: &PusherMessage,
    sockets: &mut Vec<WebSocketRef>,
) {
    #[cfg(feature = "tag-filtering")]
    let document = sockets
        .iter()
        .any(|socket| matching_subscription_requires_document(socket, channel))
        .then(|| {
            sockudo_filter::ProjectedDocument::new_bounded(
                &crate::message_predicate::NativeMessageProjection::from_message(message),
                64 * 1024,
            )
        })
        .transpose()
        .unwrap_or_else(|error| {
            tracing::warn!(error = %error, "message predicate projection failed closed");
            None
        });

    sockets.retain(|socket| {
        should_deliver_for_subscription(
            socket,
            channel,
            message,
            filtering_enabled,
            #[cfg(feature = "tag-filtering")]
            document.as_ref(),
        )
    });
}

/// Strip tags from a message if tag inclusion is disabled for the channel.
#[cfg(feature = "tag-filtering")]
pub(crate) fn strip_tags_if_disabled(message: PusherMessage, enable_tags: bool) -> PusherMessage {
    if !enable_tags && message.tags.is_some() {
        let mut msg = message;
        msg.tags = None;
        msg
    } else {
        message
    }
}

/// Resolve whether tags should be included for a channel (channel-level
/// override via `ChannelDeltaSettings` or the global default).
#[cfg(feature = "tag-filtering")]
pub(crate) fn should_enable_tags(
    #[cfg(feature = "delta")] channel_settings: Option<&sockudo_delta::ChannelDeltaSettings>,
    global_enable_tags: bool,
) -> bool {
    #[cfg(feature = "delta")]
    {
        channel_settings
            .map(|s| s.enable_tags)
            .unwrap_or(global_enable_tags)
    }
    #[cfg(not(feature = "delta"))]
    {
        global_enable_tags
    }
}

#[cfg(all(test, feature = "tag-filtering"))]
mod predicate_tests {
    use super::*;
    use sockudo_core::websocket::PerChannelState;
    use sockudo_filter::{MessagePredicate, SubscriptionView, node::FilterNodeBuilder};
    use std::{collections::BTreeMap, sync::Arc};

    fn message(event: &str, region: &str) -> PusherMessage {
        let mut message =
            PusherMessage::channel_event(event, "orders.eu", sonic_rs::json!({"total": 125}));
        message.data = Some(sockudo_protocol::messages::MessageData::Json(
            sonic_rs::json!({"total": 125}),
        ));
        message.tags = Some(BTreeMap::from([("region".into(), region.into())]));
        message
    }

    #[test]
    fn subscription_components_are_anded_within_one_view() {
        let predicate = MessagePredicate::compile(SubscriptionView {
            events: vec!["order.updated".into()],
            tags: Some(FilterNodeBuilder::eq("region", "eu")),
            expression: Some("data.total >= `100`".into()),
        })
        .unwrap();
        let state = PerChannelState {
            predicate: Some(Arc::new(predicate)),
            ..PerChannelState::default()
        };
        let accepted = message("order.updated", "eu");
        let document = sockudo_filter::ProjectedDocument::new_bounded(
            &crate::message_predicate::NativeMessageProjection::from_message(&accepted),
            64 * 1024,
        )
        .unwrap();
        assert!(subscription_view_allows(
            &state,
            &accepted,
            true,
            Some(&document)
        ));
        assert!(!subscription_view_allows(
            &state,
            &message("order.created", "eu"),
            true,
            Some(&document)
        ));
        assert!(!subscription_view_allows(
            &state,
            &message("order.updated", "us"),
            true,
            Some(&document)
        ));
    }

    #[test]
    fn annotation_permission_is_part_of_the_accepting_view() {
        let state = PerChannelState {
            predicate: Some(Arc::new(
                MessagePredicate::compile(SubscriptionView::default()).unwrap(),
            )),
            annotation_subscribe: false,
            ..PerChannelState::default()
        };
        assert!(!subscription_view_allows(
            &state,
            &message(sockudo_protocol::messages::ANNOTATION_EVENT_NAME, "eu"),
            true,
            None
        ));
    }
}
