//! V2 broadcast pipeline: tag filtering, delta compression, protocol rewriting.
//!
//! This module consolidates all feature-gated V2 logic behind clean function
//! boundaries, so that `local_adapter.rs` (and other adapters) can call into
//! these helpers without scattering `#[cfg]` attributes throughout method bodies.

use bytes::Bytes;
use sockudo_core::error::{Error, Result};
#[cfg(feature = "tag-filtering")]
use sockudo_core::namespace::Namespace;
use sockudo_core::utils::{is_wildcard_subscription_pattern, wildcard_pattern_matches};
use sockudo_core::websocket::{SocketId, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;

/// Prepare and serialize a V2 message (rewrite prefix, keep serial/message_id).
pub(crate) fn prepare_v2_message(mut message: PusherMessage) -> Result<(PusherMessage, Bytes)> {
    message.rewrite_prefix(sockudo_protocol::ProtocolVersion::V2);
    message.idempotency_key = None;
    let v2_bytes = Bytes::from(
        sonic_rs::to_vec(&message)
            .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?,
    );
    Ok((message, v2_bytes))
}

// ---------------------------------------------------------------------------
// Tag filtering helpers (only compiled with the `tag-filtering` feature)
// ---------------------------------------------------------------------------

/// Apply tag filtering to V2 sockets using the filter index.
///
/// Looks up the message's tags in the index and returns only the sockets that
/// should receive the message. Sockets with no filter receive all messages.
#[cfg(feature = "tag-filtering")]
fn matching_channel_filter_entries(
    socket: &WebSocketRef,
    channel: &str,
) -> Vec<Option<std::sync::Arc<sockudo_filter::FilterNode>>> {
    socket
        .channel_filters
        .iter()
        .filter_map(|entry| {
            let subscribed_channel = entry.key();
            let matches = subscribed_channel == channel
                || (is_wildcard_subscription_pattern(subscribed_channel)
                    && wildcard_pattern_matches(channel, subscribed_channel));
            matches.then(|| entry.value().clone())
        })
        .collect()
}

#[cfg(feature = "tag-filtering")]
fn should_deliver_for_tags(
    socket: &WebSocketRef,
    channel: &str,
    tags: Option<&std::collections::BTreeMap<String, String>>,
) -> bool {
    let matching_filters = matching_channel_filter_entries(socket, channel);
    if matching_filters.is_empty() {
        return false;
    }

    matching_filters
        .into_iter()
        .any(|filter| match (&filter, tags) {
            (None, _) => true,
            (Some(filter), Some(tags)) => sockudo_filter::matches(filter, tags),
            (Some(_), None) => false,
        })
}

fn event_name_filter_allows(socket: &WebSocketRef, channel: &str, event_name: &str) -> bool {
    let mut matched = false;

    for entry in socket.event_name_filters.iter() {
        let subscribed_channel = entry.key();
        let channel_matches = subscribed_channel == channel
            || (is_wildcard_subscription_pattern(subscribed_channel)
                && wildcard_pattern_matches(channel, subscribed_channel));
        if !channel_matches {
            continue;
        }

        matched = true;
        match entry.value() {
            None => return true,
            Some(names) if names.is_empty() => return true,
            Some(names) if names.iter().any(|name| name == event_name) => return true,
            Some(_) => {}
        }
    }

    !matched
}

#[cfg(feature = "tag-filtering")]
pub(crate) fn apply_tag_filter(
    filter_index: &crate::filter_index::FilterIndex,
    tag_filtering_enabled: bool,
    channel: &str,
    message: &PusherMessage,
    v2_sockets: Vec<WebSocketRef>,
    except: Option<&SocketId>,
    namespace: &Namespace,
) -> Vec<WebSocketRef> {
    if !tag_filtering_enabled {
        return v2_sockets;
    }

    let _ = (filter_index, except, namespace);
    let tags_btree = message.tags.as_ref().map(|tags| {
        tags.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<std::collections::BTreeMap<String, String>>()
    });

    v2_sockets
        .into_iter()
        .filter(|socket| should_deliver_for_tags(socket, channel, tags_btree.as_ref()))
        .collect()
}

/// Same as `apply_tag_filter` but also verifies matched sockets are V2.
///
/// Used by `send_with_compression` where the filter index may contain both V1
/// and V2 socket IDs (V1 sockets are sent to separately without compression).
#[cfg(feature = "tag-filtering")]
pub(crate) fn apply_tag_filter_v2_only(
    filter_index: &crate::filter_index::FilterIndex,
    tag_filtering_enabled: bool,
    channel: &str,
    message: &PusherMessage,
    v2_sockets: Vec<WebSocketRef>,
    except: Option<&SocketId>,
    namespace: &Namespace,
) -> Vec<WebSocketRef> {
    if !tag_filtering_enabled {
        return v2_sockets;
    }

    let _ = (filter_index, except, namespace);
    let tags_btree = message.tags.as_ref().map(|tags| {
        tags.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<std::collections::BTreeMap<String, String>>()
    });

    v2_sockets
        .into_iter()
        .filter(|socket| socket.protocol_version == sockudo_protocol::ProtocolVersion::V2)
        .filter(|socket| should_deliver_for_tags(socket, channel, tags_btree.as_ref()))
        .collect()
}

/// Apply event name filtering to a list of V2 sockets.
/// Sockets whose subscription has an event name filter that does not include
/// the message's event name are removed from the list.
pub(crate) fn apply_event_name_filter(
    channel: &str,
    message: &PusherMessage,
    sockets: Vec<sockudo_core::websocket::WebSocketRef>,
) -> Vec<sockudo_core::websocket::WebSocketRef> {
    let event_name = match message.event.as_deref() {
        Some(name) => name,
        None => return sockets, // no event name = deliver to all
    };

    sockets
        .into_iter()
        .filter(|socket| event_name_filter_allows(socket, channel, event_name))
        .collect()
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
