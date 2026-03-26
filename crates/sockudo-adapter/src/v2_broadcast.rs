//! V2 broadcast pipeline: tag filtering, delta compression, protocol rewriting.
//!
//! This module consolidates all feature-gated V2 logic behind clean function
//! boundaries, so that `local_adapter.rs` (and other adapters) can call into
//! these helpers without scattering `#[cfg]` attributes throughout method bodies.

use bytes::Bytes;
use sockudo_core::error::{Error, Result};
#[cfg(feature = "tag-filtering")]
use sockudo_core::namespace::Namespace;
#[cfg(feature = "tag-filtering")]
use sockudo_core::websocket::{SocketId, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;

/// Prepare and serialize a V2 message (rewrite prefix, keep serial/message_id).
pub(crate) fn prepare_v2_message(mut message: PusherMessage) -> Result<(PusherMessage, Bytes)> {
    message.rewrite_prefix(sockudo_protocol::ProtocolVersion::V2);
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

    if let Some(tags) = message.tags.as_ref() {
        let tags_btree: std::collections::BTreeMap<String, String> =
            tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        let lookup_result = filter_index.lookup(channel, &tags_btree);

        tracing::debug!(
            "FilterIndex lookup: channel={}, tags={:?}, indexed_matches={}, no_filter={}, needs_evaluation={}",
            channel,
            tags.keys().collect::<Vec<_>>(),
            lookup_result.indexed_matches.len(),
            lookup_result.no_filter.len(),
            lookup_result.needs_evaluation.len()
        );

        let mut result = Vec::with_capacity(
            lookup_result.indexed_matches.len() + lookup_result.no_filter.len(),
        );

        for socket_id in lookup_result.indexed_matches {
            if except.is_some_and(|e| *e == socket_id) {
                continue;
            }
            if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                result.push(socket_ref.value().clone());
            }
        }

        for socket_id in lookup_result.no_filter {
            if except.is_some_and(|e| *e == socket_id) {
                continue;
            }
            if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                result.push(socket_ref.value().clone());
            }
        }

        for socket_id in lookup_result.needs_evaluation {
            if except.is_some_and(|e| *e == socket_id) {
                continue;
            }
            if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                let socket_ref = socket_ref.value().clone();
                let channel_filter = socket_ref.get_channel_filter_sync(channel);
                if let Some(filter) = channel_filter {
                    if sockudo_filter::matches(&filter, tags) {
                        result.push(socket_ref);
                    }
                } else {
                    result.push(socket_ref);
                }
            }
        }

        tracing::debug!(
            "FilterIndex result: {} sockets to receive message",
            result.len()
        );

        result
    } else {
        // Tag filtering enabled but message has no tags - only no_filter sockets receive it
        let empty_tags = std::collections::BTreeMap::new();
        let lookup_result = filter_index.lookup(channel, &empty_tags);

        lookup_result
            .no_filter
            .into_iter()
            .filter(|socket_id| except != Some(socket_id))
            .filter_map(|socket_id| namespace.sockets.get(&socket_id).map(|r| r.value().clone()))
            .collect()
    }
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

    if let Some(tags) = message.tags.as_ref() {
        let tags_btree: std::collections::BTreeMap<String, String> =
            tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        let lookup_result = filter_index.lookup(channel, &tags_btree);

        let mut result = Vec::with_capacity(
            lookup_result.indexed_matches.len() + lookup_result.no_filter.len(),
        );

        for socket_id in lookup_result.indexed_matches {
            if except.is_some_and(|e| *e == socket_id) {
                continue;
            }
            if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                let sr = socket_ref.value().clone();
                if sr.protocol_version == sockudo_protocol::ProtocolVersion::V2 {
                    result.push(sr);
                }
            }
        }

        for socket_id in lookup_result.no_filter {
            if except.is_some_and(|e| *e == socket_id) {
                continue;
            }
            if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                let sr = socket_ref.value().clone();
                if sr.protocol_version == sockudo_protocol::ProtocolVersion::V2 {
                    result.push(sr);
                }
            }
        }

        for socket_id in lookup_result.needs_evaluation {
            if except.is_some_and(|e| *e == socket_id) {
                continue;
            }
            if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                let socket_ref = socket_ref.value().clone();
                if socket_ref.protocol_version != sockudo_protocol::ProtocolVersion::V2 {
                    continue;
                }
                let channel_filter = socket_ref.get_channel_filter_sync(channel);
                if let Some(filter) = channel_filter {
                    if sockudo_filter::matches(&filter, tags) {
                        result.push(socket_ref);
                    }
                } else {
                    result.push(socket_ref);
                }
            }
        }

        result
    } else {
        let empty_tags = std::collections::BTreeMap::new();
        let lookup_result = filter_index.lookup(channel, &empty_tags);

        lookup_result
            .no_filter
            .into_iter()
            .filter(|socket_id| except != Some(socket_id))
            .filter_map(|socket_id| {
                namespace.sockets.get(&socket_id).and_then(|r| {
                    let sr = r.value().clone();
                    (sr.protocol_version == sockudo_protocol::ProtocolVersion::V2).then_some(sr)
                })
            })
            .collect()
    }
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
