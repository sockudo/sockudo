use compact_str::{CompactString, format_compact};
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_protocol::messages::{
    ANNOTATION_EVENT_NAME, MESSAGE_SUMMARY_EVENT_NAME, PusherMessage,
};
use sockudo_protocol::protocol_version::{CANONICAL_PRESENCE_UPDATE, ProtocolVersion};

pub(super) fn pending_presence_key(app_id: &str, channel: &str, user_id: &str) -> CompactString {
    format_compact!("{app_id}:{channel}:{user_id}")
}

pub(super) fn pending_presence_channel_key(app_id: &str, channel: &str) -> CompactString {
    format_compact!("{app_id}:{channel}")
}

#[derive(Debug, Clone)]
pub(super) struct PendingPresenceMember {
    pub(super) member: PresenceMemberInfo,
    pub(super) socket_id: String,
    pub(super) generation: u64,
}

pub(super) fn is_v2_annotation_protocol_event(message: &PusherMessage) -> bool {
    matches!(
        message.event.as_deref(),
        Some(ANNOTATION_EVENT_NAME) | Some(MESSAGE_SUMMARY_EVENT_NAME)
    )
}

pub(super) fn is_v2_only_protocol_event(message: &PusherMessage) -> bool {
    is_v2_annotation_protocol_event(message)
        || matches!(
            message
                .event
                .as_deref()
                .and_then(|event| ProtocolVersion::parse_any_protocol_event(event)),
            Some((CANONICAL_PRESENCE_UPDATE, true))
        )
}
