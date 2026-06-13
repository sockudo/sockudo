use super::types::{
    PresenceHistoryEventCause, PresenceHistoryEventKind, PresenceHistoryRetentionPolicy,
    PresenceHistoryTransitionRecord,
};

pub(crate) fn transition(
    published_at_ms: i64,
    dedupe_key: &str,
    event: PresenceHistoryEventKind,
    user_id: &str,
) -> PresenceHistoryTransitionRecord {
    PresenceHistoryTransitionRecord {
        app_id: "app".to_string(),
        channel: "presence-room".to_string(),
        event_kind: event,
        cause: match event {
            PresenceHistoryEventKind::MemberAdded => PresenceHistoryEventCause::Join,
            PresenceHistoryEventKind::MemberUpdated => PresenceHistoryEventCause::Join,
            PresenceHistoryEventKind::MemberRemoved => PresenceHistoryEventCause::Disconnect,
        },
        user_id: user_id.to_string(),
        connection_id: Some(format!("socket-{user_id}")),
        user_info: None,
        dead_node_id: None,
        dedupe_key: dedupe_key.to_string(),
        published_at_ms,
        retention: PresenceHistoryRetentionPolicy {
            retention_window_seconds: 3600,
            max_events_per_channel: None,
            max_bytes_per_channel: None,
        },
    }
}
