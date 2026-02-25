pub use sockudo_types::app::App;

impl From<crate::options::ConfiguredApp> for App {
    fn from(value: crate::options::ConfiguredApp) -> Self {
        Self {
            id: value.id,
            key: value.key,
            secret: value.secret,
            max_connections: value.max_connections,
            enable_client_messages: value.enable_client_messages,
            enabled: value.enabled,
            max_backend_events_per_second: value.max_backend_events_per_second,
            max_client_events_per_second: value.max_client_events_per_second,
            max_read_requests_per_second: value.max_read_requests_per_second,
            max_presence_members_per_channel: value.max_presence_members_per_channel,
            max_presence_member_size_in_kb: value.max_presence_member_size_in_kb,
            max_channel_name_length: value.max_channel_name_length,
            max_event_channels_at_once: value.max_event_channels_at_once,
            max_event_name_length: value.max_event_name_length,
            max_event_payload_in_kb: value.max_event_payload_in_kb,
            max_event_batch_size: value.max_event_batch_size,
            enable_user_authentication: value.enable_user_authentication,
            webhooks: value.webhooks,
            enable_watchlist_events: value.enable_watchlist_events,
            allowed_origins: value.allowed_origins,
            channel_delta_compression: value.channel_delta_compression,
        }
    }
}
