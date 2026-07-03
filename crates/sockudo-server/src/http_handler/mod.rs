//! HTTP API handlers split by endpoint area.

#[cfg(feature = "ably-compat")]
mod ably_compat;
mod ai;
mod annotations;
mod channels;
mod errors;
mod events;
mod history;
mod presence_history;
mod system;
mod versioned_messages;

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(feature = "ably-compat")]
pub use ably_compat::{
    ably_channel_history, ably_channel_message, ably_channel_message_versions, ably_channel_status,
    ably_request_token, ably_time, global_ably_hub, handle_ably_realtime_upgrade,
};
pub use annotations::{channel_message_annotations, delete_annotation, publish_annotation};
pub use channels::{
    channel, channel_users, channels, revoke_capability_tokens, terminate_user_connections,
};
pub use errors::AppError;
pub use events::{batch_events, events};
pub use history::{
    channel_history, channel_history_purge, channel_history_reset, channel_history_state,
};
pub use presence_history::{
    channel_presence_history, channel_presence_history_reset, channel_presence_history_snapshot,
    channel_presence_history_state,
};
pub use sockudo_core::auth::EventQuery;
pub use system::{fallback_404, live, metrics, stats, up, usage};
pub use versioned_messages::{
    append_message, channel_message, channel_message_versions, delete_message, update_message,
};

#[cfg(test)]
pub use channels::ChannelQuery;
#[cfg(test)]
pub use history::HistoryQuery;
#[cfg(test)]
pub use versioned_messages::VersionMutationPath;
