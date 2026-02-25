use ahash::AHashMap;
use serde::{Deserialize, Serialize};

use crate::delta::ChannelDeltaConfig;
use crate::webhook::Webhook;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct App {
    pub id: String,
    pub key: String,
    pub secret: String,
    pub max_connections: u32,
    pub enable_client_messages: bool,
    pub enabled: bool,
    pub max_backend_events_per_second: Option<u32>,
    pub max_client_events_per_second: u32,
    pub max_read_requests_per_second: Option<u32>,
    pub max_presence_members_per_channel: Option<u32>,
    pub max_presence_member_size_in_kb: Option<u32>,
    pub max_channel_name_length: Option<u32>,
    pub max_event_channels_at_once: Option<u32>,
    pub max_event_name_length: Option<u32>,
    pub max_event_payload_in_kb: Option<u32>,
    pub max_event_batch_size: Option<u32>,
    pub enable_user_authentication: Option<bool>,
    pub webhooks: Option<Vec<Webhook>>,
    pub enable_watchlist_events: Option<bool>,
    pub allowed_origins: Option<Vec<String>>,
    pub channel_delta_compression: Option<AHashMap<String, ChannelDeltaConfig>>,
}
