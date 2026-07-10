//! Ably wire DTOs, actions, and projection formats.

use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::HashMap;

pub(crate) const ACTION_HEARTBEAT: u8 = 0;
pub(crate) const ACTION_ACK: u8 = 1;
pub(crate) const ACTION_NACK: u8 = 2;
pub(crate) const ACTION_CONNECT: u8 = 3;
pub(crate) const ACTION_CONNECTED: u8 = 4;
pub(crate) const ACTION_DISCONNECT: u8 = 5;
pub(crate) const ACTION_CLOSE: u8 = 7;
pub(crate) const ACTION_CLOSED: u8 = 8;
pub(crate) const ACTION_ERROR: u8 = 9;
pub(crate) const ACTION_ATTACH: u8 = 10;
pub(crate) const ACTION_ATTACHED: u8 = 11;
pub(crate) const ACTION_DETACH: u8 = 12;
pub(crate) const ACTION_DETACHED: u8 = 13;
pub(crate) const ACTION_PRESENCE: u8 = 14;
pub(crate) const ACTION_MESSAGE: u8 = 15;
pub(crate) const ACTION_AUTH: u8 = 17;

pub(crate) const FLAG_RESUMED: u64 = 1 << 2;
pub(crate) const FLAG_HAS_BACKLOG: u64 = 1 << 1;
pub(crate) const DEFAULT_CONNECTION_STATE_TTL_MS: u64 = 120_000;
pub(crate) const DEFAULT_MAX_IDLE_INTERVAL_MS: u64 = 15_000;
pub(crate) const DEFAULT_MAX_MESSAGE_SIZE: u64 = 64 * 1024;
pub(crate) const DEFAULT_TOKEN_TTL_MS: i64 = 60 * 60 * 1000;
pub(crate) const ABLY_COMPAT_MAX_REPLAY_MESSAGES: usize = 4096;

pub(crate) const MESSAGE_CREATE: u8 = 0;
pub(crate) const MESSAGE_UPDATE: u8 = 1;
pub(crate) const MESSAGE_DELETE: u8 = 2;
pub(crate) const MESSAGE_SUMMARY: u8 = 4;
pub(crate) const MESSAGE_APPEND: u8 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AblyMessageProjection {
    Mutation,
    Aggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AblyFormat {
    Json,
    MsgPack,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyConnectQuery {
    pub(crate) key: Option<String>,
    pub(crate) access_token: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) resume: Option<String>,
    pub(crate) recover: Option<String>,
    pub(crate) format: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyRestQuery {
    pub(crate) key: Option<String>,
    pub(crate) access_token: Option<String>,
    pub(crate) client_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyHistoryQuery {
    pub(crate) key: Option<String>,
    pub(crate) access_token: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) direction: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
    pub(crate) until_attach: Option<bool>,
    #[serde(rename = "from_serial")]
    pub(crate) from_serial: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyTokenRequest {
    pub(crate) key_name: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) ttl: Option<i64>,
    pub(crate) capability: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyErrorInfo {
    pub(crate) message: String,
    pub(crate) code: u32,
    pub(crate) status_code: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyConnectionDetails {
    pub(crate) client_id: Option<String>,
    pub(crate) connection_key: String,
    pub(crate) connection_state_ttl: u64,
    pub(crate) max_idle_interval: u64,
    pub(crate) max_message_size: u64,
    pub(crate) max_frame_size: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyProtocolMessage {
    pub(crate) action: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) flags: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<AblyErrorInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel_serial: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) msg_serial: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) messages: Option<Vec<AblyMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) presence: Option<Vec<AblyPresenceMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) auth: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connection_details: Option<AblyConnectionDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) params: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) res: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) extras: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) serial: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) version: Option<AblyMessageVersion>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyMessageVersion {
    pub(crate) serial: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyPublishResponse {
    pub(crate) serials: Vec<Option<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyPresenceMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyTokenDetails {
    pub(crate) token: String,
    pub(crate) key_name: String,
    pub(crate) issued: i64,
    pub(crate) expires: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) capability: Option<String>,
}
