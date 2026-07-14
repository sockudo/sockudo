use crate::error::{Error, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PresenceHistoryDirection {
    NewestFirst,
    OldestFirst,
}

impl PresenceHistoryDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewestFirst => "newest_first",
            Self::OldestFirst => "oldest_first",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceHistoryCursor {
    pub version: u8,
    pub app_id: String,
    pub channel: String,
    pub stream_id: String,
    pub serial: u64,
    pub direction: PresenceHistoryDirection,
    pub bounds: PresenceHistoryQueryBounds,
}

impl PresenceHistoryCursor {
    pub fn encode(&self) -> Result<String> {
        let bytes = sonic_rs::to_vec(self).map_err(|e| {
            Error::Serialization(format!("Failed to encode presence history cursor: {e}"))
        })?;
        Ok(URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|e| {
            Error::InvalidMessageFormat(format!("Invalid presence history cursor: {e}"))
        })?;
        let cursor: Self = sonic_rs::from_slice(&bytes).map_err(|e| {
            Error::InvalidMessageFormat(format!("Invalid presence history cursor: {e}"))
        })?;
        if cursor.version != 1 {
            return Err(Error::InvalidMessageFormat(format!(
                "Unsupported presence history cursor version: {}",
                cursor.version
            )));
        }
        Ok(cursor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PresenceHistoryQueryBounds {
    pub start_serial: Option<u64>,
    pub end_serial: Option<u64>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PresenceHistoryEventKind {
    MemberAdded,
    MemberUpdated,
    MemberRemoved,
}

impl PresenceHistoryEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MemberAdded => "member_added",
            Self::MemberUpdated => "member_updated",
            Self::MemberRemoved => "member_removed",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PresenceHistoryEventCause {
    Join,
    Disconnect,
    OrphanCleanup,
    Timeout,
    ForcedDisconnect,
}

impl PresenceHistoryEventCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::Disconnect => "disconnect",
            Self::OrphanCleanup => "orphan_cleanup",
            Self::Timeout => "timeout",
            Self::ForcedDisconnect => "forced_disconnect",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PresenceHistoryTransitionRecord {
    pub app_id: String,
    pub channel: String,
    pub event_kind: PresenceHistoryEventKind,
    pub cause: PresenceHistoryEventCause,
    pub user_id: String,
    pub connection_id: Option<String>,
    pub user_info: Option<Value>,
    pub dead_node_id: Option<String>,
    pub dedupe_key: String,
    pub published_at_ms: i64,
    pub retention: PresenceHistoryRetentionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceHistoryPayload {
    pub stream_id: String,
    pub serial: u64,
    pub published_at_ms: i64,
    pub event: PresenceHistoryEventKind,
    pub cause: PresenceHistoryEventCause,
    pub user_id: String,
    pub connection_id: Option<String>,
    pub user_info: Option<Value>,
    pub dead_node_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceHistoryRetentionPolicy {
    pub retention_window_seconds: u64,
    pub max_events_per_channel: Option<usize>,
    pub max_bytes_per_channel: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceHistoryItem {
    pub stream_id: String,
    pub serial: u64,
    pub published_at_ms: i64,
    pub event: PresenceHistoryEventKind,
    pub cause: PresenceHistoryEventCause,
    pub user_id: String,
    pub connection_id: Option<String>,
    pub dead_node_id: Option<String>,
    pub payload_size_bytes: usize,
    #[serde(skip)]
    pub payload_bytes: Bytes,
}

#[derive(Debug, Clone)]
pub struct PresenceHistoryReadRequest {
    pub app_id: String,
    pub channel: String,
    pub direction: PresenceHistoryDirection,
    pub limit: usize,
    pub cursor: Option<PresenceHistoryCursor>,
    pub bounds: PresenceHistoryQueryBounds,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PresenceHistoryFilter {
    pub user_id: Option<String>,
    pub connection_id: Option<String>,
}

impl PresenceHistoryFilter {
    #[must_use]
    pub fn matches(&self, item: &PresenceHistoryItem) -> bool {
        self.user_id
            .as_deref()
            .is_none_or(|user_id| item.user_id == user_id)
            && self
                .connection_id
                .as_deref()
                .is_none_or(|connection_id| item.connection_id.as_deref() == Some(connection_id))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.user_id.is_none() && self.connection_id.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PresenceHistoryRetentionStats {
    pub stream_id: Option<String>,
    pub retained_events: u64,
    pub retained_bytes: u64,
    pub oldest_serial: Option<u64>,
    pub newest_serial: Option<u64>,
    pub oldest_published_at_ms: Option<i64>,
    pub newest_published_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct PresenceHistoryPage {
    pub items: Vec<PresenceHistoryItem>,
    pub next_cursor: Option<PresenceHistoryCursor>,
    pub retained: PresenceHistoryRetentionStats,
    pub has_more: bool,
    pub complete: bool,
    pub truncated_by_retention: bool,
    pub degraded: bool,
}

#[derive(Debug, Clone)]
pub struct PresenceSnapshotRequest {
    pub app_id: String,
    pub channel: String,
    /// Reconstruct membership as of this timestamp (inclusive). If None, uses the latest state.
    pub at_time_ms: Option<i64>,
    /// Reconstruct membership as of this serial (inclusive). If None, uses the latest state.
    pub at_serial: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceSnapshotMember {
    pub user_id: String,
    pub last_event: PresenceHistoryEventKind,
    pub last_event_serial: u64,
    pub last_event_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct PresenceSnapshot {
    pub channel: String,
    pub members: Vec<PresenceSnapshotMember>,
    pub events_replayed: u64,
    pub snapshot_serial: Option<u64>,
    pub snapshot_time_ms: Option<i64>,
    pub retained: PresenceHistoryRetentionStats,
    pub complete: bool,
    pub truncated_by_retention: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PresenceHistoryDurableState {
    #[default]
    Healthy,
    Degraded,
    ResetRequired,
}

impl PresenceHistoryDurableState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::ResetRequired => "reset_required",
        }
    }

    pub fn continuity_proven(self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub fn reset_required(self) -> bool {
        matches!(self, Self::ResetRequired)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceHistoryStreamRuntimeState {
    pub app_id: String,
    pub channel: String,
    pub stream_id: Option<String>,
    pub durable_state: PresenceHistoryDurableState,
    pub continuity_proven: bool,
    pub reset_required: bool,
    pub reason: Option<String>,
    pub node_id: Option<String>,
    pub last_transition_at_ms: Option<i64>,
    pub authoritative_source: String,
    pub observed_source: String,
}

impl PresenceHistoryStreamRuntimeState {
    pub fn healthy(
        app_id: impl Into<String>,
        channel: impl Into<String>,
        stream_id: Option<String>,
        source: &str,
    ) -> Self {
        Self {
            app_id: app_id.into(),
            channel: channel.into(),
            stream_id,
            durable_state: PresenceHistoryDurableState::Healthy,
            continuity_proven: true,
            reset_required: false,
            reason: None,
            node_id: None,
            last_transition_at_ms: None,
            authoritative_source: source.to_string(),
            observed_source: source.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceHistoryStreamInspection {
    pub app_id: String,
    pub channel: String,
    pub stream_id: Option<String>,
    pub next_serial: Option<u64>,
    pub retained: PresenceHistoryRetentionStats,
    pub state: PresenceHistoryStreamRuntimeState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceHistoryResetResult {
    pub app_id: String,
    pub channel: String,
    pub previous_stream_id: Option<String>,
    pub new_stream_id: String,
    pub purged_events: u64,
    pub purged_bytes: u64,
    pub inspection: PresenceHistoryStreamInspection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PresenceHistoryRuntimeStatus {
    pub enabled: bool,
    pub backend: String,
    pub state_authority: String,
    pub degraded_channels: usize,
    pub reset_required_channels: usize,
    pub queue_depth: usize,
}

impl Default for PresenceHistoryRuntimeStatus {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: "disabled".to_string(),
            state_authority: "disabled".to_string(),
            degraded_channels: 0,
            reset_required_channels: 0,
            queue_depth: 0,
        }
    }
}

impl PresenceHistoryReadRequest {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "Presence history limit must be greater than 0".to_string(),
            ));
        }

        if let (Some(start), Some(end)) = (self.bounds.start_serial, self.bounds.end_serial)
            && start > end
        {
            return Err(Error::InvalidMessageFormat(
                "start_serial must be less than or equal to end_serial".to_string(),
            ));
        }

        if let (Some(start), Some(end)) = (self.bounds.start_time_ms, self.bounds.end_time_ms)
            && start > end
        {
            return Err(Error::InvalidMessageFormat(
                "start_time_ms must be less than or equal to end_time_ms".to_string(),
            ));
        }

        if let Some(cursor) = &self.cursor {
            if cursor.app_id != self.app_id {
                return Err(Error::InvalidMessageFormat(
                    "Presence history cursor app does not match the request".to_string(),
                ));
            }
            if cursor.channel != self.channel {
                return Err(Error::InvalidMessageFormat(
                    "Presence history cursor channel does not match the request".to_string(),
                ));
            }
            if cursor.direction != self.direction {
                return Err(Error::InvalidMessageFormat(
                    "Presence history cursor direction does not match the request".to_string(),
                ));
            }
            if cursor.bounds != self.bounds {
                return Err(Error::InvalidMessageFormat(
                    "Presence history cursor bounds do not match the request".to_string(),
                ));
            }
        }

        Ok(())
    }
}
