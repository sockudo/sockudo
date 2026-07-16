use crate::error::{Error, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

const MAX_HISTORY_CURSOR_ENCODED_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HistoryDirection {
    NewestFirst,
    OldestFirst,
}

impl HistoryDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewestFirst => "newest_first",
            Self::OldestFirst => "oldest_first",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryPosition {
    pub stream_id: String,
    pub serial: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryCursor {
    pub version: u8,
    pub app_id: String,
    pub channel: String,
    pub stream_id: String,
    pub serial: u64,
    pub direction: HistoryDirection,
    pub bounds: HistoryQueryBounds,
}

impl HistoryCursor {
    pub fn encode(&self) -> Result<String> {
        let bytes = sonic_rs::to_vec(self)
            .map_err(|e| Error::Serialization(format!("Failed to encode history cursor: {e}")))?;
        Ok(URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        if encoded.len() > MAX_HISTORY_CURSOR_ENCODED_BYTES {
            return Err(Error::InvalidMessageFormat(
                "History cursor exceeds 16 KiB".to_string(),
            ));
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid history cursor: {e}")))?;
        let cursor: Self = sonic_rs::from_slice(&bytes)
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid history cursor: {e}")))?;
        if cursor.version != 1 {
            return Err(Error::InvalidMessageFormat(format!(
                "Unsupported history cursor version: {}",
                cursor.version
            )));
        }
        Ok(cursor)
    }
}

#[derive(Debug, Clone)]
pub struct HistoryWriteReservation {
    pub stream_id: String,
    pub serial: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct HistoryQueryBounds {
    pub start_serial: Option<u64>,
    pub end_serial: Option<u64>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct HistoryAppendRecord {
    pub app_id: String,
    pub channel: String,
    pub stream_id: String,
    pub serial: u64,
    pub published_at_ms: i64,
    pub message_id: Option<String>,
    pub event_name: Option<String>,
    pub operation_kind: String,
    pub payload_bytes: Bytes,
    pub retention: HistoryRetentionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryRetentionPolicy {
    pub retention_window_seconds: u64,
    pub max_messages_per_channel: Option<usize>,
    pub max_bytes_per_channel: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryItem {
    pub stream_id: String,
    pub serial: u64,
    pub published_at_ms: i64,
    pub message_id: Option<String>,
    pub event_name: Option<String>,
    pub operation_kind: String,
    pub payload_size_bytes: usize,
    #[serde(skip)]
    pub payload_bytes: Bytes,
}

#[derive(Debug, Clone)]
pub struct HistoryReadRequest {
    pub app_id: String,
    pub channel: String,
    pub direction: HistoryDirection,
    pub limit: usize,
    pub cursor: Option<HistoryCursor>,
    pub bounds: HistoryQueryBounds,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct HistoryRetentionStats {
    pub stream_id: Option<String>,
    pub retained_messages: u64,
    pub retained_bytes: u64,
    pub oldest_serial: Option<u64>,
    pub newest_serial: Option<u64>,
    pub oldest_published_at_ms: Option<i64>,
    pub newest_published_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct HistoryPage {
    pub items: Vec<HistoryItem>,
    pub next_cursor: Option<HistoryCursor>,
    pub retained: HistoryRetentionStats,
    pub has_more: bool,
    pub complete: bool,
    pub truncated_by_retention: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum HistoryDurableState {
    #[default]
    Healthy,
    Degraded,
    ResetRequired,
}

impl HistoryDurableState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::ResetRequired => "reset_required",
        }
    }

    pub fn recovery_allowed(self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub fn reset_required(self) -> bool {
        matches!(self, Self::ResetRequired)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryStreamRuntimeState {
    pub app_id: String,
    pub channel: String,
    pub stream_id: Option<String>,
    pub durable_state: HistoryDurableState,
    pub recovery_allowed: bool,
    pub reset_required: bool,
    pub reason: Option<String>,
    pub node_id: Option<String>,
    pub last_transition_at_ms: Option<i64>,
    pub authoritative_source: String,
    pub observed_source: String,
}

impl HistoryStreamRuntimeState {
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
            durable_state: HistoryDurableState::Healthy,
            recovery_allowed: true,
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
pub struct HistoryStreamInspection {
    pub app_id: String,
    pub channel: String,
    pub stream_id: Option<String>,
    pub next_serial: Option<u64>,
    pub retained: HistoryRetentionStats,
    pub state: HistoryStreamRuntimeState,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPurgeMode {
    All,
    BeforeSerial,
    BeforeTimeMs,
}

impl HistoryPurgeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::BeforeSerial => "before_serial",
            Self::BeforeTimeMs => "before_time_ms",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryPurgeRequest {
    pub mode: HistoryPurgeMode,
    pub before_serial: Option<u64>,
    pub before_time_ms: Option<i64>,
    pub reason: String,
    pub requested_by: Option<String>,
}

impl HistoryPurgeRequest {
    pub fn validate(&self) -> Result<()> {
        if self.reason.trim().is_empty() {
            return Err(Error::InvalidMessageFormat(
                "Purge reason must not be empty".to_string(),
            ));
        }

        match self.mode {
            HistoryPurgeMode::All => {
                if self.before_serial.is_some() || self.before_time_ms.is_some() {
                    return Err(Error::InvalidMessageFormat(
                        "Purge mode 'all' does not accept bounds".to_string(),
                    ));
                }
            }
            HistoryPurgeMode::BeforeSerial => {
                if self.before_serial.is_none() || self.before_time_ms.is_some() {
                    return Err(Error::InvalidMessageFormat(
                        "Purge mode 'before_serial' requires before_serial only".to_string(),
                    ));
                }
            }
            HistoryPurgeMode::BeforeTimeMs => {
                if self.before_time_ms.is_none() || self.before_serial.is_some() {
                    return Err(Error::InvalidMessageFormat(
                        "Purge mode 'before_time_ms' requires before_time_ms only".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryResetResult {
    pub app_id: String,
    pub channel: String,
    pub previous_stream_id: Option<String>,
    pub new_stream_id: String,
    pub purged_messages: u64,
    pub purged_bytes: u64,
    pub inspection: HistoryStreamInspection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryPurgeResult {
    pub app_id: String,
    pub channel: String,
    pub mode: HistoryPurgeMode,
    pub before_serial: Option<u64>,
    pub before_time_ms: Option<i64>,
    pub purged_messages: u64,
    pub purged_bytes: u64,
    pub inspection: HistoryStreamInspection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryRuntimeStatus {
    pub enabled: bool,
    pub backend: String,
    pub state_authority: String,
    pub degraded_channels: usize,
    pub reset_required_channels: usize,
    pub queue_depth: usize,
}

impl Default for HistoryRuntimeStatus {
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

impl HistoryReadRequest {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "History limit must be greater than 0".to_string(),
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
            if cursor.version != 1 {
                return Err(Error::InvalidMessageFormat(format!(
                    "Unsupported history cursor version: {}",
                    cursor.version
                )));
            }
            if cursor.app_id != self.app_id {
                return Err(Error::InvalidMessageFormat(
                    "History cursor app does not match the request".to_string(),
                ));
            }
            if cursor.channel != self.channel {
                return Err(Error::InvalidMessageFormat(
                    "History cursor channel does not match the request".to_string(),
                ));
            }
            if cursor.direction != self.direction {
                return Err(Error::InvalidMessageFormat(
                    "History cursor direction does not match the request".to_string(),
                ));
            }
            if cursor.bounds != self.bounds {
                return Err(Error::InvalidMessageFormat(
                    "History cursor bounds do not match the request".to_string(),
                ));
            }
        }

        Ok(())
    }
}
