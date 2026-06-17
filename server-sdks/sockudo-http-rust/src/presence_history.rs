use serde::Deserialize;
use sonic_rs::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct PresenceHistoryParams {
    pub limit: Option<u32>,
    pub direction: Option<String>,
    pub cursor: Option<String>,
    pub start_serial: Option<u64>,
    pub end_serial: Option<u64>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
    /// Ably-compatible alias for `start_time_ms`
    pub start: Option<i64>,
    /// Ably-compatible alias for `end_time_ms`
    pub end: Option<i64>,
}

impl PresenceHistoryParams {
    pub fn to_map(&self) -> BTreeMap<String, String> {
        let mut params = BTreeMap::new();
        if let Some(limit) = self.limit {
            params.insert("limit".to_string(), limit.to_string());
        }
        if let Some(direction) = self.direction.as_ref() {
            params.insert("direction".to_string(), direction.clone());
        }
        if let Some(cursor) = self.cursor.as_ref() {
            params.insert("cursor".to_string(), cursor.clone());
        }
        if let Some(start_serial) = self.start_serial {
            params.insert("start_serial".to_string(), start_serial.to_string());
        }
        if let Some(end_serial) = self.end_serial {
            params.insert("end_serial".to_string(), end_serial.to_string());
        }
        let start_time = self.start_time_ms.or(self.start);
        if let Some(start_time_ms) = start_time {
            params.insert("start_time_ms".to_string(), start_time_ms.to_string());
        }
        let end_time = self.end_time_ms.or(self.end);
        if let Some(end_time_ms) = end_time {
            params.insert("end_time_ms".to_string(), end_time_ms.to_string());
        }
        params
    }
}

#[derive(Debug, Clone, Default)]
pub struct PresenceSnapshotParams {
    pub at_time_ms: Option<i64>,
    /// Ably-compatible alias for `at_time_ms`
    pub at: Option<i64>,
    pub at_serial: Option<u64>,
}

impl PresenceSnapshotParams {
    pub fn to_map(&self) -> BTreeMap<String, String> {
        let mut params = BTreeMap::new();
        let at_time = self.at_time_ms.or(self.at);
        if let Some(at_time_ms) = at_time {
            params.insert("at_time_ms".to_string(), at_time_ms.to_string());
        }
        if let Some(at_serial) = self.at_serial {
            params.insert("at_serial".to_string(), at_serial.to_string());
        }
        params
    }
}

pub type PresenceHistoryEventKind = String;
pub type PresenceHistoryEventCause = String;

#[derive(Debug, Clone, Deserialize)]
pub struct PresenceHistoryPage {
    pub items: Vec<PresenceHistoryItem>,
    pub direction: String,
    pub limit: usize,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub bounds: PresenceHistoryBounds,
    pub continuity: PresenceHistoryContinuity,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PresenceHistoryItem {
    pub stream_id: Option<String>,
    pub serial: Option<u64>,
    pub published_at_ms: Option<i64>,
    pub event: Option<PresenceHistoryEventKind>,
    pub cause: Option<PresenceHistoryEventCause>,
    pub user_id: Option<String>,
    pub connection_id: Option<String>,
    pub dead_node_id: Option<String>,
    pub payload_size_bytes: Option<usize>,
    pub presence_event: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PresenceHistoryBounds {
    pub start_serial: Option<u64>,
    pub end_serial: Option<u64>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PresenceHistoryContinuity {
    pub stream_id: Option<String>,
    pub oldest_available_serial: Option<u64>,
    pub newest_available_serial: Option<u64>,
    pub oldest_available_published_at_ms: Option<i64>,
    pub newest_available_published_at_ms: Option<i64>,
    pub retained_events: u64,
    pub retained_bytes: u64,
    pub degraded: bool,
    pub complete: bool,
    pub truncated_by_retention: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PresenceSnapshotMember {
    pub user_id: String,
    pub last_event: PresenceHistoryEventKind,
    pub last_event_serial: u64,
    pub last_event_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PresenceSnapshot {
    pub channel: String,
    pub members: Vec<PresenceSnapshotMember>,
    pub member_count: usize,
    pub events_replayed: u64,
    pub snapshot_serial: Option<u64>,
    pub snapshot_time_ms: Option<i64>,
    pub continuity: PresenceHistoryContinuity,
}
