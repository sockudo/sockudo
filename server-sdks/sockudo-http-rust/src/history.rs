use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct HistoryParams {
    pub limit: Option<u32>,
    pub direction: Option<String>,
    pub cursor: Option<String>,
    pub start_serial: Option<u64>,
    pub end_serial: Option<u64>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
}

impl HistoryParams {
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
        if let Some(start_time_ms) = self.start_time_ms {
            params.insert("start_time_ms".to_string(), start_time_ms.to_string());
        }
        if let Some(end_time_ms) = self.end_time_ms {
            params.insert("end_time_ms".to_string(), end_time_ms.to_string());
        }
        params
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryPage {
    pub items: Vec<HistoryItem>,
    pub direction: String,
    pub limit: usize,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub bounds: HistoryBounds,
    pub continuity: HistoryContinuity,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryItem {
    pub stream_id: Option<String>,
    pub serial: Option<u64>,
    pub published_at_ms: Option<i64>,
    pub message_id: Option<String>,
    pub event_name: Option<String>,
    pub operation_kind: Option<String>,
    pub payload_size_bytes: Option<usize>,
    pub message: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct HistoryBounds {
    pub start_serial: Option<u64>,
    pub end_serial: Option<u64>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct HistoryContinuity {
    pub stream_id: Option<String>,
    pub oldest_available_serial: Option<u64>,
    pub newest_available_serial: Option<u64>,
    pub oldest_available_published_at_ms: Option<i64>,
    pub newest_available_published_at_ms: Option<i64>,
    pub retained_messages: u64,
    pub retained_bytes: u64,
    pub complete: bool,
    pub truncated_by_retention: bool,
}

#[derive(Debug, Clone, Default)]
pub struct MessageVersionsParams {
    pub limit: Option<u32>,
    pub direction: Option<String>,
    pub cursor: Option<String>,
}

impl MessageVersionsParams {
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
        params
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MutationResponse {
    pub channel: String,
    pub message_serial: String,
    pub action: String,
    pub accepted: bool,
    pub version_serial: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetMessageResponse {
    pub channel: String,
    pub item: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListMessageVersionsResponse {
    pub channel: String,
    pub direction: String,
    pub limit: usize,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub items: Vec<Value>,
}

#[derive(Debug, Clone, Default)]
pub struct AnnotationEventsParams {
    pub annotation_type: Option<String>,
    pub from_serial: Option<String>,
    pub limit: Option<u32>,
    pub socket_id: Option<String>,
}

impl AnnotationEventsParams {
    pub fn to_map(&self) -> BTreeMap<String, String> {
        let mut params = BTreeMap::new();
        if let Some(annotation_type) = self.annotation_type.as_ref() {
            params.insert("type".to_string(), annotation_type.clone());
        }
        if let Some(from_serial) = self.from_serial.as_ref() {
            params.insert("from_serial".to_string(), from_serial.clone());
        }
        if let Some(limit) = self.limit {
            params.insert("limit".to_string(), limit.to_string());
        }
        if let Some(socket_id) = self.socket_id.as_ref() {
            params.insert("socket_id".to_string(), socket_id.clone());
        }
        params
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishAnnotationRequest {
    #[serde(rename = "type")]
    pub annotation_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishAnnotationResponse {
    pub annotation_serial: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteAnnotationResponse {
    pub annotation_serial: String,
    pub deleted_annotation_serial: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationEventsResponse {
    pub channel: String,
    pub message_serial: String,
    pub limit: usize,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub items: Vec<Value>,
}
