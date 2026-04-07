use crate::error::{Error, Result};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryRuntimeStatus {
    pub enabled: bool,
    pub backend: String,
    pub degraded_channels: usize,
    pub queue_depth: usize,
}

impl Default for HistoryRuntimeStatus {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: "disabled".to_string(),
            degraded_channels: 0,
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

#[async_trait]
pub trait HistoryStore: Send + Sync {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation>;

    async fn append(&self, record: HistoryAppendRecord) -> Result<()>;

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage>;

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        Ok(HistoryRuntimeStatus::default())
    }
}

#[derive(Default)]
pub struct NoopHistoryStore;

#[async_trait]
impl HistoryStore for NoopHistoryStore {
    async fn reserve_publish_position(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> Result<HistoryWriteReservation> {
        Err(Error::Configuration(
            "Durable history is not configured".to_string(),
        ))
    }

    async fn append(&self, _record: HistoryAppendRecord) -> Result<()> {
        Err(Error::Configuration(
            "Durable history is not configured".to_string(),
        ))
    }

    async fn read_page(&self, _request: HistoryReadRequest) -> Result<HistoryPage> {
        Err(Error::Configuration(
            "Durable history is not configured".to_string(),
        ))
    }

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        Ok(HistoryRuntimeStatus::default())
    }
}

#[derive(Debug, Clone)]
pub struct MemoryHistoryStoreConfig {
    pub retention_window: Duration,
    pub max_messages_per_channel: Option<usize>,
    pub max_bytes_per_channel: Option<u64>,
}

impl Default for MemoryHistoryStoreConfig {
    fn default() -> Self {
        Self {
            retention_window: Duration::from_secs(3600),
            max_messages_per_channel: None,
            max_bytes_per_channel: None,
        }
    }
}

#[derive(Debug, Clone)]
struct MemoryHistoryChannel {
    stream_id: String,
    next_serial: u64,
    retained_bytes: u64,
    records: VecDeque<HistoryAppendRecord>,
}

impl Default for MemoryHistoryChannel {
    fn default() -> Self {
        Self {
            stream_id: uuid::Uuid::new_v4().to_string(),
            next_serial: 1,
            retained_bytes: 0,
            records: VecDeque::new(),
        }
    }
}

#[derive(Clone, Default)]
pub struct MemoryHistoryStore {
    channels: Arc<RwLock<BTreeMap<String, MemoryHistoryChannel>>>,
    config: MemoryHistoryStoreConfig,
}

impl MemoryHistoryStore {
    pub fn new(config: MemoryHistoryStoreConfig) -> Self {
        Self {
            channels: Arc::new(RwLock::new(BTreeMap::new())),
            config,
        }
    }

    fn channel_key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
    }

    fn evict_channel(config: &MemoryHistoryStoreConfig, channel: &mut MemoryHistoryChannel) {
        let cutoff_ms = now_ms().saturating_sub(config.retention_window.as_millis() as i64);

        while let Some(front) = channel.records.front() {
            if front.published_at_ms < cutoff_ms {
                channel.retained_bytes = channel
                    .retained_bytes
                    .saturating_sub(front.payload_bytes.len() as u64);
                channel.records.pop_front();
            } else {
                break;
            }
        }

        if let Some(max_messages) = config.max_messages_per_channel {
            while channel.records.len() > max_messages {
                if let Some(front) = channel.records.pop_front() {
                    channel.retained_bytes = channel
                        .retained_bytes
                        .saturating_sub(front.payload_bytes.len() as u64);
                }
            }
        }

        if let Some(max_bytes) = config.max_bytes_per_channel {
            while channel.retained_bytes > max_bytes {
                if let Some(front) = channel.records.pop_front() {
                    channel.retained_bytes = channel
                        .retained_bytes
                        .saturating_sub(front.payload_bytes.len() as u64);
                } else {
                    break;
                }
            }
        }
    }
}

#[async_trait]
impl HistoryStore for MemoryHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        let key = Self::channel_key(app_id, channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();
        let reservation = HistoryWriteReservation {
            stream_id: channel_state.stream_id.clone(),
            serial: channel_state.next_serial,
        };
        channel_state.next_serial = channel_state.next_serial.saturating_add(1);
        Ok(reservation)
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        let key = Self::channel_key(&record.app_id, &record.channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();
        if channel_state.stream_id != record.stream_id {
            channel_state.stream_id = record.stream_id.clone();
        }
        channel_state.next_serial = channel_state
            .next_serial
            .max(record.serial.saturating_add(1));
        channel_state.retained_bytes = channel_state
            .retained_bytes
            .saturating_add(record.payload_bytes.len() as u64);
        channel_state.records.push_back(record);
        Self::evict_channel(&self.config, channel_state);
        Ok(())
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        request.validate()?;
        let key = Self::channel_key(&request.app_id, &request.channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();
        Self::evict_channel(&self.config, channel_state);

        let retained = HistoryRetentionStats {
            stream_id: Some(channel_state.stream_id.clone()),
            retained_messages: channel_state.records.len() as u64,
            retained_bytes: channel_state.retained_bytes,
            oldest_serial: channel_state.records.front().map(|record| record.serial),
            newest_serial: channel_state.records.back().map(|record| record.serial),
            oldest_published_at_ms: channel_state
                .records
                .front()
                .map(|record| record.published_at_ms),
            newest_published_at_ms: channel_state
                .records
                .back()
                .map(|record| record.published_at_ms),
        };

        if let Some(cursor) = request.cursor.as_ref() {
            if cursor.stream_id != channel_state.stream_id {
                return Err(Error::InvalidMessageFormat(
                    "Expired history cursor: channel stream changed".to_string(),
                ));
            }
            if let Some(oldest_serial) = retained.oldest_serial
                && cursor.serial < oldest_serial
            {
                return Err(Error::InvalidMessageFormat(
                    "Expired history cursor: cursor points before retained history".to_string(),
                ));
            }
        }

        let matcher = |record: &&HistoryAppendRecord| {
            request
                .bounds
                .start_serial
                .is_none_or(|start| record.serial >= start)
                && request
                    .bounds
                    .end_serial
                    .is_none_or(|end| record.serial <= end)
                && request
                    .bounds
                    .start_time_ms
                    .is_none_or(|start| record.published_at_ms >= start)
                && request
                    .bounds
                    .end_time_ms
                    .is_none_or(|end| record.published_at_ms <= end)
                && request.cursor.as_ref().is_none_or(|cursor| match request.direction {
                    HistoryDirection::NewestFirst => {
                        record.stream_id == cursor.stream_id && record.serial < cursor.serial
                    }
                    HistoryDirection::OldestFirst => {
                        record.stream_id == cursor.stream_id && record.serial > cursor.serial
                    }
                })
        };

        let collected: Vec<&HistoryAppendRecord> = match request.direction {
            HistoryDirection::NewestFirst => channel_state
                .records
                .iter()
                .rev()
                .filter(matcher)
                .take(request.limit + 1)
                .collect(),
            HistoryDirection::OldestFirst => channel_state
                .records
                .iter()
                .filter(matcher)
                .take(request.limit + 1)
                .collect(),
        };

        let has_more = collected.len() > request.limit;
        let items: Vec<HistoryItem> = collected
            .into_iter()
            .take(request.limit)
            .map(|record| HistoryItem {
                stream_id: record.stream_id.clone(),
                serial: record.serial,
                published_at_ms: record.published_at_ms,
                message_id: record.message_id.clone(),
                event_name: record.event_name.clone(),
                operation_kind: record.operation_kind.clone(),
                payload_size_bytes: record.payload_bytes.len(),
                payload_bytes: record.payload_bytes.clone(),
            })
            .collect();

        let next_cursor = if has_more {
            items.last().map(|item| HistoryCursor {
                version: 1,
                app_id: request.app_id.clone(),
                channel: request.channel.clone(),
                stream_id: item.stream_id.clone(),
                serial: item.serial,
                direction: request.direction,
                bounds: request.bounds.clone(),
            })
        } else {
            None
        };

        let truncated_by_retention = is_truncated_by_retention(&request.bounds, &retained);

        Ok(HistoryPage {
            items,
            next_cursor,
            retained,
            has_more,
            complete: !has_more && !truncated_by_retention,
            truncated_by_retention,
        })
    }

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "memory".to_string(),
            degraded_channels: 0,
            queue_depth: 0,
        })
    }
}

fn is_truncated_by_retention(bounds: &HistoryQueryBounds, retained: &HistoryRetentionStats) -> bool {
    if let (Some(start_serial), Some(oldest_serial)) = (bounds.start_serial, retained.oldest_serial)
        && start_serial < oldest_serial
    {
        return true;
    }
    if let (Some(start_time_ms), Some(oldest_time_ms)) =
        (bounds.start_time_ms, retained.oldest_published_at_ms)
        && start_time_ms < oldest_time_ms
    {
        return true;
    }
    bounds.start_serial.is_none()
        && bounds.start_time_ms.is_none()
        && retained.oldest_serial.is_some_and(|oldest_serial| oldest_serial > 1)
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(
        app_id: &str,
        channel: &str,
        stream_id: &str,
        serial: u64,
        published_at_ms: i64,
        payload: &str,
    ) -> HistoryAppendRecord {
        HistoryAppendRecord {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: stream_id.to_string(),
            serial,
            published_at_ms,
            message_id: Some(format!("msg-{serial}")),
            event_name: Some("event".to_string()),
            operation_kind: "append".to_string(),
            payload_bytes: Bytes::from(payload.to_string()),
        }
    }

    #[test]
    fn history_cursor_round_trip() {
        let cursor = HistoryCursor {
            version: 1,
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            stream_id: "stream-1".to_string(),
            serial: 42,
            direction: HistoryDirection::NewestFirst,
            bounds: HistoryQueryBounds::default(),
        };
        let encoded = cursor.encode().unwrap();
        let decoded = HistoryCursor::decode(&encoded).unwrap();
        assert_eq!(decoded, cursor);
    }

    #[tokio::test]
    async fn memory_history_store_orders_newest_first_with_cursor() {
        let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig::default());
        let reservation = store.reserve_publish_position("app", "chat").await.unwrap();
        assert_eq!(reservation.serial, 1);
        let stream_id = reservation.stream_id;
        let base_ts = now_ms();

        for serial in 1..=3 {
            store
                .append(make_record(
                    "app",
                    "chat",
                    &stream_id,
                    serial,
                    base_ts + serial as i64,
                    &format!("payload-{serial}"),
                ))
                .await
                .unwrap();
        }

        let first_page = store
            .read_page(HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: HistoryDirection::NewestFirst,
                limit: 2,
                cursor: None,
                bounds: HistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(first_page.items.iter().map(|item| item.serial).collect::<Vec<_>>(), vec![3, 2]);

        let second_page = store
            .read_page(HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: HistoryDirection::NewestFirst,
                limit: 2,
                cursor: first_page.next_cursor.clone(),
                bounds: HistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(
            second_page
                .items
                .iter()
                .map(|item| item.serial)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[tokio::test]
    async fn memory_history_store_orders_oldest_first_with_cursor() {
        let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig::default());
        let stream_id = store
            .reserve_publish_position("app", "chat")
            .await
            .unwrap()
            .stream_id;
        let base_ts = now_ms();

        for serial in 1..=3 {
            store
                .append(make_record(
                    "app",
                    "chat",
                    &stream_id,
                    serial,
                    base_ts + serial as i64,
                    &format!("payload-{serial}"),
                ))
                .await
                .unwrap();
        }

        let first_page = store
            .read_page(HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: 2,
                cursor: None,
                bounds: HistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(first_page.items.iter().map(|item| item.serial).collect::<Vec<_>>(), vec![1, 2]);

        let second_page = store
            .read_page(HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: 2,
                cursor: first_page.next_cursor.clone(),
                bounds: HistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(
            second_page
                .items
                .iter()
                .map(|item| item.serial)
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[tokio::test]
    async fn memory_history_store_evicts_by_retention_and_count() {
        let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig {
            retention_window: Duration::from_secs(1),
            max_messages_per_channel: Some(2),
            max_bytes_per_channel: None,
        });
        let stream_id = store
            .reserve_publish_position("app", "chat")
            .await
            .unwrap()
            .stream_id;

        let old_ts = now_ms() - 5_000;
        store
            .append(make_record("app", "chat", &stream_id, 1, old_ts, "old"))
            .await
            .unwrap();
        store
            .append(make_record("app", "chat", &stream_id, 2, now_ms(), "newer"))
            .await
            .unwrap();
        store
            .append(make_record("app", "chat", &stream_id, 3, now_ms(), "newest"))
            .await
            .unwrap();

        let page = store
            .read_page(HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: HistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.iter().map(|item| item.serial).collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!(page.retained.retained_messages, 2);
    }

    #[tokio::test]
    async fn memory_history_store_filters_by_serial_and_time() {
        let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig::default());
        let stream_id = store
            .reserve_publish_position("app", "chat")
            .await
            .unwrap()
            .stream_id;
        let base_ts = now_ms();

        for serial in 1..=5 {
            store
                .append(make_record(
                    "app",
                    "chat",
                    &stream_id,
                    serial,
                    base_ts + (serial as i64 * 10),
                    &format!("payload-{serial}"),
                ))
                .await
                .unwrap();
        }

        let page = store
            .read_page(HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: HistoryQueryBounds {
                    start_serial: Some(2),
                    end_serial: Some(4),
                    start_time_ms: Some(base_ts + 20),
                    end_time_ms: Some(base_ts + 40),
                },
            })
            .await
            .unwrap();

        assert_eq!(
            page.items.iter().map(|item| item.serial).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
    }
}
