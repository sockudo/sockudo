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
    pub stream_id: String,
    pub serial: u64,
    pub direction: HistoryDirection,
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
        sonic_rs::from_slice(&bytes)
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid history cursor: {e}")))
    }
}

#[derive(Debug, Clone)]
pub struct HistoryWriteReservation {
    pub stream_id: String,
    pub serial: u64,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct HistoryRetentionStats {
    pub retained_messages: u64,
    pub retained_bytes: u64,
    pub oldest_serial: Option<u64>,
    pub newest_serial: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct HistoryPage {
    pub items: Vec<HistoryItem>,
    pub next_cursor: Option<HistoryCursor>,
    pub retained: HistoryRetentionStats,
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
        channel_state.retained_bytes = channel_state
            .retained_bytes
            .saturating_add(record.payload_bytes.len() as u64);
        channel_state.records.push_back(record);
        Self::evict_channel(&self.config, channel_state);
        Ok(())
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        let key = Self::channel_key(&request.app_id, &request.channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();
        Self::evict_channel(&self.config, channel_state);

        let filtered: Vec<&HistoryAppendRecord> = match request.direction {
            HistoryDirection::NewestFirst => channel_state
                .records
                .iter()
                .rev()
                .filter(|record| {
                    request.cursor.as_ref().is_none_or(|cursor| {
                        record.stream_id == cursor.stream_id && record.serial < cursor.serial
                    })
                })
                .take(request.limit)
                .collect(),
            HistoryDirection::OldestFirst => channel_state
                .records
                .iter()
                .filter(|record| {
                    request.cursor.as_ref().is_none_or(|cursor| {
                        record.stream_id == cursor.stream_id && record.serial > cursor.serial
                    })
                })
                .take(request.limit)
                .collect(),
        };

        let items: Vec<HistoryItem> = filtered
            .iter()
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

        let next_cursor = items.last().map(|item| HistoryCursor {
            stream_id: item.stream_id.clone(),
            serial: item.serial,
            direction: request.direction,
        });

        let retained = HistoryRetentionStats {
            retained_messages: channel_state.records.len() as u64,
            retained_bytes: channel_state.retained_bytes,
            oldest_serial: channel_state.records.front().map(|record| record.serial),
            newest_serial: channel_state.records.back().map(|record| record.serial),
        };

        Ok(HistoryPage {
            items,
            next_cursor,
            retained,
        })
    }
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
            stream_id: "stream-1".to_string(),
            serial: 42,
            direction: HistoryDirection::NewestFirst,
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
            })
            .await
            .unwrap();

        assert_eq!(page.items.iter().map(|item| item.serial).collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!(page.retained.retained_messages, 2);
    }
}
