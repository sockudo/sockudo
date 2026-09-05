use super::store::HistoryStore;
use super::time::now_ms;
use super::types::*;
use crate::error::{Error, Result};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

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
pub(super) struct MemoryHistoryChannel {
    stream_id: String,
    next_serial: u64,
    retained_bytes: u64,
    records: VecDeque<HistoryAppendRecord>,
    // Imports may be out of order. Seek only while this invariant is proven.
    serials_ordered: bool,
    scheduled_expiry: Option<i64>,
    retention: Option<HistoryRetentionPolicy>,
}

impl Default for MemoryHistoryChannel {
    fn default() -> Self {
        Self {
            stream_id: uuid::Uuid::new_v4().to_string(),
            next_serial: 1,
            retained_bytes: 0,
            records: VecDeque::new(),
            serials_ordered: true,
            scheduled_expiry: None,
            retention: None,
        }
    }
}

#[derive(Clone, Default)]
pub struct MemoryHistoryStore {
    pub(super) channels: Arc<RwLock<BTreeMap<String, Arc<RwLock<MemoryHistoryChannel>>>>>,
    config: MemoryHistoryStoreConfig,
    expiries: Arc<parking_lot::Mutex<BTreeSet<(i64, String)>>>,
}

impl MemoryHistoryStore {
    pub fn new(config: MemoryHistoryStoreConfig) -> Self {
        Self {
            channels: Arc::new(RwLock::new(BTreeMap::new())),
            config,
            expiries: Arc::default(),
        }
    }

    fn channel_key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
    }

    async fn channel(&self, key: &str) -> Arc<RwLock<MemoryHistoryChannel>> {
        if let Some(channel) = self.channels.read().await.get(key).cloned() {
            return channel;
        }
        self.channels
            .write()
            .await
            .entry(key.to_owned())
            .or_default()
            .clone()
    }

    fn default_retention(config: &MemoryHistoryStoreConfig) -> HistoryRetentionPolicy {
        HistoryRetentionPolicy {
            retention_window_seconds: config.retention_window.as_secs(),
            max_messages_per_channel: config.max_messages_per_channel,
            max_bytes_per_channel: config.max_bytes_per_channel,
        }
    }

    fn evict_channel(retention: &HistoryRetentionPolicy, channel: &mut MemoryHistoryChannel) {
        let cutoff_ms = now_ms().saturating_sub((retention.retention_window_seconds * 1000) as i64);

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

        if let Some(max_messages) = retention.max_messages_per_channel {
            while channel.records.len() > max_messages {
                if let Some(front) = channel.records.pop_front() {
                    channel.retained_bytes = channel
                        .retained_bytes
                        .saturating_sub(front.payload_bytes.len() as u64);
                }
            }
        }

        if let Some(max_bytes) = retention.max_bytes_per_channel {
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

    fn schedule_expiry(&self, key: &str, channel: &mut MemoryHistoryChannel) {
        let retention = channel
            .retention
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Self::default_retention(&self.config));
        let deadline = channel.records.front().map(|record| {
            record
                .published_at_ms
                .saturating_add(
                    retention
                        .retention_window_seconds
                        .saturating_mul(1000)
                        .min(i64::MAX as u64) as i64,
                )
                .saturating_add(1)
        });
        if deadline == channel.scheduled_expiry {
            return;
        }
        let mut expiries = self.expiries.lock();
        if let Some(previous) = channel.scheduled_expiry {
            expiries.remove(&(previous, key.to_owned()));
        }
        if let Some(deadline) = deadline {
            expiries.insert((deadline, key.to_owned()));
        }
        channel.scheduled_expiry = deadline;
    }

    fn retained_from_channel(channel_state: &MemoryHistoryChannel) -> HistoryRetentionStats {
        HistoryRetentionStats {
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
        let channel_handle = self.channel(&key).await;
        let mut guard = channel_handle.write().await;
        let channel_state = &mut *guard;
        let reservation = HistoryWriteReservation {
            stream_id: channel_state.stream_id.clone(),
            serial: channel_state.next_serial,
        };
        channel_state.next_serial = channel_state.next_serial.saturating_add(1);
        Ok(reservation)
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        let key = Self::channel_key(&record.app_id, &record.channel);
        let channel_handle = self.channel(&key).await;
        let mut guard = channel_handle.write().await;
        let channel_state = &mut *guard;
        if channel_state.stream_id != record.stream_id {
            channel_state.stream_id.clone_from(&record.stream_id);
        }
        channel_state.next_serial = channel_state
            .next_serial
            .max(record.serial.saturating_add(1));
        channel_state.retention = Some(record.retention.clone());
        channel_state.retained_bytes = channel_state
            .retained_bytes
            .saturating_add(record.payload_bytes.len() as u64);
        channel_state.serials_ordered &= channel_state
            .records
            .back()
            .is_none_or(|last| last.serial < record.serial && last.stream_id == record.stream_id);
        channel_state.records.push_back(record);
        let retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| Self::default_retention(&self.config));
        Self::evict_channel(&retention, channel_state);
        self.schedule_expiry(&key, channel_state);
        Ok(())
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        request.validate()?;
        let key = Self::channel_key(&request.app_id, &request.channel);
        let channel_handle = self.channels.read().await.get(&key).cloned();
        let Some(channel_handle) = channel_handle else {
            return Ok(HistoryPage {
                items: Vec::new(),
                next_cursor: None,
                retained: HistoryRetentionStats {
                    stream_id: None,
                    retained_messages: 0,
                    retained_bytes: 0,
                    oldest_serial: None,
                    newest_serial: None,
                    oldest_published_at_ms: None,
                    newest_published_at_ms: None,
                },
                has_more: false,
                complete: true,
                truncated_by_retention: false,
            });
        };
        let mut guard = channel_handle.write().await;
        let channel_state = &mut *guard;
        let retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| Self::default_retention(&self.config));
        Self::evict_channel(&retention, channel_state);
        self.schedule_expiry(&key, channel_state);

        let retained = Self::retained_from_channel(channel_state);

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
                && request
                    .cursor
                    .as_ref()
                    .is_none_or(|cursor| match request.direction {
                        HistoryDirection::NewestFirst => {
                            record.stream_id == cursor.stream_id && record.serial < cursor.serial
                        }
                        HistoryDirection::OldestFirst => {
                            record.stream_id == cursor.stream_id && record.serial > cursor.serial
                        }
                    })
        };

        // Serial bounds are monotonic even when timestamps skew. On arbitrary
        // imports preserve the original scan and insertion-order behavior.
        let (start, end) = if channel_state.serials_ordered {
            let start = channel_state.records.partition_point(|record| {
                request
                    .bounds
                    .start_serial
                    .is_some_and(|start| record.serial < start)
                    || (request.direction == HistoryDirection::OldestFirst
                        && request
                            .cursor
                            .as_ref()
                            .is_some_and(|cursor| record.serial <= cursor.serial))
            });
            let end = channel_state.records.partition_point(|record| {
                request
                    .bounds
                    .end_serial
                    .is_none_or(|end| record.serial <= end)
                    && (request.direction != HistoryDirection::NewestFirst
                        || request
                            .cursor
                            .as_ref()
                            .is_none_or(|cursor| record.serial < cursor.serial))
            });
            (start.min(end), end)
        } else {
            (0, channel_state.records.len())
        };
        let collected: Vec<&HistoryAppendRecord> = match request.direction {
            HistoryDirection::NewestFirst => channel_state
                .records
                .range(start..end)
                .rev()
                .filter(matcher)
                .take(request.limit + 1)
                .collect(),
            HistoryDirection::OldestFirst => channel_state
                .records
                .range(start..end)
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

    async fn channel_head(&self, app_id: &str, channel: &str) -> Result<HistoryRetentionStats> {
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channels.read().await.get(&key).cloned();
        let Some(channel_handle) = channel_handle else {
            return Ok(HistoryRetentionStats::default());
        };
        let mut guard = channel_handle.write().await;
        let channel_state = &mut *guard;
        let retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| Self::default_retention(&self.config));
        Self::evict_channel(&retention, channel_state);
        self.schedule_expiry(&key, channel_state);
        Ok(Self::retained_from_channel(channel_state))
    }

    async fn purge_before(&self, _before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        // Memory deadlines honor each channel's retention override. Operator
        // time-based deletion is provided by purge_stream, independently.
        let now = now_ms();
        let mut deleted = 0;
        for _ in 0..batch_size {
            if deleted >= batch_size {
                break;
            }
            let due = {
                let mut expiries = self.expiries.lock();
                if expiries
                    .first()
                    .is_some_and(|(deadline, _)| *deadline <= now)
                {
                    expiries.pop_first()
                } else {
                    None
                }
            };
            let Some((deadline, key)) = due else {
                break;
            };
            let Some(channel_handle) = self.channels.read().await.get(&key).cloned() else {
                continue;
            };
            let mut channel = channel_handle.write().await;
            if channel.scheduled_expiry != Some(deadline) {
                continue;
            }
            channel.scheduled_expiry = None;
            let retention = channel
                .retention
                .as_ref()
                .cloned()
                .unwrap_or_else(|| Self::default_retention(&self.config));
            let cutoff = now.saturating_sub(
                retention
                    .retention_window_seconds
                    .saturating_mul(1000)
                    .min(i64::MAX as u64) as i64,
            );
            while deleted < batch_size
                && channel
                    .records
                    .front()
                    .is_some_and(|record| record.published_at_ms < cutoff)
            {
                if let Some(record) = channel.records.pop_front() {
                    channel.retained_bytes = channel
                        .retained_bytes
                        .saturating_sub(record.payload_bytes.len() as u64);
                    deleted += 1;
                }
            }
            self.schedule_expiry(&key, &mut channel);
        }
        let has_more = self
            .expiries
            .lock()
            .first()
            .is_some_and(|(deadline, _)| *deadline <= now);
        Ok((deleted as u64, has_more))
    }

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "memory".to_string(),
            state_authority: "in_memory".to_string(),
            degraded_channels: 0,
            reset_required_channels: 0,
            queue_depth: 0,
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channel(&key).await;
        let mut guard = channel_handle.write().await;
        let channel_state = &mut *guard;
        let retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| Self::default_retention(&self.config));
        Self::evict_channel(&retention, channel_state);
        self.schedule_expiry(&key, channel_state);

        Ok(HistoryStreamRuntimeState::healthy(
            app_id,
            channel,
            Some(channel_state.stream_id.clone()),
            "in_memory",
        ))
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channel(&key).await;
        let mut guard = channel_handle.write().await;
        let channel_state = &mut *guard;
        let retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| Self::default_retention(&self.config));
        Self::evict_channel(&retention, channel_state);
        self.schedule_expiry(&key, channel_state);

        Ok(HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(channel_state.stream_id.clone()),
            next_serial: Some(channel_state.next_serial),
            retained: Self::retained_from_channel(channel_state),
            state: HistoryStreamRuntimeState::healthy(
                app_id,
                channel,
                Some(channel_state.stream_id.clone()),
                "in_memory",
            ),
        })
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        _reason: &str,
        _requested_by: Option<&str>,
    ) -> Result<HistoryResetResult> {
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channel(&key).await;
        let mut guard = channel_handle.write().await;
        let channel_state = &mut *guard;
        let previous_stream_id = Some(channel_state.stream_id.clone());
        let purged_messages = channel_state.records.len() as u64;
        let purged_bytes = channel_state.retained_bytes;
        channel_state.records.clear();
        channel_state.serials_ordered = true;
        channel_state.retained_bytes = 0;
        channel_state.next_serial = 1;
        channel_state.stream_id = uuid::Uuid::new_v4().to_string();
        self.schedule_expiry(&key, channel_state);
        let inspection = HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(channel_state.stream_id.clone()),
            next_serial: Some(channel_state.next_serial),
            retained: Self::retained_from_channel(channel_state),
            state: HistoryStreamRuntimeState::healthy(
                app_id,
                channel,
                Some(channel_state.stream_id.clone()),
                "in_memory",
            ),
        };

        Ok(HistoryResetResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            previous_stream_id,
            new_stream_id: inspection.stream_id.clone().unwrap_or_default(),
            purged_messages,
            purged_bytes,
            inspection,
        })
    }

    async fn purge_stream(
        &self,
        app_id: &str,
        channel: &str,
        request: HistoryPurgeRequest,
    ) -> Result<HistoryPurgeResult> {
        request.validate()?;
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channel(&key).await;
        let mut guard = channel_handle.write().await;
        let channel_state = &mut *guard;

        let previous_records = channel_state.records.len();
        let previous_bytes = channel_state.retained_bytes;
        let retained: VecDeque<_> = match request.mode {
            HistoryPurgeMode::All => VecDeque::new(),
            HistoryPurgeMode::BeforeSerial => channel_state
                .records
                .iter()
                .filter(|record| record.serial >= request.before_serial.unwrap_or_default())
                .cloned()
                .collect(),
            HistoryPurgeMode::BeforeTimeMs => channel_state
                .records
                .iter()
                .filter(|record| {
                    record.published_at_ms >= request.before_time_ms.unwrap_or_default()
                })
                .cloned()
                .collect(),
        };
        channel_state.records = retained;
        channel_state.retained_bytes = channel_state
            .records
            .iter()
            .map(|record| record.payload_bytes.len() as u64)
            .sum();

        self.schedule_expiry(&key, channel_state);
        let inspection = HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(channel_state.stream_id.clone()),
            next_serial: Some(channel_state.next_serial),
            retained: Self::retained_from_channel(channel_state),
            state: HistoryStreamRuntimeState::healthy(
                app_id,
                channel,
                Some(channel_state.stream_id.clone()),
                "in_memory",
            ),
        };

        Ok(HistoryPurgeResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            mode: request.mode,
            before_serial: request.before_serial,
            before_time_ms: request.before_time_ms,
            purged_messages: previous_records.saturating_sub(channel_state.records.len()) as u64,
            purged_bytes: previous_bytes.saturating_sub(channel_state.retained_bytes),
            inspection,
        })
    }
}

fn is_truncated_by_retention(
    bounds: &HistoryQueryBounds,
    retained: &HistoryRetentionStats,
) -> bool {
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
        && retained
            .oldest_serial
            .is_some_and(|oldest_serial| oldest_serial > 1)
}
