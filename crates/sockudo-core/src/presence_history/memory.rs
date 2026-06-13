use super::store::PresenceHistoryStore;
use super::types::{
    PresenceHistoryCursor, PresenceHistoryDirection, PresenceHistoryEventCause,
    PresenceHistoryEventKind, PresenceHistoryItem, PresenceHistoryPage, PresenceHistoryPayload,
    PresenceHistoryQueryBounds, PresenceHistoryReadRequest, PresenceHistoryResetResult,
    PresenceHistoryRetentionPolicy, PresenceHistoryRetentionStats, PresenceHistoryRuntimeStatus,
    PresenceHistoryStreamInspection, PresenceHistoryStreamRuntimeState,
    PresenceHistoryTransitionRecord,
};
use crate::error::{Error, Result};
use crate::history::now_ms;
use crate::metrics::MetricsInterface;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct MemoryPresenceHistoryStoreConfig {
    pub retention_window: Duration,
    pub max_events_per_channel: Option<usize>,
    pub max_bytes_per_channel: Option<u64>,
    pub metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
}

impl Default for MemoryPresenceHistoryStoreConfig {
    fn default() -> Self {
        Self {
            retention_window: Duration::from_secs(3600),
            max_events_per_channel: None,
            max_bytes_per_channel: None,
            metrics: None,
        }
    }
}

#[derive(Debug, Clone)]
struct PresenceHistoryAppendRecord {
    stream_id: String,
    serial: u64,
    published_at_ms: i64,
    event: PresenceHistoryEventKind,
    cause: PresenceHistoryEventCause,
    user_id: String,
    connection_id: Option<String>,
    dead_node_id: Option<String>,
    payload_bytes: Bytes,
    dedupe_key: String,
}

#[derive(Debug, Clone)]
struct MemoryPresenceHistoryChannel {
    stream_id: String,
    next_serial: u64,
    retained_bytes: u64,
    records: VecDeque<PresenceHistoryAppendRecord>,
    dedupe_keys: HashSet<String>,
    latest_event_by_user: BTreeMap<String, PresenceHistoryEventKind>,
    retention: Option<PresenceHistoryRetentionPolicy>,
}

impl Default for MemoryPresenceHistoryChannel {
    fn default() -> Self {
        Self {
            stream_id: uuid::Uuid::new_v4().to_string(),
            next_serial: 1,
            retained_bytes: 0,
            records: VecDeque::new(),
            dedupe_keys: HashSet::new(),
            latest_event_by_user: BTreeMap::new(),
            retention: None,
        }
    }
}

#[derive(Clone, Default)]
pub struct MemoryPresenceHistoryStore {
    channels: Arc<RwLock<BTreeMap<String, MemoryPresenceHistoryChannel>>>,
    config: MemoryPresenceHistoryStoreConfig,
}

impl MemoryPresenceHistoryStore {
    pub fn new(config: MemoryPresenceHistoryStoreConfig) -> Self {
        Self {
            channels: Arc::new(RwLock::new(BTreeMap::new())),
            config,
        }
    }

    fn channel_key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
    }

    fn retention(&self) -> PresenceHistoryRetentionPolicy {
        PresenceHistoryRetentionPolicy {
            retention_window_seconds: self.config.retention_window.as_secs(),
            max_events_per_channel: self.config.max_events_per_channel,
            max_bytes_per_channel: self.config.max_bytes_per_channel,
        }
    }

    fn build_payload(
        stream_id: &str,
        serial: u64,
        record: &PresenceHistoryTransitionRecord,
    ) -> Result<Bytes> {
        let payload = PresenceHistoryPayload {
            stream_id: stream_id.to_string(),
            serial,
            published_at_ms: record.published_at_ms,
            event: record.event_kind,
            cause: record.cause,
            user_id: record.user_id.clone(),
            connection_id: record.connection_id.clone(),
            user_info: record.user_info.clone(),
            dead_node_id: record.dead_node_id.clone(),
        };
        sonic_rs::to_vec(&payload).map(Bytes::from).map_err(|e| {
            Error::Serialization(format!("Failed to encode presence history payload: {e}"))
        })
    }

    fn evict_channel(
        retention: &PresenceHistoryRetentionPolicy,
        channel: &mut MemoryPresenceHistoryChannel,
    ) -> (u64, u64) {
        let cutoff_ms = now_ms().saturating_sub((retention.retention_window_seconds * 1000) as i64);
        let mut evicted_events = 0_u64;
        let mut evicted_bytes = 0_u64;

        while let Some(front) = channel.records.front() {
            if front.published_at_ms < cutoff_ms {
                if let Some(removed) = channel.records.pop_front() {
                    let removed_bytes = removed.payload_bytes.len() as u64;
                    channel.retained_bytes = channel.retained_bytes.saturating_sub(removed_bytes);
                    channel.dedupe_keys.remove(&removed.dedupe_key);
                    evicted_events = evicted_events.saturating_add(1);
                    evicted_bytes = evicted_bytes.saturating_add(removed_bytes);
                }
            } else {
                break;
            }
        }

        if let Some(max_events) = retention.max_events_per_channel {
            while channel.records.len() > max_events {
                if let Some(front) = channel.records.pop_front() {
                    let removed_bytes = front.payload_bytes.len() as u64;
                    channel.retained_bytes = channel.retained_bytes.saturating_sub(removed_bytes);
                    channel.dedupe_keys.remove(&front.dedupe_key);
                    evicted_events = evicted_events.saturating_add(1);
                    evicted_bytes = evicted_bytes.saturating_add(removed_bytes);
                }
            }
        }

        if let Some(max_bytes) = retention.max_bytes_per_channel {
            while channel.retained_bytes > max_bytes {
                if let Some(front) = channel.records.pop_front() {
                    let removed_bytes = front.payload_bytes.len() as u64;
                    channel.retained_bytes = channel.retained_bytes.saturating_sub(removed_bytes);
                    channel.dedupe_keys.remove(&front.dedupe_key);
                    evicted_events = evicted_events.saturating_add(1);
                    evicted_bytes = evicted_bytes.saturating_add(removed_bytes);
                } else {
                    break;
                }
            }
        }

        if evicted_events > 0 {
            Self::rebuild_latest_event_by_user(channel);
        }

        (evicted_events, evicted_bytes)
    }

    fn rebuild_latest_event_by_user(channel: &mut MemoryPresenceHistoryChannel) {
        channel.latest_event_by_user.clear();
        for record in &channel.records {
            channel
                .latest_event_by_user
                .insert(record.user_id.clone(), record.event);
        }
    }

    fn retained_from_channel(
        channel: &MemoryPresenceHistoryChannel,
    ) -> PresenceHistoryRetentionStats {
        PresenceHistoryRetentionStats {
            stream_id: Some(channel.stream_id.clone()),
            retained_events: channel.records.len() as u64,
            retained_bytes: channel.retained_bytes,
            oldest_serial: channel.records.front().map(|record| record.serial),
            newest_serial: channel.records.back().map(|record| record.serial),
            oldest_published_at_ms: channel.records.front().map(|record| record.published_at_ms),
            newest_published_at_ms: channel.records.back().map(|record| record.published_at_ms),
        }
    }
}

#[async_trait]
impl PresenceHistoryStore for MemoryPresenceHistoryStore {
    async fn record_transition(&self, record: PresenceHistoryTransitionRecord) -> Result<()> {
        let started = Instant::now();
        let key = Self::channel_key(&record.app_id, &record.channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();
        let retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| self.retention());
        let mut evicted = Self::evict_channel(&retention, channel_state);

        if channel_state.dedupe_keys.contains(&record.dedupe_key) {
            if let Some(metrics) = self.config.metrics.as_ref() {
                metrics.track_presence_history_write_latency(
                    &record.app_id,
                    started.elapsed().as_secs_f64() * 1000.0,
                );
            }
            return Ok(());
        }

        // Presence transitions are authoritative at the user+channel edge, not at the socket or
        // reporting-node edge. Once the retained state already says "joined" or "removed" for a
        // user, another report of the same edge is a duplicate distributed notification.
        if channel_state.latest_event_by_user.get(&record.user_id) == Some(&record.event_kind) {
            if let Some(metrics) = self.config.metrics.as_ref() {
                metrics.track_presence_history_write_latency(
                    &record.app_id,
                    started.elapsed().as_secs_f64() * 1000.0,
                );
            }
            return Ok(());
        }

        let serial = channel_state.next_serial;
        channel_state.next_serial = channel_state.next_serial.saturating_add(1);
        let stream_id = channel_state.stream_id.clone();
        let payload_bytes = Self::build_payload(&stream_id, serial, &record)?;
        let user_id = record.user_id.clone();
        let event_kind = record.event_kind;
        channel_state.retention = Some(record.retention.clone());
        channel_state.retained_bytes = channel_state
            .retained_bytes
            .saturating_add(payload_bytes.len() as u64);
        channel_state.dedupe_keys.insert(record.dedupe_key.clone());
        channel_state
            .latest_event_by_user
            .insert(user_id.clone(), event_kind);
        channel_state
            .records
            .push_back(PresenceHistoryAppendRecord {
                stream_id,
                serial,
                published_at_ms: record.published_at_ms,
                event: event_kind,
                cause: record.cause,
                user_id,
                connection_id: record.connection_id,
                dead_node_id: record.dead_node_id,
                payload_bytes,
                dedupe_key: record.dedupe_key,
            });
        let applied_retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| self.retention());
        let after_eviction = Self::evict_channel(&applied_retention, channel_state);
        evicted.0 = evicted.0.saturating_add(after_eviction.0);
        evicted.1 = evicted.1.saturating_add(after_eviction.1);

        if let Some(metrics) = self.config.metrics.as_ref() {
            metrics.mark_presence_history_write(&record.app_id);
            metrics.track_presence_history_write_latency(
                &record.app_id,
                started.elapsed().as_secs_f64() * 1000.0,
            );
            if evicted.0 > 0 || evicted.1 > 0 {
                metrics.mark_presence_history_eviction(&record.app_id, evicted.0, evicted.1);
            }

            let (retained_events, retained_bytes) = channels
                .iter()
                .filter(|(channel_key, _)| channel_key.starts_with(&format!("{}\0", record.app_id)))
                .fold((0_u64, 0_u64), |(events, bytes), (_, channel)| {
                    (
                        events.saturating_add(channel.records.len() as u64),
                        bytes.saturating_add(channel.retained_bytes),
                    )
                });
            metrics.update_presence_history_retained(
                &record.app_id,
                retained_events,
                retained_bytes,
            );
        }
        Ok(())
    }

    async fn read_page(&self, request: PresenceHistoryReadRequest) -> Result<PresenceHistoryPage> {
        request.validate()?;
        let key = Self::channel_key(&request.app_id, &request.channel);
        let mut channels = self.channels.write().await;
        let Some(channel_state) = channels.get_mut(&key) else {
            return Ok(PresenceHistoryPage {
                items: Vec::new(),
                next_cursor: None,
                retained: PresenceHistoryRetentionStats {
                    stream_id: None,
                    retained_events: 0,
                    retained_bytes: 0,
                    oldest_serial: None,
                    newest_serial: None,
                    oldest_published_at_ms: None,
                    newest_published_at_ms: None,
                },
                has_more: false,
                complete: true,
                truncated_by_retention: false,
                degraded: false,
            });
        };
        let retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| self.retention());
        Self::evict_channel(&retention, channel_state);
        let retained = Self::retained_from_channel(channel_state);

        if let Some(cursor) = request.cursor.as_ref() {
            if cursor.stream_id != channel_state.stream_id {
                return Err(Error::InvalidMessageFormat(
                    "Expired presence history cursor: channel stream changed".to_string(),
                ));
            }
            if let Some(oldest_serial) = retained.oldest_serial
                && cursor.serial < oldest_serial
            {
                return Err(Error::InvalidMessageFormat(
                    "Expired presence history cursor: cursor points before retained history"
                        .to_string(),
                ));
            }
        }

        let matcher = |record: &&PresenceHistoryAppendRecord| {
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
                        PresenceHistoryDirection::NewestFirst => {
                            record.stream_id == cursor.stream_id && record.serial < cursor.serial
                        }
                        PresenceHistoryDirection::OldestFirst => {
                            record.stream_id == cursor.stream_id && record.serial > cursor.serial
                        }
                    })
        };

        let collected: Vec<&PresenceHistoryAppendRecord> = match request.direction {
            PresenceHistoryDirection::NewestFirst => channel_state
                .records
                .iter()
                .rev()
                .filter(matcher)
                .take(request.limit.saturating_add(1))
                .collect(),
            PresenceHistoryDirection::OldestFirst => channel_state
                .records
                .iter()
                .filter(matcher)
                .take(request.limit.saturating_add(1))
                .collect(),
        };

        let has_more = collected.len() > request.limit;
        let items: Vec<PresenceHistoryItem> = collected
            .into_iter()
            .take(request.limit)
            .map(|record| PresenceHistoryItem {
                stream_id: record.stream_id.clone(),
                serial: record.serial,
                published_at_ms: record.published_at_ms,
                event: record.event,
                cause: record.cause,
                user_id: record.user_id.clone(),
                connection_id: record.connection_id.clone(),
                dead_node_id: record.dead_node_id.clone(),
                payload_size_bytes: record.payload_bytes.len(),
                payload_bytes: record.payload_bytes.clone(),
            })
            .collect();

        let next_cursor = if has_more {
            items.last().map(|item| PresenceHistoryCursor {
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

        Ok(PresenceHistoryPage {
            items,
            next_cursor,
            retained,
            has_more,
            complete: !has_more && !truncated_by_retention,
            truncated_by_retention,
            degraded: false,
        })
    }

    async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
        Ok(PresenceHistoryRuntimeStatus {
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
    ) -> Result<PresenceHistoryStreamRuntimeState> {
        let key = Self::channel_key(app_id, channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();
        let retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| self.retention());
        Self::evict_channel(&retention, channel_state);

        Ok(PresenceHistoryStreamRuntimeState::healthy(
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
    ) -> Result<PresenceHistoryStreamInspection> {
        let key = Self::channel_key(app_id, channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();
        let retention = channel_state
            .retention
            .clone()
            .unwrap_or_else(|| self.retention());
        Self::evict_channel(&retention, channel_state);

        Ok(PresenceHistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(channel_state.stream_id.clone()),
            next_serial: Some(channel_state.next_serial),
            retained: Self::retained_from_channel(channel_state),
            state: PresenceHistoryStreamRuntimeState::healthy(
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
    ) -> Result<PresenceHistoryResetResult> {
        let key = Self::channel_key(app_id, channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();
        let previous_stream_id = Some(channel_state.stream_id.clone());
        let purged_events = channel_state.records.len() as u64;
        let purged_bytes = channel_state.retained_bytes;
        channel_state.records.clear();
        channel_state.dedupe_keys.clear();
        channel_state.latest_event_by_user.clear();
        channel_state.retained_bytes = 0;
        channel_state.next_serial = 1;
        channel_state.stream_id = uuid::Uuid::new_v4().to_string();

        let inspection = PresenceHistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(channel_state.stream_id.clone()),
            next_serial: Some(channel_state.next_serial),
            retained: Self::retained_from_channel(channel_state),
            state: PresenceHistoryStreamRuntimeState::healthy(
                app_id,
                channel,
                Some(channel_state.stream_id.clone()),
                "in_memory",
            ),
        };

        Ok(PresenceHistoryResetResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            previous_stream_id,
            new_stream_id: inspection.stream_id.clone().unwrap_or_default(),
            purged_events,
            purged_bytes,
            inspection,
        })
    }
}

fn is_truncated_by_retention(
    bounds: &PresenceHistoryQueryBounds,
    retained: &PresenceHistoryRetentionStats,
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

#[cfg(test)]
mod tests;
