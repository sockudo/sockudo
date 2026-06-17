use super::store::PresenceHistoryStore;
use super::types::{
    PresenceHistoryCursor, PresenceHistoryDirection, PresenceHistoryDurableState,
    PresenceHistoryEventCause, PresenceHistoryEventKind, PresenceHistoryItem, PresenceHistoryPage,
    PresenceHistoryQueryBounds, PresenceHistoryReadRequest, PresenceHistoryResetResult,
    PresenceHistoryRetentionStats, PresenceHistoryRuntimeStatus, PresenceHistoryStreamInspection,
    PresenceHistoryStreamRuntimeState, PresenceHistoryTransitionRecord,
};
use crate::error::{Error, Result};
use crate::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryQueryBounds, HistoryReadRequest,
    HistoryRetentionPolicy, HistoryStore,
};
use crate::metrics::MetricsInterface;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DurablePresenceHistoryPayload {
    pub published_at_ms: i64,
    pub event: PresenceHistoryEventKind,
    pub cause: PresenceHistoryEventCause,
    pub user_id: String,
    pub connection_id: Option<String>,
    pub user_info: Option<Value>,
    pub dead_node_id: Option<String>,
    pub dedupe_key: String,
}

#[derive(Clone)]
pub struct DurablePresenceHistoryStore {
    history_store: Arc<dyn HistoryStore + Send + Sync>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    transition_cache: Arc<RwLock<BTreeMap<String, DurablePresenceTransitionCache>>>,
}

#[derive(Default)]
struct DurablePresenceTransitionCache {
    stream_id: Option<String>,
    dedupe_keys: BTreeMap<String, u64>,
    latest_event_by_user: BTreeMap<String, (PresenceHistoryEventKind, u64)>,
}

impl DurablePresenceHistoryStore {
    pub fn new(
        history_store: Arc<dyn HistoryStore + Send + Sync>,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Self {
        Self {
            history_store,
            metrics,
            transition_cache: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn durable_channel_name(channel: &str) -> String {
        format!("[presence-history]{channel}")
    }

    fn cache_key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
    }

    fn history_retention(record: &PresenceHistoryTransitionRecord) -> HistoryRetentionPolicy {
        HistoryRetentionPolicy {
            retention_window_seconds: record.retention.retention_window_seconds,
            max_messages_per_channel: record.retention.max_events_per_channel,
            max_bytes_per_channel: record.retention.max_bytes_per_channel,
        }
    }

    fn presence_bounds_to_history(bounds: &PresenceHistoryQueryBounds) -> HistoryQueryBounds {
        HistoryQueryBounds {
            start_serial: bounds.start_serial,
            end_serial: bounds.end_serial,
            start_time_ms: bounds.start_time_ms,
            end_time_ms: bounds.end_time_ms,
        }
    }

    fn presence_direction_to_history(direction: PresenceHistoryDirection) -> HistoryDirection {
        match direction {
            PresenceHistoryDirection::NewestFirst => HistoryDirection::NewestFirst,
            PresenceHistoryDirection::OldestFirst => HistoryDirection::OldestFirst,
        }
    }

    fn history_cursor_from_presence(
        request: &PresenceHistoryReadRequest,
        channel: &str,
    ) -> Option<HistoryCursor> {
        request.cursor.as_ref().map(|cursor| HistoryCursor {
            version: cursor.version,
            app_id: cursor.app_id.clone(),
            channel: channel.to_string(),
            stream_id: cursor.stream_id.clone(),
            serial: cursor.serial,
            direction: Self::presence_direction_to_history(cursor.direction),
            bounds: Self::presence_bounds_to_history(&cursor.bounds),
        })
    }

    fn history_read_request(
        request: &PresenceHistoryReadRequest,
        limit: usize,
    ) -> HistoryReadRequest {
        let channel = Self::durable_channel_name(&request.channel);
        HistoryReadRequest {
            app_id: request.app_id.clone(),
            channel: channel.clone(),
            direction: Self::presence_direction_to_history(request.direction),
            limit,
            cursor: Self::history_cursor_from_presence(request, &channel),
            bounds: Self::presence_bounds_to_history(&request.bounds),
        }
    }

    fn decode_payload(bytes: &[u8]) -> Result<DurablePresenceHistoryPayload> {
        sonic_rs::from_slice(bytes).map_err(|e| {
            Error::Serialization(format!(
                "Failed to decode durable presence history payload: {e}"
            ))
        })
    }

    fn encode_payload(record: &PresenceHistoryTransitionRecord) -> Result<Bytes> {
        sonic_rs::to_vec(&DurablePresenceHistoryPayload {
            published_at_ms: record.published_at_ms,
            event: record.event_kind,
            cause: record.cause,
            user_id: record.user_id.clone(),
            connection_id: record.connection_id.clone(),
            user_info: record.user_info.clone(),
            dead_node_id: record.dead_node_id.clone(),
            dedupe_key: record.dedupe_key.clone(),
        })
        .map(Bytes::from)
        .map_err(|e| {
            Error::Serialization(format!(
                "Failed to encode durable presence history payload: {e}"
            ))
        })
    }

    fn decode_item(
        item: crate::history::HistoryItem,
    ) -> Result<(PresenceHistoryItem, DurablePresenceHistoryPayload)> {
        let payload = Self::decode_payload(item.payload_bytes.as_ref())?;
        Ok((
            PresenceHistoryItem {
                stream_id: item.stream_id,
                serial: item.serial,
                published_at_ms: payload.published_at_ms,
                event: payload.event,
                cause: payload.cause,
                user_id: payload.user_id.clone(),
                connection_id: payload.connection_id.clone(),
                dead_node_id: payload.dead_node_id.clone(),
                payload_size_bytes: item.payload_size_bytes,
                payload_bytes: item.payload_bytes,
            },
            payload,
        ))
    }

    fn retained_from_history(
        retained: crate::history::HistoryRetentionStats,
    ) -> PresenceHistoryRetentionStats {
        PresenceHistoryRetentionStats {
            stream_id: retained.stream_id,
            retained_events: retained.retained_messages,
            retained_bytes: retained.retained_bytes,
            oldest_serial: retained.oldest_serial,
            newest_serial: retained.newest_serial,
            oldest_published_at_ms: retained.oldest_published_at_ms,
            newest_published_at_ms: retained.newest_published_at_ms,
        }
    }

    fn map_runtime_state(
        channel: &str,
        state: crate::history::HistoryStreamRuntimeState,
    ) -> PresenceHistoryStreamRuntimeState {
        PresenceHistoryStreamRuntimeState {
            app_id: state.app_id,
            channel: channel.to_string(),
            stream_id: state.stream_id,
            durable_state: match state.durable_state {
                crate::history::HistoryDurableState::Healthy => {
                    PresenceHistoryDurableState::Healthy
                }
                crate::history::HistoryDurableState::Degraded => {
                    PresenceHistoryDurableState::Degraded
                }
                crate::history::HistoryDurableState::ResetRequired => {
                    PresenceHistoryDurableState::ResetRequired
                }
            },
            continuity_proven: state.recovery_allowed,
            reset_required: state.reset_required,
            reason: state.reason,
            node_id: state.node_id,
            last_transition_at_ms: state.last_transition_at_ms,
            authoritative_source: state.authoritative_source,
            observed_source: state.observed_source,
        }
    }

    async fn update_retained_metrics(&self, app_id: &str, channel: &str) -> Result<()> {
        let Some(metrics) = self.metrics.as_ref() else {
            return Ok(());
        };
        let retained = self.stream_inspection(app_id, channel).await?.retained;
        metrics.update_presence_history_retained(
            app_id,
            retained.retained_events,
            retained.retained_bytes,
        );
        Ok(())
    }

    async fn inspect_durable_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<crate::history::HistoryStreamInspection> {
        self.history_store
            .stream_inspection(app_id, &Self::durable_channel_name(channel))
            .await
    }

    async fn consult_transition_cache(
        &self,
        record: &PresenceHistoryTransitionRecord,
    ) -> Result<Option<(bool, bool)>> {
        let inspection = self
            .inspect_durable_channel(&record.app_id, &record.channel)
            .await?;
        let cache_key = Self::cache_key(&record.app_id, &record.channel);
        let oldest_serial = inspection.retained.oldest_serial.unwrap_or(u64::MAX);
        let mut caches = self.transition_cache.write().await;
        let cache = caches.entry(cache_key).or_default();

        if cache.stream_id != inspection.stream_id {
            cache.stream_id = inspection.stream_id;
            cache.dedupe_keys.clear();
            cache.latest_event_by_user.clear();
        }

        cache
            .dedupe_keys
            .retain(|_, serial| *serial >= oldest_serial);
        cache
            .latest_event_by_user
            .retain(|_, (_, serial)| *serial >= oldest_serial);

        if let Some(serial) = cache.dedupe_keys.get(&record.dedupe_key)
            && *serial >= oldest_serial
        {
            return Ok(Some((true, false)));
        }

        if let Some((event, serial)) = cache.latest_event_by_user.get(&record.user_id)
            && *serial >= oldest_serial
        {
            return Ok(Some((false, *event == record.event_kind)));
        }

        Ok(None)
    }

    async fn cache_scanned_transition(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        payload: &DurablePresenceHistoryPayload,
        serial: u64,
    ) {
        let cache_key = Self::cache_key(app_id, channel);
        let mut caches = self.transition_cache.write().await;
        let cache = caches.entry(cache_key).or_default();
        if cache.stream_id.as_deref() != Some(stream_id) {
            cache.stream_id = Some(stream_id.to_string());
            cache.dedupe_keys.clear();
            cache.latest_event_by_user.clear();
        }
        cache.dedupe_keys.insert(payload.dedupe_key.clone(), serial);
        cache
            .latest_event_by_user
            .entry(payload.user_id.clone())
            .or_insert((payload.event, serial));
    }

    async fn cache_appended_transition(
        &self,
        record: &PresenceHistoryTransitionRecord,
        stream_id: &str,
        serial: u64,
    ) {
        let cache_key = Self::cache_key(&record.app_id, &record.channel);
        let mut caches = self.transition_cache.write().await;
        let cache = caches.entry(cache_key).or_default();
        if cache.stream_id.as_deref() != Some(stream_id) {
            cache.stream_id = Some(stream_id.to_string());
            cache.dedupe_keys.clear();
            cache.latest_event_by_user.clear();
        }
        cache.dedupe_keys.insert(record.dedupe_key.clone(), serial);
        cache
            .latest_event_by_user
            .insert(record.user_id.clone(), (record.event_kind, serial));
    }

    async fn find_existing_transition(
        &self,
        record: &PresenceHistoryTransitionRecord,
    ) -> Result<(bool, bool)> {
        if let Some(found) = self.consult_transition_cache(record).await? {
            return Ok(found);
        }

        let mut request = PresenceHistoryReadRequest {
            app_id: record.app_id.clone(),
            channel: record.channel.clone(),
            direction: PresenceHistoryDirection::NewestFirst,
            limit: 100,
            cursor: None,
            bounds: PresenceHistoryQueryBounds::default(),
        };
        let mut found_dedupe = false;
        let mut found_same_state = false;

        loop {
            let page = self.read_page(request.clone()).await?;
            for item in &page.items {
                let payload = Self::decode_payload(item.payload_bytes.as_ref())?;
                self.cache_scanned_transition(
                    &record.app_id,
                    &record.channel,
                    &item.stream_id,
                    &payload,
                    item.serial,
                )
                .await;
                if payload.dedupe_key == record.dedupe_key {
                    found_dedupe = true;
                    break;
                }
                if payload.user_id == record.user_id {
                    found_same_state = payload.event == record.event_kind;
                    return Ok((found_dedupe, found_same_state));
                }
            }
            if found_dedupe || !page.has_more {
                return Ok((found_dedupe, found_same_state));
            }
            request.cursor = page.next_cursor;
        }
    }
}

#[async_trait]
impl PresenceHistoryStore for DurablePresenceHistoryStore {
    async fn record_transition(&self, record: PresenceHistoryTransitionRecord) -> Result<()> {
        let started = Instant::now();
        let (found_dedupe, found_same_state) = self.find_existing_transition(&record).await?;
        if found_dedupe || found_same_state {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.track_presence_history_write_latency(
                    &record.app_id,
                    started.elapsed().as_secs_f64() * 1000.0,
                );
            }
            return Ok(());
        }

        let reservation = self
            .history_store
            .reserve_publish_position(&record.app_id, &Self::durable_channel_name(&record.channel))
            .await;

        let reservation = match reservation {
            Ok(reservation) => reservation,
            Err(error) => {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.mark_presence_history_write_failure(&record.app_id);
                    metrics.track_presence_history_write_latency(
                        &record.app_id,
                        started.elapsed().as_secs_f64() * 1000.0,
                    );
                }
                return Err(error);
            }
        };

        let stream_id = reservation.stream_id.clone();
        let append = self
            .history_store
            .append(HistoryAppendRecord {
                app_id: record.app_id.clone(),
                channel: Self::durable_channel_name(&record.channel),
                stream_id,
                serial: reservation.serial,
                published_at_ms: record.published_at_ms,
                message_id: None,
                event_name: Some(format!("presence:{}", record.event_kind.as_str())),
                operation_kind: "append".to_string(),
                payload_bytes: Self::encode_payload(&record)?,
                retention: Self::history_retention(&record),
            })
            .await;

        match append {
            Ok(()) => {
                self.cache_appended_transition(&record, &reservation.stream_id, reservation.serial)
                    .await;
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.mark_presence_history_write(&record.app_id);
                    metrics.track_presence_history_write_latency(
                        &record.app_id,
                        started.elapsed().as_secs_f64() * 1000.0,
                    );
                }
                self.update_retained_metrics(&record.app_id, &record.channel)
                    .await?;
                Ok(())
            }
            Err(error) => {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.mark_presence_history_write_failure(&record.app_id);
                    metrics.track_presence_history_write_latency(
                        &record.app_id,
                        started.elapsed().as_secs_f64() * 1000.0,
                    );
                }
                Err(error)
            }
        }
    }

    async fn read_page(&self, request: PresenceHistoryReadRequest) -> Result<PresenceHistoryPage> {
        request.validate()?;
        let history_page = self
            .history_store
            .read_page(Self::history_read_request(&request, request.limit))
            .await?;
        let runtime_state = self
            .stream_runtime_state(&request.app_id, &request.channel)
            .await?;

        let mut items = Vec::with_capacity(history_page.items.len());
        for item in history_page.items {
            let (presence_item, _) = Self::decode_item(item)?;
            items.push(presence_item);
        }

        Ok(PresenceHistoryPage {
            items,
            next_cursor: history_page
                .next_cursor
                .map(|cursor| PresenceHistoryCursor {
                    version: cursor.version,
                    app_id: cursor.app_id,
                    channel: request.channel.clone(),
                    stream_id: cursor.stream_id,
                    serial: cursor.serial,
                    direction: request.direction,
                    bounds: request.bounds.clone(),
                }),
            retained: Self::retained_from_history(history_page.retained),
            has_more: history_page.has_more,
            complete: history_page.complete && runtime_state.continuity_proven,
            truncated_by_retention: history_page.truncated_by_retention,
            degraded: !runtime_state.continuity_proven,
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamRuntimeState> {
        let state = self
            .history_store
            .stream_runtime_state(app_id, &Self::durable_channel_name(channel))
            .await?;
        Ok(Self::map_runtime_state(channel, state))
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamInspection> {
        let inspection = self
            .history_store
            .stream_inspection(app_id, &Self::durable_channel_name(channel))
            .await?;
        Ok(PresenceHistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: inspection.stream_id,
            next_serial: inspection.next_serial,
            retained: Self::retained_from_history(inspection.retained),
            state: Self::map_runtime_state(channel, inspection.state),
        })
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> Result<PresenceHistoryResetResult> {
        let result = self
            .history_store
            .reset_stream(
                app_id,
                &Self::durable_channel_name(channel),
                reason,
                requested_by,
            )
            .await?;
        self.transition_cache
            .write()
            .await
            .remove(&Self::cache_key(app_id, channel));
        self.update_retained_metrics(app_id, channel).await?;
        Ok(PresenceHistoryResetResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            previous_stream_id: result.previous_stream_id,
            new_stream_id: result.new_stream_id,
            purged_events: result.purged_messages,
            purged_bytes: result.purged_bytes,
            inspection: PresenceHistoryStreamInspection {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: result.inspection.stream_id,
                next_serial: result.inspection.next_serial,
                retained: Self::retained_from_history(result.inspection.retained),
                state: Self::map_runtime_state(channel, result.inspection.state),
            },
        })
    }

    async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
        let history_status = self.history_store.runtime_status().await?;
        Ok(PresenceHistoryRuntimeStatus {
            enabled: history_status.enabled,
            backend: history_status.backend,
            state_authority: history_status.state_authority,
            degraded_channels: history_status.degraded_channels,
            reset_required_channels: history_status.reset_required_channels,
            queue_depth: history_status.queue_depth,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{MemoryHistoryStore, MemoryHistoryStoreConfig, now_ms};
    use crate::presence_history::PresenceSnapshotRequest;
    use crate::presence_history::test_support::transition;

    #[tokio::test]
    async fn durable_presence_history_round_trips_over_history_store() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = DurablePresenceHistoryStore::new(history, None);
        let base = now_ms();

        store
            .record_transition(transition(
                base,
                "join-alice",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 1,
                "join-bob",
                PresenceHistoryEventKind::MemberAdded,
                "bob",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 2,
                "leave-bob",
                PresenceHistoryEventKind::MemberRemoved,
                "bob",
            ))
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 3);
        assert_eq!(page.items[0].user_id, "alice");
        assert_eq!(page.items[1].user_id, "bob");
        assert_eq!(page.items[2].event, PresenceHistoryEventKind::MemberRemoved);

        let status = store.runtime_status().await.unwrap();
        assert_eq!(status.backend, "memory");

        let inspection = store
            .stream_inspection("app", "presence-room")
            .await
            .unwrap();
        assert_eq!(inspection.channel, "presence-room");
        assert_eq!(inspection.retained.retained_events, 3);
    }

    #[tokio::test]
    async fn durable_presence_history_dedupes_and_suppresses_same_state() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = DurablePresenceHistoryStore::new(history, None);
        let base = now_ms();

        store
            .record_transition(transition(
                base,
                "join-alice-1",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 1,
                "join-alice-1",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 2,
                "join-alice-2",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 3,
                "leave-alice-1",
                PresenceHistoryEventKind::MemberRemoved,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 4,
                "leave-alice-2",
                PresenceHistoryEventKind::MemberRemoved,
                "alice",
            ))
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
        assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
    }

    #[tokio::test]
    async fn durable_presence_history_reuses_cached_latest_state() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = DurablePresenceHistoryStore::new(history, None);
        let base = now_ms();

        store
            .record_transition(transition(
                base,
                "join-alice-1",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();

        {
            let cache = store.transition_cache.read().await;
            let channel = cache
                .get(&DurablePresenceHistoryStore::cache_key(
                    "app",
                    "presence-room",
                ))
                .unwrap();
            assert_eq!(
                channel.latest_event_by_user.get("alice"),
                Some(&(PresenceHistoryEventKind::MemberAdded, 1))
            );
        }

        store
            .record_transition(transition(
                base + 1,
                "join-alice-2",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 1);
    }

    #[tokio::test]
    async fn durable_presence_history_snapshot_and_reset_follow_presence_semantics() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = DurablePresenceHistoryStore::new(history, None);
        let base = now_ms();

        store
            .record_transition(transition(
                base,
                "join-alice",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 1,
                "join-bob",
                PresenceHistoryEventKind::MemberAdded,
                "bob",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 2,
                "leave-bob",
                PresenceHistoryEventKind::MemberRemoved,
                "bob",
            ))
            .await
            .unwrap();

        let snapshot = store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                at_time_ms: None,
                at_serial: None,
            })
            .await
            .unwrap();
        assert_eq!(snapshot.members.len(), 1);
        assert_eq!(snapshot.members[0].user_id, "alice");

        let before = store
            .stream_inspection("app", "presence-room")
            .await
            .unwrap();
        let previous_stream_id = before.stream_id.clone().unwrap();

        let reset = store
            .reset_stream("app", "presence-room", "operator reset", Some("ops"))
            .await
            .unwrap();
        assert_eq!(reset.purged_events, 3);
        assert_eq!(
            reset.previous_stream_id.as_deref(),
            Some(previous_stream_id.as_str())
        );
        assert_ne!(reset.new_stream_id, previous_stream_id);

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();
        assert!(page.items.is_empty());
    }
}
