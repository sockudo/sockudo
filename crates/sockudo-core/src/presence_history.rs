use crate::error::{Error, Result};
use crate::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryQueryBounds, HistoryReadRequest,
    HistoryRetentionPolicy, HistoryStore, now_ms,
};
use crate::metrics::MetricsInterface;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

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
    MemberRemoved,
}

impl PresenceHistoryEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MemberAdded => "member_added",
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

#[async_trait]
pub trait PresenceHistoryStore: Send + Sync {
    async fn record_transition(&self, record: PresenceHistoryTransitionRecord) -> Result<()>;

    async fn read_page(&self, request: PresenceHistoryReadRequest) -> Result<PresenceHistoryPage>;

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamRuntimeState> {
        Ok(PresenceHistoryStreamRuntimeState::healthy(
            app_id, channel, None, "disabled",
        ))
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamInspection> {
        Ok(PresenceHistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: None,
            next_serial: None,
            retained: PresenceHistoryRetentionStats::default(),
            state: self.stream_runtime_state(app_id, channel).await?,
        })
    }

    async fn reset_stream(
        &self,
        _app_id: &str,
        _channel: &str,
        _reason: &str,
        _requested_by: Option<&str>,
    ) -> Result<PresenceHistoryResetResult> {
        Err(Error::Configuration(
            "Presence history reset is not supported by this store".to_string(),
        ))
    }

    async fn snapshot_at(&self, request: PresenceSnapshotRequest) -> Result<PresenceSnapshot> {
        let mut members: BTreeMap<String, PresenceSnapshotMember> = BTreeMap::new();
        let mut events_replayed = 0_u64;
        let mut snapshot_serial = None;
        let mut snapshot_time_ms = None;
        let mut cursor = None;
        let bounds = PresenceHistoryQueryBounds {
            start_serial: None,
            end_serial: request.at_serial,
            start_time_ms: None,
            end_time_ms: request.at_time_ms,
        };
        let (retained, complete, truncated_by_retention) = loop {
            let page = self
                .read_page(PresenceHistoryReadRequest {
                    app_id: request.app_id.clone(),
                    channel: request.channel.clone(),
                    direction: PresenceHistoryDirection::OldestFirst,
                    limit: 1000,
                    cursor: cursor.clone(),
                    bounds: bounds.clone(),
                })
                .await?;

            for item in &page.items {
                events_replayed = events_replayed.saturating_add(1);
                snapshot_serial = Some(item.serial);
                snapshot_time_ms = Some(item.published_at_ms);
                match item.event {
                    PresenceHistoryEventKind::MemberAdded => {
                        members.insert(
                            item.user_id.clone(),
                            PresenceSnapshotMember {
                                user_id: item.user_id.clone(),
                                last_event: item.event,
                                last_event_serial: item.serial,
                                last_event_at_ms: item.published_at_ms,
                            },
                        );
                    }
                    PresenceHistoryEventKind::MemberRemoved => {
                        members.remove(&item.user_id);
                    }
                }
            }

            if !page.has_more {
                break (page.retained, page.complete, page.truncated_by_retention);
            }
            cursor = page.next_cursor;
        };

        Ok(PresenceSnapshot {
            channel: request.channel,
            members: members.into_values().collect(),
            events_replayed,
            snapshot_serial,
            snapshot_time_ms,
            retained,
            complete,
            truncated_by_retention,
        })
    }

    async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
        Ok(PresenceHistoryRuntimeStatus::default())
    }
}

#[derive(Default)]
pub struct NoopPresenceHistoryStore;

#[async_trait]
impl PresenceHistoryStore for NoopPresenceHistoryStore {
    async fn record_transition(&self, _record: PresenceHistoryTransitionRecord) -> Result<()> {
        Ok(())
    }

    async fn read_page(&self, request: PresenceHistoryReadRequest) -> Result<PresenceHistoryPage> {
        request.validate()?;
        Ok(PresenceHistoryPage {
            items: Vec::new(),
            next_cursor: None,
            retained: PresenceHistoryRetentionStats::default(),
            has_more: false,
            complete: true,
            truncated_by_retention: false,
            degraded: false,
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamRuntimeState> {
        Ok(PresenceHistoryStreamRuntimeState {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: None,
            durable_state: PresenceHistoryDurableState::ResetRequired,
            continuity_proven: false,
            reset_required: true,
            reason: Some("presence_history_disabled".to_string()),
            node_id: None,
            last_transition_at_ms: None,
            authoritative_source: "disabled".to_string(),
            observed_source: "disabled".to_string(),
        })
    }
}

#[derive(Clone)]
pub struct TrackingPresenceHistoryStore {
    inner: Arc<dyn PresenceHistoryStore + Send + Sync>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    state_authority: String,
    runtime_states: Arc<RwLock<BTreeMap<String, PresenceHistoryStreamRuntimeState>>>,
}

impl TrackingPresenceHistoryStore {
    pub fn new(
        inner: Arc<dyn PresenceHistoryStore + Send + Sync>,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        state_authority: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            metrics,
            state_authority: state_authority.into(),
            runtime_states: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn channel_key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
    }

    async fn current_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Option<PresenceHistoryStreamRuntimeState> {
        let key = Self::channel_key(app_id, channel);
        self.runtime_states.read().await.get(&key).cloned()
    }

    async fn publish_metrics(&self, app_id: &str) {
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };

        let states = self.runtime_states.read().await;
        let prefix = format!("{app_id}\0");
        let (degraded, reset_required) = states
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .fold(
                (0usize, 0usize),
                |(degraded, reset_required), (_, state)| match state.durable_state {
                    PresenceHistoryDurableState::Healthy => (degraded, reset_required),
                    PresenceHistoryDurableState::Degraded => (degraded + 1, reset_required),
                    PresenceHistoryDurableState::ResetRequired => {
                        (degraded + 1, reset_required + 1)
                    }
                },
            );
        metrics.update_presence_history_degraded_channels(app_id, degraded);
        metrics.update_presence_history_reset_required_channels(app_id, reset_required);
    }
}

#[async_trait]
impl PresenceHistoryStore for TrackingPresenceHistoryStore {
    async fn record_transition(&self, record: PresenceHistoryTransitionRecord) -> Result<()> {
        let key = Self::channel_key(&record.app_id, &record.channel);
        let inspection_before = self
            .inner
            .stream_inspection(&record.app_id, &record.channel)
            .await?;
        match self.inner.record_transition(record).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let durable_state = if inspection_before.retained.retained_events > 0 {
                    PresenceHistoryDurableState::ResetRequired
                } else {
                    PresenceHistoryDurableState::Degraded
                };
                self.runtime_states.write().await.insert(
                    key,
                    PresenceHistoryStreamRuntimeState {
                        app_id: inspection_before.app_id.clone(),
                        channel: inspection_before.channel.clone(),
                        stream_id: inspection_before.stream_id.clone(),
                        durable_state,
                        continuity_proven: durable_state.continuity_proven(),
                        reset_required: durable_state.reset_required(),
                        reason: Some(
                            match durable_state {
                                PresenceHistoryDurableState::Healthy => "presence_history_healthy",
                                PresenceHistoryDurableState::Degraded => {
                                    "presence_history_write_failed"
                                }
                                PresenceHistoryDurableState::ResetRequired => {
                                    "presence_history_reset_required_after_write_failure"
                                }
                            }
                            .to_string(),
                        ),
                        node_id: None,
                        last_transition_at_ms: Some(now_ms()),
                        authoritative_source: self.state_authority.clone(),
                        observed_source: self.state_authority.clone(),
                    },
                );
                self.publish_metrics(&inspection_before.app_id).await;
                Err(error)
            }
        }
    }

    async fn read_page(&self, request: PresenceHistoryReadRequest) -> Result<PresenceHistoryPage> {
        let mut page = self.inner.read_page(request.clone()).await?;
        if self
            .current_state(&request.app_id, &request.channel)
            .await
            .is_some_and(|state| state.durable_state != PresenceHistoryDurableState::Healthy)
        {
            page.complete = false;
            page.degraded = true;
        }
        Ok(page)
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamRuntimeState> {
        let inner_state = self.inner.stream_runtime_state(app_id, channel).await?;
        Ok(self
            .current_state(app_id, channel)
            .await
            .unwrap_or(inner_state))
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamInspection> {
        let mut inspection = self.inner.stream_inspection(app_id, channel).await?;
        if let Some(state) = self.current_state(app_id, channel).await {
            inspection.state = state;
        }
        Ok(inspection)
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> Result<PresenceHistoryResetResult> {
        let mut result = self
            .inner
            .reset_stream(app_id, channel, reason, requested_by)
            .await?;
        let key = Self::channel_key(app_id, channel);
        self.runtime_states.write().await.remove(&key);
        self.publish_metrics(app_id).await;
        result.inspection.state = self.inner.stream_runtime_state(app_id, channel).await?;
        Ok(result)
    }

    async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
        let mut status = self.inner.runtime_status().await?;
        status.state_authority = self.state_authority.clone();
        let states = self.runtime_states.read().await;
        let (degraded, reset_required) = states.values().fold(
            (0usize, 0usize),
            |(degraded, reset_required), state| match state.durable_state {
                PresenceHistoryDurableState::Healthy => (degraded, reset_required),
                PresenceHistoryDurableState::Degraded => (degraded + 1, reset_required),
                PresenceHistoryDurableState::ResetRequired => (degraded + 1, reset_required + 1),
            },
        );
        status.degraded_channels = degraded;
        status.reset_required_channels = reset_required;
        Ok(status)
    }
}

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
}

impl DurablePresenceHistoryStore {
    pub fn new(
        history_store: Arc<dyn HistoryStore + Send + Sync>,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Self {
        Self {
            history_store,
            metrics,
        }
    }

    fn durable_channel_name(channel: &str) -> String {
        format!("[presence-history]{channel}")
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

    async fn find_existing_transition(
        &self,
        record: &PresenceHistoryTransitionRecord,
    ) -> Result<(bool, bool)> {
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

        let append = self
            .history_store
            .append(HistoryAppendRecord {
                app_id: record.app_id.clone(),
                channel: Self::durable_channel_name(&record.channel),
                stream_id: reservation.stream_id,
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
        let channel_state = channels.entry(key).or_default();
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
mod tests {
    use super::*;
    use crate::history::{MemoryHistoryStore, MemoryHistoryStoreConfig};

    fn transition(
        published_at_ms: i64,
        dedupe_key: &str,
        event: PresenceHistoryEventKind,
        user_id: &str,
    ) -> PresenceHistoryTransitionRecord {
        PresenceHistoryTransitionRecord {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            event_kind: event,
            cause: match event {
                PresenceHistoryEventKind::MemberAdded => PresenceHistoryEventCause::Join,
                PresenceHistoryEventKind::MemberRemoved => PresenceHistoryEventCause::Disconnect,
            },
            user_id: user_id.to_string(),
            connection_id: Some(format!("socket-{user_id}")),
            user_info: None,
            dead_node_id: None,
            dedupe_key: dedupe_key.to_string(),
            published_at_ms,
            retention: PresenceHistoryRetentionPolicy {
                retention_window_seconds: 3600,
                max_events_per_channel: None,
                max_bytes_per_channel: None,
            },
        }
    }

    #[tokio::test]
    async fn memory_presence_history_orders_oldest_and_newest_first() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base = now_ms();
        store
            .record_transition(transition(
                base,
                "join-1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 1,
                "leave-1",
                PresenceHistoryEventKind::MemberRemoved,
                "u1",
            ))
            .await
            .unwrap();

        let newest = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::NewestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();
        assert_eq!(newest.items.len(), 2);
        assert_eq!(newest.items[0].serial, 2);
        assert_eq!(newest.items[1].serial, 1);

        let oldest = store
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
        assert_eq!(oldest.items.len(), 2);
        assert_eq!(oldest.items[0].serial, 1);
        assert_eq!(oldest.items[1].serial, 2);
    }

    #[tokio::test]
    async fn memory_presence_history_deduplicates_same_transition_key() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let record = transition(
            now_ms(),
            "join-1",
            PresenceHistoryEventKind::MemberAdded,
            "u1",
        );
        store.record_transition(record.clone()).await.unwrap();
        store.record_transition(record).await.unwrap();

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
        assert_eq!(page.retained.retained_events, 1);
    }

    #[tokio::test]
    async fn memory_presence_history_applies_time_and_count_retention() {
        let store = MemoryPresenceHistoryStore::new(MemoryPresenceHistoryStoreConfig {
            retention_window: Duration::from_secs(5),
            max_events_per_channel: Some(2),
            max_bytes_per_channel: None,
            metrics: None,
        });

        let now = now_ms();
        let capped_retention = PresenceHistoryRetentionPolicy {
            retention_window_seconds: 5,
            max_events_per_channel: Some(2),
            max_bytes_per_channel: None,
        };

        let mut old = transition(
            now - 10_000,
            "join-1",
            PresenceHistoryEventKind::MemberAdded,
            "u1",
        );
        old.retention = capped_retention.clone();
        store.record_transition(old).await.unwrap();

        let mut newer = transition(
            now - 2_000,
            "join-2",
            PresenceHistoryEventKind::MemberAdded,
            "u2",
        );
        newer.retention = capped_retention.clone();
        store.record_transition(newer).await.unwrap();

        let mut newest = transition(
            now - 1_000,
            "join-3",
            PresenceHistoryEventKind::MemberAdded,
            "u3",
        );
        newest.retention = capped_retention;
        store.record_transition(newest).await.unwrap();

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
        assert_eq!(page.items[0].user_id, "u2");
        assert_eq!(page.items[1].user_id, "u3");
    }

    #[tokio::test]
    async fn memory_presence_history_suppresses_consecutive_duplicate_user_transitions() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base = now_ms();

        let mut first_join = transition(
            base,
            "join-node-a",
            PresenceHistoryEventKind::MemberAdded,
            "u1",
        );
        first_join.connection_id = Some("socket-a".to_string());
        store.record_transition(first_join).await.unwrap();

        let mut duplicate_join = transition(
            base + 1,
            "join-node-b",
            PresenceHistoryEventKind::MemberAdded,
            "u1",
        );
        duplicate_join.connection_id = Some("socket-b".to_string());
        store.record_transition(duplicate_join).await.unwrap();

        let mut first_leave = transition(
            base + 2,
            "leave-disconnect",
            PresenceHistoryEventKind::MemberRemoved,
            "u1",
        );
        first_leave.connection_id = Some("socket-a".to_string());
        store.record_transition(first_leave).await.unwrap();

        let mut duplicate_leave = transition(
            base + 3,
            "leave-orphan-cleanup",
            PresenceHistoryEventKind::MemberRemoved,
            "u1",
        );
        duplicate_leave.connection_id = None;
        duplicate_leave.cause = PresenceHistoryEventCause::OrphanCleanup;
        duplicate_leave.dead_node_id = Some("dead-node".to_string());
        store.record_transition(duplicate_leave).await.unwrap();

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
        assert_eq!(page.items[1].cause, PresenceHistoryEventCause::Disconnect);
    }

    struct FailingPresenceHistoryStore;

    #[async_trait]
    impl PresenceHistoryStore for FailingPresenceHistoryStore {
        async fn record_transition(&self, _record: PresenceHistoryTransitionRecord) -> Result<()> {
            Err(Error::Internal(
                "forced presence history failure".to_string(),
            ))
        }

        async fn read_page(
            &self,
            request: PresenceHistoryReadRequest,
        ) -> Result<PresenceHistoryPage> {
            request.validate()?;
            Ok(PresenceHistoryPage {
                items: Vec::new(),
                next_cursor: None,
                retained: PresenceHistoryRetentionStats::default(),
                has_more: false,
                complete: true,
                truncated_by_retention: false,
                degraded: false,
            })
        }

        async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
            Ok(PresenceHistoryRuntimeStatus {
                enabled: true,
                backend: "failing".to_string(),
                state_authority: "failing".to_string(),
                degraded_channels: 0,
                reset_required_channels: 0,
                queue_depth: 0,
            })
        }
    }

    #[tokio::test]
    async fn tracking_presence_history_store_marks_failed_channels_degraded() {
        let store = TrackingPresenceHistoryStore::new(
            Arc::new(FailingPresenceHistoryStore),
            None,
            "tracking_wrapper",
        );
        let error = store
            .record_transition(transition(
                now_ms(),
                "join-1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Internal(_)));

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
        assert!(page.degraded);
        assert!(!page.complete);

        let status = store.runtime_status().await.unwrap();
        assert_eq!(status.degraded_channels, 1);
        assert_eq!(status.reset_required_channels, 0);
    }

    #[tokio::test]
    async fn tracking_presence_history_store_escalates_existing_stream_failures_to_reset_required()
    {
        let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        inner
            .record_transition(transition(
                now_ms(),
                "join-1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await
            .unwrap();

        struct ExistingStreamFailingStore {
            inner: Arc<MemoryPresenceHistoryStore>,
        }

        #[async_trait]
        impl PresenceHistoryStore for ExistingStreamFailingStore {
            async fn record_transition(
                &self,
                _record: PresenceHistoryTransitionRecord,
            ) -> Result<()> {
                Err(Error::Internal(
                    "forced failure after existing history".to_string(),
                ))
            }

            async fn read_page(
                &self,
                request: PresenceHistoryReadRequest,
            ) -> Result<PresenceHistoryPage> {
                self.inner.read_page(request).await
            }

            async fn stream_runtime_state(
                &self,
                app_id: &str,
                channel: &str,
            ) -> Result<PresenceHistoryStreamRuntimeState> {
                self.inner.stream_runtime_state(app_id, channel).await
            }

            async fn stream_inspection(
                &self,
                app_id: &str,
                channel: &str,
            ) -> Result<PresenceHistoryStreamInspection> {
                self.inner.stream_inspection(app_id, channel).await
            }

            async fn reset_stream(
                &self,
                app_id: &str,
                channel: &str,
                reason: &str,
                requested_by: Option<&str>,
            ) -> Result<PresenceHistoryResetResult> {
                self.inner
                    .reset_stream(app_id, channel, reason, requested_by)
                    .await
            }

            async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
                self.inner.runtime_status().await
            }
        }

        let store = TrackingPresenceHistoryStore::new(
            Arc::new(ExistingStreamFailingStore {
                inner: inner.clone(),
            }),
            None,
            "tracking_wrapper",
        );
        store
            .record_transition(transition(
                now_ms() + 1,
                "leave-1",
                PresenceHistoryEventKind::MemberRemoved,
                "u1",
            ))
            .await
            .unwrap_err();

        let state = store
            .stream_runtime_state("app", "presence-room")
            .await
            .unwrap();
        assert_eq!(
            state.durable_state,
            PresenceHistoryDurableState::ResetRequired
        );
        assert!(!state.continuity_proven);
        assert!(state.reset_required);

        let reset = store
            .reset_stream("app", "presence-room", "operator reset", Some("ops"))
            .await
            .unwrap();
        assert_eq!(reset.purged_events, 1);
        assert_eq!(
            reset.inspection.state.durable_state,
            PresenceHistoryDurableState::Healthy
        );
    }

    #[tokio::test]
    async fn snapshot_at_reconstructs_membership_from_events() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base = now_ms();

        // u1 joins, u2 joins, u1 leaves
        store
            .record_transition(transition(
                base,
                "join-u1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 1,
                "join-u2",
                PresenceHistoryEventKind::MemberAdded,
                "u2",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 2,
                "leave-u1",
                PresenceHistoryEventKind::MemberRemoved,
                "u1",
            ))
            .await
            .unwrap();

        // Snapshot at latest: only u2 should remain
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
        assert_eq!(snapshot.members[0].user_id, "u2");
        assert_eq!(snapshot.events_replayed, 3);
        assert!(snapshot.complete);

        // Snapshot at serial 2: both u1 and u2 should be present
        let snapshot_at_2 = store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                at_time_ms: None,
                at_serial: Some(2),
            })
            .await
            .unwrap();

        assert_eq!(snapshot_at_2.members.len(), 2);
        assert_eq!(snapshot_at_2.events_replayed, 2);
        assert_eq!(snapshot_at_2.snapshot_serial, Some(2));

        // Snapshot at time base+1: only u1 and u2 joined, u1 not yet left
        let snapshot_at_time = store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                at_time_ms: Some(base + 1),
                at_serial: None,
            })
            .await
            .unwrap();

        assert_eq!(snapshot_at_time.members.len(), 2);
        assert_eq!(snapshot_at_time.events_replayed, 2);
    }

    // --- Prompt 13: Chaos, load, and failover tests ---

    #[tokio::test]
    async fn rapid_join_leave_churn_maintains_correct_count() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base = now_ms();
        let num_users = 100u64;

        // All users join
        for i in 0..num_users {
            store
                .record_transition(PresenceHistoryTransitionRecord {
                    app_id: "app".to_string(),
                    channel: "presence-room".to_string(),
                    event_kind: PresenceHistoryEventKind::MemberAdded,
                    cause: PresenceHistoryEventCause::Join,
                    user_id: format!("u{i}"),
                    connection_id: Some(format!("s{i}")),
                    user_info: None,
                    dead_node_id: None,
                    dedupe_key: format!("join-{i}"),
                    published_at_ms: base + i as i64,
                    retention: PresenceHistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_events_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }

        // Half the users leave
        for i in 0..num_users / 2 {
            store
                .record_transition(PresenceHistoryTransitionRecord {
                    app_id: "app".to_string(),
                    channel: "presence-room".to_string(),
                    event_kind: PresenceHistoryEventKind::MemberRemoved,
                    cause: PresenceHistoryEventCause::Disconnect,
                    user_id: format!("u{i}"),
                    connection_id: Some(format!("s{i}")),
                    user_info: None,
                    dead_node_id: None,
                    dedupe_key: format!("leave-{i}"),
                    published_at_ms: base + num_users as i64 + i as i64,
                    retention: PresenceHistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_events_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }

        let snapshot = store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                at_time_ms: None,
                at_serial: None,
            })
            .await
            .unwrap();

        assert_eq!(
            snapshot.members.len(),
            50,
            "50 users should remain after half left"
        );
        assert_eq!(snapshot.events_replayed, 150);
        assert!(snapshot.complete);
    }

    #[tokio::test]
    async fn retention_eviction_under_count_cap_preserves_newest() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base = now_ms();

        // Write 200 events with cap of 50
        for i in 0..200u64 {
            store
                .record_transition(PresenceHistoryTransitionRecord {
                    app_id: "app".to_string(),
                    channel: "presence-room".to_string(),
                    event_kind: if i % 2 == 0 {
                        PresenceHistoryEventKind::MemberAdded
                    } else {
                        PresenceHistoryEventKind::MemberRemoved
                    },
                    cause: PresenceHistoryEventCause::Join,
                    user_id: format!("u{i}"),
                    connection_id: Some(format!("s{i}")),
                    user_info: None,
                    dead_node_id: None,
                    dedupe_key: format!("evt-{i}"),
                    published_at_ms: base + i as i64,
                    retention: PresenceHistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_events_per_channel: Some(50),
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::NewestFirst,
                limit: 100,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert!(page.items.len() <= 50, "should not exceed retention cap");
        // Newest event should still be present
        let newest_serial = page.items.first().map(|i| i.serial).unwrap_or(0);
        assert!(
            newest_serial >= 150,
            "newest events should survive eviction"
        );
        assert!(page.truncated_by_retention, "should flag truncation");
    }

    #[tokio::test]
    async fn multinode_dedupe_collapses_fanout_duplicates() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base = now_ms();

        // Simulate 3 nodes publishing the same join with the same dedupe_key
        for node in 0..3 {
            store
                .record_transition(PresenceHistoryTransitionRecord {
                    app_id: "app".to_string(),
                    channel: "presence-room".to_string(),
                    event_kind: PresenceHistoryEventKind::MemberAdded,
                    cause: PresenceHistoryEventCause::Join,
                    user_id: "u1".to_string(),
                    connection_id: Some("s1".to_string()),
                    user_info: None,
                    dead_node_id: None,
                    dedupe_key: "join-u1-same-key".to_string(),
                    published_at_ms: base + node,
                    retention: PresenceHistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_events_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert_eq!(
            page.items.len(),
            1,
            "dedupe should collapse 3 identical transitions to 1"
        );
    }

    #[tokio::test]
    async fn orphan_cleanup_records_dead_node_id() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base = now_ms();

        // User joins normally
        store
            .record_transition(transition(
                base,
                "join-u1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await
            .unwrap();

        // Node dies, orphan cleanup removes the user
        store
            .record_transition(PresenceHistoryTransitionRecord {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                event_kind: PresenceHistoryEventKind::MemberRemoved,
                cause: PresenceHistoryEventCause::OrphanCleanup,
                user_id: "u1".to_string(),
                connection_id: None,
                user_info: None,
                dead_node_id: Some("dead-node-abc".to_string()),
                dedupe_key: "orphan-u1".to_string(),
                published_at_ms: base + 1,
                retention: PresenceHistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_events_per_channel: None,
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::NewestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        let orphan_event = &page.items[0];
        assert_eq!(orphan_event.cause, PresenceHistoryEventCause::OrphanCleanup);
        assert_eq!(orphan_event.dead_node_id.as_deref(), Some("dead-node-abc"));

        // Snapshot should show u1 as removed
        let snapshot = store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                at_time_ms: None,
                at_serial: None,
            })
            .await
            .unwrap();

        assert_eq!(
            snapshot.members.len(),
            0,
            "orphan-cleaned user should not appear in snapshot"
        );
    }

    #[tokio::test]
    async fn tracking_store_fail_closed_degrades_on_write_failure() {
        let failing_store = Arc::new(FailingPresenceHistoryStore);
        let tracker = TrackingPresenceHistoryStore::new(failing_store, None, "test-node");
        let base = now_ms();

        // Write should fail but tracker should mark degraded, not panic
        let result = tracker
            .record_transition(transition(
                base,
                "k1",
                PresenceHistoryEventKind::MemberAdded,
                "u1",
            ))
            .await;
        assert!(result.is_err());

        // Stream should be degraded
        let state = tracker
            .stream_runtime_state("app", "presence-room")
            .await
            .unwrap();
        assert!(
            !state.continuity_proven,
            "continuity should not be proven after write failure"
        );
    }

    #[tokio::test]
    async fn paging_through_large_retained_window_returns_all_events() {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base = now_ms();

        // Write 500 events
        for i in 0..500u64 {
            store
                .record_transition(PresenceHistoryTransitionRecord {
                    app_id: "app".to_string(),
                    channel: "presence-room".to_string(),
                    event_kind: if i % 2 == 0 {
                        PresenceHistoryEventKind::MemberAdded
                    } else {
                        PresenceHistoryEventKind::MemberRemoved
                    },
                    cause: PresenceHistoryEventCause::Join,
                    user_id: format!("u{i}"),
                    connection_id: Some(format!("s{i}")),
                    user_info: None,
                    dead_node_id: None,
                    dedupe_key: format!("evt-{i}"),
                    published_at_ms: base + i as i64,
                    retention: PresenceHistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_events_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }

        // Page through with page size 50
        let mut total_items = 0u64;
        let mut cursor: Option<PresenceHistoryCursor> = None;
        let mut pages = 0u64;

        loop {
            let page = store
                .read_page(PresenceHistoryReadRequest {
                    app_id: "app".to_string(),
                    channel: "presence-room".to_string(),
                    direction: PresenceHistoryDirection::OldestFirst,
                    limit: 50,
                    cursor: cursor.clone(),
                    bounds: Default::default(),
                })
                .await
                .unwrap();

            total_items += page.items.len() as u64;
            pages += 1;

            if !page.has_more || page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;

            // Safety valve
            if pages > 20 {
                panic!("too many pages, possible infinite loop");
            }
        }

        assert_eq!(total_items, 500, "paging should return all 500 events");
        assert_eq!(pages, 10, "500 events / 50 per page = 10 pages");
    }

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
