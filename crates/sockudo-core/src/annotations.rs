use crate::error::{Error, Result};
use crate::history::now_ms;
use crate::versioned_messages::{MAX_VERSIONED_SERIAL_LENGTH, MessageSerial};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

pub const MAX_ANNOTATION_TYPE_LENGTH: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct AnnotationId(String);

impl AnnotationId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier("annotation id", &value, MAX_VERSIONED_SERIAL_LENGTH)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct AnnotationSerial(String);

impl AnnotationSerial {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier("annotation serial", &value, MAX_VERSIONED_SERIAL_LENGTH)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnnotationSummarizer {
    Total,
    Flag,
    Distinct,
    Unique,
    Multiple,
}

impl AnnotationSummarizer {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Total => "total",
            Self::Flag => "flag",
            Self::Distinct => "distinct",
            Self::Unique => "unique",
            Self::Multiple => "multiple",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct AnnotationType(String);

impl AnnotationType {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        parse_annotation_type(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn namespace(&self) -> Result<&str> {
        Ok(parse_annotation_type(self.as_str())?.namespace)
    }

    pub fn summarizer(&self) -> Result<AnnotationSummarizer> {
        Ok(parse_annotation_type(self.as_str())?.summarizer)
    }

    pub fn version(&self) -> Result<u16> {
        Ok(parse_annotation_type(self.as_str())?.version)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AnnotationAction {
    #[serde(rename = "annotation.create")]
    Create,
    #[serde(rename = "annotation.delete")]
    Delete,
}

impl AnnotationAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "annotation.create",
            Self::Delete => "annotation.delete",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Annotation {
    pub id: AnnotationId,
    pub action: AnnotationAction,
    pub serial: AnnotationSerial,
    pub message_serial: MessageSerial,
    #[serde(rename = "type")]
    pub annotation_type: AnnotationType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    pub timestamp: i64,
}

impl Annotation {
    pub fn validate(&self) -> Result<()> {
        validate_identifier(
            "annotation id",
            self.id.as_str(),
            MAX_VERSIONED_SERIAL_LENGTH,
        )?;
        validate_identifier(
            "annotation serial",
            self.serial.as_str(),
            MAX_VERSIONED_SERIAL_LENGTH,
        )?;

        let summarizer = self.annotation_type.summarizer()?;

        if matches!(
            summarizer,
            AnnotationSummarizer::Distinct
                | AnnotationSummarizer::Unique
                | AnnotationSummarizer::Multiple
        ) {
            validate_required_text("annotation name", self.name.as_deref())?;
        }

        if matches!(
            summarizer,
            AnnotationSummarizer::Flag
                | AnnotationSummarizer::Distinct
                | AnnotationSummarizer::Unique
        ) {
            validate_required_text("annotation client_id", self.client_id.as_deref())?;
        }

        if self.count == Some(0) {
            return Err(Error::InvalidMessageFormat(
                "annotation count must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }

    pub fn effective_count(&self) -> u64 {
        self.count.unwrap_or(1)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TotalAnnotationSummary {
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IdentifiedAnnotationSummary {
    pub total: u64,
    pub client_ids: Vec<String>,
    pub clipped: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MultipleAnnotationSummary {
    pub total: u64,
    pub client_counts: BTreeMap<String, u64>,
    pub total_unidentified: u64,
    pub clipped: bool,
    pub total_client_ids: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum AnnotationSummary {
    Total(TotalAnnotationSummary),
    Flag(IdentifiedAnnotationSummary),
    Distinct(BTreeMap<String, IdentifiedAnnotationSummary>),
    Unique(BTreeMap<String, IdentifiedAnnotationSummary>),
    Multiple(BTreeMap<String, MultipleAnnotationSummary>),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AnnotationProjectionOptions {
    pub client_id_limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnnotationProjectionKey {
    pub channel_id: String,
    pub message_serial: MessageSerial,
    #[serde(rename = "type")]
    pub annotation_type: AnnotationType,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AnnotationProjection {
    pub key: AnnotationProjectionKey,
    pub summary: AnnotationSummary,
    pub last_serial: Option<AnnotationSerial>,
    pub applied_events: u64,
}

impl AnnotationProjection {
    pub fn rebuild<I>(
        channel_id: impl Into<String>,
        message_serial: MessageSerial,
        annotation_type: AnnotationType,
        events: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = Annotation>,
    {
        Self::rebuild_with_options(
            channel_id,
            message_serial,
            annotation_type,
            events,
            AnnotationProjectionOptions::default(),
        )
    }

    pub fn rebuild_with_options<I>(
        channel_id: impl Into<String>,
        message_serial: MessageSerial,
        annotation_type: AnnotationType,
        events: I,
        options: AnnotationProjectionOptions,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = Annotation>,
    {
        let summarizer = annotation_type.summarizer()?;
        let mut engine = crate::annotation_summarizers::new_engine(summarizer, options);
        let mut sorted_events: Vec<_> = events.into_iter().collect();
        sorted_events.sort_by(|left, right| left.serial.cmp(&right.serial));

        let mut seen_serials = HashSet::new();
        let mut last_serial = None;
        let mut applied_events = 0;

        for event in sorted_events {
            validate_event_key(&event, &message_serial, &annotation_type)?;
            if !seen_serials.insert(event.serial.as_str().to_string()) {
                continue;
            }

            last_serial = Some(event.serial.clone());
            engine.apply(&event)?;
            applied_events += 1;
        }

        Ok(Self {
            key: AnnotationProjectionKey {
                channel_id: channel_id.into(),
                message_serial,
                annotation_type,
            },
            summary: engine.finish(),
            last_serial,
            applied_events,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAnnotationEvent {
    pub app_id: String,
    pub channel_id: String,
    pub annotation: Annotation,
    pub stored_at_ms: i64,
}

impl StoredAnnotationEvent {
    pub fn validate(&self) -> Result<()> {
        validate_required_text("annotation app_id", Some(self.app_id.as_str()))?;
        validate_required_text("annotation channel_id", Some(self.channel_id.as_str()))?;
        self.annotation.validate()
    }

    pub fn message_serial(&self) -> &MessageSerial {
        &self.annotation.message_serial
    }

    pub fn annotation_serial(&self) -> &AnnotationSerial {
        &self.annotation.serial
    }

    pub fn annotation_type(&self) -> &AnnotationType {
        &self.annotation.annotation_type
    }
}

#[derive(Debug, Clone)]
pub struct AnnotationEventsRequest {
    pub app_id: String,
    pub channel_id: String,
    pub message_serial: MessageSerial,
    pub annotation_type: AnnotationType,
}

impl AnnotationEventsRequest {
    pub fn validate(&self) -> Result<()> {
        validate_required_text("annotation app_id", Some(self.app_id.as_str()))?;
        validate_required_text("annotation channel_id", Some(self.channel_id.as_str()))?;
        self.annotation_type.summarizer()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RawAnnotationReplayRequest {
    pub app_id: String,
    pub channel_id: String,
    pub after_annotation_serial: Option<AnnotationSerial>,
    pub limit: usize,
}

impl RawAnnotationReplayRequest {
    pub fn validate(&self) -> Result<()> {
        validate_required_text("annotation app_id", Some(self.app_id.as_str()))?;
        validate_required_text("annotation channel_id", Some(self.channel_id.as_str()))?;
        if self.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "annotation replay limit must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AnnotationEventLookupRequest {
    pub app_id: String,
    pub channel_id: String,
    pub annotation_serial: AnnotationSerial,
}

impl AnnotationEventLookupRequest {
    pub fn validate(&self) -> Result<()> {
        validate_required_text("annotation app_id", Some(self.app_id.as_str()))?;
        validate_required_text("annotation channel_id", Some(self.channel_id.as_str()))?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AnnotationProjectionRequest {
    pub app_id: String,
    pub channel_id: String,
    pub message_serial: MessageSerial,
    pub annotation_type: AnnotationType,
}

impl AnnotationProjectionRequest {
    pub fn validate(&self) -> Result<()> {
        validate_required_text("annotation app_id", Some(self.app_id.as_str()))?;
        validate_required_text("annotation channel_id", Some(self.channel_id.as_str()))?;
        self.annotation_type.summarizer()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AnnotationProjectionsForChannelRequest {
    pub app_id: String,
    pub channel_id: String,
}

impl AnnotationProjectionsForChannelRequest {
    pub fn validate(&self) -> Result<()> {
        validate_required_text("annotation app_id", Some(self.app_id.as_str()))?;
        validate_required_text("annotation channel_id", Some(self.channel_id.as_str()))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StoredAnnotationProjection {
    pub app_id: String,
    pub channel_id: String,
    pub message_serial: MessageSerial,
    #[serde(rename = "type")]
    pub annotation_type: AnnotationType,
    pub summary: AnnotationSummary,
    pub last_annotation_serial: Option<AnnotationSerial>,
    pub updated_at_ms: i64,
}

impl StoredAnnotationProjection {
    fn from_projection(app_id: String, projection: AnnotationProjection) -> Self {
        Self {
            app_id,
            channel_id: projection.key.channel_id,
            message_serial: projection.key.message_serial,
            annotation_type: projection.key.annotation_type,
            summary: projection.summary,
            last_annotation_serial: projection.last_serial,
            updated_at_ms: now_ms(),
        }
    }

    pub fn projection_key(&self) -> AnnotationProjectionRequest {
        AnnotationProjectionRequest {
            app_id: self.app_id.clone(),
            channel_id: self.channel_id.clone(),
            message_serial: self.message_serial.clone(),
            annotation_type: self.annotation_type.clone(),
        }
    }
}

#[async_trait]
pub trait AnnotationStore: Send + Sync {
    async fn append_event(
        &self,
        record: StoredAnnotationEvent,
    ) -> Result<StoredAnnotationProjection>;

    async fn get_events(
        &self,
        request: AnnotationEventsRequest,
    ) -> Result<Vec<StoredAnnotationEvent>>;

    async fn replay_raw(
        &self,
        request: RawAnnotationReplayRequest,
    ) -> Result<Vec<StoredAnnotationEvent>>;

    async fn get_event_by_serial(
        &self,
        request: AnnotationEventLookupRequest,
    ) -> Result<Option<StoredAnnotationEvent>>;

    async fn get_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<Option<StoredAnnotationProjection>>;

    async fn list_projections_for_channel(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<Vec<StoredAnnotationProjection>> {
        request.validate()?;
        Ok(Vec::new())
    }

    async fn list_projections_for_channel_with_rebuild_count(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<(Vec<StoredAnnotationProjection>, usize)> {
        let projections = self.list_projections_for_channel(request).await?;
        Ok((projections, 0))
    }

    async fn rebuild_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<StoredAnnotationProjection>;

    async fn rebuild_projection_with_options(
        &self,
        request: AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        let _ = options;
        self.rebuild_projection(request).await
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        let _ = (before_ms, batch_size);
        Ok((0, false))
    }
}

#[derive(Default)]
pub struct NoopAnnotationStore;

#[async_trait]
impl AnnotationStore for NoopAnnotationStore {
    async fn append_event(
        &self,
        _record: StoredAnnotationEvent,
    ) -> Result<StoredAnnotationProjection> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn get_events(
        &self,
        _request: AnnotationEventsRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn replay_raw(
        &self,
        _request: RawAnnotationReplayRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn get_event_by_serial(
        &self,
        _request: AnnotationEventLookupRequest,
    ) -> Result<Option<StoredAnnotationEvent>> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn get_projection(
        &self,
        _request: AnnotationProjectionRequest,
    ) -> Result<Option<StoredAnnotationProjection>> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn rebuild_projection(
        &self,
        _request: AnnotationProjectionRequest,
    ) -> Result<StoredAnnotationProjection> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }

    async fn rebuild_projection_with_options(
        &self,
        _request: AnnotationProjectionRequest,
        _options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        Err(Error::Configuration(
            "Annotation storage is not configured".to_string(),
        ))
    }
}

#[derive(Clone, Default)]
pub struct MemoryAnnotationStore {
    state: Arc<RwLock<MemoryAnnotationState>>,
}

#[derive(Default)]
struct MemoryAnnotationState {
    events_by_projection: BTreeMap<String, BTreeMap<AnnotationSerial, StoredAnnotationEvent>>,
    raw_by_channel: BTreeMap<String, BTreeMap<AnnotationSerial, StoredAnnotationEvent>>,
    projections: BTreeMap<String, StoredAnnotationProjection>,
}

impl MemoryAnnotationStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn channel_key(app_id: &str, channel_id: &str) -> String {
        format!("{app_id}\0{channel_id}")
    }

    fn projection_key(
        app_id: &str,
        channel_id: &str,
        message_serial: &MessageSerial,
        annotation_type: &AnnotationType,
    ) -> String {
        format!(
            "{}\0{}\0{}",
            Self::channel_key(app_id, channel_id),
            message_serial.as_str(),
            annotation_type.as_str()
        )
    }

    fn request_projection_key(request: &AnnotationProjectionRequest) -> String {
        Self::projection_key(
            &request.app_id,
            &request.channel_id,
            &request.message_serial,
            &request.annotation_type,
        )
    }

    fn event_projection_key(record: &StoredAnnotationEvent) -> String {
        Self::projection_key(
            &record.app_id,
            &record.channel_id,
            record.message_serial(),
            record.annotation_type(),
        )
    }

    fn projection_max_serial(
        state: &MemoryAnnotationState,
        projection_key: &str,
    ) -> Option<AnnotationSerial> {
        state
            .events_by_projection
            .get(projection_key)
            .and_then(|events| events.keys().next_back().cloned())
    }

    fn projection_events(
        state: &MemoryAnnotationState,
        projection_key: &str,
    ) -> Vec<StoredAnnotationEvent> {
        state
            .events_by_projection
            .get(projection_key)
            .map(|events| events.values().cloned().collect())
            .unwrap_or_default()
    }

    fn build_projection(
        request: &AnnotationProjectionRequest,
        events: Vec<StoredAnnotationEvent>,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        let projection = AnnotationProjection::rebuild_with_options(
            request.channel_id.clone(),
            request.message_serial.clone(),
            request.annotation_type.clone(),
            events.into_iter().map(|record| record.annotation),
            options,
        )?;
        Ok(StoredAnnotationProjection::from_projection(
            request.app_id.clone(),
            projection,
        ))
    }

    fn rebuild_projection_from_state(
        state: &mut MemoryAnnotationState,
        request: AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        let projection_key = Self::request_projection_key(&request);
        let events = Self::projection_events(state, &projection_key);
        let stored = Self::build_projection(&request, events, options)?;
        state.projections.insert(projection_key, stored.clone());
        Ok(stored)
    }

    async fn rebuild_projection_optimistic(
        &self,
        request: AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        request.validate()?;
        let projection_key = Self::request_projection_key(&request);

        loop {
            let (events, expected_last_serial) = {
                let state = self.state.read().await;
                (
                    Self::projection_events(&state, &projection_key),
                    Self::projection_max_serial(&state, &projection_key),
                )
            };

            let projection = Self::build_projection(&request, events, options)?;
            let mut state = self.state.write().await;
            let current_last_serial = Self::projection_max_serial(&state, &projection_key);
            if current_last_serial == expected_last_serial {
                state
                    .projections
                    .insert(projection_key.clone(), projection.clone());
                return Ok(projection);
            }
        }
    }
}

#[async_trait]
impl AnnotationStore for MemoryAnnotationStore {
    async fn append_event(
        &self,
        mut record: StoredAnnotationEvent,
    ) -> Result<StoredAnnotationProjection> {
        record.validate()?;
        if record.stored_at_ms == 0 {
            record.stored_at_ms = now_ms();
        }

        let projection_request = AnnotationProjectionRequest {
            app_id: record.app_id.clone(),
            channel_id: record.channel_id.clone(),
            message_serial: record.message_serial().clone(),
            annotation_type: record.annotation_type().clone(),
        };
        let projection_key = Self::event_projection_key(&record);
        let channel_key = Self::channel_key(&record.app_id, &record.channel_id);

        {
            let mut state = self.state.write().await;
            let events = state
                .events_by_projection
                .entry(projection_key)
                .or_default();
            events
                .entry(record.annotation_serial().clone())
                .or_insert_with(|| record.clone());
            state
                .raw_by_channel
                .entry(channel_key)
                .or_default()
                .entry(record.annotation_serial().clone())
                .or_insert(record);
        }

        self.rebuild_projection_optimistic(
            projection_request,
            AnnotationProjectionOptions::default(),
        )
        .await
    }

    async fn get_events(
        &self,
        request: AnnotationEventsRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        let key = Self::projection_key(
            &request.app_id,
            &request.channel_id,
            &request.message_serial,
            &request.annotation_type,
        );
        let state = self.state.read().await;
        Ok(state
            .events_by_projection
            .get(&key)
            .map(|events| events.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn replay_raw(
        &self,
        request: RawAnnotationReplayRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        let key = Self::channel_key(&request.app_id, &request.channel_id);
        let state = self.state.read().await;
        let Some(events) = state.raw_by_channel.get(&key) else {
            return Ok(Vec::new());
        };

        let items = events
            .iter()
            .filter(|(serial, _)| {
                request
                    .after_annotation_serial
                    .as_ref()
                    .is_none_or(|after| *serial > after)
            })
            .map(|(_, record)| record.clone())
            .take(request.limit)
            .collect();
        Ok(items)
    }

    async fn get_event_by_serial(
        &self,
        request: AnnotationEventLookupRequest,
    ) -> Result<Option<StoredAnnotationEvent>> {
        request.validate()?;
        let key = Self::channel_key(&request.app_id, &request.channel_id);
        let state = self.state.read().await;
        Ok(state
            .raw_by_channel
            .get(&key)
            .and_then(|events| events.get(&request.annotation_serial).cloned()))
    }

    async fn get_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<Option<StoredAnnotationProjection>> {
        request.validate()?;
        let key = Self::request_projection_key(&request);
        let state = self.state.read().await;
        let projection = state.projections.get(&key).cloned();
        let max_serial = Self::projection_max_serial(&state, &key);
        if projection
            .as_ref()
            .is_some_and(|projection| projection.last_annotation_serial == max_serial)
        {
            return Ok(projection);
        }
        if projection.is_none() && max_serial.is_none() {
            return Ok(None);
        }
        drop(state);

        self.rebuild_projection_optimistic(request, AnnotationProjectionOptions::default())
            .await
            .map(Some)
    }

    async fn list_projections_for_channel(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<Vec<StoredAnnotationProjection>> {
        let (projections, _) = self
            .list_projections_for_channel_with_rebuild_count(request)
            .await?;
        Ok(projections)
    }

    async fn list_projections_for_channel_with_rebuild_count(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<(Vec<StoredAnnotationProjection>, usize)> {
        request.validate()?;
        let requests = {
            let state = self.state.read().await;
            let mut requests = BTreeMap::new();
            for events in state.events_by_projection.values() {
                let Some(record) = events.values().next() else {
                    continue;
                };
                if record.app_id == request.app_id && record.channel_id == request.channel_id {
                    let projection_request = AnnotationProjectionRequest {
                        app_id: record.app_id.clone(),
                        channel_id: record.channel_id.clone(),
                        message_serial: record.message_serial().clone(),
                        annotation_type: record.annotation_type().clone(),
                    };
                    requests.insert(
                        Self::request_projection_key(&projection_request),
                        projection_request,
                    );
                }
            }
            for projection in state.projections.values() {
                if projection.app_id == request.app_id
                    && projection.channel_id == request.channel_id
                {
                    let projection_request = projection.projection_key();
                    requests.insert(
                        Self::request_projection_key(&projection_request),
                        projection_request,
                    );
                }
            }
            requests.into_values().collect::<Vec<_>>()
        };

        let mut projections = Vec::new();
        let mut rebuild_count = 0;
        for projection_request in requests {
            let should_rebuild = {
                let state = self.state.read().await;
                let key = Self::request_projection_key(&projection_request);
                let projection = state.projections.get(&key);
                let max_serial = Self::projection_max_serial(&state, &key);
                match (projection, max_serial) {
                    (None, Some(_)) => true,
                    (Some(projection), max_serial) => {
                        projection.last_annotation_serial != max_serial
                    }
                    _ => false,
                }
            };
            if let Some(projection) = self.get_projection(projection_request).await? {
                if should_rebuild {
                    rebuild_count += 1;
                }
                projections.push(projection);
            }
        }
        projections.sort_by(|left, right| {
            left.message_serial
                .cmp(&right.message_serial)
                .then_with(|| left.annotation_type.cmp(&right.annotation_type))
        });
        Ok((projections, rebuild_count))
    }

    async fn rebuild_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<StoredAnnotationProjection> {
        self.rebuild_projection_optimistic(request, AnnotationProjectionOptions::default())
            .await
    }

    async fn rebuild_projection_with_options(
        &self,
        request: AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        self.rebuild_projection_optimistic(request, options).await
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        if batch_size == 0 {
            return Ok((0, false));
        }

        let mut state = self.state.write().await;
        let mut deleted = 0_u64;
        let mut has_more = false;
        let mut affected_projection_keys = BTreeSet::new();
        let mut raw_removals = Vec::new();

        for (projection_key, events) in state.events_by_projection.iter_mut() {
            let remaining = batch_size.saturating_sub(deleted as usize);
            if remaining == 0 {
                has_more = true;
                break;
            }

            let to_remove = events
                .iter()
                .filter(|(_, record)| record.stored_at_ms < before_ms)
                .map(|(serial, _)| serial.clone())
                .take(remaining)
                .collect::<Vec<_>>();

            for serial in to_remove {
                if let Some(record) = events.remove(&serial) {
                    deleted += 1;
                    affected_projection_keys.insert(projection_key.clone());
                    raw_removals.push((
                        Self::channel_key(&record.app_id, &record.channel_id),
                        serial,
                    ));
                }
            }
        }

        for (channel_key, serial) in raw_removals {
            if let Some(raw) = state.raw_by_channel.get_mut(&channel_key) {
                raw.remove(&serial);
            }
        }

        state
            .events_by_projection
            .retain(|_, events| !events.is_empty());
        state.raw_by_channel.retain(|_, events| !events.is_empty());

        let affected_projection_keys = affected_projection_keys.into_iter().collect::<Vec<_>>();
        let requests = affected_projection_keys
            .iter()
            .filter_map(|key| {
                state
                    .events_by_projection
                    .get(key)
                    .and_then(|events| events.values().next())
                    .map(|record| AnnotationProjectionRequest {
                        app_id: record.app_id.clone(),
                        channel_id: record.channel_id.clone(),
                        message_serial: record.message_serial().clone(),
                        annotation_type: record.annotation_type().clone(),
                    })
            })
            .collect::<Vec<_>>();

        for request in requests {
            Self::rebuild_projection_from_state(
                &mut state,
                request,
                AnnotationProjectionOptions::default(),
            )?;
        }
        for key in affected_projection_keys {
            if !state.events_by_projection.contains_key(&key) {
                state.projections.remove(&key);
            }
        }

        if !has_more
            && state.events_by_projection.values().any(|events| {
                events
                    .values()
                    .any(|record| record.stored_at_ms < before_ms)
            })
        {
            has_more = true;
        }

        Ok((deleted, has_more))
    }
}

struct ParsedAnnotationType<'a> {
    namespace: &'a str,
    summarizer: AnnotationSummarizer,
    version: u16,
}

fn parse_annotation_type(value: &str) -> Result<ParsedAnnotationType<'_>> {
    validate_identifier("annotation type", value, MAX_ANNOTATION_TYPE_LENGTH)?;

    let (namespace, rest) = value.split_once(':').ok_or_else(|| {
        Error::InvalidMessageFormat(
            "annotation type must use namespace:summarizer.version".to_string(),
        )
    })?;
    validate_identifier(
        "annotation namespace",
        namespace,
        MAX_ANNOTATION_TYPE_LENGTH,
    )?;

    let (summarizer, version) = rest.rsplit_once(".v").ok_or_else(|| {
        Error::InvalidMessageFormat(
            "annotation type must use namespace:summarizer.version".to_string(),
        )
    })?;

    let summarizer = match summarizer {
        "total" => AnnotationSummarizer::Total,
        "flag" => AnnotationSummarizer::Flag,
        "distinct" => AnnotationSummarizer::Distinct,
        "unique" => AnnotationSummarizer::Unique,
        "multiple" => AnnotationSummarizer::Multiple,
        _ => {
            return Err(Error::InvalidMessageFormat(format!(
                "unsupported annotation summarizer: {summarizer}"
            )));
        }
    };

    let version = version.parse::<u16>().map_err(|_| {
        Error::InvalidMessageFormat(format!("invalid annotation summarizer version: {version}"))
    })?;
    if version != 1 {
        return Err(Error::InvalidMessageFormat(format!(
            "unsupported annotation summarizer version: {version}"
        )));
    }

    Ok(ParsedAnnotationType {
        namespace,
        summarizer,
        version,
    })
}

fn validate_identifier(label: &str, value: &str, max_len: usize) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::InvalidMessageFormat(format!(
            "{label} must not be empty"
        )));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(Error::InvalidMessageFormat(format!(
            "{label} must not contain whitespace"
        )));
    }
    if value.len() > max_len {
        return Err(Error::InvalidMessageFormat(format!(
            "{label} must be at most {max_len} bytes"
        )));
    }
    Ok(())
}

fn validate_required_text(label: &str, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(()),
        _ => Err(Error::InvalidMessageFormat(format!(
            "{label} is required for this annotation type"
        ))),
    }
}

fn validate_event_key(
    event: &Annotation,
    message_serial: &MessageSerial,
    annotation_type: &AnnotationType,
) -> Result<()> {
    event.validate()?;

    if &event.message_serial != message_serial {
        return Err(Error::InvalidMessageFormat(format!(
            "annotation event messageSerial {} does not match projection messageSerial {}",
            event.message_serial.as_str(),
            message_serial.as_str()
        )));
    }

    if &event.annotation_type != annotation_type {
        return Err(Error::InvalidMessageFormat(format!(
            "annotation event type {} does not match projection type {}",
            event.annotation_type.as_str(),
            annotation_type.as_str()
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::json;

    fn annotation_type(value: &str) -> AnnotationType {
        AnnotationType::new(value).unwrap()
    }

    fn serial(value: &str) -> AnnotationSerial {
        AnnotationSerial::new(value).unwrap()
    }

    fn event(
        serial_value: &str,
        annotation_type: &str,
        action: AnnotationAction,
        name: Option<&str>,
        client_id: Option<&str>,
        count: Option<u64>,
    ) -> Annotation {
        Annotation {
            id: AnnotationId::new(format!("ann:{serial_value}")).unwrap(),
            action,
            serial: serial(serial_value),
            message_serial: MessageSerial::new("msg:1").unwrap(),
            annotation_type: annotation_type.into(),
            name: name.map(str::to_string),
            client_id: client_id.map(str::to_string),
            count,
            data: Some(json!({"raw": serial_value})),
            encoding: None,
            timestamp: 1,
        }
    }

    impl From<&str> for AnnotationType {
        fn from(value: &str) -> Self {
            AnnotationType::new(value).unwrap()
        }
    }

    fn projection(annotation_type_value: &str, events: Vec<Annotation>) -> AnnotationProjection {
        AnnotationProjection::rebuild(
            "channel:1",
            MessageSerial::new("msg:1").unwrap(),
            annotation_type(annotation_type_value),
            events,
        )
        .unwrap()
    }

    fn projection_with_options(
        annotation_type_value: &str,
        events: Vec<Annotation>,
        options: AnnotationProjectionOptions,
    ) -> AnnotationProjection {
        AnnotationProjection::rebuild_with_options(
            "channel:1",
            MessageSerial::new("msg:1").unwrap(),
            annotation_type(annotation_type_value),
            events,
            options,
        )
        .unwrap()
    }

    fn stored_event(
        serial_value: &str,
        annotation_type: &str,
        action: AnnotationAction,
        name: Option<&str>,
        client_id: Option<&str>,
        count: Option<u64>,
        stored_at_ms: i64,
    ) -> StoredAnnotationEvent {
        StoredAnnotationEvent {
            app_id: "app".to_string(),
            channel_id: "chat".to_string(),
            annotation: event(
                serial_value,
                annotation_type,
                action,
                name,
                client_id,
                count,
            ),
            stored_at_ms,
        }
    }

    fn projection_request(annotation_type: &str) -> AnnotationProjectionRequest {
        AnnotationProjectionRequest {
            app_id: "app".to_string(),
            channel_id: "chat".to_string(),
            message_serial: MessageSerial::new("msg:1").unwrap(),
            annotation_type: annotation_type.into(),
        }
    }

    fn events_request(annotation_type: &str) -> AnnotationEventsRequest {
        AnnotationEventsRequest {
            app_id: "app".to_string(),
            channel_id: "chat".to_string(),
            message_serial: MessageSerial::new("msg:1").unwrap(),
            annotation_type: annotation_type.into(),
        }
    }

    #[test]
    fn annotation_type_parses_namespace_summarizer_and_version() {
        let ty = annotation_type("reaction:distinct.v1");

        assert_eq!(ty.namespace().unwrap(), "reaction");
        assert_eq!(ty.summarizer().unwrap(), AnnotationSummarizer::Distinct);
        assert_eq!(ty.version().unwrap(), 1);
    }

    #[test]
    fn annotation_type_rejects_unknown_summarizer() {
        let error = AnnotationType::new("reaction:unknown.v1").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported annotation summarizer"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_requires_name_for_named_summarizers() {
        let error = event(
            "ann:1",
            "reaction:distinct.v1",
            AnnotationAction::Create,
            None,
            Some("client-1"),
            None,
        )
        .validate()
        .unwrap_err();

        assert!(error.to_string().contains("annotation name is required"));
    }

    #[test]
    fn validation_requires_client_for_ownership_summarizers() {
        for (serial, annotation_type, name) in [
            ("ann:1", "reaction:flag.v1", None),
            ("ann:2", "reaction:distinct.v1", Some("like")),
            ("ann:3", "reaction:unique.v1", Some("like")),
        ] {
            let error = event(
                serial,
                annotation_type,
                AnnotationAction::Create,
                name,
                None,
                None,
            )
            .validate()
            .unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("annotation client_id is required"),
                "unexpected error for {annotation_type}: {error}"
            );
        }
    }

    #[test]
    fn total_counts_unidentified_and_repeated_client_events() {
        let projection = projection(
            "reaction:total.v1",
            vec![
                event(
                    "ann:1",
                    "reaction:total.v1",
                    AnnotationAction::Create,
                    None,
                    None,
                    None,
                ),
                event(
                    "ann:2",
                    "reaction:total.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:3",
                    "reaction:total.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:4",
                    "reaction:total.v1",
                    AnnotationAction::Delete,
                    None,
                    None,
                    None,
                ),
            ],
        );

        assert_eq!(
            projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 2 })
        );
    }

    #[test]
    fn flag_counts_each_identified_client_once() {
        let projection = projection(
            "reaction:flag.v1",
            vec![
                event(
                    "ann:1",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:2",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:3",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-2"),
                    None,
                ),
                event(
                    "ann:4",
                    "reaction:flag.v1",
                    AnnotationAction::Delete,
                    None,
                    Some("client-1"),
                    None,
                ),
            ],
        );

        assert_eq!(
            projection.summary,
            AnnotationSummary::Flag(IdentifiedAnnotationSummary {
                total: 1,
                client_ids: vec!["client-2".to_string()],
                clipped: false,
            })
        );
    }

    #[test]
    fn distinct_allows_one_client_in_multiple_names() {
        let projection = projection(
            "reaction:distinct.v1",
            vec![
                event(
                    "ann:1",
                    "reaction:distinct.v1",
                    AnnotationAction::Create,
                    Some("like"),
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:2",
                    "reaction:distinct.v1",
                    AnnotationAction::Create,
                    Some("laugh"),
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:3",
                    "reaction:distinct.v1",
                    AnnotationAction::Create,
                    Some("like"),
                    Some("client-1"),
                    None,
                ),
            ],
        );

        let AnnotationSummary::Distinct(names) = projection.summary else {
            panic!("expected distinct summary");
        };

        assert_eq!(names["like"].total, 1);
        assert_eq!(names["laugh"].total, 1);
    }

    #[test]
    fn unique_moves_client_between_names() {
        let projection = projection(
            "reaction:unique.v1",
            vec![
                event(
                    "ann:1",
                    "reaction:unique.v1",
                    AnnotationAction::Create,
                    Some("like"),
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:2",
                    "reaction:unique.v1",
                    AnnotationAction::Create,
                    Some("laugh"),
                    Some("client-1"),
                    None,
                ),
            ],
        );

        let AnnotationSummary::Unique(names) = projection.summary else {
            panic!("expected unique summary");
        };

        assert!(!names.contains_key("like"));
        assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
    }

    #[test]
    fn multiple_tracks_counts_and_removes_identified_client_contribution() {
        let projection = projection(
            "reaction:multiple.v1",
            vec![
                event(
                    "ann:1",
                    "reaction:multiple.v1",
                    AnnotationAction::Create,
                    Some("vote"),
                    Some("client-1"),
                    Some(2),
                ),
                event(
                    "ann:2",
                    "reaction:multiple.v1",
                    AnnotationAction::Create,
                    Some("vote"),
                    Some("client-1"),
                    Some(3),
                ),
                event(
                    "ann:3",
                    "reaction:multiple.v1",
                    AnnotationAction::Create,
                    Some("vote"),
                    None,
                    Some(4),
                ),
                event(
                    "ann:4",
                    "reaction:multiple.v1",
                    AnnotationAction::Delete,
                    Some("vote"),
                    Some("client-1"),
                    None,
                ),
            ],
        );

        let AnnotationSummary::Multiple(names) = projection.summary else {
            panic!("expected multiple summary");
        };

        let vote = &names["vote"];
        assert_eq!(vote.total, 4);
        assert_eq!(vote.total_unidentified, 4);
        assert_eq!(vote.total_client_ids, 0);
        assert!(vote.client_counts.is_empty());
    }

    #[test]
    fn rebuild_sorts_by_serial_and_ignores_duplicate_serials() {
        let mut duplicate = event(
            "ann:2",
            "reaction:unique.v1",
            AnnotationAction::Create,
            Some("sad"),
            Some("client-1"),
            None,
        );
        duplicate.id = AnnotationId::new("ann:duplicate").unwrap();

        let projection = projection(
            "reaction:unique.v1",
            vec![
                event(
                    "ann:2",
                    "reaction:unique.v1",
                    AnnotationAction::Create,
                    Some("laugh"),
                    Some("client-1"),
                    None,
                ),
                duplicate,
                event(
                    "ann:1",
                    "reaction:unique.v1",
                    AnnotationAction::Create,
                    Some("like"),
                    Some("client-1"),
                    None,
                ),
            ],
        );

        let AnnotationSummary::Unique(names) = projection.summary else {
            panic!("expected unique summary");
        };

        assert_eq!(projection.applied_events, 2);
        assert_eq!(projection.last_serial.as_ref().unwrap().as_str(), "ann:2");
        assert!(!names.contains_key("like"));
        assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
    }

    #[test]
    fn summary_serialization_excludes_raw_annotation_payloads() {
        let projection = projection(
            "reaction:total.v1",
            vec![event(
                "ann:1",
                "reaction:total.v1",
                AnnotationAction::Create,
                None,
                None,
                None,
            )],
        );

        let serialized = sonic_rs::to_string(&projection.summary).unwrap();

        assert_eq!(serialized, r#"{"total":1}"#);
        assert!(!serialized.contains("raw"));
    }

    #[test]
    fn client_id_clipping_reports_only_when_over_limit() {
        let exact = projection_with_options(
            "reaction:flag.v1",
            vec![
                event(
                    "ann:1",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:2",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-2"),
                    None,
                ),
            ],
            AnnotationProjectionOptions {
                client_id_limit: Some(2),
            },
        );
        let AnnotationSummary::Flag(exact) = exact.summary else {
            panic!("expected flag summary");
        };
        assert!(!exact.clipped);
        assert_eq!(exact.client_ids.len(), 2);

        let clipped = projection_with_options(
            "reaction:flag.v1",
            vec![
                event(
                    "ann:1",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-1"),
                    None,
                ),
                event(
                    "ann:2",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-2"),
                    None,
                ),
                event(
                    "ann:3",
                    "reaction:flag.v1",
                    AnnotationAction::Create,
                    None,
                    Some("client-3"),
                    None,
                ),
            ],
            AnnotationProjectionOptions {
                client_id_limit: Some(2),
            },
        );
        let AnnotationSummary::Flag(clipped) = clipped.summary else {
            panic!("expected flag summary");
        };
        assert!(clipped.clipped);
        assert_eq!(clipped.total, 3);
        assert_eq!(clipped.client_ids.len(), 2);
    }

    #[tokio::test]
    async fn memory_store_appends_events_and_materializes_projection() {
        let store = MemoryAnnotationStore::new();

        let projection = store
            .append_event(stored_event(
                "ann:1",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
                1,
            ))
            .await
            .unwrap();
        store
            .append_event(stored_event(
                "ann:2",
                "reaction:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-2"),
                None,
                2,
            ))
            .await
            .unwrap();

        assert_eq!(projection.last_annotation_serial.unwrap().as_str(), "ann:1");

        let projection = store
            .get_projection(projection_request("reaction:flag.v1"))
            .await
            .unwrap()
            .unwrap();
        let AnnotationSummary::Flag(summary) = projection.summary else {
            panic!("expected flag summary");
        };
        assert_eq!(summary.total, 2);
        assert_eq!(
            summary.client_ids,
            vec!["client-1".to_string(), "client-2".to_string()]
        );
    }

    #[tokio::test]
    async fn memory_store_deduplicates_by_annotation_serial() {
        let store = MemoryAnnotationStore::new();

        store
            .append_event(stored_event(
                "ann:1",
                "reaction:total.v1",
                AnnotationAction::Create,
                None,
                None,
                None,
                1,
            ))
            .await
            .unwrap();
        store
            .append_event(stored_event(
                "ann:1",
                "reaction:total.v1",
                AnnotationAction::Create,
                None,
                None,
                None,
                2,
            ))
            .await
            .unwrap();

        let events = store
            .get_events(events_request("reaction:total.v1"))
            .await
            .unwrap();
        let projection = store
            .rebuild_projection(projection_request("reaction:total.v1"))
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(
            projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 1 })
        );
    }

    #[tokio::test]
    async fn memory_store_replays_raw_channel_events_by_annotation_serial() {
        let store = MemoryAnnotationStore::new();

        for (serial, annotation_type, name) in [
            ("ann:1", "reaction:total.v1", None),
            ("ann:2", "reaction:distinct.v1", Some("like")),
            ("ann:3", "reaction:total.v1", None),
        ] {
            store
                .append_event(stored_event(
                    serial,
                    annotation_type,
                    AnnotationAction::Create,
                    name,
                    Some("client-1"),
                    None,
                    1,
                ))
                .await
                .unwrap();
        }

        let replay = store
            .replay_raw(RawAnnotationReplayRequest {
                app_id: "app".to_string(),
                channel_id: "chat".to_string(),
                after_annotation_serial: Some(AnnotationSerial::new("ann:1").unwrap()),
                limit: 10,
            })
            .await
            .unwrap();

        assert_eq!(
            replay
                .iter()
                .map(|record| record.annotation_serial().as_str())
                .collect::<Vec<_>>(),
            vec!["ann:2", "ann:3"]
        );
    }

    #[tokio::test]
    async fn memory_store_get_projection_repairs_stale_watermark() {
        let store = MemoryAnnotationStore::new();

        store
            .append_event(stored_event(
                "ann:1",
                "reaction:total.v1",
                AnnotationAction::Create,
                None,
                None,
                None,
                1,
            ))
            .await
            .unwrap();

        let late_event = stored_event(
            "ann:2",
            "reaction:total.v1",
            AnnotationAction::Create,
            None,
            None,
            None,
            2,
        );
        let projection_key = MemoryAnnotationStore::event_projection_key(&late_event);
        let channel_key =
            MemoryAnnotationStore::channel_key(&late_event.app_id, &late_event.channel_id);
        {
            let mut state = store.state.write().await;
            state
                .events_by_projection
                .entry(projection_key)
                .or_default()
                .insert(late_event.annotation_serial().clone(), late_event.clone());
            state
                .raw_by_channel
                .entry(channel_key)
                .or_default()
                .insert(late_event.annotation_serial().clone(), late_event);
        }

        let projection = store
            .get_projection(projection_request("reaction:total.v1"))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(projection.last_annotation_serial.unwrap().as_str(), "ann:2");
        assert_eq!(
            projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 2 })
        );
    }

    #[tokio::test]
    async fn memory_store_list_projections_rebuilds_cold_projection_cache() {
        let store = MemoryAnnotationStore::new();
        let event = stored_event(
            "ann:1",
            "reaction:total.v1",
            AnnotationAction::Create,
            None,
            None,
            None,
            1,
        );
        let projection_key = MemoryAnnotationStore::event_projection_key(&event);
        let channel_key = MemoryAnnotationStore::channel_key(&event.app_id, &event.channel_id);
        {
            let mut state = store.state.write().await;
            state
                .events_by_projection
                .entry(projection_key)
                .or_default()
                .insert(event.annotation_serial().clone(), event.clone());
            state
                .raw_by_channel
                .entry(channel_key)
                .or_default()
                .insert(event.annotation_serial().clone(), event);
            state.projections.clear();
        }

        let projections = store
            .list_projections_for_channel(AnnotationProjectionsForChannelRequest {
                app_id: "app".to_string(),
                channel_id: "chat".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(projections.len(), 1);
        assert_eq!(
            projections[0].summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 1 })
        );
    }

    #[tokio::test]
    async fn memory_store_reports_cold_projection_cache_rebuild_count() {
        let store = MemoryAnnotationStore::new();
        let event = stored_event(
            "ann:1",
            "reaction:total.v1",
            AnnotationAction::Create,
            None,
            None,
            None,
            1,
        );
        let projection_key = MemoryAnnotationStore::event_projection_key(&event);
        let channel_key = MemoryAnnotationStore::channel_key(&event.app_id, &event.channel_id);
        {
            let mut state = store.state.write().await;
            state
                .events_by_projection
                .entry(projection_key)
                .or_default()
                .insert(event.annotation_serial().clone(), event.clone());
            state
                .raw_by_channel
                .entry(channel_key)
                .or_default()
                .insert(event.annotation_serial().clone(), event);
            state.projections.clear();
        }

        let (projections, rebuild_count) = store
            .list_projections_for_channel_with_rebuild_count(
                AnnotationProjectionsForChannelRequest {
                    app_id: "app".to_string(),
                    channel_id: "chat".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(projections.len(), 1);
        assert_eq!(rebuild_count, 1);
    }

    #[tokio::test]
    async fn memory_store_converges_after_out_of_order_unique_events() {
        let store = MemoryAnnotationStore::new();

        store
            .append_event(stored_event(
                "ann:2",
                "reaction:unique.v1",
                AnnotationAction::Create,
                Some("laugh"),
                Some("client-1"),
                None,
                2,
            ))
            .await
            .unwrap();
        let projection = store
            .append_event(stored_event(
                "ann:1",
                "reaction:unique.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
                1,
            ))
            .await
            .unwrap();

        let AnnotationSummary::Unique(names) = projection.summary else {
            panic!("expected unique summary");
        };
        assert!(!names.contains_key("like"));
        assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
        assert_eq!(projection.last_annotation_serial.unwrap().as_str(), "ann:2");
    }

    #[tokio::test]
    async fn memory_store_converges_under_concurrent_unique_churn() {
        let store = MemoryAnnotationStore::new();

        let earlier = store.append_event(stored_event(
            "ann:1",
            "reaction:unique.v1",
            AnnotationAction::Create,
            Some("like"),
            Some("client-1"),
            None,
            1,
        ));
        let later = store.append_event(stored_event(
            "ann:2",
            "reaction:unique.v1",
            AnnotationAction::Create,
            Some("laugh"),
            Some("client-1"),
            None,
            2,
        ));

        let (earlier_result, later_result) = tokio::join!(earlier, later);
        earlier_result.unwrap();
        later_result.unwrap();

        let projection = store
            .get_projection(projection_request("reaction:unique.v1"))
            .await
            .unwrap()
            .unwrap();
        let AnnotationSummary::Unique(names) = projection.summary else {
            panic!("expected unique summary");
        };
        assert!(!names.contains_key("like"));
        assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
        assert_eq!(projection.last_annotation_serial.unwrap().as_str(), "ann:2");
    }

    #[tokio::test]
    async fn memory_store_purge_removes_events_and_stale_projection() {
        let store = MemoryAnnotationStore::new();

        store
            .append_event(stored_event(
                "ann:1",
                "reaction:total.v1",
                AnnotationAction::Create,
                None,
                None,
                None,
                1,
            ))
            .await
            .unwrap();

        let (deleted, has_more) = store.purge_before(2, 10).await.unwrap();

        assert_eq!(deleted, 1);
        assert!(!has_more);
        assert!(
            store
                .get_projection(projection_request("reaction:total.v1"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .replay_raw(RawAnnotationReplayRequest {
                    app_id: "app".to_string(),
                    channel_id: "chat".to_string(),
                    after_annotation_serial: None,
                    limit: 10,
                })
                .await
                .unwrap()
                .is_empty()
        );
    }
}
