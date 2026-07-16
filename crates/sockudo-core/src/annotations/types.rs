use crate::error::{Error, Result};
use crate::history::now_ms;
use crate::versioned_messages::{MAX_VERSIONED_SERIAL_LENGTH, MessageSerial};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::{BTreeMap, HashSet};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TotalAnnotationSummary {
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IdentifiedAnnotationSummary {
    pub total: u64,
    pub client_ids: Vec<String>,
    pub clipped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MultipleAnnotationSummary {
    pub total: u64,
    pub client_counts: BTreeMap<String, u64>,
    pub total_unidentified: u64,
    pub clipped: bool,
    pub total_client_ids: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

        let create_ids = sorted_events
            .iter()
            .filter(|event| event.action == AnnotationAction::Create)
            .map(|event| event.id.clone())
            .collect::<HashSet<_>>();
        let mut seen_serials = HashSet::new();
        let mut seen_create_ids = HashSet::new();
        let mut deleted_create_ids = HashSet::new();
        let mut last_serial = None;
        let mut applied_events = 0;

        for event in sorted_events {
            validate_event_key(&event, &message_serial, &annotation_type)?;
            if !seen_serials.insert(event.serial.as_str().to_string()) {
                continue;
            }

            last_serial = Some(event.serial.clone());
            match event.action {
                AnnotationAction::Create => {
                    if deleted_create_ids.contains(&event.id) {
                        continue;
                    }
                    seen_create_ids.insert(event.id.clone());
                }
                AnnotationAction::Delete if create_ids.contains(&event.id) => {
                    deleted_create_ids.insert(event.id.clone());
                    if !seen_create_ids.contains(&event.id) {
                        continue;
                    }
                }
                AnnotationAction::Delete => {}
            }
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
    /// Restrict replay to one message before applying `limit`.
    ///
    /// `None` retains channel-wide replay for reconnect consumers.
    pub message_serial: Option<MessageSerial>,
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
    pub(super) fn from_projection(app_id: String, projection: AnnotationProjection) -> Self {
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

#[derive(Debug, Clone)]
pub struct AnnotationAppendOutcome {
    pub projection: StoredAnnotationProjection,
    pub canonical_serial: AnnotationSerial,
    pub inserted: bool,
}

#[async_trait]
pub trait AnnotationStore: Send + Sync {
    /// Reserve the next durable per-channel annotation serial.
    ///
    /// Shared stores override this with an atomic backend allocation. The
    /// `None` fallback preserves local-store compatibility, where the handler
    /// generates a process-qualified serial.
    async fn reserve_annotation_serial(
        &self,
        app_id: &str,
        channel_id: &str,
    ) -> Result<Option<AnnotationSerial>> {
        let _ = (app_id, channel_id);
        Ok(None)
    }

    async fn append_event(
        &self,
        record: StoredAnnotationEvent,
    ) -> Result<StoredAnnotationProjection>;

    /// Atomically suppress a repeated create carrying the same annotation ID.
    ///
    /// Stores with shared or transactional state should override this method.
    /// The fallback preserves existing store compatibility but cannot close a
    /// race between concurrent writers.
    async fn append_create_idempotent(
        &self,
        record: StoredAnnotationEvent,
    ) -> Result<AnnotationAppendOutcome> {
        let canonical_serial = record.annotation.serial.clone();
        let projection = self.append_event(record).await?;
        Ok(AnnotationAppendOutcome {
            projection,
            canonical_serial,
            inserted: true,
        })
    }

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
