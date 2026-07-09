use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::domain::{
    DeadLetter, DeliveryBatch, DeliveryFeedback, DeliveryResult, FanoutConfig, FanoutRegime,
    PublishCounters, PublishIntent, PublishLifecycleState, PublishLogEvent, PublishStatus,
    PushCursor, PushCursorKind, PushDomainError, PushProviderKind, RetryScheduleEntry,
    provider_key, stable_hash,
};
use crate::meta::{PushMetaEvent, emit_push_meta_event};
use crate::metrics::PushMetrics;
use crate::storage::{DynPushStore, IdempotencyRecord, Page, PushStorageError};
use crate::transform::{render_all_provider_payloads, resolve_template_payload};

pub type DynPushQueue = Arc<dyn PushQueue + Send + Sync>;
pub type PushQueueResult<T> = Result<T, PushQueueError>;
pub type PushPipelineResult<T> = Result<T, PushPipelineError>;
const DEFAULT_IDEMPOTENCY_TTL_MS: u64 = 24 * 60 * 60 * 1000;
const PUBLISH_STATUS_LOCK_SHARDS: usize = 256;

static PUBLISH_STATUS_LOCKS: OnceLock<Box<[tokio::sync::Mutex<()>]>> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PushQueueStage {
    PublishLog,
    ShardJobs,
    DeliveryJobs(crate::domain::PushProviderKind),
    DeliveryResults,
    DeadLetters,
    RetrySchedule,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum PushQueuePayload {
    PublishLog(Box<PublishLogEvent>),
    ShardJob(Box<crate::domain::ShardJob>),
    DeliveryBatch(Box<DeliveryBatch>),
    DeliveryResult(Box<DeliveryResult>),
    DeliveryFeedback(Box<DeliveryFeedback>),
    DeadLetter(Box<DeadLetter>),
    RetrySchedule(Box<RetryScheduleEntry>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueAckToken {
    pub stage: PushQueueStage,
    pub message_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueMessage {
    pub message_id: String,
    pub stage: PushQueueStage,
    pub key: String,
    pub partition_key: String,
    pub partition: u32,
    pub payload: PushQueuePayload,
    pub attempt: u32,
    #[serde(default = "now_ms")]
    pub enqueued_at_ms: u64,
    pub not_before_ms: Option<u64>,
    pub lease_deadline_ms: u64,
    pub ack: QueueAckToken,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueLagMetrics {
    pub ready_depth: u64,
    pub delayed_depth: u64,
    pub inflight_depth: u64,
    pub dead_letter_depth: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_ready_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_delayed_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_inflight_age_ms: Option<u64>,
}

impl QueueLagMetrics {
    pub fn oldest_actionable_age_ms(&self) -> Option<u64> {
        max_optional_u64(self.oldest_ready_age_ms, self.oldest_inflight_age_ms)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueHealth {
    pub backend: PushQueueBackendKind,
    pub healthy: bool,
    pub details: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueRoute {
    pub logical_topic: String,
    pub key: String,
    pub partition_key: String,
    pub partition: u32,
    pub partitions: u32,
}

impl QueueRoute {
    pub fn for_message(stage: PushQueueStage, key: &str, payload: &PushQueuePayload) -> Self {
        let partitions = default_partitions(stage);
        let partition_key = partition_key_for_payload(stage, payload);
        let partition = partition_for_key(&partition_key, partitions);
        Self {
            logical_topic: stage.logical_topic(),
            key: key.to_owned(),
            partition_key,
            partition,
            partitions,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadLetterQueueEntry {
    pub dead_letter_id: String,
    pub dead_letter: DeadLetter,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<PushProviderKind>,
    pub replayable: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeadLetterQueueFilter {
    pub provider: Option<PushProviderKind>,
    pub since_ms: Option<u64>,
    pub until_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadLetterReplayResult {
    pub dead_letter_id: String,
    pub replayed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

pub fn dead_letter_entry_id(dead_letter: &DeadLetter, source_message_id: &str) -> String {
    stable_hash(
        format!(
            "{}\0{}\0{}\0{}\0{}\0{}\0{}",
            dead_letter.app_id,
            dead_letter.publish_id,
            dead_letter.stage,
            dead_letter.key,
            dead_letter.reason,
            dead_letter.occurred_at_ms,
            source_message_id
        )
        .as_bytes(),
    )
}

pub fn dead_letter_entry_position(entry: &DeadLetterQueueEntry) -> String {
    format!(
        "{:020}:{}",
        entry.dead_letter.occurred_at_ms, entry.dead_letter_id
    )
}

pub fn page_dead_letter_entries(
    app_id: &str,
    filter: DeadLetterQueueFilter,
    limit: usize,
    cursor: Option<PushCursor>,
    mut entries: Vec<DeadLetterQueueEntry>,
) -> PushQueueResult<Page<DeadLetterQueueEntry>> {
    let position = if let Some(cursor) = cursor {
        if cursor.app_id != app_id {
            return Err(PushQueueError::Backend(format!(
                "dead-letter cursor belongs to app {}, expected {}",
                cursor.app_id, app_id
            )));
        }
        if cursor.kind != PushCursorKind::DeadLetter {
            return Err(PushQueueError::Backend(
                "cursor is not a dead-letter cursor".to_owned(),
            ));
        }
        Some(cursor.position)
    } else {
        None
    };

    entries.retain(|entry| {
        entry.dead_letter.app_id == app_id
            && filter
                .provider
                .is_none_or(|provider| entry.provider == Some(provider))
            && filter
                .since_ms
                .is_none_or(|since_ms| entry.dead_letter.occurred_at_ms >= since_ms)
            && filter
                .until_ms
                .is_none_or(|until_ms| entry.dead_letter.occurred_at_ms <= until_ms)
            && position.as_deref().is_none_or(|cursor_position| {
                dead_letter_entry_position(entry).as_str() > cursor_position
            })
    });
    entries.sort_by_key(dead_letter_entry_position);

    let limit = limit.max(1);
    let mut page_items = entries
        .into_iter()
        .take(limit.saturating_add(1))
        .collect::<Vec<_>>();
    let next_cursor = if page_items.len() > limit {
        page_items.truncate(limit);
        page_items.last().map(|entry| PushCursor {
            app_id: app_id.to_owned(),
            kind: PushCursorKind::DeadLetter,
            position: dead_letter_entry_position(entry),
            issued_at_ms: now_ms(),
        })
    } else {
        None
    };

    Ok(Page {
        items: page_items,
        next_cursor,
    })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PushQueueBackendKind {
    #[default]
    Memory,
    Redis,
    RedisCluster,
    Nats,
    Pulsar,
    RabbitMq,
    GooglePubsub,
    Kafka,
    Iggy,
    Sqs,
    Sns,
}

impl PushQueueBackendKind {
    pub fn startup_check(self) -> PushQueueResult<()> {
        match self {
            Self::Memory => Ok(()),
            Self::Sns => Err(PushQueueError::UnsupportedBackend {
                backend: "sns",
                reason: "SNS is publish fanout only and must be paired with a worker queue",
            }),
            #[cfg(feature = "redis")]
            Self::Redis => Ok(()),
            #[cfg(feature = "redis-cluster")]
            Self::RedisCluster => Ok(()),
            #[cfg(feature = "nats")]
            Self::Nats => Ok(()),
            #[cfg(feature = "pulsar")]
            Self::Pulsar => Ok(()),
            #[cfg(feature = "rabbitmq")]
            Self::RabbitMq => Ok(()),
            #[cfg(feature = "google-pubsub")]
            Self::GooglePubsub => Ok(()),
            #[cfg(feature = "kafka")]
            Self::Kafka => Ok(()),
            #[cfg(feature = "iggy")]
            Self::Iggy => Ok(()),
            #[cfg(feature = "sqs")]
            Self::Sqs => Ok(()),
            #[cfg(not(feature = "redis"))]
            Self::Redis => queue_feature_disabled("redis", "redis"),
            #[cfg(not(feature = "redis-cluster"))]
            Self::RedisCluster => queue_feature_disabled("redis-cluster", "redis-cluster"),
            #[cfg(not(feature = "nats"))]
            Self::Nats => queue_feature_disabled("nats", "nats"),
            #[cfg(not(feature = "pulsar"))]
            Self::Pulsar => queue_feature_disabled("pulsar", "pulsar"),
            #[cfg(not(feature = "rabbitmq"))]
            Self::RabbitMq => queue_feature_disabled("rabbitmq", "rabbitmq"),
            #[cfg(not(feature = "google-pubsub"))]
            Self::GooglePubsub => queue_feature_disabled("google-pubsub", "google-pubsub"),
            #[cfg(not(feature = "kafka"))]
            Self::Kafka => queue_feature_disabled("kafka", "kafka"),
            #[cfg(not(feature = "iggy"))]
            Self::Iggy => queue_feature_disabled("iggy", "iggy"),
            #[cfg(not(feature = "sqs"))]
            Self::Sqs => queue_feature_disabled("sqs", "sqs"),
        }
    }
}

impl PushQueueStage {
    pub fn logical_topic(self) -> String {
        match self {
            Self::PublishLog => "push.publish.v1".to_owned(),
            Self::ShardJobs => "push.shards.v1".to_owned(),
            Self::DeliveryJobs(provider) => {
                format!("push.delivery.{}.v1", provider_key(provider))
            }
            Self::DeliveryResults => "push.results.v1".to_owned(),
            Self::DeadLetters => "push.deadletters.v1".to_owned(),
            Self::RetrySchedule => "push.retry.v1".to_owned(),
        }
    }
}

#[allow(dead_code)]
fn queue_feature_disabled(backend: &'static str, feature: &'static str) -> PushQueueResult<()> {
    Err(PushQueueError::FeatureDisabled { backend, feature })
}

fn default_partitions(stage: PushQueueStage) -> u32 {
    match stage {
        PushQueueStage::PublishLog => 64,
        PushQueueStage::ShardJobs => 128,
        PushQueueStage::DeliveryJobs(_) => 256,
        PushQueueStage::DeliveryResults => 128,
        PushQueueStage::DeadLetters => 32,
        PushQueueStage::RetrySchedule => 64,
    }
}

fn partition_key_for_payload(stage: PushQueueStage, payload: &PushQueuePayload) -> String {
    match (stage, payload) {
        (PushQueueStage::PublishLog, PushQueuePayload::PublishLog(event)) => {
            format!("{}:{}", event.app_id, event.publish_id)
        }
        (PushQueueStage::DeliveryJobs(_), PushQueuePayload::DeliveryBatch(batch)) => {
            batch.app_id.clone()
        }
        (PushQueueStage::ShardJobs, PushQueuePayload::ShardJob(job)) => job.app_id.clone(),
        (PushQueueStage::DeliveryResults, PushQueuePayload::DeliveryResult(result)) => {
            result.app_id.clone()
        }
        (PushQueueStage::DeliveryResults, PushQueuePayload::DeliveryFeedback(feedback)) => {
            feedback.result.app_id.clone()
        }
        (PushQueueStage::RetrySchedule, PushQueuePayload::RetrySchedule(entry)) => {
            entry.app_id.clone()
        }
        (PushQueueStage::DeadLetters, PushQueuePayload::DeadLetter(dead_letter)) => {
            dead_letter.app_id.clone()
        }
        _ => payload_app_id(payload).unwrap_or_else(|| "unknown".to_owned()),
    }
}

fn payload_app_id(payload: &PushQueuePayload) -> Option<String> {
    match payload {
        PushQueuePayload::PublishLog(event) => Some(event.app_id.clone()),
        PushQueuePayload::ShardJob(job) => Some(job.app_id.clone()),
        PushQueuePayload::DeliveryBatch(batch) => Some(batch.app_id.clone()),
        PushQueuePayload::DeliveryResult(result) => Some(result.app_id.clone()),
        PushQueuePayload::DeliveryFeedback(feedback) => Some(feedback.result.app_id.clone()),
        PushQueuePayload::DeadLetter(dead_letter) => Some(dead_letter.app_id.clone()),
        PushQueuePayload::RetrySchedule(entry) => Some(entry.app_id.clone()),
    }
}

fn payload_provider(payload: &PushQueuePayload) -> Option<PushProviderKind> {
    match payload {
        PushQueuePayload::DeliveryBatch(batch) => Some(batch.provider),
        PushQueuePayload::DeliveryResult(result) => Some(result.provider),
        PushQueuePayload::DeliveryFeedback(feedback) => Some(feedback.result.provider),
        PushQueuePayload::RetrySchedule(entry) => entry.provider,
        PushQueuePayload::PublishLog(_)
        | PushQueuePayload::ShardJob(_)
        | PushQueuePayload::DeadLetter(_) => None,
    }
}

fn partition_for_key(key: &str, partitions: u32) -> u32 {
    let hash = stable_hash(key.as_bytes());
    let prefix = hash
        .get(..16)
        .expect("stable_hash always returns at least 16 hex characters");
    let value = u64::from_str_radix(prefix, 16)
        .expect("stable_hash prefix always contains valid hex characters");
    (value % u64::from(partitions.max(1))) as u32
}

#[derive(Debug, Error)]
pub enum PushQueueError {
    #[error("push queue backend {backend} is not implemented yet: {reason}")]
    BackendDeferred {
        backend: &'static str,
        reason: &'static str,
    },
    #[error("push queue backend {backend} is not compiled in; enable feature `{feature}`")]
    FeatureDisabled {
        backend: &'static str,
        feature: &'static str,
    },
    #[error("push queue backend {backend} is not supported for this stage: {reason}")]
    UnsupportedBackend {
        backend: &'static str,
        reason: &'static str,
    },
    #[error("push queue error: {0}")]
    Backend(String),
}

#[async_trait]
pub trait PushQueue: Send + Sync {
    fn backend(&self) -> PushQueueBackendKind;

    async fn produce(
        &self,
        stage: PushQueueStage,
        key: String,
        payload: PushQueuePayload,
    ) -> PushQueueResult<String>;

    async fn retry_at(
        &self,
        stage: PushQueueStage,
        key: String,
        payload: PushQueuePayload,
        not_before_ms: u64,
    ) -> PushQueueResult<String>;

    async fn consume(
        &self,
        stage: PushQueueStage,
        consumer_group: &str,
        max_messages: usize,
        lease_timeout_ms: u64,
    ) -> PushQueueResult<Vec<QueueMessage>>;

    async fn ack(&self, token: QueueAckToken) -> PushQueueResult<()>;

    async fn nack(&self, token: QueueAckToken, retry_at_ms: Option<u64>) -> PushQueueResult<()>;

    async fn dead_letter(&self, token: QueueAckToken, reason: String) -> PushQueueResult<()>;

    async fn list_dead_letters(
        &self,
        _app_id: &str,
        _filter: DeadLetterQueueFilter,
        _limit: usize,
        _cursor: Option<PushCursor>,
    ) -> PushQueueResult<Page<DeadLetterQueueEntry>> {
        Ok(Page {
            items: Vec::new(),
            next_cursor: None,
        })
    }

    async fn replay_dead_letter(
        &self,
        _app_id: &str,
        dead_letter_id: &str,
    ) -> PushQueueResult<Option<DeadLetterReplayResult>> {
        Ok(Some(DeadLetterReplayResult {
            dead_letter_id: dead_letter_id.to_owned(),
            replayed: false,
            message_id: None,
            reason: Some("dead-letter replay is not supported by this queue backend".to_owned()),
        }))
    }

    async fn health(&self) -> PushQueueResult<QueueHealth>;

    async fn lag(&self, stage: PushQueueStage) -> PushQueueResult<QueueLagMetrics>;

    fn route_for(
        &self,
        stage: PushQueueStage,
        key: &str,
        payload: &PushQueuePayload,
    ) -> QueueRoute {
        QueueRoute::for_message(stage, key, payload)
    }
}

#[derive(Clone, Default)]
pub struct MemoryPushQueue {
    inner: Arc<Mutex<MemoryQueueState>>,
    backend: PushQueueBackendKind,
}

#[derive(Default)]
struct MemoryQueueState {
    next_id: u64,
    ready: BTreeMap<PushQueueStage, VecDeque<StoredMessage>>,
    inflight: BTreeMap<(PushQueueStage, String), StoredMessage>,
    produced_keys: BTreeMap<(PushQueueStage, String), String>,
    dead_letters: Vec<StoredDeadLetter>,
}

#[derive(Clone)]
struct StoredMessage {
    message_id: String,
    key: String,
    route: QueueRoute,
    payload: PushQueuePayload,
    attempt: u32,
    enqueued_at_ms: u64,
    not_before_ms: Option<u64>,
    lease_deadline_ms: u64,
}

#[derive(Clone)]
struct StoredDeadLetter {
    dead_letter_id: String,
    dead_letter: DeadLetter,
    provider: Option<PushProviderKind>,
    original_stage: Option<PushQueueStage>,
    original_key: Option<String>,
    original_payload: Option<PushQueuePayload>,
}

impl StoredDeadLetter {
    fn entry(&self) -> DeadLetterQueueEntry {
        DeadLetterQueueEntry {
            dead_letter_id: self.dead_letter_id.clone(),
            dead_letter: self.dead_letter.clone(),
            provider: self.provider,
            replayable: self.original_stage.is_some()
                && self.original_key.is_some()
                && self.original_payload.is_some(),
        }
    }
}

impl MemoryPushQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn for_backend(backend: PushQueueBackendKind) -> PushQueueResult<Self> {
        backend.startup_check()?;
        if matches!(backend, PushQueueBackendKind::Sns) {
            return Err(PushQueueError::UnsupportedBackend {
                backend: "sns",
                reason: "SNS is publish fanout only and must be paired with a worker queue",
            });
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(MemoryQueueState::default())),
            backend,
        })
    }
}

#[async_trait]
impl PushQueue for MemoryPushQueue {
    fn backend(&self) -> PushQueueBackendKind {
        self.backend
    }

    async fn produce(
        &self,
        stage: PushQueueStage,
        key: String,
        payload: PushQueuePayload,
    ) -> PushQueueResult<String> {
        self.enqueue(stage, key, payload, None)
    }

    async fn retry_at(
        &self,
        stage: PushQueueStage,
        key: String,
        payload: PushQueuePayload,
        not_before_ms: u64,
    ) -> PushQueueResult<String> {
        self.enqueue(stage, key, payload, Some(not_before_ms))
    }

    async fn consume(
        &self,
        stage: PushQueueStage,
        _consumer_group: &str,
        max_messages: usize,
        lease_timeout_ms: u64,
    ) -> PushQueueResult<Vec<QueueMessage>> {
        let now = now_ms();
        let mut inner = self.lock()?;
        reclaim_expired_leases(&mut inner, now);
        let mut queue = inner.ready.remove(&stage).unwrap_or_default();
        let mut deferred = VecDeque::new();
        let mut out = Vec::new();

        while let Some(mut stored) = queue.pop_front() {
            if out.len() >= max_messages {
                deferred.push_back(stored);
                continue;
            }
            if stored
                .not_before_ms
                .is_some_and(|not_before| not_before > now)
            {
                deferred.push_back(stored);
                continue;
            }

            stored.lease_deadline_ms = now.saturating_add(lease_timeout_ms);
            let token = QueueAckToken {
                stage,
                message_id: stored.message_id.clone(),
            };
            let message = QueueMessage {
                message_id: stored.message_id.clone(),
                stage,
                key: stored.key.clone(),
                partition_key: stored.route.partition_key.clone(),
                partition: stored.route.partition,
                payload: stored.payload.clone(),
                attempt: stored.attempt,
                enqueued_at_ms: stored.enqueued_at_ms,
                not_before_ms: stored.not_before_ms,
                lease_deadline_ms: stored.lease_deadline_ms,
                ack: token.clone(),
            };
            inner
                .inflight
                .insert((stage, stored.message_id.clone()), stored);
            out.push(message);
        }

        inner.ready.insert(stage, deferred);
        Ok(out)
    }

    async fn ack(&self, token: QueueAckToken) -> PushQueueResult<()> {
        let mut inner = self.lock()?;
        if let Some(stored) = inner.inflight.remove(&(token.stage, token.message_id)) {
            inner.produced_keys.remove(&(token.stage, stored.key));
        }
        Ok(())
    }

    async fn nack(&self, token: QueueAckToken, retry_at_ms: Option<u64>) -> PushQueueResult<()> {
        let mut inner = self.lock()?;
        if let Some(mut stored) = inner.inflight.remove(&(token.stage, token.message_id)) {
            stored.attempt = stored.attempt.saturating_add(1);
            stored.not_before_ms = retry_at_ms;
            stored.lease_deadline_ms = 0;
            inner
                .ready
                .entry(token.stage)
                .or_default()
                .push_back(stored);
        }
        Ok(())
    }

    async fn dead_letter(&self, token: QueueAckToken, reason: String) -> PushQueueResult<()> {
        let mut inner = self.lock()?;
        if let Some(stored) = inner.inflight.remove(&(token.stage, token.message_id)) {
            inner
                .produced_keys
                .remove(&(token.stage, stored.key.clone()));
            let dead_letter = dead_letter_from_message(token.stage, &stored, reason);
            inner.dead_letters.push(StoredDeadLetter {
                dead_letter_id: dead_letter_entry_id(&dead_letter, &stored.message_id),
                dead_letter: dead_letter.clone(),
                provider: payload_provider(&stored.payload),
                original_stage: Some(token.stage),
                original_key: Some(stored.key.clone()),
                original_payload: Some(stored.payload.clone()),
            });
            inner
                .ready
                .entry(PushQueueStage::DeadLetters)
                .or_default()
                .push_back(StoredMessage {
                    message_id: format!("dlq-{}", stored.message_id),
                    key: dead_letter.key.clone(),
                    route: QueueRoute::for_message(
                        PushQueueStage::DeadLetters,
                        &dead_letter.key,
                        &PushQueuePayload::DeadLetter(Box::new(dead_letter.clone())),
                    ),
                    payload: PushQueuePayload::DeadLetter(Box::new(dead_letter)),
                    attempt: 1,
                    enqueued_at_ms: now_ms(),
                    not_before_ms: None,
                    lease_deadline_ms: 0,
                });
        }
        Ok(())
    }

    async fn list_dead_letters(
        &self,
        app_id: &str,
        filter: DeadLetterQueueFilter,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushQueueResult<Page<DeadLetterQueueEntry>> {
        let entries = self
            .lock()?
            .dead_letters
            .iter()
            .map(StoredDeadLetter::entry)
            .collect::<Vec<_>>();
        page_dead_letter_entries(app_id, filter, limit, cursor, entries)
    }

    async fn replay_dead_letter(
        &self,
        app_id: &str,
        dead_letter_id: &str,
    ) -> PushQueueResult<Option<DeadLetterReplayResult>> {
        let replay = {
            let inner = self.lock()?;
            let Some(stored) = inner.dead_letters.iter().find(|stored| {
                stored.dead_letter.app_id == app_id && stored.dead_letter_id == dead_letter_id
            }) else {
                return Ok(None);
            };
            let (Some(stage), Some(key), Some(payload)) = (
                stored.original_stage,
                stored.original_key.clone(),
                stored.original_payload.clone(),
            ) else {
                return Ok(Some(DeadLetterReplayResult {
                    dead_letter_id: dead_letter_id.to_owned(),
                    replayed: false,
                    message_id: None,
                    reason: Some(
                        "dead-letter does not retain an original queue payload".to_owned(),
                    ),
                }));
            };
            (stage, key, payload)
        };
        let message_id = self.enqueue(replay.0, replay.1, replay.2, None)?;
        Ok(Some(DeadLetterReplayResult {
            dead_letter_id: dead_letter_id.to_owned(),
            replayed: true,
            message_id: Some(message_id),
            reason: None,
        }))
    }

    async fn health(&self) -> PushQueueResult<QueueHealth> {
        Ok(QueueHealth {
            backend: self.backend,
            healthy: true,
            details: if self.backend == PushQueueBackendKind::Memory {
                "memory queue is tests/dev only and not durable".to_owned()
            } else {
                format!(
                    "{:?} push queue compatibility runtime is active",
                    self.backend
                )
            },
        })
    }

    async fn lag(&self, stage: PushQueueStage) -> PushQueueResult<QueueLagMetrics> {
        let now = now_ms();
        let inner = self.lock()?;
        let mut metrics = QueueLagMetrics::default();
        if let Some(queue) = inner.ready.get(&stage) {
            for message in queue {
                if message
                    .not_before_ms
                    .is_some_and(|not_before| not_before > now)
                {
                    metrics.delayed_depth += 1;
                    metrics.oldest_delayed_age_ms = max_optional_u64(
                        metrics.oldest_delayed_age_ms,
                        Some(queue_message_age_ms(message, now)),
                    );
                } else {
                    metrics.ready_depth += 1;
                    metrics.oldest_ready_age_ms = max_optional_u64(
                        metrics.oldest_ready_age_ms,
                        Some(queue_message_age_ms(message, now)),
                    );
                }
            }
        }
        for ((message_stage, _), message) in &inner.inflight {
            if *message_stage == stage {
                metrics.inflight_depth += 1;
                metrics.oldest_inflight_age_ms = max_optional_u64(
                    metrics.oldest_inflight_age_ms,
                    Some(queue_message_age_ms(message, now)),
                );
            }
        }
        metrics.dead_letter_depth = inner.dead_letters.len() as u64;
        Ok(metrics)
    }
}

impl MemoryPushQueue {
    fn enqueue(
        &self,
        stage: PushQueueStage,
        key: String,
        payload: PushQueuePayload,
        not_before_ms: Option<u64>,
    ) -> PushQueueResult<String> {
        let mut inner = self.lock()?;
        if let Some(existing) = inner.produced_keys.get(&(stage, key.clone())) {
            return Ok(existing.clone());
        }

        inner.next_id = inner.next_id.saturating_add(1);
        let message_id = format!("mem-{}", inner.next_id);
        let route = QueueRoute::for_message(stage, &key, &payload);
        inner
            .produced_keys
            .insert((stage, key.clone()), message_id.clone());
        if let (PushQueueStage::DeadLetters, PushQueuePayload::DeadLetter(dead_letter)) =
            (stage, &payload)
        {
            inner.dead_letters.push(StoredDeadLetter {
                dead_letter_id: dead_letter_entry_id(dead_letter, &message_id),
                dead_letter: dead_letter.as_ref().clone(),
                provider: None,
                original_stage: None,
                original_key: None,
                original_payload: None,
            });
        }
        inner
            .ready
            .entry(stage)
            .or_default()
            .push_back(StoredMessage {
                message_id: message_id.clone(),
                key,
                route,
                payload,
                attempt: 1,
                enqueued_at_ms: now_ms(),
                not_before_ms,
                lease_deadline_ms: 0,
            });
        Ok(message_id)
    }

    fn lock(&self) -> PushQueueResult<std::sync::MutexGuard<'_, MemoryQueueState>> {
        self.inner
            .lock()
            .map_err(|_| PushQueueError::Backend("memory queue lock poisoned".to_owned()))
    }
}

fn reclaim_expired_leases(inner: &mut MemoryQueueState, now_ms: u64) {
    let expired = inner
        .inflight
        .iter()
        .filter_map(|((stage, message_id), message)| {
            (message.lease_deadline_ms <= now_ms).then_some((*stage, message_id.clone()))
        })
        .collect::<Vec<_>>();

    for (stage, message_id) in expired {
        if let Some(mut message) = inner.inflight.remove(&(stage, message_id)) {
            message.attempt = message.attempt.saturating_add(1);
            message.lease_deadline_ms = 0;
            inner.ready.entry(stage).or_default().push_back(message);
        }
    }
}

fn queue_message_age_ms(message: &StoredMessage, now_ms: u64) -> u64 {
    now_ms.saturating_sub(message.enqueued_at_ms)
}

fn max_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn dead_letter_from_message(
    stage: PushQueueStage,
    message: &StoredMessage,
    reason: String,
) -> DeadLetter {
    let (app_id, publish_id) = match &message.payload {
        PushQueuePayload::PublishLog(event) => (event.app_id.clone(), event.publish_id.clone()),
        PushQueuePayload::ShardJob(job) => (job.app_id.clone(), job.publish_id.clone()),
        PushQueuePayload::DeliveryBatch(batch) => (batch.app_id.clone(), batch.publish_id.clone()),
        PushQueuePayload::DeliveryResult(result) => {
            (result.app_id.clone(), result.publish_id.clone())
        }
        PushQueuePayload::DeliveryFeedback(feedback) => (
            feedback.result.app_id.clone(),
            feedback.result.publish_id.clone(),
        ),
        PushQueuePayload::DeadLetter(dead_letter) => {
            (dead_letter.app_id.clone(), dead_letter.publish_id.clone())
        }
        PushQueuePayload::RetrySchedule(entry) => (entry.app_id.clone(), entry.publish_id.clone()),
    };
    DeadLetter {
        app_id,
        publish_id,
        stage: format!("{stage:?}"),
        key: message.key.clone(),
        reason,
        occurred_at_ms: now_ms(),
    }
}

#[derive(Clone)]
pub struct PushPipeline {
    store: DynPushStore,
    queue: DynPushQueue,
    config: FanoutConfig,
    max_publish_log_lag: u64,
    metrics: PushMetrics,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushAcceptRequest {
    pub intent: PublishIntent,
    pub expected_recipients: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushAcceptOutcome {
    pub publish_id: String,
    pub status: PublishStatus,
    pub publish_event: PublishLogEvent,
    pub publish_log_message_id: String,
    pub duplicate: bool,
}

#[derive(Debug, Error)]
pub enum PushPipelineError {
    #[error(transparent)]
    Domain(#[from] PushDomainError),
    #[error(transparent)]
    Storage(#[from] PushStorageError),
    #[error(transparent)]
    Queue(#[from] PushQueueError),
    #[error("push pipeline backpressure: {0}")]
    Backpressure(String),
    #[error("push pipeline invalid payload: {0}")]
    InvalidPayload(String),
}

/// Serialize publish-status read/modify/write sections within a process without an unbounded
/// per-publish lock map. This closes local worker races; storage-level CAS remains the authority
/// needed to prevent conflicting writes from separate nodes.
pub(crate) async fn lock_publish_status(
    app_id: &str,
    publish_id: &str,
) -> tokio::sync::MutexGuard<'static, ()> {
    let locks = PUBLISH_STATUS_LOCKS.get_or_init(|| {
        (0..PUBLISH_STATUS_LOCK_SHARDS)
            .map(|_| tokio::sync::Mutex::new(()))
            .collect::<Vec<_>>()
            .into_boxed_slice()
    });
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    app_id.hash(&mut hasher);
    publish_id.hash(&mut hasher);
    let index = (hasher.finish() as usize) % locks.len();
    locks[index].lock().await
}

pub(crate) fn guard_publish_status_transition(
    metrics: &PushMetrics,
    component: &'static str,
    status: &PublishStatus,
    next: PublishLifecycleState,
) -> bool {
    if status.state.can_transition_to(next) {
        return true;
    }

    metrics.status_transition_invariant_violation();
    tracing::warn!(
        invariant = "status_transition",
        component,
        app_id = %status.app_id,
        publish_id = %status.publish_id,
        from = ?status.state,
        to = ?next,
        "ignored non-monotonic push publish status transition"
    );
    false
}

impl PushPipeline {
    pub fn new(store: DynPushStore, queue: DynPushQueue, config: FanoutConfig) -> Self {
        Self {
            store,
            queue,
            config,
            max_publish_log_lag: 100_000,
            metrics: PushMetrics::default(),
        }
    }

    pub fn with_max_publish_log_lag(mut self, max_publish_log_lag: u64) -> Self {
        self.max_publish_log_lag = max_publish_log_lag;
        self
    }

    pub fn with_metrics(mut self, metrics: PushMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn queue(&self) -> &DynPushQueue {
        &self.queue
    }

    pub fn store(&self) -> &DynPushStore {
        &self.store
    }

    pub async fn accept_publish(
        &self,
        mut request: PushAcceptRequest,
        occurred_at_ms: u64,
    ) -> PushPipelineResult<PushAcceptOutcome> {
        let started = Instant::now();
        self.config.validate()?;
        request.intent.validate()?;
        request.intent = self.resolve_intent_templates(request.intent).await?;
        request.intent.validate()?;
        render_all_provider_payloads(&request.intent.payload, &request.intent.provider_overrides)
            .map_err(|error| PushPipelineError::InvalidPayload(error.to_string()))?;
        let lag = self.queue.lag(PushQueueStage::PublishLog).await?;
        self.metrics.publish_log_lag_seconds(lag.ready_depth as f64);
        self.metrics.queue_lag(PushQueueStage::PublishLog, &lag);
        if lag.ready_depth.saturating_add(lag.delayed_depth) >= self.max_publish_log_lag {
            self.metrics.publish_accepted(
                &request.intent.app_id,
                "backpressure",
                started.elapsed(),
            );
            tracing::warn!(
                app_id = %request.intent.app_id,
                ready_depth = lag.ready_depth,
                delayed_depth = lag.delayed_depth,
                "push publish rejected by publish-log backpressure"
            );
            return Err(PushPipelineError::Backpressure(
                "publish_log lag exceeds admission ceiling".to_owned(),
            ));
        }

        let fanout_regime = if request.expected_recipients < self.config.fast_threshold {
            FanoutRegime::FastPath
        } else {
            FanoutRegime::ShardPath
        };
        let publish_id_key =
            publish_idempotency_key(&request.intent.app_id, &request.intent.publish_id);
        let publish_uid_key =
            publish_uid_key(&request.intent.app_id, &request.intent.idempotency_key());
        let expires_at_ms = request
            .intent
            .expires_at_ms
            .unwrap_or_else(|| occurred_at_ms.saturating_add(DEFAULT_IDEMPOTENCY_TTL_MS));
        if let Some(record) = self
            .store
            .get_idempotency_record(&request.intent.app_id, &publish_id_key)
            .await?
        {
            return self
                .duplicate_accept(&request.intent.app_id, &record.publish_id)
                .await;
        }
        if let Some(record) = self
            .store
            .get_idempotency_record(&request.intent.app_id, &publish_uid_key)
            .await?
        {
            return self
                .duplicate_accept(&request.intent.app_id, &record.publish_id)
                .await;
        }
        let status = PublishStatus {
            app_id: request.intent.app_id.clone(),
            publish_id: request.intent.publish_id.clone(),
            state: PublishLifecycleState::Queued,
            counters: PublishCounters {
                planned: request.expected_recipients,
                dispatched: 0,
                succeeded: 0,
                failed: 0,
                expired: 0,
                retry_scheduled: 0,
                retry_attempted: 0,
                dead_lettered: 0,
            },
            fanout_regime: Some(fanout_regime),
            retry_after_ms: None,
            error_reason: None,
        };
        self.store.put_publish_status(status.clone()).await?;

        let publish_id_record = IdempotencyRecord {
            app_id: request.intent.app_id.clone(),
            key: publish_id_key.clone(),
            publish_id: request.intent.publish_id.clone(),
            expires_at_ms,
        };
        if !self
            .store
            .put_idempotency_record_if_absent(publish_id_record)
            .await?
        {
            let record = self
                .store
                .get_idempotency_record(&request.intent.app_id, &publish_id_key)
                .await?
                .ok_or_else(|| {
                    PushPipelineError::Backpressure(
                        "duplicate publish id record disappeared".to_owned(),
                    )
                })?;
            return self
                .duplicate_accept(&request.intent.app_id, &record.publish_id)
                .await;
        }
        let publish_uid_record = IdempotencyRecord {
            app_id: request.intent.app_id.clone(),
            key: publish_uid_key.clone(),
            publish_id: request.intent.publish_id.clone(),
            expires_at_ms,
        };
        if !self
            .store
            .put_idempotency_record_if_absent(publish_uid_record)
            .await?
        {
            let record = self
                .store
                .get_idempotency_record(&request.intent.app_id, &publish_uid_key)
                .await?
                .ok_or_else(|| {
                    PushPipelineError::Backpressure(
                        "duplicate publish uid record disappeared".to_owned(),
                    )
                })?;
            return self
                .duplicate_accept(&request.intent.app_id, &record.publish_id)
                .await;
        }

        let event = PublishLogEvent {
            app_id: request.intent.app_id.clone(),
            publish_id: request.intent.publish_id.clone(),
            event_id: format!("publish-{}-{occurred_at_ms}", request.intent.publish_id),
            occurred_at_ms,
            intent: request.intent,
            fanout_regime,
            expected_recipients: request.expected_recipients,
            fast_threshold: self.config.fast_threshold,
            shard_size: self.config.shard_size,
        };
        self.store.append_publish_log_event(event.clone()).await?;
        let publish_log_message_id = self
            .queue
            .produce(
                PushQueueStage::PublishLog,
                event.queue_key(),
                PushQueuePayload::PublishLog(Box::new(event.clone())),
            )
            .await?;
        self.metrics
            .publish_accepted(&event.app_id, "accepted", started.elapsed());
        self.metrics
            .fanout_size(&event.app_id, event.expected_recipients);
        emit_push_meta_event(PushMetaEvent::accepted(
            &event.app_id,
            &event.publish_id,
            event.expected_recipients,
        ));
        tracing::info!(
            app_id = %event.app_id,
            publish_id = %event.publish_id,
            expected_recipients = event.expected_recipients,
            fanout_regime = ?event.fanout_regime,
            "push publish accepted"
        );

        Ok(PushAcceptOutcome {
            publish_id: event.publish_id.clone(),
            status,
            publish_event: event,
            publish_log_message_id,
            duplicate: false,
        })
    }

    async fn resolve_intent_templates(
        &self,
        mut intent: PublishIntent,
    ) -> PushPipelineResult<PublishIntent> {
        let Some(template_id) = intent.payload.template_id.clone() else {
            return Ok(intent);
        };
        let template = self
            .store
            .get_template(&intent.app_id, &template_id)
            .await?
            .ok_or_else(|| {
                PushPipelineError::InvalidPayload(format!("push template {template_id} not found"))
            })?;
        let effective =
            resolve_template_payload(&template, &intent.payload, &intent.provider_overrides)
                .map_err(|error| PushPipelineError::InvalidPayload(error.to_string()))?;
        intent.payload = effective.payload;
        intent.provider_overrides = effective.provider_overrides;
        Ok(intent)
    }

    async fn duplicate_accept(
        &self,
        app_id: &str,
        publish_id: &str,
    ) -> PushPipelineResult<PushAcceptOutcome> {
        let status = self
            .store
            .get_publish_status(app_id, publish_id)
            .await?
            .ok_or_else(|| {
                PushPipelineError::Backpressure(
                    "duplicate publish is missing persisted status".to_owned(),
                )
            })?;
        self.metrics.duplicate_suppressed();
        Ok(PushAcceptOutcome {
            publish_id: publish_id.to_owned(),
            publish_event: PublishLogEvent {
                app_id: app_id.to_owned(),
                publish_id: publish_id.to_owned(),
                event_id: "duplicate".to_owned(),
                occurred_at_ms: now_ms(),
                intent: PublishIntent {
                    app_id: app_id.to_owned(),
                    publish_id: publish_id.to_owned(),
                    targets: vec![],
                    payload: crate::domain::PushPayload {
                        template_id: None,
                        template_data: sonic_rs::Value::new_null(),
                        title: None,
                        body: None,
                        icon: None,
                        sound: None,
                        collapse_key: None,
                    },
                    provider_overrides: vec![],
                    not_before_ms: None,
                    expires_at_ms: None,
                },
                fanout_regime: status.fanout_regime.unwrap_or(FanoutRegime::FastPath),
                expected_recipients: status.counters.planned,
                fast_threshold: self.config.fast_threshold,
                shard_size: self.config.shard_size,
            },
            status,
            publish_log_message_id: "duplicate".to_owned(),
            duplicate: true,
        })
    }
}

pub fn publish_idempotency_key(app_id: &str, publish_id: &str) -> String {
    format!("publish-id:{app_id}:{publish_id}")
}

pub fn publish_uid_key(app_id: &str, uid: &str) -> String {
    format!("publish-uid:{app_id}:{uid}")
}

pub fn channel_delivery_idempotency_key(app_id: &str, message_id: &str, device_id: &str) -> String {
    format!("channel-delivery:{app_id}:{message_id}:{device_id}")
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(crate) fn dedupe_key(seed: impl AsRef<str>, existing: &mut BTreeSet<String>) -> String {
    let seed = seed.as_ref();
    if existing.insert(seed.to_owned()) {
        return seed.to_owned();
    }
    let mut index = existing.len() as u64;
    for _ in 0..8 {
        let candidate = format!(
            "{seed}-{index}-{}",
            &stable_hash(format!("{seed}:{index}").as_bytes())[..8]
        );
        if existing.insert(candidate.clone()) {
            return candidate;
        }
        index = index.saturating_add(1);
    }
    let candidate = format!(
        "{seed}-{}",
        stable_hash(format!("{seed}:{}", existing.len()).as_bytes())
    );
    existing.insert(candidate.clone());
    candidate
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sonic_rs::json;

    use crate::dispatch::{AcceptAllDispatcher, ProviderDispatchWorker, RetryAfterDispatcher};
    use crate::domain::{
        ChannelSubscription, DeviceDetails, DevicePushDetails, DevicePushState, FanoutConfig,
        FormFactor, NotificationTemplate, Platform, PublishIntent, PublishLifecycleState,
        PublishTarget, PushPayload, PushProviderKind, PushRecipient, SecretString, TemplateContent,
        hash_device_identity_token,
    };
    use crate::feedback::PushFeedbackProcessor;
    use crate::memory::MemoryPushStore;
    use crate::planner::{PushPlanner, PushShardWorker};
    use crate::retry::{PushRetryScheduler, RetryPolicy};
    use crate::storage::{
        PushDeviceStore, PushFanoutShardStore, PushPublishStatusStore, PushSubscriptionStore,
        PushTemplateStore,
    };

    use super::*;

    #[test]
    fn push_queue_startup_checks_are_feature_aware() {
        for backend in [
            PushQueueBackendKind::Redis,
            PushQueueBackendKind::RedisCluster,
            PushQueueBackendKind::Nats,
            PushQueueBackendKind::Pulsar,
            PushQueueBackendKind::RabbitMq,
            PushQueueBackendKind::GooglePubsub,
            PushQueueBackendKind::Kafka,
            PushQueueBackendKind::Iggy,
            PushQueueBackendKind::Sqs,
        ] {
            let result = backend.startup_check();
            let enabled = matches!(backend, PushQueueBackendKind::Redis) && cfg!(feature = "redis")
                || matches!(backend, PushQueueBackendKind::RedisCluster)
                    && cfg!(feature = "redis-cluster")
                || matches!(backend, PushQueueBackendKind::Nats) && cfg!(feature = "nats")
                || matches!(backend, PushQueueBackendKind::Pulsar) && cfg!(feature = "pulsar")
                || matches!(backend, PushQueueBackendKind::RabbitMq) && cfg!(feature = "rabbitmq")
                || matches!(backend, PushQueueBackendKind::GooglePubsub)
                    && cfg!(feature = "google-pubsub")
                || matches!(backend, PushQueueBackendKind::Kafka) && cfg!(feature = "kafka")
                || matches!(backend, PushQueueBackendKind::Iggy) && cfg!(feature = "iggy")
                || matches!(backend, PushQueueBackendKind::Sqs) && cfg!(feature = "sqs");
            if enabled {
                assert!(result.is_ok(), "{backend:?}");
            } else {
                assert!(matches!(
                    result,
                    Err(PushQueueError::FeatureDisabled { .. })
                ));
            }
        }
        assert!(matches!(
            PushQueueBackendKind::Sns.startup_check(),
            Err(PushQueueError::UnsupportedBackend { .. })
        ));
    }

    #[tokio::test]
    async fn feature_queue_runtime_preserves_ack_retry_and_lag_contract() {
        let queue = MemoryPushQueue::for_backend(PushQueueBackendKind::Memory).unwrap();
        let payload = PushQueuePayload::PublishLog(Box::new(sample_publish_log_event()));
        queue
            .produce(
                PushQueueStage::PublishLog,
                "key-1".to_owned(),
                payload.clone(),
            )
            .await
            .unwrap();
        queue
            .retry_at(
                PushQueueStage::PublishLog,
                "key-2".to_owned(),
                payload,
                u64::MAX,
            )
            .await
            .unwrap();

        let lag = queue.lag(PushQueueStage::PublishLog).await.unwrap();
        assert_eq!(lag.ready_depth, 1);
        assert_eq!(lag.delayed_depth, 1);
        assert!(lag.oldest_ready_age_ms.is_some());
        assert!(lag.oldest_delayed_age_ms.is_some());
        assert_eq!(lag.oldest_inflight_age_ms, None);

        let mut messages = queue
            .consume(PushQueueStage::PublishLog, "worker", 1, 30_000)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        let message = messages.pop().unwrap();
        assert_eq!(message.attempt, 1);
        assert!(message.enqueued_at_ms > 0);

        let lag = queue.lag(PushQueueStage::PublishLog).await.unwrap();
        assert_eq!(lag.ready_depth, 0);
        assert_eq!(lag.inflight_depth, 1);
        assert!(lag.oldest_inflight_age_ms.is_some());

        queue.nack(message.ack.clone(), None).await.unwrap();
        let message = queue
            .consume(PushQueueStage::PublishLog, "worker", 1, 30_000)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(message.attempt, 2);
        queue.ack(message.ack).await.unwrap();

        let lag = queue.lag(PushQueueStage::PublishLog).await.unwrap();
        assert_eq!(lag.ready_depth, 0);
        assert_eq!(lag.delayed_depth, 1);
        assert_eq!(lag.oldest_ready_age_ms, None);
        assert!(lag.oldest_delayed_age_ms.is_some());
    }

    #[tokio::test]
    async fn memory_queue_lists_and_replays_dead_letters() {
        let queue = MemoryPushQueue::new();
        let event = sample_publish_log_event();
        queue
            .produce(
                PushQueueStage::PublishLog,
                event.queue_key(),
                PushQueuePayload::PublishLog(Box::new(event.clone())),
            )
            .await
            .unwrap();
        let message = queue
            .consume(PushQueueStage::PublishLog, "worker", 1, 30_000)
            .await
            .unwrap()
            .pop()
            .unwrap();

        queue
            .dead_letter(message.ack, "planner failed".to_owned())
            .await
            .unwrap();

        let page = queue
            .list_dead_letters("app-1", DeadLetterQueueFilter::default(), 10, None)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        let entry = &page.items[0];
        assert_eq!(entry.dead_letter.publish_id, "publish-1");
        assert_eq!(entry.dead_letter.stage, "PublishLog");
        assert!(entry.replayable);

        let replay = queue
            .replay_dead_letter("app-1", &entry.dead_letter_id)
            .await
            .unwrap()
            .unwrap();
        assert!(replay.replayed);
        assert!(replay.message_id.is_some());

        let replayed = queue
            .consume(PushQueueStage::PublishLog, "worker", 1, 30_000)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let PushQueuePayload::PublishLog(replayed_event) = replayed.payload else {
            panic!("expected replayed publish log");
        };
        assert_eq!(replayed_event.publish_id, event.publish_id);
    }

    #[tokio::test]
    async fn memory_queue_lists_marker_dead_letters_as_not_replayable() {
        let queue = MemoryPushQueue::new();
        let dead_letter = DeadLetter {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            stage: "retry_schedule".to_owned(),
            key: "retry-key".to_owned(),
            reason: "retry attempts exhausted".to_owned(),
            occurred_at_ms: 20,
        };
        queue
            .produce(
                PushQueueStage::DeadLetters,
                dead_letter.key.clone(),
                PushQueuePayload::DeadLetter(Box::new(dead_letter)),
            )
            .await
            .unwrap();

        let page = queue
            .list_dead_letters("app-1", DeadLetterQueueFilter::default(), 10, None)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert!(!page.items[0].replayable);

        let replay = queue
            .replay_dead_letter("app-1", &page.items[0].dead_letter_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!replay.replayed);
        assert!(replay.reason.unwrap().contains("original queue payload"));
    }

    #[tokio::test]
    async fn memory_queue_reclaims_expired_leases() {
        let queue = MemoryPushQueue::new();
        let payload = PushQueuePayload::PublishLog(Box::new(sample_publish_log_event()));
        queue
            .produce(PushQueueStage::PublishLog, "key-1".to_owned(), payload)
            .await
            .unwrap();

        let message = queue
            .consume(PushQueueStage::PublishLog, "worker", 1, 0)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(message.attempt, 1);

        let message = queue
            .consume(PushQueueStage::PublishLog, "worker", 1, 30_000)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(message.attempt, 2);
        queue.ack(message.ack).await.unwrap();
    }

    #[tokio::test]
    async fn accept_persists_status_and_enqueues_publish_log_before_returning() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .upsert_device(sample_device("device-1"))
            .await
            .unwrap();

        let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default());
        let outcome = pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent: sample_intent(vec![PublishTarget::Device {
                        device_id: "device-1".to_owned(),
                    }]),
                    expected_recipients: 1,
                },
                10,
            )
            .await
            .unwrap();

        assert_eq!(outcome.status.state, PublishLifecycleState::Queued);
        assert!(
            store
                .get_publish_status("app-1", "publish-1")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            queue
                .lag(PushQueueStage::PublishLog)
                .await
                .unwrap()
                .ready_depth,
            1
        );
    }

    #[tokio::test]
    async fn duplicate_accept_increments_duplicate_suppressed_metric() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let metrics = PushMetrics::default();
        let pipeline =
            PushPipeline::new(store, queue, FanoutConfig::default()).with_metrics(metrics.clone());
        let request = PushAcceptRequest {
            intent: sample_intent(vec![PublishTarget::Recipient {
                recipient: recipient(PushProviderKind::Fcm),
            }]),
            expected_recipients: 1,
        };

        let accepted = pipeline.accept_publish(request.clone(), 10).await.unwrap();
        let duplicate = pipeline.accept_publish(request, 11).await.unwrap();

        assert!(!accepted.duplicate);
        assert!(duplicate.duplicate);
        assert_eq!(metrics.get("sockudo_push_duplicate_suppressed_total"), 1);
    }

    #[tokio::test]
    async fn planner_fast_path_enqueues_provider_batches_without_dispatching() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .upsert_device(sample_device("device-1"))
            .await
            .unwrap();
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default());
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent: sample_intent(vec![PublishTarget::Device {
                        device_id: "device-1".to_owned(),
                    }]),
                    expected_recipients: 1,
                },
                10,
            )
            .await
            .unwrap();

        let planner = PushPlanner::new(store, queue.clone(), FanoutConfig::default());
        assert_eq!(planner.run_once("planner").await.unwrap(), 1);
        assert_eq!(
            queue
                .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
                .await
                .unwrap()
                .ready_depth,
            1
        );
    }

    #[tokio::test]
    async fn accept_rejects_missing_template_before_queueing() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let mut intent = sample_intent(vec![PublishTarget::Recipient {
            recipient: recipient(PushProviderKind::Fcm),
        }]);
        intent.payload.template_id = Some("missing".to_owned());
        let pipeline = PushPipeline::new(store, queue.clone(), FanoutConfig::default());

        let error = pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent,
                    expected_recipients: 1,
                },
                10,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, PushPipelineError::InvalidPayload(_)));
        assert_eq!(
            queue
                .lag(PushQueueStage::PublishLog)
                .await
                .unwrap()
                .ready_depth,
            0
        );
    }

    #[tokio::test]
    async fn template_edit_after_accept_does_not_change_planned_delivery() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .upsert_device(sample_device("device-1"))
            .await
            .unwrap();
        store.put_template(sample_template("Hello")).await.unwrap();

        let mut intent = sample_intent(vec![PublishTarget::Device {
            device_id: "device-1".to_owned(),
        }]);
        intent.payload.template_id = Some("welcome".to_owned());
        intent.payload.template_data = json!({"name": "Ada"});
        intent.payload.title = None;
        intent.payload.body = None;
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default());
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent,
                    expected_recipients: 1,
                },
                10,
            )
            .await
            .unwrap();

        store.put_template(sample_template("Edited")).await.unwrap();
        let planner = PushPlanner::new(store, queue.clone(), FanoutConfig::default());
        assert_eq!(planner.run_once("planner").await.unwrap(), 1);
        let message = queue
            .consume(
                PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                "delivery",
                1,
                30_000,
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        let PushQueuePayload::DeliveryBatch(batch) = message.payload else {
            panic!("expected delivery batch");
        };
        let rendered = batch.jobs[0].rendered_payload.as_ref().unwrap();
        assert_eq!(
            rendered.payload["message"]["notification"]["title"],
            "Hello Ada"
        );
    }

    #[tokio::test]
    async fn planner_reuses_rendered_payload_for_large_channel_fanout() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        for index in 0..128 {
            let device = sample_device(&format!("device-{index}"));
            store.upsert_device(device.clone()).await.unwrap();
            store
                .upsert_subscription(ChannelSubscription::from_device("room", &device))
                .await
                .unwrap();
        }
        let config = FanoutConfig {
            fast_threshold: 1_000,
            shard_size: 1_000,
            page_size: 64,
            provider_batch_size: 256,
            status_retention_days: 30,
        };
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), config.clone());
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent: sample_intent(vec![PublishTarget::Channel {
                        channel: "room".to_owned(),
                    }]),
                    expected_recipients: 128,
                },
                10,
            )
            .await
            .unwrap();

        let planner = PushPlanner::new(store, queue.clone(), config);
        assert_eq!(planner.run_once("planner").await.unwrap(), 1);
        let message = queue
            .consume(
                PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                "delivery",
                1,
                30_000,
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        let PushQueuePayload::DeliveryBatch(batch) = message.payload else {
            panic!("expected delivery batch");
        };
        assert_eq!(batch.jobs.len(), 128);
        let rendered = batch.jobs[0].rendered_payload.as_ref().unwrap();
        assert!(batch.jobs.iter().all(|job| {
            job.rendered_payload
                .as_ref()
                .is_some_and(|candidate| Arc::ptr_eq(candidate, rendered))
        }));
    }

    #[tokio::test]
    async fn planner_fast_path_copies_delivery_timing_fields() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .upsert_device(sample_device("device-1"))
            .await
            .unwrap();
        let mut intent = sample_intent(vec![PublishTarget::Device {
            device_id: "device-1".to_owned(),
        }]);
        intent.not_before_ms = Some(0);
        intent.expires_at_ms = Some(123_456);
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default());
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent,
                    expected_recipients: 1,
                },
                10,
            )
            .await
            .unwrap();

        let planner = PushPlanner::new(store, queue.clone(), FanoutConfig::default());
        assert_eq!(planner.run_once("planner").await.unwrap(), 1);
        let message = queue
            .consume(
                PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                "delivery",
                1,
                30_000,
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(message.not_before_ms, Some(0));
        let PushQueuePayload::DeliveryBatch(batch) = message.payload else {
            panic!("expected delivery batch");
        };
        assert_eq!(batch.jobs[0].not_before_ms, Some(0));
        assert_eq!(batch.jobs[0].expires_at_ms, Some(123_456));
    }

    #[tokio::test]
    async fn shard_path_copies_timing_into_shard_and_delivery_jobs() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        for index in 0..2 {
            let device = sample_device(&format!("device-{index}"));
            store.upsert_device(device.clone()).await.unwrap();
            store
                .upsert_subscription(ChannelSubscription::from_device("room", &device))
                .await
                .unwrap();
        }
        let config = FanoutConfig {
            fast_threshold: 1,
            shard_size: 10,
            page_size: 10,
            provider_batch_size: 10,
            status_retention_days: 30,
        };
        let mut intent = sample_intent(vec![PublishTarget::Channel {
            channel: "room".to_owned(),
        }]);
        intent.not_before_ms = Some(0);
        intent.expires_at_ms = Some(123_456);
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), config.clone());
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent,
                    expected_recipients: 2,
                },
                10,
            )
            .await
            .unwrap();

        let planner = PushPlanner::new(store.clone(), queue.clone(), config.clone());
        assert_eq!(planner.run_once("planner").await.unwrap(), 1);
        let shard = store
            .get_fanout_shard("app-1", "publish-1", "shard-0")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(shard.not_before_ms, Some(0));
        assert_eq!(shard.expires_at_ms, Some(123_456));

        let worker = PushShardWorker::new(store, queue.clone(), config);
        assert_eq!(worker.run_once("shards").await.unwrap(), 1);
        let message = queue
            .consume(
                PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                "delivery",
                1,
                30_000,
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        let PushQueuePayload::DeliveryBatch(batch) = message.payload else {
            panic!("expected delivery batch");
        };
        assert_eq!(batch.jobs.len(), 2);
        assert!(batch.jobs.iter().all(|job| job.not_before_ms == Some(0)));
        assert!(
            batch
                .jobs
                .iter()
                .all(|job| job.expires_at_ms == Some(123_456))
        );
    }

    #[tokio::test]
    async fn shard_worker_paginates_and_requeues_continuation() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        for index in 0..5 {
            let device = sample_device(&format!("device-{index}"));
            store.upsert_device(device.clone()).await.unwrap();
            store
                .upsert_subscription(ChannelSubscription::from_device("room", &device))
                .await
                .unwrap();
        }
        let config = FanoutConfig {
            fast_threshold: 2,
            shard_size: 2,
            page_size: 2,
            provider_batch_size: 10,
            status_retention_days: 30,
        };
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), config.clone());
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent: sample_intent(vec![PublishTarget::Channel {
                        channel: "room".to_owned(),
                    }]),
                    expected_recipients: 5,
                },
                10,
            )
            .await
            .unwrap();

        let planner = PushPlanner::new(store.clone(), queue.clone(), config.clone());
        planner.run_once("planner").await.unwrap();
        assert_eq!(
            queue
                .lag(PushQueueStage::ShardJobs)
                .await
                .unwrap()
                .ready_depth,
            1
        );

        let worker = PushShardWorker::new(store, queue.clone(), config);
        assert_eq!(worker.run_once("shards").await.unwrap(), 1);
        assert_eq!(
            queue
                .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
                .await
                .unwrap()
                .ready_depth,
            1
        );
        assert_eq!(
            queue
                .lag(PushQueueStage::ShardJobs)
                .await
                .unwrap()
                .ready_depth,
            1
        );
        assert_eq!(worker.run_once("shards").await.unwrap(), 1);
        assert_eq!(
            queue
                .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
                .await
                .unwrap()
                .ready_depth,
            2
        );
    }

    #[tokio::test]
    async fn dispatch_and_feedback_update_status_and_device_state() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .upsert_device(sample_device("device-1"))
            .await
            .unwrap();
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default());
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent: sample_intent(vec![PublishTarget::Device {
                        device_id: "device-1".to_owned(),
                    }]),
                    expected_recipients: 1,
                },
                10,
            )
            .await
            .unwrap();
        PushPlanner::new(store.clone(), queue.clone(), FanoutConfig::default())
            .run_once("planner")
            .await
            .unwrap();

        let dispatcher = Arc::new(AcceptAllDispatcher::new(PushProviderKind::Fcm));
        let mut worker =
            ProviderDispatchWorker::new(PushProviderKind::Fcm, queue.clone(), dispatcher);
        assert_eq!(worker.run_once("fcm").await.unwrap(), 1);

        let feedback = PushFeedbackProcessor::new(store.clone(), queue);
        assert_eq!(feedback.run_once("feedback").await.unwrap(), 1);
        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.succeeded, 1);
        assert_eq!(status.state, PublishLifecycleState::Succeeded);
        let device = store
            .get_device("app-1", "device-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(device.push.state, DevicePushState::Active);
    }

    #[tokio::test]
    async fn retryable_once_then_accepted_completes_publish() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .upsert_device(sample_device("device-1"))
            .await
            .unwrap();
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default());
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent: sample_intent(vec![PublishTarget::Device {
                        device_id: "device-1".to_owned(),
                    }]),
                    expected_recipients: 1,
                },
                10,
            )
            .await
            .unwrap();
        PushPlanner::new(store.clone(), queue.clone(), FanoutConfig::default())
            .run_once("planner")
            .await
            .unwrap();

        let retry_dispatcher = Arc::new(RetryAfterDispatcher::new(PushProviderKind::Fcm, now_ms()));
        let mut retry_worker =
            ProviderDispatchWorker::new(PushProviderKind::Fcm, queue.clone(), retry_dispatcher);
        assert_eq!(retry_worker.run_once("fcm").await.unwrap(), 1);
        let feedback = PushFeedbackProcessor::new(store.clone(), queue.clone()).with_retry_policy(
            RetryPolicy {
                jitter_ratio_percent: 0,
                ..RetryPolicy::default()
            },
        );
        assert_eq!(feedback.run_once("feedback").await.unwrap(), 1);
        assert_eq!(
            store
                .get_publish_status("app-1", "publish-1")
                .await
                .unwrap()
                .unwrap()
                .state,
            PublishLifecycleState::Dispatching
        );

        let retry_scheduler = PushRetryScheduler::new(store.clone(), queue.clone(), "node-a");
        assert_eq!(retry_scheduler.run_once("retry").await.unwrap(), 1);
        let accept_dispatcher = Arc::new(AcceptAllDispatcher::new(PushProviderKind::Fcm));
        let mut accept_worker =
            ProviderDispatchWorker::new(PushProviderKind::Fcm, queue.clone(), accept_dispatcher);
        assert_eq!(accept_worker.run_once("fcm").await.unwrap(), 1);
        assert_eq!(feedback.run_once("feedback").await.unwrap(), 1);

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.dispatched, 1);
        assert_eq!(status.counters.retry_scheduled, 1);
        assert_eq!(status.counters.retry_attempted, 1);
        assert_eq!(status.counters.succeeded, 1);
        assert_eq!(status.state, PublishLifecycleState::Succeeded);
    }

    #[tokio::test]
    async fn retryable_until_attempts_exhausted_dead_letters_publish() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .upsert_device(sample_device("device-1"))
            .await
            .unwrap();
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default());
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent: sample_intent(vec![PublishTarget::Device {
                        device_id: "device-1".to_owned(),
                    }]),
                    expected_recipients: 1,
                },
                10,
            )
            .await
            .unwrap();
        PushPlanner::new(store.clone(), queue.clone(), FanoutConfig::default())
            .run_once("planner")
            .await
            .unwrap();

        let policy = RetryPolicy {
            max_attempts: 2,
            jitter_ratio_percent: 0,
            ..RetryPolicy::default()
        };
        let feedback = PushFeedbackProcessor::new(store.clone(), queue.clone())
            .with_retry_policy(policy.clone());
        let dispatcher = Arc::new(RetryAfterDispatcher::new(PushProviderKind::Fcm, now_ms()));
        let mut worker =
            ProviderDispatchWorker::new(PushProviderKind::Fcm, queue.clone(), dispatcher);
        assert_eq!(worker.run_once("fcm").await.unwrap(), 1);
        assert_eq!(feedback.run_once("feedback").await.unwrap(), 1);

        let retry_scheduler = PushRetryScheduler::new(store.clone(), queue.clone(), "node-a");
        assert_eq!(retry_scheduler.run_once("retry").await.unwrap(), 1);
        let dispatcher = Arc::new(RetryAfterDispatcher::new(PushProviderKind::Fcm, now_ms()));
        let mut worker =
            ProviderDispatchWorker::new(PushProviderKind::Fcm, queue.clone(), dispatcher);
        assert_eq!(worker.run_once("fcm").await.unwrap(), 1);
        assert_eq!(feedback.run_once("feedback").await.unwrap(), 1);
        assert_eq!(retry_scheduler.run_once("retry").await.unwrap(), 1);

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.dead_lettered, 1);
        assert_eq!(status.state, PublishLifecycleState::DeadLettered);
        assert_eq!(
            queue
                .lag(PushQueueStage::DeadLetters)
                .await
                .unwrap()
                .ready_depth,
            1
        );
    }

    #[tokio::test]
    async fn publish_expiry_before_dispatch_marks_publish_expired() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .upsert_device(sample_device("device-1"))
            .await
            .unwrap();
        let pipeline = PushPipeline::new(store.clone(), queue.clone(), FanoutConfig::default());
        let mut intent = sample_intent(vec![PublishTarget::Device {
            device_id: "device-1".to_owned(),
        }]);
        intent.expires_at_ms = Some(11);
        pipeline
            .accept_publish(
                PushAcceptRequest {
                    intent,
                    expected_recipients: 1,
                },
                10,
            )
            .await
            .unwrap();
        PushPlanner::new(store.clone(), queue.clone(), FanoutConfig::default())
            .run_once("planner")
            .await
            .unwrap();
        let dispatcher = Arc::new(RetryAfterDispatcher::new(PushProviderKind::Fcm, now_ms()));
        let mut worker =
            ProviderDispatchWorker::new(PushProviderKind::Fcm, queue.clone(), dispatcher);
        assert_eq!(worker.run_once("fcm").await.unwrap(), 1);
        let feedback = PushFeedbackProcessor::new(store.clone(), queue.clone());
        assert_eq!(feedback.run_once("feedback").await.unwrap(), 1);

        assert_eq!(
            queue
                .lag(PushQueueStage::RetrySchedule)
                .await
                .unwrap()
                .ready_depth,
            0
        );
        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.expired, 1);
        assert_eq!(status.state, PublishLifecycleState::Expired);
    }

    fn sample_intent(targets: Vec<PublishTarget>) -> PublishIntent {
        PublishIntent {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            targets,
            payload: PushPayload {
                template_id: None,
                template_data: json!({}),
                title: Some("Hello".to_owned()),
                body: Some("Body".to_owned()),
                icon: None,
                sound: None,
                collapse_key: None,
            },
            provider_overrides: vec![],
            not_before_ms: None,
            expires_at_ms: None,
        }
    }

    fn sample_device(device_id: &str) -> DeviceDetails {
        DeviceDetails {
            app_id: "app-1".to_owned(),
            id: device_id.to_owned(),
            client_id: Some("client-1".to_owned()),
            form_factor: FormFactor::Phone,
            platform: Platform::Android,
            metadata: json!({}),
            device_secret: hash_device_identity_token(&SecretString::new("device-token").unwrap()),
            timezone: "UTC".to_owned(),
            locale: "en".to_owned(),
            last_active_at_ms: 1,
            push: DevicePushDetails {
                recipient: PushRecipient::Fcm {
                    registration_token: SecretString::new(format!("token-{device_id}")).unwrap(),
                },
                state: DevicePushState::Active,
                failure_count: 0,
                error_reason: None,
            },
            push_rate_policy: None,
        }
    }

    fn sample_publish_log_event() -> PublishLogEvent {
        PublishLogEvent {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            event_id: "event-1".to_owned(),
            occurred_at_ms: 1,
            intent: sample_intent(vec![PublishTarget::Device {
                device_id: "device-1".to_owned(),
            }]),
            fanout_regime: FanoutRegime::FastPath,
            expected_recipients: 1,
            fast_threshold: 10_000,
            shard_size: 100_000,
        }
    }

    fn recipient(provider: PushProviderKind) -> PushRecipient {
        match provider {
            PushProviderKind::Fcm => PushRecipient::Fcm {
                registration_token: SecretString::new("fcm-token").unwrap(),
            },
            PushProviderKind::Apns => PushRecipient::Apns {
                device_token: SecretString::new("apns-token").unwrap(),
            },
            PushProviderKind::WebPush => PushRecipient::Web {
                endpoint: SecretString::new("https://push.example/subscription").unwrap(),
                p256dh: SecretString::new("p256dh").unwrap(),
                auth: SecretString::new("auth").unwrap(),
            },
            PushProviderKind::Hms => PushRecipient::Hms {
                registration_token: SecretString::new("hms-token").unwrap(),
            },
            PushProviderKind::Wns => PushRecipient::Wns {
                channel_uri: SecretString::new("https://wns.example/channel").unwrap(),
            },
        }
    }

    fn sample_template(title_prefix: &str) -> NotificationTemplate {
        NotificationTemplate {
            app_id: "app-1".to_owned(),
            template_id: "welcome".to_owned(),
            default_locale: "en".to_owned(),
            locales: BTreeMap::from([(
                "en".to_owned(),
                TemplateContent {
                    title: format!("{title_prefix} {{{{ data.name }}}}"),
                    body: "Body".to_owned(),
                    icon: None,
                    sound: None,
                    collapse_key: Some("welcome".to_owned()),
                },
            )]),
            provider_overrides: BTreeMap::new(),
        }
    }
}
