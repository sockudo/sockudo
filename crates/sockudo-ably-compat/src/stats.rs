//! Time-bucketed application statistics for the Ably compatibility surface.
//!
//! The canonical persistence unit is one UTC minute. Larger units are derived
//! during reads so retries cannot partially update multiple rollups. Live
//! observations use a bounded batching worker; calls that precede an ACK wait
//! for the corresponding durable cache merge.

use async_trait::async_trait;
use base64::Engine as _;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use crossfire::{TrySendError, mpsc, oneshot};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sockudo_core::cache::CacheManager;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use thiserror::Error;

const STATS_KEY_VERSION: &str = "stats:v1";
const STATS_CURSOR_VERSION: u8 = 1;
const MAX_FIXTURE_FIELDS: usize = 512;
const MAX_FIELD_NAME_BYTES: usize = 256;
const MAX_FIXTURE_DEPTH: usize = 16;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum StatsUnit {
    Minute,
    Hour,
    Day,
    Month,
}

impl StatsUnit {
    pub(crate) fn parse(value: &str) -> Result<Self, StatsError> {
        match value {
            "minute" => Ok(Self::Minute),
            "hour" => Ok(Self::Hour),
            "day" => Ok(Self::Day),
            "month" => Ok(Self::Month),
            other => Err(StatsError::InvalidQuery(format!(
                "invalid stats unit '{other}'"
            ))),
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
        }
    }

    fn interval_id(self, timestamp_ms: i64) -> Result<String, StatsError> {
        let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
            .ok_or_else(|| StatsError::InvalidQuery("invalid stats timestamp".to_string()))?;
        Ok(match self {
            Self::Minute => timestamp.format("%Y-%m-%d:%H:%M").to_string(),
            Self::Hour => timestamp.format("%Y-%m-%d:%H").to_string(),
            Self::Day => timestamp.format("%Y-%m-%d").to_string(),
            Self::Month => timestamp.format("%Y-%m").to_string(),
        })
    }

    fn rollup_id(self, minute_id: &str) -> Result<String, StatsError> {
        let timestamp = parse_interval_start(minute_id)?.timestamp_millis();
        self.interval_id(timestamp)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum StatsDirection {
    Backwards,
    Forwards,
}

impl StatsDirection {
    pub(crate) fn parse(value: Option<&str>) -> Result<Self, StatsError> {
        match value.unwrap_or("backwards") {
            "backwards" => Ok(Self::Backwards),
            "forwards" => Ok(Self::Forwards),
            other => Err(StatsError::InvalidQuery(format!(
                "invalid stats direction '{other}'"
            ))),
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Backwards => "backwards",
            Self::Forwards => "forwards",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum FieldAggregation {
    Sum,
    Peak,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StatsFieldValue {
    value: u64,
    aggregation: FieldAggregation,
}

impl StatsFieldValue {
    fn merge(&mut self, other: &Self) {
        self.value = match self.aggregation {
            FieldAggregation::Sum => self.value.saturating_add(other.value),
            FieldAggregation::Peak => self.value.max(other.value),
        };
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StatsBucket {
    app_id: String,
    interval_id: String,
    entries: BTreeMap<String, StatsFieldValue>,
}

impl StatsBucket {
    fn new(app_id: impl Into<String>, timestamp_ms: i64) -> Result<Self, StatsError> {
        Ok(Self {
            app_id: app_id.into(),
            interval_id: StatsUnit::Minute.interval_id(timestamp_ms)?,
            entries: BTreeMap::new(),
        })
    }

    fn from_interval_id(app_id: impl Into<String>, interval_id: &str) -> Result<Self, StatsError> {
        let timestamp = parse_interval_start(interval_id)?;
        if interval_id.len() != 16 {
            return Err(StatsError::InvalidFixture(
                "stats fixtures require minute intervalId values".to_string(),
            ));
        }
        Self::new(app_id, timestamp.timestamp_millis())
    }

    fn add(&mut self, name: impl Into<String>, value: u64, aggregation: FieldAggregation) {
        let name = name.into();
        let incoming = StatsFieldValue { value, aggregation };
        self.entries
            .entry(name)
            .and_modify(|current| current.merge(&incoming))
            .or_insert(incoming);
    }

    fn merge(&mut self, other: &Self) {
        for (name, value) in &other.entries {
            self.entries
                .entry(name.clone())
                .and_modify(|current| current.merge(value))
                .or_insert_with(|| value.clone());
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StatsObservation {
    bucket: StatsBucket,
    observed_at_ms: i64,
}

impl StatsObservation {
    fn new(app_id: &str, timestamp_ms: i64) -> Result<Self, StatsError> {
        Ok(Self {
            bucket: StatsBucket::new(app_id, timestamp_ms)?,
            observed_at_ms: timestamp_ms,
        })
    }

    pub(crate) fn messages(
        app_id: &str,
        timestamp_ms: i64,
        direction: &str,
        transport: &str,
        count: u64,
        data: u64,
    ) -> Result<Self, StatsError> {
        let mut observation = Self::new(app_id, timestamp_ms)?;
        for scope in ["all", transport] {
            observation.bucket.add(
                format!("messages.{direction}.{scope}.messages.count"),
                count,
                FieldAggregation::Sum,
            );
            observation.bucket.add(
                format!("messages.{direction}.{scope}.messages.data"),
                data,
                FieldAggregation::Sum,
            );
        }
        Ok(observation)
    }

    pub(crate) fn api_request(
        app_id: &str,
        timestamp_ms: i64,
        succeeded: bool,
        request_bytes: u64,
        response_bytes: u64,
    ) -> Result<Self, StatsError> {
        let mut observation = Self::new(app_id, timestamp_ms)?;
        observation.bucket.add(
            if succeeded {
                "apiRequests.succeeded"
            } else {
                "apiRequests.failed"
            },
            1,
            FieldAggregation::Sum,
        );
        observation.bucket.add(
            "apiRequests.requestBytes",
            request_bytes,
            FieldAggregation::Sum,
        );
        observation.bucket.add(
            "apiRequests.responseBytes",
            response_bytes,
            FieldAggregation::Sum,
        );
        Ok(observation)
    }

    pub(crate) fn token_request(
        app_id: &str,
        timestamp_ms: i64,
        succeeded: bool,
    ) -> Result<Self, StatsError> {
        let mut observation = Self::new(app_id, timestamp_ms)?;
        observation.bucket.add(
            if succeeded {
                "tokenRequests.succeeded"
            } else {
                "tokenRequests.failed"
            },
            1,
            FieldAggregation::Sum,
        );
        Ok(observation)
    }

    pub(crate) fn connection_opened(
        app_id: &str,
        timestamp_ms: i64,
        transport: &str,
        current: u64,
    ) -> Result<Self, StatsError> {
        let mut observation = Self::new(app_id, timestamp_ms)?;
        for scope in ["all", transport] {
            observation.bucket.add(
                format!("connections.{scope}.opened"),
                1,
                FieldAggregation::Sum,
            );
            observation.bucket.add(
                format!("connections.{scope}.peak"),
                current,
                FieldAggregation::Peak,
            );
        }
        Ok(observation)
    }

    pub(crate) fn connection_closed(
        app_id: &str,
        timestamp_ms: i64,
        transport: &str,
    ) -> Result<Self, StatsError> {
        let mut observation = Self::new(app_id, timestamp_ms)?;
        for scope in ["all", transport] {
            observation.bucket.add(
                format!("connections.{scope}.closed"),
                1,
                FieldAggregation::Sum,
            );
        }
        Ok(observation)
    }

    pub(crate) fn channel_opened(
        app_id: &str,
        timestamp_ms: i64,
        current: u64,
    ) -> Result<Self, StatsError> {
        let mut observation = Self::new(app_id, timestamp_ms)?;
        observation
            .bucket
            .add("channels.opened", 1, FieldAggregation::Sum);
        observation
            .bucket
            .add("channels.peak", current, FieldAggregation::Peak);
        Ok(observation)
    }

    pub(crate) fn channel_closed(app_id: &str, timestamp_ms: i64) -> Result<Self, StatsError> {
        let mut observation = Self::new(app_id, timestamp_ms)?;
        observation
            .bucket
            .add("channels.closed", 1, FieldAggregation::Sum);
        Ok(observation)
    }

    pub(crate) fn presence(
        app_id: &str,
        timestamp_ms: i64,
        count: u64,
        data: u64,
    ) -> Result<Self, StatsError> {
        let mut observation = Self::new(app_id, timestamp_ms)?;
        observation
            .bucket
            .add("persisted.presence.count", count, FieldAggregation::Sum);
        observation
            .bucket
            .add("persisted.presence.data", data, FieldAggregation::Sum);
        Ok(observation)
    }

    #[cfg(feature = "push")]
    pub(crate) fn push(
        app_id: &str,
        timestamp_ms: i64,
        succeeded: bool,
        recipients: u64,
        data: u64,
    ) -> Result<Self, StatsError> {
        let mut observation = Self::new(app_id, timestamp_ms)?;
        observation.bucket.add(
            if succeeded {
                "push.succeeded"
            } else {
                "push.failed"
            },
            1,
            FieldAggregation::Sum,
        );
        observation
            .bucket
            .add("push.recipients", recipients, FieldAggregation::Sum);
        observation
            .bucket
            .add("push.data", data, FieldAggregation::Sum);
        Ok(observation)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StatsQuery {
    pub(crate) unit: StatsUnit,
    pub(crate) direction: StatsDirection,
    pub(crate) start: Option<String>,
    pub(crate) end: Option<String>,
    pub(crate) limit: usize,
    pub(crate) cursor: Option<String>,
}

impl StatsQuery {
    pub(crate) fn parse(
        unit: &str,
        direction: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<Self, StatsError> {
        let unit = StatsUnit::parse(unit)?;
        Ok(Self {
            unit,
            direction: StatsDirection::parse(direction)?,
            start: start
                .map(|bound| normalize_bound(bound, unit, false))
                .transpose()?,
            end: end
                .map(|bound| normalize_bound(bound, unit, true))
                .transpose()?,
            limit: limit.unwrap_or(100).clamp(1, 1_000),
            cursor,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatsInterval {
    pub(crate) interval_id: String,
    pub(crate) unit: String,
    pub(crate) app_id: String,
    pub(crate) schema: String,
    pub(crate) entries: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) in_progress: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct StatsPage {
    pub(crate) items: Vec<StatsInterval>,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StatsCursor {
    version: u8,
    app_id: String,
    unit: StatsUnit,
    direction: StatsDirection,
    start: Option<String>,
    end: Option<String>,
    horizon: String,
    after: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StatsRuntimeConfig {
    pub(crate) queue_capacity: usize,
    pub(crate) flush_interval: Duration,
    pub(crate) retention_seconds: u64,
    pub(crate) max_scan_entries: usize,
    pub(crate) cas_retries: usize,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsRuntimeSnapshot {
    pub backlog: usize,
    pub queue_capacity: usize,
    pub dropped: u64,
    pub flush_failures: u64,
    pub flushed_batches: u64,
    pub flushed_observations: u64,
    pub last_flush_lag_ms: u64,
}

#[derive(Debug, Default)]
struct StatsRuntimeMetrics {
    backlog: AtomicUsize,
    queue_capacity: usize,
    dropped: AtomicU64,
    flush_failures: AtomicU64,
    flushed_batches: AtomicU64,
    flushed_observations: AtomicU64,
    last_flush_lag_ms: AtomicU64,
}

impl StatsRuntimeMetrics {
    fn snapshot(&self) -> StatsRuntimeSnapshot {
        StatsRuntimeSnapshot {
            backlog: self.backlog.load(Ordering::Acquire),
            queue_capacity: self.queue_capacity,
            dropped: self.dropped.load(Ordering::Acquire),
            flush_failures: self.flush_failures.load(Ordering::Acquire),
            flushed_batches: self.flushed_batches.load(Ordering::Acquire),
            flushed_observations: self.flushed_observations.load(Ordering::Acquire),
            last_flush_lag_ms: self.last_flush_lag_ms.load(Ordering::Acquire),
        }
    }
}

#[derive(Clone, Debug, Error)]
pub(crate) enum StatsError {
    #[error("{0}")]
    InvalidQuery(String),
    #[error("{0}")]
    InvalidFixture(String),
    #[error("stats store failed: {0}")]
    Store(String),
    #[error("stats recorder is unavailable")]
    Closed,
    #[error("stats recorder queue is full")]
    QueueFull,
}

#[async_trait]
trait StatsStore: Send + Sync {
    async fn merge(&self, buckets: &[StatsBucket]) -> Result<(), StatsError>;
    async fn put(&self, buckets: &[StatsBucket]) -> Result<(), StatsError>;
    async fn list(&self, app_id: &str) -> Result<Vec<StatsBucket>, StatsError>;
}

struct CacheStatsStore {
    cache: Arc<dyn CacheManager>,
    retention_seconds: u64,
    max_scan_entries: usize,
    cas_retries: usize,
}

impl CacheStatsStore {
    fn new(cache: Arc<dyn CacheManager>, config: StatsRuntimeConfig) -> Self {
        Self {
            cache,
            retention_seconds: config.retention_seconds,
            max_scan_entries: config.max_scan_entries,
            cas_retries: config.cas_retries,
        }
    }

    fn app_prefix(app_id: &str) -> String {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(app_id.as_bytes());
        format!("{STATS_KEY_VERSION}:{encoded}:")
    }

    fn bucket_key(bucket: &StatsBucket) -> String {
        format!("{}{}", Self::app_prefix(&bucket.app_id), bucket.interval_id)
    }

    async fn merge_one(&self, bucket: &StatsBucket) -> Result<(), StatsError> {
        let key = Self::bucket_key(bucket);
        for _ in 0..self.cas_retries.max(1) {
            let current = self
                .cache
                .get(&key)
                .await
                .map_err(|error| StatsError::Store(error.to_string()))?;
            match current {
                Some(encoded) => {
                    let mut merged: StatsBucket = serde_json::from_str(&encoded)
                        .map_err(|error| StatsError::Store(error.to_string()))?;
                    merged.merge(bucket);
                    let replacement = serde_json::to_string(&merged)
                        .map_err(|error| StatsError::Store(error.to_string()))?;
                    if self
                        .cache
                        .compare_and_swap(&key, &encoded, &replacement, self.retention_seconds)
                        .await
                        .map_err(|error| StatsError::Store(error.to_string()))?
                    {
                        return Ok(());
                    }
                }
                None => {
                    let encoded = serde_json::to_string(bucket)
                        .map_err(|error| StatsError::Store(error.to_string()))?;
                    if self
                        .cache
                        .set_if_not_exists(&key, &encoded, self.retention_seconds)
                        .await
                        .map_err(|error| StatsError::Store(error.to_string()))?
                    {
                        return Ok(());
                    }
                }
            }
        }
        Err(StatsError::Store(
            "stats bucket compare-and-swap retries exhausted".to_string(),
        ))
    }
}

#[async_trait]
impl StatsStore for CacheStatsStore {
    async fn merge(&self, buckets: &[StatsBucket]) -> Result<(), StatsError> {
        for bucket in buckets {
            self.merge_one(bucket).await?;
        }
        Ok(())
    }

    async fn put(&self, buckets: &[StatsBucket]) -> Result<(), StatsError> {
        for bucket in buckets {
            let encoded = serde_json::to_string(bucket)
                .map_err(|error| StatsError::Store(error.to_string()))?;
            self.cache
                .set(&Self::bucket_key(bucket), &encoded, self.retention_seconds)
                .await
                .map_err(|error| StatsError::Store(error.to_string()))?;
        }
        Ok(())
    }

    async fn list(&self, app_id: &str) -> Result<Vec<StatsBucket>, StatsError> {
        let prefix = Self::app_prefix(app_id);
        let mut cursor = None;
        let mut buckets: Vec<StatsBucket> = Vec::new();
        loop {
            let remaining = self
                .max_scan_entries
                .saturating_add(1)
                .saturating_sub(buckets.len());
            if remaining == 0 {
                return Err(StatsError::Store(format!(
                    "stats query exceeded the bounded {}-interval scan",
                    self.max_scan_entries
                )));
            }
            let page = self
                .cache
                .scan_prefix_page(&prefix, cursor, remaining.min(1_000))
                .await
                .map_err(|error| StatsError::Store(error.to_string()))?;
            for (_, value) in page.entries {
                buckets.push(
                    serde_json::from_str(&value)
                        .map_err(|error| StatsError::Store(error.to_string()))?,
                );
                if buckets.len() > self.max_scan_entries {
                    return Err(StatsError::Store(format!(
                        "stats query exceeded the bounded {}-interval scan",
                        self.max_scan_entries
                    )));
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        buckets.sort_by(|left, right| left.interval_id.cmp(&right.interval_id));
        Ok(buckets)
    }
}

#[derive(Default)]
struct MemoryStatsStore {
    buckets: RwLock<BTreeMap<(String, String), StatsBucket>>,
}

#[async_trait]
impl StatsStore for MemoryStatsStore {
    async fn merge(&self, buckets: &[StatsBucket]) -> Result<(), StatsError> {
        let mut store = self
            .buckets
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for bucket in buckets {
            store
                .entry((bucket.app_id.clone(), bucket.interval_id.clone()))
                .and_modify(|current| current.merge(bucket))
                .or_insert_with(|| bucket.clone());
        }
        Ok(())
    }

    async fn put(&self, buckets: &[StatsBucket]) -> Result<(), StatsError> {
        let mut store = self
            .buckets
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for bucket in buckets {
            store.insert(
                (bucket.app_id.clone(), bucket.interval_id.clone()),
                bucket.clone(),
            );
        }
        Ok(())
    }

    async fn list(&self, app_id: &str) -> Result<Vec<StatsBucket>, StatsError> {
        let store = self
            .buckets
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(store
            .values()
            .filter(|bucket| bucket.app_id == app_id)
            .cloned()
            .collect())
    }
}

enum StatsCommand {
    Observe {
        observation: StatsObservation,
        acknowledgement: Option<crossfire::oneshot::TxOneshot<Result<(), StatsError>>>,
    },
    Flush(crossfire::oneshot::TxOneshot<Result<(), StatsError>>),
}

type StatsChannel = mpsc::Array<StatsCommand>;
type StatsSender = crossfire::MAsyncTx<StatsChannel>;
type StatsReceiver = crossfire::AsyncRx<StatsChannel>;

pub(crate) struct StatsAggregator {
    store: Arc<dyn StatsStore>,
    sender: Option<StatsSender>,
    metrics: Arc<StatsRuntimeMetrics>,
    connections: dashmap::DashMap<String, Arc<AtomicU64>>,
    channels: dashmap::DashMap<String, Arc<AtomicU64>>,
}

impl Default for StatsAggregator {
    fn default() -> Self {
        Self {
            store: Arc::new(MemoryStatsStore::default()),
            sender: None,
            metrics: Arc::new(StatsRuntimeMetrics::default()),
            connections: dashmap::DashMap::new(),
            channels: dashmap::DashMap::new(),
        }
    }
}

impl StatsAggregator {
    pub(crate) fn new(
        cache: Option<Arc<dyn CacheManager>>,
        config: StatsRuntimeConfig,
    ) -> Arc<Self> {
        let store: Arc<dyn StatsStore> = cache.map_or_else(
            || Arc::new(MemoryStatsStore::default()) as Arc<dyn StatsStore>,
            |cache| Arc::new(CacheStatsStore::new(cache, config)) as Arc<dyn StatsStore>,
        );
        let metrics = Arc::new(StatsRuntimeMetrics {
            queue_capacity: config.queue_capacity,
            ..StatsRuntimeMetrics::default()
        });
        let sender = tokio::runtime::Handle::try_current().ok().map(|runtime| {
            let (sender, receiver) = mpsc::bounded_async(config.queue_capacity.max(1));
            runtime.spawn(run_stats_worker(
                Arc::clone(&store),
                receiver,
                Arc::clone(&metrics),
                config.flush_interval,
            ));
            sender
        });
        Arc::new(Self {
            store,
            sender,
            metrics,
            connections: dashmap::DashMap::new(),
            channels: dashmap::DashMap::new(),
        })
    }

    pub(crate) fn snapshot(&self) -> StatsRuntimeSnapshot {
        self.metrics.snapshot()
    }

    pub(crate) async fn record(&self, observation: StatsObservation) -> Result<(), StatsError> {
        let Some(sender) = &self.sender else {
            return self.store.merge(&[observation.bucket]).await;
        };
        let (acknowledgement, receiver) = oneshot::oneshot();
        self.metrics.backlog.fetch_add(1, Ordering::AcqRel);
        if sender
            .send(StatsCommand::Observe {
                observation,
                acknowledgement: Some(acknowledgement),
            })
            .await
            .is_err()
        {
            self.metrics.backlog.fetch_sub(1, Ordering::AcqRel);
            return Err(StatsError::Closed);
        }
        receiver.await.map_err(|_| StatsError::Closed)?
    }

    pub(crate) fn try_record(&self, observation: StatsObservation) -> Result<(), StatsError> {
        let Some(sender) = &self.sender else {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(StatsError::Closed);
        };
        self.metrics.backlog.fetch_add(1, Ordering::AcqRel);
        match sender.try_send(StatsCommand::Observe {
            observation,
            acknowledgement: None,
        }) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.metrics.backlog.fetch_sub(1, Ordering::AcqRel);
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                Err(StatsError::QueueFull)
            }
            Err(TrySendError::Disconnected(_)) => {
                self.metrics.backlog.fetch_sub(1, Ordering::AcqRel);
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                Err(StatsError::Closed)
            }
        }
    }

    pub(crate) async fn flush(&self) -> Result<(), StatsError> {
        let Some(sender) = &self.sender else {
            return Ok(());
        };
        let (acknowledgement, receiver) = oneshot::oneshot();
        sender
            .send(StatsCommand::Flush(acknowledgement))
            .await
            .map_err(|_| StatsError::Closed)?;
        receiver.await.map_err(|_| StatsError::Closed)?
    }

    pub(crate) async fn ingest_fixtures(
        &self,
        app_id: &str,
        fixtures: Vec<Value>,
    ) -> Result<(), StatsError> {
        let mut buckets = Vec::with_capacity(fixtures.len());
        for fixture in fixtures {
            buckets.push(bucket_from_fixture(app_id, &fixture)?);
        }
        self.store.put(&buckets).await
    }

    pub(crate) async fn query(
        &self,
        app_id: &str,
        query: &StatsQuery,
        now_ms: i64,
    ) -> Result<StatsPage, StatsError> {
        self.flush().await?;
        let minute_buckets = self.store.list(app_id).await?;
        let mut rollups = BTreeMap::<String, StatsBucket>::new();
        for bucket in minute_buckets {
            let interval_id = query.unit.rollup_id(&bucket.interval_id)?;
            let rollup = rollups
                .entry(interval_id.clone())
                .or_insert_with(|| StatsBucket {
                    app_id: app_id.to_string(),
                    interval_id,
                    entries: BTreeMap::new(),
                });
            rollup.merge(&bucket);
        }

        let decoded_cursor = query.cursor.as_deref().map(decode_cursor).transpose()?;
        if let Some(cursor) = &decoded_cursor {
            validate_cursor(cursor, app_id, query)?;
        }
        let horizon = decoded_cursor
            .as_ref()
            .map(|cursor| cursor.horizon.clone())
            .or_else(|| rollups.keys().next_back().cloned())
            .unwrap_or_default();

        let mut buckets = rollups
            .into_values()
            .filter(|bucket| {
                query
                    .start
                    .as_ref()
                    .is_none_or(|start| bucket.interval_id >= *start)
                    && query
                        .end
                        .as_ref()
                        .is_none_or(|end| bucket.interval_id <= *end)
                    && (horizon.is_empty() || bucket.interval_id <= horizon)
            })
            .collect::<Vec<_>>();
        buckets.sort_by(|left, right| left.interval_id.cmp(&right.interval_id));
        if query.direction == StatsDirection::Backwards {
            buckets.reverse();
        }
        if let Some(cursor) = &decoded_cursor {
            buckets.retain(|bucket| match query.direction {
                StatsDirection::Forwards => bucket.interval_id > cursor.after,
                StatsDirection::Backwards => bucket.interval_id < cursor.after,
            });
        }

        let has_more = buckets.len() > query.limit;
        buckets.truncate(query.limit);
        let current_id = query.unit.interval_id(now_ms)?;
        let items = buckets
            .iter()
            .map(|bucket| StatsInterval {
                interval_id: bucket.interval_id.clone(),
                unit: query.unit.as_str().to_string(),
                app_id: bucket.app_id.clone(),
                schema: "sockudo:ably-stats:v1".to_string(),
                entries: bucket
                    .entries
                    .iter()
                    .map(|(name, value)| (name.clone(), value.value))
                    .collect(),
                in_progress: (bucket.interval_id == current_id).then(|| bucket.interval_id.clone()),
            })
            .collect::<Vec<_>>();
        let next_cursor = has_more
            .then(|| buckets.last())
            .flatten()
            .map(|last| {
                encode_cursor(&StatsCursor {
                    version: STATS_CURSOR_VERSION,
                    app_id: app_id.to_string(),
                    unit: query.unit,
                    direction: query.direction,
                    start: query.start.clone(),
                    end: query.end.clone(),
                    horizon,
                    after: last.interval_id.clone(),
                })
            })
            .transpose()?;
        Ok(StatsPage { items, next_cursor })
    }

    pub(crate) fn connection_opened(&self, app_id: &str) -> u64 {
        let current = self
            .connections
            .entry(app_id.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        current.fetch_add(1, Ordering::AcqRel).saturating_add(1)
    }

    pub(crate) fn connection_closed(&self, app_id: &str) {
        if let Some(current) = self.connections.get(app_id) {
            let _ = current.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                Some(value.saturating_sub(1))
            });
        }
    }

    pub(crate) fn channel_opened(&self, app_id: &str) -> u64 {
        let current = self
            .channels
            .entry(app_id.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        current.fetch_add(1, Ordering::AcqRel).saturating_add(1)
    }

    pub(crate) fn channel_closed(&self, app_id: &str) {
        if let Some(current) = self.channels.get(app_id) {
            let _ = current.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                Some(value.saturating_sub(1))
            });
        }
    }
}

async fn run_stats_worker(
    store: Arc<dyn StatsStore>,
    receiver: StatsReceiver,
    metrics: Arc<StatsRuntimeMetrics>,
    flush_interval: Duration,
) {
    while let Ok(first) = receiver.recv().await {
        let should_wait_for_batch = !flush_interval.is_zero()
            && matches!(&first, StatsCommand::Observe { .. })
            && receiver.is_empty();
        let mut commands = vec![first];
        if should_wait_for_batch {
            tokio::time::sleep(flush_interval).await;
        }
        while commands.len() < 256 {
            match receiver.try_recv() {
                Ok(command) => commands.push(command),
                Err(_) => break,
            }
        }

        let mut buckets = BTreeMap::<(String, String), StatsBucket>::new();
        let mut acknowledgements = Vec::new();
        let mut barriers = Vec::new();
        let mut oldest = None::<i64>;
        let mut observations = 0u64;
        for command in commands {
            match command {
                StatsCommand::Observe {
                    observation,
                    acknowledgement,
                } => {
                    oldest = Some(oldest.map_or(observation.observed_at_ms, |current| {
                        current.min(observation.observed_at_ms)
                    }));
                    let key = (
                        observation.bucket.app_id.clone(),
                        observation.bucket.interval_id.clone(),
                    );
                    buckets
                        .entry(key)
                        .and_modify(|bucket| bucket.merge(&observation.bucket))
                        .or_insert(observation.bucket);
                    if let Some(acknowledgement) = acknowledgement {
                        acknowledgements.push(acknowledgement);
                    }
                    observations = observations.saturating_add(1);
                }
                StatsCommand::Flush(acknowledgement) => barriers.push(acknowledgement),
            }
        }
        let batch = buckets.into_values().collect::<Vec<_>>();
        let result = if batch.is_empty() {
            Ok(())
        } else {
            store.merge(&batch).await
        };
        if result.is_err() {
            metrics.flush_failures.fetch_add(1, Ordering::Relaxed);
        } else if !batch.is_empty() {
            metrics.flushed_batches.fetch_add(1, Ordering::Relaxed);
            metrics
                .flushed_observations
                .fetch_add(observations, Ordering::Relaxed);
            let lag = oldest
                .map(|oldest| Utc::now().timestamp_millis().saturating_sub(oldest))
                .and_then(|lag| u64::try_from(lag).ok())
                .unwrap_or_default();
            metrics.last_flush_lag_ms.store(lag, Ordering::Release);
        }
        metrics.backlog.fetch_sub(
            usize::try_from(observations).unwrap_or(usize::MAX),
            Ordering::AcqRel,
        );
        for acknowledgement in acknowledgements {
            acknowledgement.send(result.clone());
        }
        for barrier in barriers {
            barrier.send(result.clone());
        }
    }
}

fn bucket_from_fixture(app_id: &str, fixture: &Value) -> Result<StatsBucket, StatsError> {
    let object = fixture.as_object().ok_or_else(|| {
        StatsError::InvalidFixture("stats interval must be an object".to_string())
    })?;
    let interval_id = object
        .get("intervalId")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            StatsError::InvalidFixture("stats interval requires intervalId".to_string())
        })?;
    let mut bucket = StatsBucket::from_interval_id(app_id, interval_id)?;
    for direction in ["inbound", "outbound"] {
        let Some(transports) = object.get(direction).and_then(Value::as_object) else {
            continue;
        };
        for values in transports.values() {
            collect_fixture_numbers(&format!("messages.{direction}.all"), values, &mut bucket, 0)?;
        }
    }
    for (name, value) in object {
        if matches!(name.as_str(), "intervalId" | "inbound" | "outbound") {
            continue;
        }
        collect_fixture_numbers(name, value, &mut bucket, 0)?;
    }
    Ok(bucket)
}

fn collect_fixture_numbers(
    prefix: &str,
    value: &Value,
    bucket: &mut StatsBucket,
    depth: usize,
) -> Result<(), StatsError> {
    if prefix.len() > MAX_FIELD_NAME_BYTES {
        return Err(StatsError::InvalidFixture(format!(
            "stats field name exceeds {MAX_FIELD_NAME_BYTES} bytes"
        )));
    }
    if let Some(number) = value.as_u64() {
        if !bucket.entries.contains_key(prefix) && bucket.entries.len() >= MAX_FIXTURE_FIELDS {
            return Err(StatsError::InvalidFixture(format!(
                "stats interval exceeds the {MAX_FIXTURE_FIELDS}-field limit"
            )));
        }
        bucket.add(
            prefix,
            number,
            if prefix.ends_with(".peak") || prefix == "channels.peak" {
                FieldAggregation::Peak
            } else {
                FieldAggregation::Sum
            },
        );
        return Ok(());
    }
    if let Some(object) = value.as_object() {
        if depth >= MAX_FIXTURE_DEPTH {
            return Err(StatsError::InvalidFixture(format!(
                "stats fixture nesting exceeds {MAX_FIXTURE_DEPTH} levels"
            )));
        }
        for (name, value) in object {
            collect_fixture_numbers(&format!("{prefix}.{name}"), value, bucket, depth + 1)?;
        }
    }
    Ok(())
}

fn parse_interval_start(value: &str) -> Result<DateTime<Utc>, StatsError> {
    let parsed = match value.len() {
        7 => NaiveDate::parse_from_str(&format!("{value}-01"), "%Y-%m-%d")
            .ok()
            .and_then(|date| date.and_hms_opt(0, 0, 0)),
        10 => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .ok()
            .and_then(|date| date.and_hms_opt(0, 0, 0)),
        13 => NaiveDateTime::parse_from_str(&format!("{value}:00"), "%Y-%m-%d:%H:%M").ok(),
        16 => NaiveDateTime::parse_from_str(value, "%Y-%m-%d:%H:%M").ok(),
        _ => None,
    }
    .ok_or_else(|| StatsError::InvalidQuery("invalid stats interval ID".to_string()))?;
    Ok(Utc.from_utc_datetime(&parsed))
}

fn normalize_bound(value: &str, unit: StatsUnit, is_end: bool) -> Result<String, StatsError> {
    if let Ok(timestamp_ms) = value.parse::<i64>() {
        return unit.interval_id(timestamp_ms);
    }
    let start = parse_interval_start(value)?;
    let timestamp = if is_end {
        match value.len() {
            7 => {
                let (year, month) = if start.month() == 12 {
                    (start.year() + 1, 1)
                } else {
                    (start.year(), start.month() + 1)
                };
                Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
                    .single()
                    .ok_or_else(|| {
                        StatsError::InvalidQuery("invalid stats interval ID".to_string())
                    })?
                    - chrono::Duration::milliseconds(1)
            }
            10 => start + chrono::Duration::days(1) - chrono::Duration::milliseconds(1),
            13 => start + chrono::Duration::hours(1) - chrono::Duration::milliseconds(1),
            16 => start + chrono::Duration::minutes(1) - chrono::Duration::milliseconds(1),
            _ => start,
        }
    } else {
        start
    };
    unit.interval_id(timestamp.timestamp_millis())
}

fn encode_cursor(cursor: &StatsCursor) -> Result<String, StatsError> {
    let encoded =
        serde_json::to_vec(cursor).map_err(|error| StatsError::Store(error.to_string()))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(encoded))
}

fn decode_cursor(cursor: &str) -> Result<StatsCursor, StatsError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| StatsError::InvalidQuery("invalid stats cursor".to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| StatsError::InvalidQuery("invalid stats cursor".to_string()))
}

fn validate_cursor(
    cursor: &StatsCursor,
    app_id: &str,
    query: &StatsQuery,
) -> Result<(), StatsError> {
    if cursor.version != STATS_CURSOR_VERSION
        || cursor.app_id != app_id
        || cursor.unit != query.unit
        || cursor.direction != query.direction
        || cursor.start != query.start
        || cursor.end != query.end
    {
        return Err(StatsError::InvalidQuery(
            "stats cursor does not match this query".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sockudo_cache::{RedisCacheManager, memory_cache_manager::MemoryCacheManager};
    use sockudo_core::options::MemoryCacheOptions;

    fn config() -> StatsRuntimeConfig {
        StatsRuntimeConfig {
            queue_capacity: 32,
            flush_interval: Duration::ZERO,
            retention_seconds: 60,
            max_scan_entries: 100,
            cas_retries: 4,
        }
    }

    #[tokio::test]
    async fn fixture_ingestion_and_rollups_share_the_store_contract() {
        let aggregator = StatsAggregator::new(None, config());
        aggregator
            .ingest_fixtures(
                "app",
                vec![
                    json!({
                        "intervalId": "2026-02-03:15:03",
                        "inbound": { "realtime": { "messages": { "count": 50, "data": 5000 } } },
                        "connections": { "tls": { "peak": 20, "opened": 10 } }
                    }),
                    json!({
                        "intervalId": "2026-02-03:15:04",
                        "inbound": { "realtime": { "messages": { "count": 60, "data": 6000 } } },
                        "connections": { "tls": { "peak": 15, "opened": 5 } }
                    }),
                ],
            )
            .await
            .unwrap();

        let hour = aggregator
            .query(
                "app",
                &StatsQuery::parse(
                    "hour",
                    Some("forwards"),
                    Some("2026-02-03:15"),
                    Some("2026-02-03:15"),
                    None,
                    None,
                )
                .unwrap(),
                0,
            )
            .await
            .unwrap();
        assert_eq!(hour.items.len(), 1);
        assert_eq!(
            hour.items[0].entries["messages.inbound.all.messages.count"],
            110
        );
        assert_eq!(hour.items[0].entries["connections.tls.opened"], 15);
        assert_eq!(hour.items[0].entries["connections.tls.peak"], 20);

        for (unit, expected) in [("day", "2026-02-03"), ("month", "2026-02")] {
            let page = aggregator
                .query(
                    "app",
                    &StatsQuery::parse(unit, Some("forwards"), None, None, None, None).unwrap(),
                    0,
                )
                .await
                .unwrap();
            assert_eq!(page.items[0].interval_id, expected);
        }
    }

    #[tokio::test]
    async fn fixture_ingestion_enforces_field_and_depth_bounds() {
        let aggregator = StatsAggregator::new(None, config());
        let mut too_many = serde_json::Map::new();
        too_many.insert(
            "intervalId".to_string(),
            Value::String("2026-02-03:15:03".to_string()),
        );
        for index in 0..=MAX_FIXTURE_FIELDS {
            too_many.insert(format!("field{index}"), Value::from(index));
        }
        let error = aggregator
            .ingest_fixtures("app", vec![Value::Object(too_many)])
            .await
            .unwrap_err();
        assert!(matches!(error, StatsError::InvalidFixture(_)));

        let mut nested = Value::from(1);
        for _ in 0..=MAX_FIXTURE_DEPTH {
            nested = json!({ "nested": nested });
        }
        let error = aggregator
            .ingest_fixtures(
                "app",
                vec![json!({
                    "intervalId": "2026-02-03:15:03",
                    "nested": nested,
                })],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, StatsError::InvalidFixture(_)));
    }

    #[tokio::test]
    async fn cursor_is_interval_anchored_and_rejects_query_changes() {
        let aggregator = StatsAggregator::new(None, config());
        aggregator
            .ingest_fixtures(
                "app",
                [3, 4, 5]
                    .into_iter()
                    .map(|minute| {
                        json!({
                            "intervalId": format!("2026-02-03:15:{minute:02}"),
                            "inbound": { "realtime": { "messages": { "data": minute * 1000 } } }
                        })
                    })
                    .collect(),
            )
            .await
            .unwrap();
        let query = StatsQuery::parse(
            "minute",
            Some("forwards"),
            None,
            Some("2026-02-03:15:05"),
            Some(1),
            None,
        )
        .unwrap();
        let first = aggregator.query("app", &query, 0).await.unwrap();
        assert_eq!(first.items[0].interval_id, "2026-02-03:15:03");
        let cursor = first.next_cursor.unwrap();

        aggregator
            .ingest_fixtures(
                "app",
                vec![json!({
                    "intervalId": "2026-02-03:15:02",
                    "inbound": { "realtime": { "messages": { "data": 2000 } } }
                })],
            )
            .await
            .unwrap();
        let second_query = StatsQuery {
            cursor: Some(cursor.clone()),
            ..query.clone()
        };
        let second = aggregator.query("app", &second_query, 0).await.unwrap();
        assert_eq!(second.items[0].interval_id, "2026-02-03:15:04");

        let changed = StatsQuery {
            direction: StatsDirection::Backwards,
            cursor: Some(cursor),
            ..query
        };
        assert!(aggregator.query("app", &changed, 0).await.is_err());
    }

    #[tokio::test]
    async fn cache_store_merges_concurrent_nodes_and_survives_runtime_recreation() {
        let cache = Arc::new(MemoryCacheManager::new(
            "stats-store-test".to_string(),
            MemoryCacheOptions::default(),
        )) as Arc<dyn CacheManager>;
        let first = StatsAggregator::new(Some(Arc::clone(&cache)), config());
        let second = StatsAggregator::new(Some(Arc::clone(&cache)), config());
        let timestamp = 1_770_134_580_000;
        let (first_result, second_result) = tokio::join!(
            first.record(
                StatsObservation::messages("app", timestamp, "inbound", "realtime", 2, 20,)
                    .unwrap()
            ),
            second.record(
                StatsObservation::messages("app", timestamp, "inbound", "realtime", 3, 30,)
                    .unwrap()
            )
        );
        first_result.unwrap();
        second_result.unwrap();

        let recreated = StatsAggregator::new(Some(cache), config());
        let page = recreated
            .query(
                "app",
                &StatsQuery::parse("minute", None, None, None, None, None).unwrap(),
                timestamp,
            )
            .await
            .unwrap();
        assert_eq!(
            page.items[0].entries["messages.inbound.all.messages.count"],
            5
        );
        assert_eq!(
            page.items[0].entries["messages.inbound.all.messages.data"],
            50
        );
    }

    #[tokio::test]
    async fn redis_store_merges_independent_nodes_and_survives_rolling_restart() {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string());
        let prefix = format!("ably-stats-test-{}", uuid::Uuid::new_v4().simple());
        let first_cache = Arc::new(
            RedisCacheManager::with_url(&redis_url, Some(&prefix))
                .await
                .expect("configured Redis test service must be available"),
        ) as Arc<dyn CacheManager>;
        let second_cache = Arc::new(
            RedisCacheManager::with_url(&redis_url, Some(&prefix))
                .await
                .expect("configured Redis test service must be available"),
        ) as Arc<dyn CacheManager>;
        let first = StatsAggregator::new(Some(first_cache), config());
        let second = StatsAggregator::new(Some(second_cache), config());
        let timestamp = 1_770_134_580_000;

        let (first_result, second_result) = tokio::join!(
            first.record(
                StatsObservation::messages("app", timestamp, "inbound", "realtime", 2, 20).unwrap()
            ),
            second.record(
                StatsObservation::messages("app", timestamp, "inbound", "realtime", 3, 30).unwrap()
            )
        );
        first_result.unwrap();
        second_result.unwrap();
        drop(first);
        drop(second);

        let restarted_cache = Arc::new(
            RedisCacheManager::with_url(&redis_url, Some(&prefix))
                .await
                .expect("configured Redis test service must be available"),
        ) as Arc<dyn CacheManager>;
        let restarted = StatsAggregator::new(Some(restarted_cache), config());
        let page = restarted
            .query(
                "app",
                &StatsQuery::parse("minute", None, None, None, None, None).unwrap(),
                timestamp,
            )
            .await
            .unwrap();
        assert_eq!(
            page.items[0].entries["messages.inbound.all.messages.count"],
            5
        );
        assert_eq!(
            page.items[0].entries["messages.inbound.all.messages.data"],
            50
        );
    }

    #[tokio::test]
    async fn acknowledged_observation_is_flushed_before_record_returns() {
        let aggregator = StatsAggregator::new(None, config());
        aggregator
            .record(
                StatsObservation::messages("app", 1_770_134_580_000, "inbound", "realtime", 1, 4)
                    .unwrap(),
            )
            .await
            .unwrap();
        let page = aggregator
            .query(
                "app",
                &StatsQuery::parse("minute", None, None, None, None, None).unwrap(),
                1_770_134_580_000,
            )
            .await
            .unwrap();
        assert_eq!(
            page.items[0].entries["messages.inbound.all.messages.count"],
            1
        );
        assert_eq!(aggregator.snapshot().backlog, 0);
    }

    #[tokio::test]
    async fn live_observations_reconcile_with_worker_counters() {
        let aggregator = StatsAggregator::new(None, config());
        let timestamp = 1_770_134_580_000;
        let current_connections = aggregator.connection_opened("app");
        let current_channels = aggregator.channel_opened("app");
        let observations = [
            StatsObservation::messages("app", timestamp, "inbound", "rest", 2, 10).unwrap(),
            StatsObservation::messages("app", timestamp, "outbound", "realtime", 3, 30).unwrap(),
            StatsObservation::api_request("app", timestamp, true, 40, 50).unwrap(),
            StatsObservation::token_request("app", timestamp, false).unwrap(),
            StatsObservation::connection_opened("app", timestamp, "tls", current_connections)
                .unwrap(),
            StatsObservation::channel_opened("app", timestamp, current_channels).unwrap(),
            StatsObservation::presence("app", timestamp, 4, 60).unwrap(),
        ];
        for observation in observations {
            aggregator.record(observation).await.unwrap();
        }

        let page = aggregator
            .query(
                "app",
                &StatsQuery::parse("minute", None, None, None, None, None).unwrap(),
                timestamp,
            )
            .await
            .unwrap();
        let entries = &page.items[0].entries;
        assert_eq!(entries["messages.inbound.all.messages.count"], 2);
        assert_eq!(entries["messages.outbound.realtime.messages.data"], 30);
        assert_eq!(entries["apiRequests.succeeded"], 1);
        assert_eq!(entries["apiRequests.requestBytes"], 40);
        assert_eq!(entries["tokenRequests.failed"], 1);
        assert_eq!(entries["connections.tls.peak"], 1);
        assert_eq!(entries["channels.peak"], 1);
        assert_eq!(entries["persisted.presence.count"], 4);

        let snapshot = aggregator.snapshot();
        assert_eq!(snapshot.flushed_observations, 7);
        assert_eq!(snapshot.backlog, 0);
        assert_eq!(snapshot.dropped, 0);
    }

    #[tokio::test]
    async fn nonblocking_recording_never_exceeds_the_bounded_queue() {
        let aggregator = StatsAggregator::new(
            None,
            StatsRuntimeConfig {
                queue_capacity: 2,
                ..config()
            },
        );
        for _ in 0..2 {
            aggregator
                .try_record(
                    StatsObservation::messages(
                        "app",
                        1_770_134_580_000,
                        "outbound",
                        "realtime",
                        1,
                        4,
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let error = aggregator
            .try_record(
                StatsObservation::messages("app", 1_770_134_580_000, "outbound", "realtime", 1, 4)
                    .unwrap(),
            )
            .unwrap_err();

        assert!(matches!(error, StatsError::QueueFull));
        let snapshot = aggregator.snapshot();
        assert_eq!(snapshot.backlog, snapshot.queue_capacity);
        assert_eq!(snapshot.dropped, 1);
    }
}
