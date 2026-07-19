use crate::ArcJobProcessorFn;
use crate::redis_connection::QueueRedisProvider;
use crate::redis_scripts::QueueScripts;
use futures_util::FutureExt;
use metrics::{Counter, Histogram};
use parking_lot::Mutex;
use redis::FromRedisValue;
use serde::{Deserialize, Serialize};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::QueueReliabilityConfig;
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueHealth, QueueJobId, QueueJobOptions, QueueJobRequest,
    QueueStats,
};
use sockudo_core::webhook_types::JobData;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const ENQUEUE_ATTEMPTS: usize = 3;
const ENQUEUE_RETRY_BASE: Duration = Duration::from_millis(50);
const MAX_ID_LENGTH: usize = 512;
const MAX_REDIS_SCRIPT_BATCH: usize = 1_000;
const MAX_FAST_TRANSACTION_JOBS: usize = 20_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisJobEnvelope {
    version: u8,
    id: String,
    data: JobData,
}

struct PreparedJob {
    id: String,
    options: QueueJobOptions,
    serialized: String,
}

fn pack_generated_batch(jobs: &[PreparedJob]) -> Result<Vec<u8>> {
    let capacity = jobs.iter().try_fold(5_usize, |capacity, job| {
        capacity
            .checked_add(5)
            .and_then(|capacity| capacity.checked_add(job.serialized.len()))
            .ok_or_else(|| Error::Queue("generated queue batch is too large".to_string()))
    })?;
    let batch_count = u32::try_from(jobs.len())
        .map_err(|_| Error::Queue("generated queue batch is too large".to_string()))?;
    let mut batch = Vec::with_capacity(capacity);
    batch.push(b'B');
    batch.extend_from_slice(&batch_count.to_be_bytes());
    for job in jobs {
        let record_length = job
            .serialized
            .len()
            .checked_add(1)
            .and_then(|length| u32::try_from(length).ok())
            .ok_or_else(|| Error::Queue("generated queue job is too large".to_string()))?;
        batch.extend_from_slice(&record_length.to_be_bytes());
        batch.push(b'J');
        batch.extend_from_slice(job.serialized.as_bytes());
    }
    Ok(batch)
}

#[derive(Debug)]
struct ClaimedJob {
    id: String,
    data: JobData,
    attempt: u32,
    maximum: u32,
    token: Arc<str>,
}

struct NackRequest<'a> {
    id: &'a str,
    token: &'a str,
    reason: &'a str,
    force_dead_letter: bool,
    delay_ms: u64,
}

struct SettlementRequest {
    id: String,
    worker_token: Arc<str>,
}

type LeaseRegistry = Arc<Mutex<HashMap<String, Arc<str>>>>;

#[derive(Clone)]
struct WorkerMetrics {
    claimed: Counter,
    completed: Counter,
    retried: Counter,
    dead_lettered: Counter,
    panicked_or_lease_lost: Counter,
    processing_duration: Histogram,
}

impl WorkerMetrics {
    fn new(backend: QueueBackendKind, queue_name: &str) -> Self {
        let backend = backend.as_str();
        Self {
            claimed: metrics::counter!(
                "sockudo_queue_claimed_total",
                "backend" => backend,
                "queue" => queue_name.to_string(),
            ),
            completed: metrics::counter!(
                "sockudo_queue_settled_total",
                "backend" => backend,
                "queue" => queue_name.to_string(),
                "outcome" => "completed",
            ),
            retried: metrics::counter!(
                "sockudo_queue_settled_total",
                "backend" => backend,
                "queue" => queue_name.to_string(),
                "outcome" => "retry",
            ),
            dead_lettered: metrics::counter!(
                "sockudo_queue_settled_total",
                "backend" => backend,
                "queue" => queue_name.to_string(),
                "outcome" => "dead-letter",
            ),
            panicked_or_lease_lost: metrics::counter!(
                "sockudo_queue_settled_total",
                "backend" => backend,
                "queue" => queue_name.to_string(),
                "outcome" => "panic-or-lease-lost",
            ),
            processing_duration: metrics::histogram!(
                "sockudo_queue_processing_duration_seconds",
                "backend" => backend,
                "queue" => queue_name.to_string(),
            ),
        }
    }
}

#[derive(Clone)]
struct WorkerShared {
    keys: RedisQueueKeys,
    processor: ArcJobProcessorFn,
    metrics: WorkerMetrics,
    settlement_tx: mpsc::Sender<SettlementRequest>,
    leases: LeaseRegistry,
}

struct WorkerContext {
    queue_name: String,
    worker_id: usize,
    jobs: mpsc::Receiver<ClaimedJob>,
    shared: WorkerShared,
}

#[derive(Debug, Clone)]
struct RedisQueueKeys {
    jobs: String,
    ready: String,
    inline_ready: String,
    inline_ready_count: String,
    delayed: String,
    active: String,
    tokens: String,
    attempts: String,
    maxima: String,
    dead_letter: String,
    failures: String,
    completed: String,
    dedup: String,
    dedup_expiry: String,
    notify: String,
    events: String,
}

impl RedisQueueKeys {
    fn new(prefix: &str, queue_name: &str) -> Result<Self> {
        let queue_id = encode_queue_name(queue_name)?;
        let base = format!("{}:v2:{{{queue_id}}}", prefix.trim_end_matches(':'));
        Ok(Self {
            jobs: format!("{base}:jobs"),
            ready: format!("{base}:wait"),
            inline_ready: format!("{base}:inline-wait"),
            inline_ready_count: format!("{base}:inline-wait-count"),
            delayed: format!("{base}:delayed"),
            active: format!("{base}:active"),
            tokens: format!("{base}:tokens"),
            attempts: format!("{base}:attempts"),
            maxima: format!("{base}:max-attempts"),
            dead_letter: format!("{base}:dlq"),
            failures: format!("{base}:failures"),
            completed: format!("{base}:completed"),
            dedup: format!("{base}:dedup"),
            dedup_expiry: format!("{base}:dedup-expiry"),
            notify: format!("{base}:notify"),
            events: format!("{base}:events"),
        })
    }

    #[cfg(test)]
    fn all(&self) -> [&str; 16] {
        [
            &self.jobs,
            &self.ready,
            &self.inline_ready,
            &self.inline_ready_count,
            &self.delayed,
            &self.active,
            &self.tokens,
            &self.attempts,
            &self.maxima,
            &self.dead_letter,
            &self.failures,
            &self.completed,
            &self.dedup,
            &self.dedup_expiry,
            &self.notify,
            &self.events,
        ]
    }
}

fn encode_queue_name(queue_name: &str) -> Result<String> {
    if queue_name.is_empty() {
        return Err(Error::Queue("queue name must not be empty".to_string()));
    }
    if queue_name.len() > MAX_ID_LENGTH {
        return Err(Error::Queue(format!(
            "queue name exceeds {MAX_ID_LENGTH} bytes"
        )));
    }
    let mut encoded = String::with_capacity(queue_name.len());
    for byte in queue_name.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':') {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(&mut encoded, "%{byte:02X}");
        }
    }
    Ok(encoded)
}

struct Inner<P: QueueRedisProvider> {
    provider: P,
    prefix: String,
    concurrency: usize,
    response_timeout: Option<Duration>,
    config: QueueReliabilityConfig,
    scripts: QueueScripts,
    shutdown: CancellationToken,
    accepting: AtomicBool,
    workers: Mutex<Vec<JoinHandle<()>>>,
    registered_queues: Mutex<HashSet<String>>,
}

pub(crate) struct ReliableRedisQueue<P: QueueRedisProvider> {
    inner: Arc<Inner<P>>,
}

impl<P: QueueRedisProvider> Clone for ReliableRedisQueue<P> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<P: QueueRedisProvider> ReliableRedisQueue<P> {
    pub(crate) fn new(
        provider: P,
        prefix: &str,
        concurrency: usize,
        response_timeout_ms: u64,
        config: QueueReliabilityConfig,
    ) -> Result<Self> {
        config.validate().map_err(Error::Config)?;
        if concurrency == 0 {
            return Err(Error::Config(
                "queue concurrency must be greater than zero".to_string(),
            ));
        }
        let prefix = prefix.trim().trim_end_matches(':');
        if prefix.is_empty() || prefix.contains('{') || prefix.contains('}') {
            return Err(Error::Config(
                "queue prefix must be non-empty and must not contain Redis hash-tag braces"
                    .to_string(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                provider,
                prefix: prefix.to_string(),
                concurrency,
                response_timeout: (response_timeout_ms > 0)
                    .then(|| Duration::from_millis(response_timeout_ms)),
                config,
                scripts: QueueScripts::default(),
                shutdown: CancellationToken::new(),
                accepting: AtomicBool::new(true),
                workers: Mutex::new(Vec::new()),
                registered_queues: Mutex::new(HashSet::new()),
            }),
        })
    }

    pub(crate) fn backend(&self) -> QueueBackendKind {
        self.inner.provider.backend()
    }

    pub(crate) fn capabilities(&self) -> QueueCapabilities {
        QueueCapabilities {
            consume: true,
            acknowledgements: true,
            delayed_delivery: true,
            retries: true,
            dead_letter: true,
            deduplication: true,
            leasing: true,
            durable: true,
            batch_enqueue: true,
            observable_lag: true,
        }
    }

    fn redis_batch_size(&self) -> usize {
        self.inner.config.max_batch_size.min(MAX_REDIS_SCRIPT_BATCH)
    }

    async fn await_redis<T, F>(&self, operation: &'static str, future: F) -> Result<T>
    where
        T: FromRedisValue,
        F: Future<Output = redis::RedisResult<T>>,
    {
        let result = match self.inner.response_timeout {
            Some(timeout) => tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| Error::Queue(format!("Redis queue {operation} timed out")))?,
            None => future.await,
        };
        result.map_err(|error| {
            self.inner.provider.invalidate();
            Error::Queue(format!("Redis queue {operation} failed: {error}"))
        })
    }

    fn envelope(
        &self,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<(RedisJobEnvelope, QueueJobOptions)> {
        let id = options
            .job_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
        if id.is_empty() || id.len() > MAX_ID_LENGTH {
            return Err(Error::Queue(format!(
                "queue job ID must contain 1..={MAX_ID_LENGTH} bytes"
            )));
        }
        if options.max_attempts == Some(0) {
            return Err(Error::Queue(
                "queue job max_attempts must be greater than zero".to_string(),
            ));
        }
        if options
            .deduplication_key
            .as_ref()
            .is_some_and(|key| key.is_empty() || key.len() > MAX_ID_LENGTH)
        {
            return Err(Error::Queue(format!(
                "queue deduplication key must contain 1..={MAX_ID_LENGTH} bytes"
            )));
        }
        Ok((
            RedisJobEnvelope {
                version: 2,
                id,
                data,
            },
            options,
        ))
    }

    pub(crate) async fn enqueue(
        &self,
        queue_name: &str,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<QueueJobId> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(Error::Queue("queue is shutting down".to_string()));
        }
        let keys = RedisQueueKeys::new(&self.inner.prefix, queue_name)?;
        let started = Instant::now();
        let (envelope, options) = self.envelope(data, options)?;
        let serialized = sonic_rs::to_string(&envelope)?;
        let now = now_ms();
        let maximum = options
            .max_attempts
            .unwrap_or(self.inner.config.max_attempts);
        let deduplication_key = options.deduplication_key.as_deref().unwrap_or("");
        let dedup_expires = now.saturating_add(self.inner.config.deduplication_ttl_ms);
        let mut last_error = None;

        for attempt in 0..ENQUEUE_ATTEMPTS {
            let mut connection = match self.inner.provider.command_connection().await {
                Ok(connection) => connection,
                Err(error) => {
                    last_error = Some(error);
                    self.inner.provider.invalidate();
                    continue;
                }
            };
            let mut invocation = self.inner.scripts.enqueue.prepare_invoke();
            invocation
                .key(&keys.jobs)
                .key(&keys.ready)
                .key(&keys.delayed)
                .key(&keys.attempts)
                .key(&keys.maxima)
                .key(&keys.dedup)
                .key(&keys.dedup_expiry)
                .key(&keys.notify)
                .key(&keys.events)
                .arg(&envelope.id)
                .arg(&serialized)
                .arg(maximum)
                .arg(now)
                .arg(options.delay_ms)
                .arg(deduplication_key)
                .arg(dedup_expires)
                .arg(self.inner.config.event_retention);
            let future = invocation.invoke_async::<(i64, String)>(&mut connection);
            match self.await_redis("enqueue", future).await {
                Ok((-1, _)) => {
                    return Err(Error::Queue(format!(
                        "queue job ID {} already exists with a different payload",
                        envelope.id
                    )));
                }
                Ok((status, id)) => {
                    let outcome = if status == 0 { "duplicate" } else { "enqueued" };
                    metrics::counter!(
                        "sockudo_queue_enqueue_total",
                        "backend" => self.backend().as_str(),
                        "queue" => queue_name.to_string(),
                        "outcome" => outcome,
                    )
                    .increment(1);
                    metrics::histogram!(
                        "sockudo_queue_enqueue_duration_seconds",
                        "backend" => self.backend().as_str(),
                        "queue" => queue_name.to_string(),
                    )
                    .record(started.elapsed().as_secs_f64());
                    return Ok(QueueJobId(id));
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 < ENQUEUE_ATTEMPTS {
                        tokio::time::sleep(ENQUEUE_RETRY_BASE * (attempt as u32 + 1)).await;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| Error::Queue("Redis enqueue failed".to_string())))
    }

    pub(crate) async fn enqueue_batch(
        &self,
        queue_name: &str,
        jobs: Vec<QueueJobRequest>,
    ) -> Result<Vec<QueueJobId>> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(Error::Queue("queue is shutting down".to_string()));
        }
        let keys = RedisQueueKeys::new(&self.inner.prefix, queue_name)?;
        let generated_fast_path = self.inner.config.event_retention == 0
            && jobs.iter().all(|job| {
                job.options.job_id.is_none()
                    && job.options.delay_ms == 0
                    && job.options.deduplication_key.is_none()
                    && job
                        .options
                        .max_attempts
                        .unwrap_or(self.inner.config.max_attempts)
                        == self.inner.config.max_attempts
            });
        let mut prepared = Vec::with_capacity(jobs.len());
        let mut generated_batch_prefix = uuid::Uuid::new_v4().simple().to_string();
        generated_batch_prefix.truncate(20);
        for (index, request) in jobs.into_iter().enumerate() {
            if generated_fast_path {
                let mut id = String::with_capacity(32);
                id.push_str(&generated_batch_prefix);
                let _ = write!(&mut id, "{index:012x}");
                let data = sonic_rs::to_string(&request.data)?;
                let mut serialized = String::with_capacity(id.len() + data.len());
                serialized.push_str(&id);
                serialized.push_str(&data);
                prepared.push(PreparedJob {
                    id,
                    options: request.options,
                    serialized,
                });
            } else {
                let (envelope, options) = self.envelope(request.data, request.options)?;
                let serialized = sonic_rs::to_string(&envelope)?;
                prepared.push(PreparedJob {
                    id: envelope.id,
                    options,
                    serialized,
                });
            }
        }
        let started = Instant::now();
        let mut ids = Vec::with_capacity(prepared.len());
        let mut enqueued_total = 0_u64;
        let mut duplicate_total = 0_u64;
        let fast_path = prepared.iter().all(|job| {
            job.options.delay_ms == 0
                && job.options.deduplication_key.is_none()
                && job
                    .options
                    .max_attempts
                    .unwrap_or(self.inner.config.max_attempts)
                    == self.inner.config.max_attempts
        });
        if generated_fast_path {
            self.enqueue_generated_batch_fast(&keys, &prepared).await?;
            enqueued_total = prepared.len() as u64;
            ids.extend(prepared.iter().map(|job| QueueJobId(job.id.clone())));
        }
        for chunk in prepared.chunks(self.redis_batch_size()) {
            if generated_fast_path {
                break;
            }
            let now = now_ms();
            if fast_path {
                let mut last_error = None;
                let mut completed = false;
                for attempt in 0..ENQUEUE_ATTEMPTS {
                    let mut connection = match self.inner.provider.command_connection().await {
                        Ok(connection) => connection,
                        Err(error) => {
                            last_error = Some(error);
                            self.inner.provider.invalidate();
                            continue;
                        }
                    };
                    let mut invocation = self.inner.scripts.enqueue_batch_fast.prepare_invoke();
                    invocation
                        .key(&keys.jobs)
                        .key(&keys.ready)
                        .key(&keys.attempts)
                        .key(&keys.maxima)
                        .key(&keys.notify)
                        .key(&keys.events)
                        .arg(now)
                        .arg(chunk.len())
                        .arg(self.inner.config.event_retention)
                        .arg(self.inner.config.max_attempts);
                    for job in chunk {
                        invocation.arg(&job.id).arg(&job.serialized);
                    }
                    let future = invocation.invoke_async::<(i64, String)>(&mut connection);
                    match self.await_redis("fast batch enqueue", future).await {
                        Ok((-1, id)) => {
                            return Err(Error::Queue(format!(
                                "queue job ID {id} already exists with a different payload"
                            )));
                        }
                        Ok((inserted, _)) => {
                            let inserted = u64::try_from(inserted).map_err(|_| {
                                Error::Queue(
                                    "Redis fast batch enqueue returned an invalid count"
                                        .to_string(),
                                )
                            })?;
                            enqueued_total += inserted;
                            duplicate_total += (chunk.len() as u64).saturating_sub(inserted);
                            ids.extend(chunk.iter().map(|job| QueueJobId(job.id.clone())));
                            completed = true;
                            break;
                        }
                        Err(error) => {
                            last_error = Some(error);
                            if attempt + 1 < ENQUEUE_ATTEMPTS {
                                tokio::time::sleep(ENQUEUE_RETRY_BASE * (attempt as u32 + 1)).await;
                            }
                        }
                    }
                }
                if !completed {
                    return Err(last_error.unwrap_or_else(|| {
                        Error::Queue("Redis fast batch enqueue failed".to_string())
                    }));
                }
                continue;
            }
            let mut last_error = None;
            let mut completed = false;
            for attempt in 0..ENQUEUE_ATTEMPTS {
                let mut connection = match self.inner.provider.command_connection().await {
                    Ok(connection) => connection,
                    Err(error) => {
                        last_error = Some(error);
                        self.inner.provider.invalidate();
                        continue;
                    }
                };
                let mut invocation = self.inner.scripts.enqueue_batch.prepare_invoke();
                invocation
                    .key(&keys.jobs)
                    .key(&keys.ready)
                    .key(&keys.delayed)
                    .key(&keys.attempts)
                    .key(&keys.maxima)
                    .key(&keys.dedup)
                    .key(&keys.dedup_expiry)
                    .key(&keys.notify)
                    .key(&keys.events)
                    .arg(now)
                    .arg(chunk.len())
                    .arg(self.inner.config.event_retention);
                for job in chunk {
                    invocation
                        .arg(&job.id)
                        .arg(&job.serialized)
                        .arg(
                            job.options
                                .max_attempts
                                .unwrap_or(self.inner.config.max_attempts),
                        )
                        .arg(job.options.delay_ms)
                        .arg(job.options.deduplication_key.as_deref().unwrap_or(""))
                        .arg(now.saturating_add(self.inner.config.deduplication_ttl_ms));
                }
                let future = invocation.invoke_async::<Vec<(i64, String)>>(&mut connection);
                match self.await_redis("batch enqueue", future).await {
                    Ok(results) => {
                        if let Some((_, id)) = results.iter().find(|(status, _)| *status == -1) {
                            return Err(Error::Queue(format!(
                                "queue job ID {id} already exists with a different payload"
                            )));
                        }
                        if results.len() != chunk.len() {
                            return Err(Error::Queue(format!(
                                "Redis batch enqueue returned {} results for {} jobs",
                                results.len(),
                                chunk.len()
                            )));
                        }
                        for (status, id) in results {
                            if status == 0 {
                                duplicate_total += 1;
                            } else {
                                enqueued_total += 1;
                            }
                            ids.push(QueueJobId(id));
                        }
                        completed = true;
                        break;
                    }
                    Err(error) => {
                        last_error = Some(error);
                        if attempt + 1 < ENQUEUE_ATTEMPTS {
                            tokio::time::sleep(ENQUEUE_RETRY_BASE * (attempt as u32 + 1)).await;
                        }
                    }
                }
            }
            if !completed {
                return Err(last_error
                    .unwrap_or_else(|| Error::Queue("Redis batch enqueue failed".to_string())));
            }
        }
        if enqueued_total > 0 {
            metrics::counter!(
                "sockudo_queue_enqueue_total",
                "backend" => self.backend().as_str(),
                "queue" => queue_name.to_string(),
                "outcome" => "enqueued",
            )
            .increment(enqueued_total);
        }
        if duplicate_total > 0 {
            metrics::counter!(
                "sockudo_queue_enqueue_total",
                "backend" => self.backend().as_str(),
                "queue" => queue_name.to_string(),
                "outcome" => "duplicate",
            )
            .increment(duplicate_total);
        }
        metrics::histogram!(
            "sockudo_queue_batch_enqueue_duration_seconds",
            "backend" => self.backend().as_str(),
            "queue" => queue_name.to_string(),
        )
        .record(started.elapsed().as_secs_f64());
        Ok(ids)
    }

    async fn enqueue_generated_batch_fast(
        &self,
        keys: &RedisQueueKeys,
        jobs: &[PreparedJob],
    ) -> Result<()> {
        for transaction_jobs in jobs.chunks(MAX_FAST_TRANSACTION_JOBS) {
            let batches = transaction_jobs
                .chunks(self.redis_batch_size())
                .map(pack_generated_batch)
                .collect::<Result<Vec<_>>>()?;
            let mut connection = self.inner.provider.command_connection().await?;
            let mut pipeline = redis::pipe();
            pipeline
                .atomic()
                .cmd("LPUSH")
                .arg(&keys.inline_ready)
                .arg(&batches)
                .cmd("INCRBY")
                .arg(&keys.inline_ready_count)
                .arg(transaction_jobs.len())
                .cmd("LPUSH")
                .arg(&keys.notify)
                .arg(1)
                .cmd("LTRIM")
                .arg(&keys.notify)
                .arg(0)
                .arg(1023);
            let future = pipeline.query_async::<()>(&mut connection);
            self.await_redis("generated batch enqueue", future).await?;
        }
        Ok(())
    }

    pub(crate) async fn process_queue(
        &self,
        queue_name: &str,
        processor: ArcJobProcessorFn,
    ) -> Result<()> {
        if self.inner.shutdown.is_cancelled() {
            return Err(Error::Queue("queue is shutting down".to_string()));
        }
        let keys = RedisQueueKeys::new(&self.inner.prefix, queue_name)?;
        {
            let mut registered = self.inner.registered_queues.lock();
            if !registered.insert(queue_name.to_string()) {
                return Err(Error::Queue(format!(
                    "a processor is already registered for queue {queue_name}"
                )));
            }
        }

        let worker_metrics = WorkerMetrics::new(self.backend(), queue_name);
        let buffered_jobs = self
            .inner
            .concurrency
            .saturating_mul(self.inner.config.worker_prefetch)
            .max(1);
        let (settlement_tx, settlement_rx) = mpsc::channel(buffered_jobs.saturating_mul(2));
        let leases: LeaseRegistry = Arc::new(Mutex::new(HashMap::with_capacity(buffered_jobs)));
        let mut worker_senders = Vec::with_capacity(self.inner.concurrency);
        let mut worker_receivers = Vec::with_capacity(self.inner.concurrency);
        for _ in 0..self.inner.concurrency {
            let (sender, receiver) = mpsc::channel(self.inner.config.worker_prefetch);
            worker_senders.push(sender);
            worker_receivers.push(receiver);
        }

        let mut handles = Vec::with_capacity(self.inner.concurrency + 3);
        let coordinator = self.clone();
        let coordinator_keys = keys.clone();
        let coordinator_metrics = worker_metrics.clone();
        let coordinator_leases = Arc::clone(&leases);
        handles.push(tokio::spawn(async move {
            coordinator
                .settlement_loop(
                    coordinator_keys,
                    settlement_rx,
                    coordinator_metrics,
                    coordinator_leases,
                )
                .await;
        }));

        let renewer = self.clone();
        let renewer_keys = keys.clone();
        let renewer_leases = Arc::clone(&leases);
        handles.push(tokio::spawn(async move {
            renewer
                .lease_renewal_loop(renewer_keys, renewer_leases)
                .await;
        }));

        for (worker_id, jobs) in worker_receivers.into_iter().enumerate() {
            let queue = self.clone();
            let context = WorkerContext {
                queue_name: queue_name.to_string(),
                worker_id,
                jobs,
                shared: WorkerShared {
                    keys: keys.clone(),
                    processor: Arc::clone(&processor),
                    metrics: worker_metrics.clone(),
                    settlement_tx: settlement_tx.clone(),
                    leases: Arc::clone(&leases),
                },
            };
            handles.push(tokio::spawn(async move {
                queue.worker_loop(context).await;
            }));
        }
        drop(settlement_tx);

        let prefetcher = self.clone();
        let prefetcher_keys = keys.clone();
        let prefetcher_leases = Arc::clone(&leases);
        handles.push(tokio::spawn(async move {
            prefetcher
                .prefetch_loop(prefetcher_keys, worker_senders, prefetcher_leases)
                .await;
        }));
        self.inner.workers.lock().extend(handles);
        info!(
            backend = ?self.backend(),
            queue = queue_name,
            workers = self.inner.concurrency,
            "started reliable queue workers"
        );
        Ok(())
    }

    async fn worker_loop(&self, mut context: WorkerContext) {
        let mut connection = None;
        while let Some(job) = context.jobs.recv().await {
            context.shared.metrics.claimed.increment(1);
            self.process_claim(&mut connection, &context.shared, job)
                .await;
        }
        debug!(
            backend = ?self.backend(),
            queue = context.queue_name,
            worker_id = context.worker_id,
            "queue worker stopped"
        );
    }

    async fn settlement_loop(
        &self,
        keys: RedisQueueKeys,
        mut requests: mpsc::Receiver<SettlementRequest>,
        metrics: WorkerMetrics,
        leases: LeaseRegistry,
    ) {
        let maximum_batch = self.redis_batch_size();
        let maintenance_interval =
            Duration::from_millis(self.inner.config.worker_poll_interval_ms.max(10));
        let mut next_maintenance = Instant::now() + maintenance_interval;
        let mut connection = None;

        while let Some(first) = requests.recv().await {
            let mut batch = Vec::with_capacity(maximum_batch);
            batch.push(first);
            tokio::task::yield_now().await;
            while batch.len() < maximum_batch {
                match requests.try_recv() {
                    Ok(request) => batch.push(request),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            if connection.is_none() {
                match self.inner.provider.worker_connection().await {
                    Ok(value) => connection = Some(value),
                    Err(error) => {
                        warn!(error = %error, "queue settlement connection failed");
                        for request in &batch {
                            remove_lease(&leases, &request.id, request.worker_token.as_ref());
                        }
                        self.inner.provider.invalidate();
                        continue;
                    }
                }
            }

            let run_maintenance = Instant::now() >= next_maintenance;
            if run_maintenance {
                next_maintenance = Instant::now() + maintenance_interval;
            }
            let result = self
                .ack_batch_and_claim(
                    connection
                        .as_mut()
                        .expect("settlement connection is initialized"),
                    &keys,
                    &batch,
                    run_maintenance,
                )
                .await;
            match result {
                Ok(settled) => {
                    for (request, settled) in batch.into_iter().zip(settled) {
                        remove_lease(&leases, &request.id, request.worker_token.as_ref());
                        if settled {
                            metrics.completed.increment(1);
                        } else {
                            warn!(job_id = request.id, "ignored stale queue acknowledgement");
                        }
                    }
                }
                Err(error) => {
                    warn!(jobs = batch.len(), error = %error, "queue settlement batch failed");
                    connection = None;
                    self.inner.provider.invalidate();
                    for request in &batch {
                        remove_lease(&leases, &request.id, request.worker_token.as_ref());
                    }
                }
            }
        }
        debug!("queue settlement coordinator stopped");
    }

    async fn prefetch_loop(
        &self,
        keys: RedisQueueKeys,
        workers: Vec<mpsc::Sender<ClaimedJob>>,
        leases: LeaseRegistry,
    ) {
        let mut connection = None;
        let mut next_worker = 0_usize;
        loop {
            if self.inner.shutdown.is_cancelled() {
                break;
            }
            let available = workers.iter().map(mpsc::Sender::capacity).sum::<usize>();
            if available == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            if connection.is_none() {
                match self.inner.provider.worker_connection().await {
                    Ok(value) => connection = Some(value),
                    Err(error) => {
                        warn!(error = %error, "queue prefetch connection failed");
                        if self.wait_or_shutdown(Duration::from_millis(250)).await {
                            break;
                        }
                        continue;
                    }
                }
            }

            let limit = available.min(self.redis_batch_size());
            let claimed = self
                .claim_batch(
                    connection
                        .as_mut()
                        .expect("prefetch connection is initialized"),
                    &keys,
                    limit,
                )
                .await;
            match claimed {
                Ok(jobs) if jobs.is_empty() => {
                    if self
                        .wait_for_notification(
                            connection
                                .as_mut()
                                .expect("prefetch connection is initialized"),
                            &keys,
                        )
                        .await
                        .is_err()
                    {
                        connection = None;
                        self.inner.provider.invalidate();
                    }
                }
                Ok(jobs) => {
                    for job in jobs {
                        leases.lock().insert(job.id.clone(), Arc::clone(&job.token));
                        let id = job.id.clone();
                        let token = Arc::clone(&job.token);
                        let worker = &workers[next_worker];
                        next_worker = (next_worker + 1) % workers.len();
                        if worker.send(job).await.is_err() {
                            remove_lease(&leases, &id, token.as_ref());
                        }
                    }
                }
                Err(error) => {
                    warn!(error = %error, "queue batch claim failed; reconnecting");
                    connection = None;
                    self.inner.provider.invalidate();
                    if self.wait_or_shutdown(Duration::from_millis(250)).await {
                        break;
                    }
                }
            }
        }
        debug!("queue prefetcher stopped");
    }

    async fn lease_renewal_loop(&self, keys: RedisQueueKeys, leases: LeaseRegistry) {
        let renew_interval = Duration::from_millis(self.inner.config.lease_renew_interval_ms);
        let shutdown_poll = Duration::from_millis(5);
        let mut next_renewal = Instant::now() + renew_interval;
        let mut connection = None;
        loop {
            let shutting_down = self.inner.shutdown.is_cancelled();
            if shutting_down && leases.lock().is_empty() {
                break;
            }
            let remaining = next_renewal.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                tokio::time::sleep(if shutting_down {
                    remaining.min(shutdown_poll)
                } else {
                    remaining
                })
                .await;
                continue;
            }
            next_renewal = Instant::now() + renew_interval;
            let snapshot = leases
                .lock()
                .iter()
                .map(|(id, token)| (id.clone(), Arc::clone(token)))
                .collect::<Vec<_>>();
            if snapshot.is_empty() {
                if self.inner.shutdown.is_cancelled() {
                    break;
                }
                continue;
            }
            if connection.is_none() {
                match self.inner.provider.worker_connection().await {
                    Ok(value) => connection = Some(value),
                    Err(error) => {
                        warn!(error = %error, "queue lease renewal connection failed");
                        self.inner.provider.invalidate();
                        continue;
                    }
                }
            }
            for batch in snapshot.chunks(self.redis_batch_size()) {
                let result = self
                    .renew_batch(
                        connection
                            .as_mut()
                            .expect("lease renewal connection is initialized"),
                        &keys,
                        batch,
                    )
                    .await;
                match result {
                    Ok(renewed) => {
                        for ((id, token), renewed) in batch.iter().zip(renewed) {
                            if !renewed {
                                remove_lease(&leases, id, token.as_ref());
                                warn!(job_id = id, "queue delivery lease was lost");
                            }
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "queue lease renewal batch failed");
                        connection = None;
                        self.inner.provider.invalidate();
                        break;
                    }
                }
            }
        }
        debug!("queue lease renewer stopped");
    }

    async fn renew_batch(
        &self,
        connection: &mut P::Connection,
        keys: &RedisQueueKeys,
        batch: &[(String, Arc<str>)],
    ) -> Result<Vec<bool>> {
        let deadline = now_ms().saturating_add(self.inner.config.lease_duration_ms);
        let mut invocation = self.inner.scripts.renew.prepare_invoke();
        invocation
            .key(&keys.active)
            .key(&keys.tokens)
            .arg(deadline)
            .arg(batch.len());
        for (id, token) in batch {
            invocation.arg(id).arg(token.as_ref());
        }
        let future = invocation.invoke_async::<Vec<i64>>(connection);
        let renewed = self.await_redis("batch lease renewal", future).await?;
        if renewed.len() != batch.len() {
            return Err(Error::Queue(format!(
                "Redis lease renewal returned {} results for {} jobs",
                renewed.len(),
                batch.len()
            )));
        }
        Ok(renewed.into_iter().map(|value| value == 1).collect())
    }

    async fn claim_batch(
        &self,
        connection: &mut P::Connection,
        keys: &RedisQueueKeys,
        limit: usize,
    ) -> Result<Vec<ClaimedJob>> {
        let now = now_ms();
        let lease_deadline = now.saturating_add(self.inner.config.lease_duration_ms);
        let token_prefix = uuid::Uuid::new_v4().simple().to_string();
        let mut invocation = self.inner.scripts.claim.prepare_invoke();
        invocation
            .key(&keys.jobs)
            .key(&keys.ready)
            .key(&keys.delayed)
            .key(&keys.active)
            .key(&keys.tokens)
            .key(&keys.attempts)
            .key(&keys.maxima)
            .key(&keys.dead_letter)
            .key(&keys.failures)
            .key(&keys.notify)
            .key(&keys.completed)
            .key(&keys.events)
            .key(&keys.inline_ready)
            .key(&keys.inline_ready_count)
            .arg(now)
            .arg(lease_deadline)
            .arg(&token_prefix)
            .arg(self.inner.config.stalled_batch_size)
            .arg(self.inner.config.failed_retention)
            .arg(self.inner.config.event_retention)
            .arg(limit)
            .arg(self.inner.config.max_attempts);
        let future = invocation.invoke_async::<Vec<(String, String, u32, u32, String)>>(connection);
        let results = self.await_redis("batch claim", future).await?;
        let mut jobs = Vec::with_capacity(results.len());
        for (id, serialized, attempt, maximum, token) in results {
            let data = match sonic_rs::from_str::<RedisJobEnvelope>(&serialized) {
                Ok(envelope) if envelope.version == 2 && envelope.id == id => envelope.data,
                Ok(_) | Err(_) => match sonic_rs::from_str::<JobData>(&serialized) {
                    Ok(data) => data,
                    Err(_) => {
                        self.nack(
                            connection,
                            keys,
                            NackRequest {
                                id: &id,
                                token: &token,
                                reason: "invalid-envelope",
                                force_dead_letter: true,
                                delay_ms: 0,
                            },
                        )
                        .await?;
                        warn!(job_id = id, "quarantined invalid Redis queue envelope");
                        continue;
                    }
                },
            };
            jobs.push(ClaimedJob {
                id,
                data,
                attempt,
                maximum,
                token: token.into(),
            });
        }
        Ok(jobs)
    }

    async fn wait_for_notification(
        &self,
        connection: &mut P::Connection,
        keys: &RedisQueueKeys,
    ) -> Result<()> {
        let timeout_seconds = (self.inner.config.worker_poll_interval_ms.max(10) as f64) / 1_000.0;
        let mut command = redis::cmd("BLPOP");
        command.arg(&keys.notify).arg(timeout_seconds);
        let query = command.query_async::<Option<(String, String)>>(connection);
        tokio::select! {
            _ = self.inner.shutdown.cancelled() => Ok(()),
            result = query => result
                .map(|_| ())
                .map_err(|error| Error::Queue(format!("Redis queue notification wait failed: {error}"))),
        }
    }

    async fn process_claim(
        &self,
        connection: &mut Option<P::Connection>,
        shared: &WorkerShared,
        mut job: ClaimedJob,
    ) {
        let processing_started = Instant::now();
        let callback =
            AssertUnwindSafe((shared.processor)(std::mem::take(&mut job.data))).catch_unwind();
        let result = callback.await;
        match result {
            Ok(Ok(())) => {
                let request = SettlementRequest {
                    id: job.id,
                    worker_token: job.token,
                };
                if let Err(error) = shared.settlement_tx.send(request).await {
                    let request = error.0;
                    remove_lease(&shared.leases, &request.id, request.worker_token.as_ref());
                    warn!(
                        job_id = request.id,
                        "queue settlement coordinator is unavailable"
                    );
                }
            }
            Ok(Err(_)) => {
                if connection.is_none() {
                    *connection = self.inner.provider.worker_connection().await.ok();
                }
                let delay = self.retry_delay_ms(&job.id, job.attempt);
                let result = match connection.as_mut() {
                    Some(worker_connection) => {
                        self.nack(
                            worker_connection,
                            &shared.keys,
                            NackRequest {
                                id: &job.id,
                                token: job.token.as_ref(),
                                reason: "processor-error",
                                force_dead_letter: false,
                                delay_ms: delay,
                            },
                        )
                        .await
                    }
                    None => Err(Error::Queue(
                        "could not connect to retry queue delivery".to_string(),
                    )),
                };
                if result.is_ok() {
                    if job.attempt >= job.maximum {
                        shared.metrics.dead_lettered.increment(1);
                    } else {
                        shared.metrics.retried.increment(1);
                    }
                } else {
                    *connection = None;
                    self.inner.provider.invalidate();
                }
                remove_lease(&shared.leases, &job.id, job.token.as_ref());
            }
            Err(_) => {
                if connection.is_none() {
                    *connection = self.inner.provider.worker_connection().await.ok();
                }
                let delay = self.retry_delay_ms(&job.id, job.attempt);
                let result = match connection.as_mut() {
                    Some(worker_connection) => {
                        self.nack(
                            worker_connection,
                            &shared.keys,
                            NackRequest {
                                id: &job.id,
                                token: job.token.as_ref(),
                                reason: "processor-panic",
                                force_dead_letter: false,
                                delay_ms: delay,
                            },
                        )
                        .await
                    }
                    None => Err(Error::Queue(
                        "could not connect to retry panicked queue delivery".to_string(),
                    )),
                };
                if result.is_ok() {
                    shared.metrics.panicked_or_lease_lost.increment(1);
                } else {
                    *connection = None;
                    self.inner.provider.invalidate();
                }
                remove_lease(&shared.leases, &job.id, job.token.as_ref());
            }
        }
        shared
            .metrics
            .processing_duration
            .record(processing_started.elapsed().as_secs_f64());
    }

    async fn ack_batch_and_claim(
        &self,
        connection: &mut P::Connection,
        keys: &RedisQueueKeys,
        batch: &[SettlementRequest],
        run_maintenance: bool,
    ) -> Result<Vec<bool>> {
        if let [request] = batch {
            return self
                .ack_and_claim_one(connection, keys, request, run_maintenance)
                .await
                .map(|settled| vec![settled]);
        }
        let now = now_ms();
        let mut invocation = self.inner.scripts.ack_batch_and_claim.prepare_invoke();
        invocation
            .key(&keys.jobs)
            .key(&keys.ready)
            .key(&keys.delayed)
            .key(&keys.active)
            .key(&keys.tokens)
            .key(&keys.attempts)
            .key(&keys.maxima)
            .key(&keys.dead_letter)
            .key(&keys.failures)
            .key(&keys.completed)
            .key(&keys.notify)
            .key(&keys.events)
            .arg(now)
            .arg(now.saturating_add(self.inner.config.lease_duration_ms))
            .arg(self.inner.config.completed_retention)
            .arg(self.inner.config.failed_retention)
            .arg(self.inner.config.event_retention)
            .arg(self.inner.config.stalled_batch_size)
            .arg(i64::from(run_maintenance))
            .arg(batch.len())
            .arg(self.inner.config.max_attempts);
        for request in batch {
            invocation
                .arg(&request.id)
                .arg(request.worker_token.as_ref())
                .arg(0);
        }
        let future = invocation.invoke_async::<Vec<(i64, String, String, u32, u32)>>(connection);
        let results = self.await_redis("batch ack and claim", future).await?;
        if results.len() != batch.len() {
            return Err(Error::Queue(format!(
                "Redis settlement returned {} results for {} jobs",
                results.len(),
                batch.len()
            )));
        }
        Ok(results
            .into_iter()
            .map(|(settled, _, _, _, _)| settled == 1)
            .collect())
    }

    async fn ack_and_claim_one(
        &self,
        connection: &mut P::Connection,
        keys: &RedisQueueKeys,
        request: &SettlementRequest,
        run_maintenance: bool,
    ) -> Result<bool> {
        let now = now_ms();
        let mut invocation = self.inner.scripts.ack_and_claim.prepare_invoke();
        invocation
            .key(&keys.jobs)
            .key(&keys.ready)
            .key(&keys.delayed)
            .key(&keys.active)
            .key(&keys.tokens)
            .key(&keys.attempts)
            .key(&keys.maxima)
            .key(&keys.dead_letter)
            .key(&keys.failures)
            .key(&keys.completed)
            .key(&keys.notify)
            .key(&keys.events)
            .arg(&request.id)
            .arg(request.worker_token.as_ref())
            .arg(now)
            .arg(now.saturating_add(self.inner.config.lease_duration_ms))
            .arg(self.inner.config.completed_retention)
            .arg(self.inner.config.failed_retention)
            .arg(self.inner.config.event_retention)
            .arg(self.inner.config.stalled_batch_size)
            .arg(i64::from(run_maintenance))
            .arg(0)
            .arg(self.inner.config.max_attempts);
        let future = invocation.invoke_async::<(i64, String, String, u32, u32)>(connection);
        let (settled, id, serialized, attempt, maximum) =
            self.await_redis("ack and claim", future).await?;
        debug_assert!(id.is_empty() && serialized.is_empty() && attempt == 0 && maximum == 0);
        Ok(settled == 1)
    }

    async fn nack(
        &self,
        connection: &mut P::Connection,
        keys: &RedisQueueKeys,
        request: NackRequest<'_>,
    ) -> Result<i64> {
        let now = now_ms();
        let mut invocation = self.inner.scripts.nack.prepare_invoke();
        invocation
            .key(&keys.jobs)
            .key(&keys.active)
            .key(&keys.tokens)
            .key(&keys.attempts)
            .key(&keys.maxima)
            .key(&keys.delayed)
            .key(&keys.ready)
            .key(&keys.dead_letter)
            .key(&keys.failures)
            .key(&keys.notify)
            .key(&keys.events)
            .arg(request.id)
            .arg(request.token)
            .arg(now)
            .arg(now.saturating_add(request.delay_ms))
            .arg(request.reason)
            .arg(i64::from(request.force_dead_letter))
            .arg(self.inner.config.failed_retention)
            .arg(self.inner.config.event_retention)
            .arg(self.inner.config.max_attempts);
        let future = invocation.invoke_async::<i64>(connection);
        self.await_redis("nack", future).await
    }

    fn retry_delay_ms(&self, id: &str, attempt: u32) -> u64 {
        let exponent = attempt.saturating_sub(1).min(31);
        let base = self
            .inner
            .config
            .retry_base_delay_ms
            .saturating_mul(1_u64 << exponent)
            .min(self.inner.config.retry_max_delay_ms);
        if base == 0 || self.inner.config.retry_jitter == 0.0 {
            return base;
        }
        let hash = id.bytes().fold(0xcbf29ce484222325_u64, |value, byte| {
            (value ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
        let unit = (hash as f64) / (u64::MAX as f64);
        let factor =
            1.0 - self.inner.config.retry_jitter + (2.0 * self.inner.config.retry_jitter * unit);
        ((base as f64) * factor).round() as u64
    }

    async fn wait_or_shutdown(&self, duration: Duration) -> bool {
        tokio::select! {
            _ = self.inner.shutdown.cancelled() => true,
            _ = tokio::time::sleep(duration) => false,
        }
    }

    pub(crate) async fn disconnect(&self) -> Result<()> {
        if !self.inner.accepting.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.shutdown.cancel();
        let handles = std::mem::take(&mut *self.inner.workers.lock());
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.inner.config.shutdown_timeout_ms);
        for mut handle in handles {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut handle).await.is_err() {
                handle.abort();
                let _ = handle.await;
            }
        }
        self.inner.registered_queues.lock().clear();
        Ok(())
    }

    pub(crate) async fn check_health(&self) -> Result<()> {
        let mut connection = self.inner.provider.command_connection().await?;
        let command = redis::cmd("PING");
        let ping = command.query_async::<String>(&mut connection);
        let response = self.await_redis("health check", ping).await?;
        if response == "PONG" {
            Ok(())
        } else {
            Err(Error::Redis(format!(
                "queue Redis PING returned {response}"
            )))
        }
    }

    pub(crate) async fn health(&self) -> Result<QueueHealth> {
        self.check_health().await?;
        let workers = self
            .inner
            .workers
            .lock()
            .iter()
            .filter(|handle| !handle.is_finished())
            .count();
        Ok(QueueHealth {
            backend: self.backend(),
            healthy: true,
            accepting_jobs: self.inner.accepting.load(Ordering::Acquire),
            workers,
            capabilities: self.capabilities(),
        })
    }

    pub(crate) async fn stats(&self, queue_name: &str) -> Result<QueueStats> {
        let keys = RedisQueueKeys::new(&self.inner.prefix, queue_name)?;
        let mut connection = self.inner.provider.command_connection().await?;
        let mut pipeline = redis::pipe();
        pipeline
            .cmd("LLEN")
            .arg(&keys.ready)
            .cmd("GET")
            .arg(&keys.inline_ready_count)
            .cmd("ZCARD")
            .arg(&keys.active)
            .cmd("ZCARD")
            .arg(&keys.delayed)
            .cmd("ZCARD")
            .arg(&keys.dead_letter)
            .cmd("ZCARD")
            .arg(&keys.completed);
        let query = pipeline.query_async::<(u64, Option<u64>, u64, u64, u64, u64)>(&mut connection);
        let (ready, inline_ready, active, delayed, dead_letter, completed) =
            self.await_redis("stats", query).await?;
        let ready = ready.saturating_add(inline_ready.unwrap_or_default());
        for (state, value) in [
            ("ready", ready),
            ("active", active),
            ("delayed", delayed),
            ("dead-letter", dead_letter),
            ("completed", completed),
        ] {
            metrics::gauge!(
                "sockudo_queue_depth",
                "backend" => self.backend().as_str(),
                "queue" => queue_name.to_string(),
                "state" => state,
            )
            .set(value as f64);
        }
        Ok(QueueStats {
            ready: Some(ready),
            active: Some(active),
            delayed: Some(delayed),
            dead_letter: Some(dead_letter),
            completed: Some(completed),
            oldest_ready_age_ms: None,
        })
    }

    pub(crate) async fn replay_dead_letters(&self, queue_name: &str, limit: u32) -> Result<u64> {
        if limit == 0 {
            return Ok(0);
        }
        let keys = RedisQueueKeys::new(&self.inner.prefix, queue_name)?;
        let mut connection = self.inner.provider.command_connection().await?;
        let mut invocation = self.inner.scripts.replay_dead_letters.prepare_invoke();
        invocation
            .key(&keys.jobs)
            .key(&keys.dead_letter)
            .key(&keys.failures)
            .key(&keys.attempts)
            .key(&keys.ready)
            .key(&keys.notify)
            .key(&keys.events)
            .arg(limit)
            .arg(now_ms())
            .arg(self.inner.config.event_retention);
        let future = invocation.invoke_async::<u64>(&mut connection);
        let replayed = self.await_redis("dead-letter replay", future).await?;
        if replayed > 0 {
            metrics::counter!(
                "sockudo_queue_dead_letter_replayed_total",
                "backend" => self.backend().as_str(),
                "queue" => queue_name.to_string(),
            )
            .increment(replayed);
        }
        Ok(replayed)
    }
}

fn now_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default()
}

fn remove_lease(leases: &LeaseRegistry, id: &str, token: &str) {
    let mut leases = leases.lock();
    if leases
        .get(id)
        .is_some_and(|current| current.as_ref() == token)
    {
        leases.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_schema_forces_all_state_into_one_cluster_slot() {
        let keys = RedisQueueKeys::new("sockudo_queue:", "push/{delivery}").unwrap();
        let tags: Vec<_> = keys
            .all()
            .iter()
            .map(|key| {
                let start = key.find('{').unwrap();
                let end = key.find('}').unwrap();
                &key[start + 1..end]
            })
            .collect();
        assert!(tags.iter().all(|tag| tag == &tags[0]));
        assert!(!tags[0].contains('{'));
    }

    #[test]
    fn queue_name_encoding_is_stable_and_safe() {
        assert_eq!(encode_queue_name("webhooks").unwrap(), "webhooks");
        assert_eq!(encode_queue_name("push/{fcm}").unwrap(), "push%2F%7Bfcm%7D");
        assert!(encode_queue_name("").is_err());
    }

    #[test]
    fn generated_batches_are_counted_and_length_delimited() {
        let jobs = [
            PreparedJob {
                id: "a".repeat(32),
                options: QueueJobOptions::default(),
                serialized: format!("{}{{\"one\":1}}", "a".repeat(32)),
            },
            PreparedJob {
                id: "b".repeat(32),
                options: QueueJobOptions::default(),
                serialized: format!("{}{{\"two\":2}}", "b".repeat(32)),
            },
        ];

        let batch = pack_generated_batch(&jobs).unwrap();
        assert_eq!(batch[0], b'B');
        assert_eq!(u32::from_be_bytes(batch[1..5].try_into().unwrap()), 2);
        let mut position = 5;
        for job in &jobs {
            let length = u32::from_be_bytes(batch[position..position + 4].try_into().unwrap());
            position += 4;
            assert_eq!(usize::try_from(length).unwrap(), job.serialized.len() + 1);
            assert_eq!(batch[position], b'J');
            position += 1;
            assert_eq!(
                &batch[position..position + job.serialized.len()],
                job.serialized.as_bytes()
            );
            position += job.serialized.len();
        }
        assert_eq!(position, batch.len());
    }
}
