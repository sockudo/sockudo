use crate::ArcJobProcessorFn;
use ahash::AHashMap as HashMap;
use async_trait::async_trait;
use futures_util::FutureExt;
use parking_lot::{Mutex, RwLock};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::QueueReliabilityConfig;
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueHealth, QueueInterface, QueueJobId, QueueJobOptions,
    QueueJobRequest, QueueStats,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const MAX_CONCURRENT_JOBS_PER_QUEUE: usize = 64;
const MAX_ID_LENGTH: usize = 512;

struct MemoryJob {
    id: String,
    data: JobData,
    attempt: u32,
    maximum: u32,
}

#[derive(Default)]
struct QueueState {
    ready: VecDeque<MemoryJob>,
    delayed: BTreeMap<(u64, u64), MemoryJob>,
    dead_letter: VecDeque<MemoryJob>,
    active: usize,
    sequence: u64,
    fingerprints: HashMap<String, u64>,
    outstanding: HashSet<String>,
    dedup: HashMap<String, (String, u64)>,
    completed: VecDeque<String>,
}

struct Inner {
    queues: dashmap::DashMap<String, Arc<Mutex<QueueState>>, ahash::RandomState>,
    processors: RwLock<HashMap<String, ArcJobProcessorFn>>,
    config: QueueReliabilityConfig,
    shutdown: CancellationToken,
    notify: tokio::sync::Notify,
    started: AtomicBool,
    accepting: AtomicBool,
    supervisor: Mutex<Option<JoinHandle<()>>>,
}

/// Reliable bounded in-memory queue used for development and deterministic tests.
/// It provides Queue v2 semantics within one process but is intentionally not durable.
#[derive(Clone)]
pub struct MemoryQueueManager {
    inner: Arc<Inner>,
}

impl Default for MemoryQueueManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryQueueManager {
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_config(QueueReliabilityConfig::default())
            .expect("default memory queue configuration is valid")
    }

    pub fn new_with_config(config: QueueReliabilityConfig) -> Result<Self> {
        config.validate().map_err(Error::Config)?;
        Ok(Self {
            inner: Arc::new(Inner {
                queues: dashmap::DashMap::with_hasher(ahash::RandomState::new()),
                processors: RwLock::new(HashMap::new()),
                config,
                shutdown: CancellationToken::new(),
                notify: tokio::sync::Notify::new(),
                started: AtomicBool::new(false),
                accepting: AtomicBool::new(true),
                supervisor: Mutex::new(None),
            }),
        })
    }

    /// Starts the event-driven supervisor once. Calling this repeatedly is safe.
    pub fn start_processing(&self) {
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let queue = self.clone();
        let handle = tokio::spawn(async move { queue.supervisor_loop().await });
        *self.inner.supervisor.lock() = Some(handle);
        info!("started reliable memory queue supervisor");
    }

    fn state(&self, queue_name: &str) -> Arc<Mutex<QueueState>> {
        self.inner
            .queues
            .entry(queue_name.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(QueueState::default())))
            .clone()
    }

    fn prepare_job(
        &self,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<(MemoryJob, QueueJobOptions, u64)> {
        let id = options
            .job_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
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
        let serialized = sonic_rs::to_string(&data)?;
        let fingerprint = serialized
            .bytes()
            .fold(0xcbf29ce484222325_u64, |value, byte| {
                (value ^ u64::from(byte)).wrapping_mul(0x100000001b3)
            });
        Ok((
            MemoryJob {
                id,
                data,
                attempt: 0,
                maximum: options
                    .max_attempts
                    .unwrap_or(self.inner.config.max_attempts),
            },
            options,
            fingerprint,
        ))
    }

    pub async fn enqueue_v2(
        &self,
        queue_name: &str,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<QueueJobId> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(Error::Queue("queue is shutting down".to_string()));
        }
        let (job, options, fingerprint) = self.prepare_job(data, options)?;
        let state = self.state(queue_name);
        let now = now_ms();
        let id = {
            let mut state = state.lock();
            Self::cleanup_dedup(&mut state, now);
            if let Some(existing) = state.fingerprints.get(&job.id) {
                return if *existing == fingerprint {
                    Ok(QueueJobId(job.id))
                } else {
                    Err(Error::Queue(format!(
                        "queue job ID {} already exists with a different payload",
                        job.id
                    )))
                };
            }
            if let Some(key) = options.deduplication_key.as_deref()
                && let Some((existing, _)) = state.dedup.get(key)
            {
                return Ok(QueueJobId(existing.clone()));
            }
            let depth =
                state.ready.len() + state.delayed.len() + state.active + state.dead_letter.len();
            if depth >= self.inner.config.memory_capacity {
                return Err(Error::BufferFull(format!(
                    "memory queue {queue_name} reached configured capacity {}",
                    self.inner.config.memory_capacity
                )));
            }
            let id = job.id.clone();
            state.fingerprints.insert(id.clone(), fingerprint);
            state.outstanding.insert(id.clone());
            if let Some(key) = options.deduplication_key {
                state.dedup.insert(
                    key,
                    (
                        id.clone(),
                        now.saturating_add(self.inner.config.deduplication_ttl_ms),
                    ),
                );
            }
            if options.delay_ms == 0 {
                state.ready.push_back(job);
            } else {
                let sequence = state.sequence;
                state.sequence = state.sequence.saturating_add(1);
                state
                    .delayed
                    .insert((now.saturating_add(options.delay_ms), sequence), job);
            }
            id
        };
        self.inner.notify.notify_one();
        Ok(QueueJobId(id))
    }

    fn cleanup_dedup(state: &mut QueueState, now: u64) {
        state.dedup.retain(|_, (_, expires)| *expires > now);
    }

    async fn supervisor_loop(&self) {
        let mut tasks = JoinSet::new();
        let poll = Duration::from_millis(self.inner.config.worker_poll_interval_ms.max(10));
        let mut draining = false;
        loop {
            if !draining {
                self.dispatch_ready(&mut tasks);
            }
            if draining && tasks.is_empty() {
                break;
            }

            tokio::select! {
                biased;
                _ = self.inner.shutdown.cancelled(), if !draining => draining = true,
                Some(completion) = tasks.join_next(), if !tasks.is_empty() => {
                    match completion {
                        Ok(completion) => self.finish_job(completion),
                        Err(error) => warn!(error = %error, "memory queue processor task terminated unexpectedly"),
                    }
                }
                _ = self.inner.notify.notified(), if !draining => {}
                _ = tokio::time::sleep(poll), if !draining => {}
            }
        }
        debug!("memory queue supervisor stopped");
    }

    fn dispatch_ready(&self, tasks: &mut JoinSet<MemoryCompletion>) {
        let now = now_ms();
        let queue_names: Vec<String> = self
            .inner
            .queues
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for queue_name in queue_names {
            let Some(processor) = self.inner.processors.read().get(&queue_name).cloned() else {
                continue;
            };
            let Some(state_ref) = self.inner.queues.get(&queue_name) else {
                continue;
            };
            let state_arc = state_ref.clone();
            drop(state_ref);
            let jobs = {
                let mut state = state_arc.lock();
                while state
                    .delayed
                    .first_key_value()
                    .is_some_and(|((due, _), _)| *due <= now)
                {
                    if let Some((_, job)) = state.delayed.pop_first() {
                        state.ready.push_back(job);
                    }
                }
                let available = MAX_CONCURRENT_JOBS_PER_QUEUE.saturating_sub(state.active);
                let count = available.min(state.ready.len());
                let mut jobs = Vec::with_capacity(count);
                for _ in 0..count {
                    if let Some(mut job) = state.ready.pop_front() {
                        job.attempt = job.attempt.saturating_add(1);
                        jobs.push(job);
                    }
                }
                state.active = state.active.saturating_add(jobs.len());
                jobs
            };
            for job in jobs {
                let processor = Arc::clone(&processor);
                let queue_name = queue_name.clone();
                tasks.spawn(async move {
                    let id = job.id.clone();
                    let attempt = job.attempt;
                    let outcome = AssertUnwindSafe(processor(job.data.clone()))
                        .catch_unwind()
                        .await;
                    MemoryCompletion {
                        queue_name,
                        job,
                        succeeded: matches!(outcome, Ok(Ok(()))),
                        panicked: outcome.is_err(),
                        id,
                        attempt,
                    }
                });
            }
        }
    }

    fn finish_job(&self, completion: MemoryCompletion) {
        let Some(state_ref) = self.inner.queues.get(&completion.queue_name) else {
            return;
        };
        let state_arc = state_ref.clone();
        drop(state_ref);
        let mut state = state_arc.lock();
        state.active = state.active.saturating_sub(1);
        if completion.succeeded {
            state.outstanding.remove(&completion.id);
            state.completed.push_back(completion.id);
            while state.completed.len() > self.inner.config.completed_retention as usize {
                if let Some(id) = state.completed.pop_front()
                    && !state.outstanding.contains(&id)
                {
                    state.fingerprints.remove(&id);
                }
            }
        } else if completion.attempt >= completion.job.maximum {
            state.dead_letter.push_back(completion.job);
            while state.dead_letter.len() > self.inner.config.failed_retention as usize {
                if let Some(job) = state.dead_letter.pop_front() {
                    state.outstanding.remove(&job.id);
                    state.fingerprints.remove(&job.id);
                }
            }
            warn!(
                queue = completion.queue_name,
                job_id = completion.id,
                panicked = completion.panicked,
                "memory queue job moved to dead letter"
            );
        } else {
            let delay = retry_delay_ms(&self.inner.config, &completion.id, completion.attempt);
            let sequence = state.sequence;
            state.sequence = state.sequence.saturating_add(1);
            state
                .delayed
                .insert((now_ms().saturating_add(delay), sequence), completion.job);
        }
        drop(state);
        self.inner.notify.notify_one();
    }
}

struct MemoryCompletion {
    queue_name: String,
    job: MemoryJob,
    succeeded: bool,
    panicked: bool,
    id: String,
    attempt: u32,
}

#[async_trait]
impl QueueInterface for MemoryQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        self.enqueue_v2(queue_name, data, QueueJobOptions::default())
            .await
            .map(|_| ())
    }

    async fn enqueue(
        &self,
        queue_name: &str,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<QueueJobId> {
        self.enqueue_v2(queue_name, data, options).await
    }

    async fn add_batch_to_queue(&self, queue_name: &str, data: Vec<JobData>) -> Result<()> {
        let jobs = data
            .into_iter()
            .map(|data| QueueJobRequest {
                data,
                options: QueueJobOptions::default(),
            })
            .collect();
        self.enqueue_batch(queue_name, jobs).await.map(|_| ())
    }

    async fn enqueue_batch(
        &self,
        queue_name: &str,
        jobs: Vec<QueueJobRequest>,
    ) -> Result<Vec<QueueJobId>> {
        let mut ids = Vec::with_capacity(jobs.len());
        for job in jobs {
            ids.push(self.enqueue_v2(queue_name, job.data, job.options).await?);
        }
        Ok(ids)
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
        if self
            .inner
            .processors
            .write()
            .insert(queue_name.to_string(), Arc::from(callback))
            .is_some()
        {
            return Err(Error::Queue(format!(
                "a processor is already registered for queue {queue_name}"
            )));
        }
        self.start_processing();
        self.inner.notify.notify_one();
        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        if !self.inner.accepting.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.shutdown.cancel();
        let handle = self.inner.supervisor.lock().take();
        if let Some(mut handle) = handle {
            let timeout = Duration::from_millis(self.inner.config.shutdown_timeout_ms);
            if tokio::time::timeout(timeout, &mut handle).await.is_err() {
                handle.abort();
                let _ = handle.await;
            }
        }
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        if self.inner.started.load(Ordering::Acquire)
            && self
                .inner
                .supervisor
                .lock()
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
            && !self.inner.shutdown.is_cancelled()
        {
            return Err(Error::Queue(
                "memory queue supervisor stopped unexpectedly".to_string(),
            ));
        }
        Ok(())
    }

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::Memory
    }

    fn capabilities(&self) -> QueueCapabilities {
        QueueCapabilities {
            consume: true,
            acknowledgements: true,
            delayed_delivery: true,
            retries: true,
            dead_letter: true,
            deduplication: true,
            leasing: false,
            durable: false,
            batch_enqueue: true,
            observable_lag: true,
        }
    }

    async fn health(&self) -> Result<QueueHealth> {
        self.check_health().await?;
        Ok(QueueHealth {
            backend: self.backend(),
            healthy: true,
            accepting_jobs: self.inner.accepting.load(Ordering::Acquire),
            workers: usize::from(self.inner.started.load(Ordering::Acquire)),
            capabilities: self.capabilities(),
        })
    }

    async fn stats(&self, queue_name: &str) -> Result<QueueStats> {
        let state = self.state(queue_name);
        let state = state.lock();
        Ok(QueueStats {
            ready: Some(state.ready.len() as u64),
            active: Some(state.active as u64),
            delayed: Some(state.delayed.len() as u64),
            dead_letter: Some(state.dead_letter.len() as u64),
            completed: Some(state.completed.len() as u64),
            oldest_ready_age_ms: None,
        })
    }

    async fn replay_dead_letters(&self, queue_name: &str, limit: u32) -> Result<u64> {
        if limit == 0 {
            return Ok(0);
        }
        let state = self.state(queue_name);
        let replayed = {
            let mut state = state.lock();
            let count = (limit as usize).min(state.dead_letter.len());
            for _ in 0..count {
                if let Some(mut job) = state.dead_letter.pop_front() {
                    job.attempt = 0;
                    state.ready.push_back(job);
                }
            }
            count as u64
        };
        if replayed > 0 {
            self.inner.notify.notify_one();
        }
        Ok(replayed)
    }
}

fn retry_delay_ms(config: &QueueReliabilityConfig, id: &str, attempt: u32) -> u64 {
    let exponent = attempt.saturating_sub(1).min(31);
    let base = config
        .retry_base_delay_ms
        .saturating_mul(1_u64 << exponent)
        .min(config.retry_max_delay_ms);
    if base == 0 || config.retry_jitter == 0.0 {
        return base;
    }
    let hash = id.bytes().fold(0xcbf29ce484222325_u64, |value, byte| {
        (value ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    let unit = (hash as f64) / (u64::MAX as f64);
    let factor = 1.0 - config.retry_jitter + (2.0 * config.retry_jitter * unit);
    ((base as f64) * factor).round() as u64
}

fn now_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::webhook_types::JobPayload;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    fn job() -> JobData {
        JobData {
            app_key: "test-key".to_string(),
            app_id: "test-id".to_string(),
            app_secret: "test-secret".to_string(),
            payload: JobPayload::default(),
            original_signature: "signature".to_string(),
        }
    }

    #[tokio::test]
    async fn capacity_fails_closed_instead_of_dropping() {
        let manager = MemoryQueueManager::new_with_config(QueueReliabilityConfig {
            memory_capacity: 1,
            ..QueueReliabilityConfig::default()
        })
        .unwrap();
        manager.add_to_queue("bounded", job()).await.unwrap();
        assert!(manager.add_to_queue("bounded", job()).await.is_err());
        assert_eq!(manager.stats("bounded").await.unwrap().ready, Some(1));
    }

    #[tokio::test]
    async fn processor_errors_retry_then_succeed() {
        let manager = MemoryQueueManager::new_with_config(QueueReliabilityConfig {
            retry_base_delay_ms: 1,
            retry_max_delay_ms: 1,
            retry_jitter: 0.0,
            worker_poll_interval_ms: 1,
            ..QueueReliabilityConfig::default()
        })
        .unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(Notify::new());
        let attempts_clone = Arc::clone(&attempts);
        let done_clone = Arc::clone(&done);
        manager
            .process_queue(
                "retry",
                Box::new(move |_| {
                    let attempts = Arc::clone(&attempts_clone);
                    let done = Arc::clone(&done_clone);
                    Box::pin(async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            return Err(Error::Other("retry".to_string()));
                        }
                        done.notify_one();
                        Ok(())
                    })
                }),
            )
            .await
            .unwrap();
        manager.add_to_queue("retry", job()).await.unwrap();
        timeout(Duration::from_secs(2), done.notified())
            .await
            .expect("job should be retried");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        manager.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn stable_id_is_idempotent_and_conflicts_on_other_payload() {
        let manager = MemoryQueueManager::new();
        let options = QueueJobOptions {
            job_id: Some("stable".to_string()),
            ..QueueJobOptions::default()
        };
        let first = manager.enqueue("id", job(), options.clone()).await.unwrap();
        let second = manager.enqueue("id", job(), options.clone()).await.unwrap();
        assert_eq!(first, second);
        let mut changed = job();
        changed.app_id = "other".to_string();
        assert!(manager.enqueue("id", changed, options).await.is_err());
    }

    #[tokio::test]
    async fn exhausted_job_can_be_replayed_from_dead_letter() {
        let manager = MemoryQueueManager::new_with_config(QueueReliabilityConfig {
            max_attempts: 1,
            retry_base_delay_ms: 1,
            retry_max_delay_ms: 1,
            retry_jitter: 0.0,
            worker_poll_interval_ms: 1,
            ..QueueReliabilityConfig::default()
        })
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = Arc::clone(&calls);
        manager
            .process_queue(
                "dlq",
                Box::new(move |_| {
                    let calls = Arc::clone(&calls_clone);
                    Box::pin(async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err(Error::Other("fail".to_string()))
                    })
                }),
            )
            .await
            .unwrap();
        manager.add_to_queue("dlq", job()).await.unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if manager.stats("dlq").await.unwrap().dead_letter == Some(1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(manager.replay_dead_letters("dlq", 1).await.unwrap(), 1);
        timeout(Duration::from_secs(2), async {
            loop {
                if calls.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        manager.disconnect().await.unwrap();
    }
}
