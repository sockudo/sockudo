use crate::domain::{PublishLifecycleState, PushCursor};
use crate::metrics::PushMetrics;
use crate::pipeline::{DynPushQueue, PushPipelineResult, PushQueuePayload, PushQueueStage};
use crate::storage::{DynPushStore, SchedulerLock};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const DEFAULT_REPAIR_BATCH_SIZE: usize = 100;
const DEFAULT_REPAIR_MIN_AGE_MS: u64 = 30_000;
const DEFAULT_REPAIR_LOCK_TTL_MS: u64 = 30_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushRepairPolicy {
    pub batch_size: usize,
    pub min_age_ms: u64,
    pub lock_ttl_ms: u64,
}

impl Default for PushRepairPolicy {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_REPAIR_BATCH_SIZE,
            min_age_ms: DEFAULT_REPAIR_MIN_AGE_MS,
            lock_ttl_ms: DEFAULT_REPAIR_LOCK_TTL_MS,
        }
    }
}

impl PushRepairPolicy {
    pub fn bounded(mut self) -> Self {
        self.batch_size = self.batch_size.clamp(1, 10_000);
        self.lock_ttl_ms = self.lock_ttl_ms.max(1_000);
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PushRepairReport {
    pub scanned: u64,
    pub requeued: u64,
    pub skipped_fresh: u64,
    pub skipped_not_queued: u64,
    pub skipped_locked: u64,
    pub missing_status: u64,
}

impl PushRepairReport {
    pub fn repaired_any(&self) -> bool {
        self.requeued > 0
    }
}

#[derive(Clone)]
pub struct PushPublishLogRepairWorker {
    store: DynPushStore,
    queue: DynPushQueue,
    owner_id: String,
    policy: PushRepairPolicy,
    metrics: PushMetrics,
    cursors: Arc<Mutex<BTreeMap<String, PushCursor>>>,
}

impl PushPublishLogRepairWorker {
    pub fn new(store: DynPushStore, queue: DynPushQueue, owner_id: impl Into<String>) -> Self {
        Self {
            store,
            queue,
            owner_id: owner_id.into(),
            policy: PushRepairPolicy::default(),
            metrics: PushMetrics::default(),
            cursors: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_policy(mut self, policy: PushRepairPolicy) -> Self {
        self.policy = policy.bounded();
        self
    }

    pub fn with_metrics(mut self, metrics: PushMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub async fn run_once_for_app(
        &self,
        app_id: &str,
        now_ms: u64,
    ) -> PushPipelineResult<PushRepairReport> {
        let mut cursor = self
            .cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(app_id)
            .cloned();
        let mut report = PushRepairReport::default();
        let batch_size = self.policy.batch_size.max(1);

        while (report.scanned as usize) < batch_size {
            let remaining = batch_size.saturating_sub(report.scanned as usize);
            let page = self
                .store
                .list_publish_log_events(app_id, remaining.min(1_000), cursor)
                .await?;
            if page.items.is_empty() {
                let mut cursors = self
                    .cursors
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(next) = page.next_cursor {
                    cursors.insert(app_id.to_owned(), next);
                } else {
                    cursors.remove(app_id);
                }
                break;
            }

            for event in page.items {
                report.scanned = report.scanned.saturating_add(1);
                self.metrics.repair_scanned("publish_log", 1);
                let age_ms = now_ms.saturating_sub(event.occurred_at_ms);
                if age_ms < self.policy.min_age_ms {
                    report.skipped_fresh = report.skipped_fresh.saturating_add(1);
                    self.metrics.repair_skipped("fresh");
                    continue;
                }

                let Some(status) = self
                    .store
                    .get_publish_status(&event.app_id, &event.publish_id)
                    .await?
                else {
                    report.missing_status = report.missing_status.saturating_add(1);
                    self.metrics.repair_skipped("missing_status");
                    continue;
                };
                if status.state != PublishLifecycleState::Queued {
                    report.skipped_not_queued = report.skipped_not_queued.saturating_add(1);
                    self.metrics.repair_skipped("not_queued");
                    continue;
                }

                let lock = SchedulerLock {
                    app_id: event.app_id.clone(),
                    publish_id: repair_lock_id(&event.publish_id),
                    owner_id: self.owner_id.clone(),
                    expires_at_ms: now_ms.saturating_add(self.policy.lock_ttl_ms),
                };
                if !self.store.acquire_scheduler_lock(lock, now_ms).await? {
                    report.skipped_locked = report.skipped_locked.saturating_add(1);
                    self.metrics.repair_skipped("locked");
                    continue;
                }

                self.queue
                    .produce(
                        PushQueueStage::PublishLog,
                        event.queue_key(),
                        PushQueuePayload::PublishLog(Box::new(event)),
                    )
                    .await?;
                report.requeued = report.requeued.saturating_add(1);
                self.metrics.repair_requeued("publish_log", "queued_stale");
            }

            cursor = page.next_cursor;
            {
                let mut cursors = self
                    .cursors
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(next) = &cursor {
                    cursors.insert(app_id.to_owned(), next.clone());
                } else {
                    cursors.remove(app_id);
                }
            }
            if cursor.is_none() {
                break;
            }
        }

        Ok(report)
    }
}

fn repair_lock_id(publish_id: &str) -> String {
    format!("repair:publish-log:{publish_id}")
}
