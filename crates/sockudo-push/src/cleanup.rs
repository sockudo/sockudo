use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::domain::PublishLifecycleState;
use crate::meta::{PushMetaEvent, emit_push_meta_event};
use crate::metrics::PushMetrics;
use crate::storage::{DynPushStore, PushStorageResult};

const MILLIS_PER_DAY: u64 = 86_400_000;

pub use crate::feedback::device_is_terminally_failed;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushCleanupPolicy {
    pub publish_status_retention_ms: u64,
    pub delivery_event_retention_ms: u64,
    pub operator_event_retention_ms: u64,
    pub dead_letter_retention_ms: u64,
    pub batch_size: usize,
    pub max_deleted_per_tick: usize,
}

impl PushCleanupPolicy {
    pub fn from_days(
        publish_status_ttl_days: u64,
        analytics_retention_days: u64,
        batch_size: usize,
        max_deleted_per_tick: usize,
    ) -> Self {
        let analytics_retention_ms = days_to_ms(analytics_retention_days);
        Self {
            publish_status_retention_ms: days_to_ms(publish_status_ttl_days),
            delivery_event_retention_ms: analytics_retention_ms,
            operator_event_retention_ms: analytics_retention_ms,
            dead_letter_retention_ms: analytics_retention_ms,
            batch_size: batch_size.max(1),
            max_deleted_per_tick: max_deleted_per_tick.max(1),
        }
    }

    pub fn request_at(&self, now_ms: u64) -> PushCleanupRequest {
        PushCleanupRequest {
            now_ms,
            policy: self.clone(),
        }
    }
}

impl Default for PushCleanupPolicy {
    fn default() -> Self {
        Self::from_days(30, 30, 1_000, 100_000)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushCleanupRequest {
    pub now_ms: u64,
    pub policy: PushCleanupPolicy,
}

impl PushCleanupRequest {
    pub fn publish_status_cutoff_ms(&self) -> Option<u64> {
        retention_cutoff(self.now_ms, self.policy.publish_status_retention_ms)
    }

    pub fn delivery_event_cutoff_ms(&self) -> Option<u64> {
        retention_cutoff(self.now_ms, self.policy.delivery_event_retention_ms)
    }

    pub fn operator_event_cutoff_ms(&self) -> Option<u64> {
        retention_cutoff(self.now_ms, self.policy.operator_event_retention_ms)
    }

    pub fn dead_letter_cutoff_ms(&self) -> Option<u64> {
        retention_cutoff(self.now_ms, self.policy.dead_letter_retention_ms)
    }

    pub fn limit_for(&self, remaining: usize) -> usize {
        self.policy.batch_size.min(remaining).max(1)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushCleanupCounters {
    pub scanned: u64,
    pub deleted: u64,
}

impl PushCleanupCounters {
    pub fn record(&mut self, scanned: u64, deleted: u64) {
        self.scanned = self.scanned.saturating_add(scanned);
        self.deleted = self.deleted.saturating_add(deleted);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushCleanupReport {
    pub publish_statuses: PushCleanupCounters,
    pub delivery_events: PushCleanupCounters,
    pub idempotency_records: PushCleanupCounters,
    pub scheduler_locks: PushCleanupCounters,
    pub operator_invalidations: PushCleanupCounters,
    pub dead_letters: PushCleanupCounters,
    pub retry_entries: PushCleanupCounters,
    pub scheduled_jobs: PushCleanupCounters,
    pub stale_device_failures: PushCleanupCounters,
}

impl PushCleanupReport {
    pub fn total_deleted(&self) -> u64 {
        self.categories()
            .into_iter()
            .map(|(_, counters)| counters.deleted)
            .sum()
    }

    pub fn total_scanned(&self) -> u64 {
        self.categories()
            .into_iter()
            .map(|(_, counters)| counters.scanned)
            .sum()
    }

    pub fn categories(&self) -> [(&'static str, PushCleanupCounters); 9] {
        [
            ("publish_status", self.publish_statuses),
            ("delivery_event", self.delivery_events),
            ("idempotency", self.idempotency_records),
            ("scheduler_lock", self.scheduler_locks),
            ("operator_invalidation", self.operator_invalidations),
            ("dead_letter", self.dead_letters),
            ("retry_entry", self.retry_entries),
            ("scheduled_job", self.scheduled_jobs),
            ("stale_device_failure", self.stale_device_failures),
        ]
    }
}

#[derive(Clone)]
pub struct PushCleanupWorker {
    store: DynPushStore,
    policy: PushCleanupPolicy,
    metrics: PushMetrics,
    backend_label: &'static str,
}

impl PushCleanupWorker {
    pub fn new(store: DynPushStore, policy: PushCleanupPolicy) -> Self {
        Self {
            store,
            policy,
            metrics: PushMetrics::default(),
            backend_label: "unknown",
        }
    }

    pub fn with_metrics(mut self, metrics: PushMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_backend_label(mut self, backend_label: &'static str) -> Self {
        self.backend_label = backend_label;
        self
    }

    pub async fn run_once(&self, now_ms: u64) -> PushStorageResult<PushCleanupReport> {
        let started = Instant::now();
        let request = self.policy.request_at(now_ms);
        match self.store.cleanup_expired_push_data(request).await {
            Ok(report) => {
                self.metrics.cleanup_tick_duration(started.elapsed());
                for (category, counters) in report.categories() {
                    if counters.scanned > 0 {
                        self.metrics.cleanup_scanned(category, counters.scanned);
                    }
                    if counters.deleted > 0 {
                        self.metrics.cleanup_deleted(category, counters.deleted);
                    }
                }
                if report.total_deleted() > 0 {
                    emit_push_meta_event(PushMetaEvent::cleanup_event(
                        report.total_scanned(),
                        report.total_deleted(),
                    ));
                }
                Ok(report)
            }
            Err(error) => {
                self.metrics.cleanup_error(self.backend_label, "tick");
                emit_push_meta_event(PushMetaEvent::cleanup_error());
                Err(error)
            }
        }
    }
}

pub fn terminal_publish_state(state: PublishLifecycleState) -> bool {
    matches!(
        state,
        PublishLifecycleState::Succeeded
            | PublishLifecycleState::PartiallySucceeded
            | PublishLifecycleState::Failed
            | PublishLifecycleState::Expired
            | PublishLifecycleState::Cancelled
            | PublishLifecycleState::QuotaExceeded
            | PublishLifecycleState::DeadLettered
    )
}

pub fn days_to_ms(days: u64) -> u64 {
    days.saturating_mul(MILLIS_PER_DAY)
}

fn retention_cutoff(now_ms: u64, retention_ms: u64) -> Option<u64> {
    (retention_ms > 0).then(|| now_ms.saturating_sub(retention_ms))
}

pub fn interval_from_secs(interval_secs: u64) -> Duration {
    Duration::from_secs(interval_secs.max(1))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::domain::{
        DeliveryEvent, DeliveryOutcome, DeliveryResult, PublishCounters, PublishLifecycleState,
        PublishStatus, PushProviderKind,
    };
    use crate::memory::MemoryPushStore;
    use crate::storage::{
        IdempotencyRecord, OperatorInvalidationEvent, PushCleanupStore, PushDeliveryEventStore,
        PushIdempotencyStore, PushOperatorEventStore, PushPublishStatusStore,
        PushSchedulerLockStore, SchedulerLock,
    };

    use super::*;

    #[tokio::test]
    async fn cleanup_deletes_expired_state_and_retains_active_state() {
        let now = 4_000_000_020_000;
        let old = now - 10_000;
        let fresh = now - 1_000;
        let store = MemoryPushStore::new();
        seed_status(&store, "old-terminal", PublishLifecycleState::Succeeded).await;
        seed_status(&store, "old-active", PublishLifecycleState::Dispatching).await;
        seed_status(&store, "new-terminal", PublishLifecycleState::Failed).await;
        store
            .set_publish_status_updated_at_for_test("app-1", "old-terminal", old)
            .await;
        store
            .set_publish_status_updated_at_for_test("app-1", "old-active", old)
            .await;
        store
            .set_publish_status_updated_at_for_test("app-1", "new-terminal", fresh)
            .await;

        store
            .append_delivery_event(delivery_event("old-event", old))
            .await
            .unwrap();
        store
            .append_delivery_event(delivery_event("new-event", fresh))
            .await
            .unwrap();
        store
            .put_idempotency_record_if_absent(IdempotencyRecord {
                app_id: "app-1".to_owned(),
                key: "expired".to_owned(),
                publish_id: "publish-1".to_owned(),
                expires_at_ms: old,
            })
            .await
            .unwrap();
        store
            .put_idempotency_record_if_absent(IdempotencyRecord {
                app_id: "app-1".to_owned(),
                key: "active".to_owned(),
                publish_id: "publish-1".to_owned(),
                expires_at_ms: now + 10_000,
            })
            .await
            .unwrap();
        store
            .acquire_scheduler_lock(
                SchedulerLock {
                    app_id: "app-1".to_owned(),
                    publish_id: "expired-lock".to_owned(),
                    owner_id: "node-a".to_owned(),
                    expires_at_ms: old,
                },
                old - 500,
            )
            .await
            .unwrap();
        store
            .acquire_scheduler_lock(
                SchedulerLock {
                    app_id: "app-1".to_owned(),
                    publish_id: "active-lock".to_owned(),
                    owner_id: "node-a".to_owned(),
                    expires_at_ms: now + 10_000,
                },
                old - 500,
            )
            .await
            .unwrap();
        store
            .append_operator_invalidation(OperatorInvalidationEvent {
                app_id: "app-1".to_owned(),
                event_id: "old-op".to_owned(),
                subject: "client:old".to_owned(),
                occurred_at_ms: old,
            })
            .await
            .unwrap();
        store
            .append_operator_invalidation(OperatorInvalidationEvent {
                app_id: "app-1".to_owned(),
                event_id: "new-op".to_owned(),
                subject: "client:new".to_owned(),
                occurred_at_ms: fresh,
            })
            .await
            .unwrap();

        let request = PushCleanupRequest {
            now_ms: now,
            policy: PushCleanupPolicy {
                publish_status_retention_ms: 5_000,
                delivery_event_retention_ms: 5_000,
                operator_event_retention_ms: 5_000,
                dead_letter_retention_ms: 5_000,
                batch_size: 100,
                max_deleted_per_tick: 100,
            },
        };
        let report = store.cleanup_expired_push_data(request).await.unwrap();

        assert_eq!(report.publish_statuses.deleted, 1);
        assert_eq!(report.delivery_events.deleted, 1);
        assert_eq!(report.idempotency_records.deleted, 1);
        assert_eq!(report.scheduler_locks.deleted, 1);
        assert_eq!(report.operator_invalidations.deleted, 1);
        assert!(
            store
                .get_publish_status("app-1", "old-terminal")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_publish_status("app-1", "old-active")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .get_publish_status("app-1", "new-terminal")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .list_delivery_events("app-1", "publish-1", 10, None)
                .await
                .unwrap()
                .items
                .into_iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>(),
            vec!["new-event".to_owned()]
        );
        assert!(
            store
                .get_idempotency_record("app-1", "expired")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_idempotency_record("app-1", "active")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .acquire_scheduler_lock(
                    SchedulerLock {
                        app_id: "app-1".to_owned(),
                        publish_id: "expired-lock".to_owned(),
                        owner_id: "node-b".to_owned(),
                        expires_at_ms: now + 20_000,
                    },
                    now,
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .acquire_scheduler_lock(
                    SchedulerLock {
                        app_id: "app-1".to_owned(),
                        publish_id: "active-lock".to_owned(),
                        owner_id: "node-b".to_owned(),
                        expires_at_ms: now + 20_000,
                    },
                    now,
                )
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .list_operator_invalidations("app-1", 10, None)
                .await
                .unwrap()
                .items
                .into_iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>(),
            vec!["new-op".to_owned()]
        );
    }

    #[tokio::test]
    async fn cleanup_respects_tick_budget() {
        let now = 4_000_000_020_000;
        let store = MemoryPushStore::new();
        for index in 0..3 {
            store
                .append_delivery_event(delivery_event(
                    &format!("old-{index}"),
                    now - 10_000 + index,
                ))
                .await
                .unwrap();
        }
        let request = PushCleanupRequest {
            now_ms: now,
            policy: PushCleanupPolicy {
                publish_status_retention_ms: 0,
                delivery_event_retention_ms: 5_000,
                operator_event_retention_ms: 0,
                dead_letter_retention_ms: 0,
                batch_size: 10,
                max_deleted_per_tick: 2,
            },
        };

        let report = store
            .cleanup_expired_push_data(request.clone())
            .await
            .unwrap();
        assert_eq!(report.delivery_events.deleted, 2);
        assert_eq!(
            store
                .list_delivery_events("app-1", "publish-1", 10, None)
                .await
                .unwrap()
                .items
                .len(),
            1
        );

        let report = store.cleanup_expired_push_data(request).await.unwrap();
        assert_eq!(report.delivery_events.deleted, 1);
    }

    #[tokio::test]
    async fn cleanup_worker_records_metrics() {
        let now = 4_000_000_020_000;
        let store = Arc::new(MemoryPushStore::new());
        store
            .append_delivery_event(delivery_event("old-event", now - 10_000))
            .await
            .unwrap();
        let metrics = PushMetrics::default();
        let worker = PushCleanupWorker::new(
            store,
            PushCleanupPolicy {
                publish_status_retention_ms: 0,
                delivery_event_retention_ms: 5_000,
                operator_event_retention_ms: 0,
                dead_letter_retention_ms: 0,
                batch_size: 100,
                max_deleted_per_tick: 100,
            },
        )
        .with_metrics(metrics.clone());

        let report = worker.run_once(now).await.unwrap();

        assert_eq!(report.delivery_events.deleted, 1);
        assert_eq!(metrics.get("sockudo_push_cleanup_deleted_total"), 1);
        assert_eq!(metrics.get("sockudo_push_cleanup_scanned_total"), 1);
    }

    async fn seed_status(store: &MemoryPushStore, publish_id: &str, state: PublishLifecycleState) {
        store
            .put_publish_status(PublishStatus {
                app_id: "app-1".to_owned(),
                publish_id: publish_id.to_owned(),
                state,
                counters: PublishCounters {
                    planned: 1,
                    dispatched: 1,
                    succeeded: u64::from(state == PublishLifecycleState::Succeeded),
                    failed: u64::from(state == PublishLifecycleState::Failed),
                    expired: 0,
                    retry_scheduled: 0,
                    retry_attempted: 0,
                    dead_lettered: 0,
                },
                fanout_regime: None,
                retry_after_ms: None,
                error_reason: None,
            })
            .await
            .unwrap();
    }

    fn delivery_event(event_id: &str, occurred_at_ms: u64) -> DeliveryEvent {
        DeliveryEvent {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            event_id: event_id.to_owned(),
            occurred_at_ms,
            result: DeliveryResult {
                app_id: "app-1".to_owned(),
                publish_id: "publish-1".to_owned(),
                provider: PushProviderKind::Fcm,
                batch_id: "batch-1".to_owned(),
                device_id: Some("device-1".to_owned()),
                outcome: DeliveryOutcome::Accepted,
                provider_message_id: None,
                error: None,
                attempt: 1,
            },
        }
    }
}
