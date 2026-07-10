use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use dashmap::DashMap;
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use serde::{Deserialize, Serialize};

use crate::domain::{DeliveryOutcome, DevicePushState, PushProviderKind};
use crate::pipeline::{PushQueueStage, QueueLagMetrics};

pub type PushMetricLabels = Vec<(&'static str, String)>;
pub type PushMetricKey = (&'static str, PushMetricLabels);
pub type PushMetricSnapshotMap = BTreeMap<PushMetricKey, PushMetricSnapshot>;

pub const PUSH_METRIC_SPECS: &[PushMetricSpec] = &[
    PushMetricSpec::counter(
        "sockudo_push_dispatched_total",
        &["provider", "status", "app"],
    ),
    PushMetricSpec::histogram(
        "sockudo_push_dispatch_duration_seconds",
        &["provider", "app"],
    ),
    PushMetricSpec::gauge("sockudo_push_dispatch_inflight", &["provider", "app"]),
    PushMetricSpec::counter("sockudo_push_publish_accepted_total", &["app", "result"]),
    PushMetricSpec::histogram("sockudo_push_publish_acceptance_duration_seconds", &["app"]),
    PushMetricSpec::gauge("sockudo_push_publish_log_lag_seconds", &[]),
    PushMetricSpec::gauge("sockudo_push_queue_oldest_age_seconds", &["stage", "state"]),
    PushMetricSpec::histogram("sockudo_push_planner_duration_seconds", &[]),
    PushMetricSpec::histogram("sockudo_push_fanout_size", &["app"]),
    PushMetricSpec::counter(
        "sockudo_push_delivery_jobs_emitted_total",
        &["provider", "app"],
    ),
    PushMetricSpec::gauge("sockudo_push_delivery_jobs_lag_seconds", &["provider"]),
    PushMetricSpec::gauge("sockudo_push_worker_pool_size", &["provider"]),
    PushMetricSpec::gauge("sockudo_push_worker_pool_busy", &["provider"]),
    PushMetricSpec::gauge("sockudo_push_provider_connections", &["provider", "tenant"]),
    PushMetricSpec::gauge(
        "sockudo_push_provider_streams_active",
        &["provider", "tenant"],
    ),
    PushMetricSpec::gauge(
        "sockudo_push_circuit_breaker_state",
        &["provider", "tenant"],
    ),
    PushMetricSpec::counter(
        "sockudo_push_circuit_breaker_open_total",
        &["provider", "tenant"],
    ),
    PushMetricSpec::counter(
        "sockudo_push_rate_limiter_throttled_total",
        &["provider", "tenant"],
    ),
    PushMetricSpec::counter("sockudo_push_duplicate_suppressed_total", &[]),
    PushMetricSpec::counter("sockudo_push_invariant_violations_total", &["invariant"]),
    PushMetricSpec::counter("sockudo_push_status_cas_conflicts_total", &["component"]),
    PushMetricSpec::counter("sockudo_push_status_cas_exhausted_total", &["component"]),
    PushMetricSpec::gauge("sockudo_push_devices_total", &["app", "state"]),
    PushMetricSpec::counter(
        "sockudo_push_device_state_transitions_total",
        &["from", "to", "app"],
    ),
    PushMetricSpec::counter(
        "sockudo_push_token_invalidations_total",
        &["provider", "app"],
    ),
    PushMetricSpec::counter(
        "sockudo_push_provider_failures_total",
        &["provider", "failure_class", "app"],
    ),
    PushMetricSpec::counter(
        "sockudo_push_token_invalidation_guard_total",
        &["provider", "app"],
    ),
    PushMetricSpec::counter("sockudo_push_quota_acceptance_rejections_total", &["app"]),
    PushMetricSpec::counter("sockudo_push_quota_delivery_rejections_total", &["app"]),
    PushMetricSpec::gauge("sockudo_push_quota_consumed_acceptance", &["app"]),
    PushMetricSpec::gauge("sockudo_push_quota_consumed_delivery", &["app"]),
    PushMetricSpec::counter("sockudo_push_channel_publish_total", &[]),
    PushMetricSpec::counter(
        "sockudo_push_circuit_breaker_deferred_total",
        &["provider", "app"],
    ),
    PushMetricSpec::gauge("sockudo_push_scheduled_jobs_total", &["status"]),
    PushMetricSpec::gauge("sockudo_push_scheduler_lag_seconds", &[]),
    PushMetricSpec::counter("sockudo_push_stale_devices_removed_total", &["app"]),
    PushMetricSpec::counter("sockudo_push_rate_dropped_total", &["app"]),
    PushMetricSpec::counter("sockudo_push_rate_queued_total", &["app"]),
    PushMetricSpec::counter("sockudo_push_delivery_status_total", &["status", "app"]),
    PushMetricSpec::counter("sockudo_push_wfq_dispatched_total", &["provider", "app"]),
    PushMetricSpec::gauge("sockudo_push_wfq_starvation_seconds", &["provider", "app"]),
    PushMetricSpec::counter("sockudo_push_retry_scheduled_total", &["provider", "app"]),
    PushMetricSpec::counter("sockudo_push_retry_attempted_total", &["provider", "app"]),
    PushMetricSpec::counter("sockudo_push_retry_deferred_total", &["provider", "app"]),
    PushMetricSpec::gauge(
        "sockudo_push_retry_deferred_delay_seconds",
        &["provider", "app"],
    ),
    PushMetricSpec::counter("sockudo_push_retry_expired_total", &["provider", "app"]),
    PushMetricSpec::counter(
        "sockudo_push_retry_dead_lettered_total",
        &["provider", "app"],
    ),
    PushMetricSpec::counter(
        "sockudo_push_retry_malformed_total",
        &["provider", "reason"],
    ),
    PushMetricSpec::counter(
        "sockudo_push_worker_exits_total",
        &["kind", "worker", "reason"],
    ),
    PushMetricSpec::counter(
        "sockudo_push_queue_local_requeued_total",
        &["stage", "reason"],
    ),
    PushMetricSpec::counter("sockudo_push_repair_scanned_total", &["category"]),
    PushMetricSpec::counter("sockudo_push_repair_requeued_total", &["stage", "reason"]),
    PushMetricSpec::counter("sockudo_push_repair_skipped_total", &["reason"]),
    PushMetricSpec::counter("sockudo_push_cleanup_scanned_total", &["category"]),
    PushMetricSpec::counter("sockudo_push_cleanup_deleted_total", &["category"]),
    PushMetricSpec::counter(
        "sockudo_push_cleanup_errors_total",
        &["backend", "category"],
    ),
    PushMetricSpec::histogram("sockudo_push_cleanup_tick_duration_seconds", &[]),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushMetricSpec {
    pub name: &'static str,
    pub kind: PushMetricKind,
    pub labels: &'static [&'static str],
}

impl PushMetricSpec {
    pub const fn counter(name: &'static str, labels: &'static [&'static str]) -> Self {
        Self {
            name,
            kind: PushMetricKind::Counter,
            labels,
        }
    }

    pub const fn gauge(name: &'static str, labels: &'static [&'static str]) -> Self {
        Self {
            name,
            kind: PushMetricKind::Gauge,
            labels,
        }
    }

    pub const fn histogram(name: &'static str, labels: &'static [&'static str]) -> Self {
        Self {
            name,
            kind: PushMetricKind::Histogram,
            labels,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PushMetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushMetricSnapshot {
    pub value: f64,
    pub count: u64,
    pub sum: f64,
    #[serde(default)]
    pub buckets: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MetricKey {
    name: &'static str,
    labels: Vec<(&'static str, String)>,
}

#[derive(Clone, Debug)]
pub struct PushMetrics {
    samples: Arc<DashMap<MetricKey, PushMetricSnapshot>>,
}

impl Default for PushMetrics {
    fn default() -> Self {
        describe_push_metrics();
        Self {
            samples: Arc::new(DashMap::new()),
        }
    }
}

impl PushMetrics {
    pub fn increment(&self, name: &'static str, value: u64) {
        self.counter(name, &[], value);
    }

    pub fn counter(&self, name: &'static str, labels: &[(&'static str, &str)], value: u64) {
        self.with_sample(name, labels, |sample| {
            sample.value += value as f64;
        });
        let labels = metrics_labels(labels);
        counter!(name, &labels).increment(value);
    }

    pub fn gauge(&self, name: &'static str, labels: &[(&'static str, &str)], value: f64) {
        self.with_sample(name, labels, |sample| {
            sample.value = value;
        });
        let labels = metrics_labels(labels);
        gauge!(name, &labels).set(value);
    }

    pub fn add_gauge(&self, name: &'static str, labels: &[(&'static str, &str)], delta: f64) {
        self.with_sample(name, labels, |sample| {
            sample.value = (sample.value + delta).max(0.0);
        });
        let labels = metrics_labels(labels);
        let handle = gauge!(name, &labels);
        if delta.is_sign_negative() {
            handle.decrement(delta.abs());
        } else {
            handle.increment(delta);
        }
    }

    pub fn observe(&self, name: &'static str, labels: &[(&'static str, &str)], value: f64) {
        self.with_sample(name, labels, |sample| {
            sample.value = value;
            sample.count = sample.count.saturating_add(1);
            sample.sum += value;
            for bucket in HISTOGRAM_BUCKETS {
                if value <= *bucket {
                    let key = bucket.to_string();
                    *sample.buckets.entry(key).or_default() += 1;
                }
            }
            *sample.buckets.entry("+Inf".to_owned()).or_default() += 1;
        });
        let labels = metrics_labels(labels);
        histogram!(name, &labels).record(value);
    }

    pub fn publish_accepted(&self, app_id: &str, result: &str, duration: Duration) {
        self.counter(
            "sockudo_push_publish_accepted_total",
            &[("app", app_id), ("result", result)],
            1,
        );
        self.observe(
            "sockudo_push_publish_acceptance_duration_seconds",
            &[("app", app_id)],
            duration.as_secs_f64(),
        );
    }

    pub fn publish_log_lag_seconds(&self, seconds: f64) {
        self.gauge("sockudo_push_publish_log_lag_seconds", &[], seconds);
    }

    pub fn queue_lag(&self, stage: PushQueueStage, lag: &QueueLagMetrics) {
        let stage = queue_stage_label(stage);
        for (state, age_ms) in [
            ("ready", lag.oldest_ready_age_ms),
            ("delayed", lag.oldest_delayed_age_ms),
            ("inflight", lag.oldest_inflight_age_ms),
        ] {
            self.gauge(
                "sockudo_push_queue_oldest_age_seconds",
                &[("stage", stage.as_str()), ("state", state)],
                age_ms.map_or(0.0, |age_ms| age_ms as f64 / 1000.0),
            );
        }
    }

    pub fn planner_duration(&self, duration: Duration) {
        self.observe(
            "sockudo_push_planner_duration_seconds",
            &[],
            duration.as_secs_f64(),
        );
    }

    pub fn fanout_size(&self, app_id: &str, recipients: u64) {
        self.observe(
            "sockudo_push_fanout_size",
            &[("app", app_id)],
            recipients as f64,
        );
    }

    pub fn delivery_jobs_emitted(&self, provider: PushProviderKind, app_id: &str, count: u64) {
        self.counter(
            "sockudo_push_delivery_jobs_emitted_total",
            &[("provider", provider_label(provider)), ("app", app_id)],
            count,
        );
    }

    pub fn delivery_jobs_lag_seconds(&self, provider: PushProviderKind, seconds: f64) {
        self.gauge(
            "sockudo_push_delivery_jobs_lag_seconds",
            &[("provider", provider_label(provider))],
            seconds,
        );
    }

    pub fn dispatch_started(&self, provider: PushProviderKind, app_id: &str) {
        self.add_gauge(
            "sockudo_push_dispatch_inflight",
            &[("provider", provider_label(provider)), ("app", app_id)],
            1.0,
        );
    }

    pub fn dispatch_finished(
        &self,
        provider: PushProviderKind,
        app_id: &str,
        outcome: DeliveryOutcome,
        duration: Duration,
    ) {
        self.counter(
            "sockudo_push_dispatched_total",
            &[
                ("provider", provider_label(provider)),
                ("status", delivery_outcome_label(outcome)),
                ("app", app_id),
            ],
            1,
        );
        self.observe(
            "sockudo_push_dispatch_duration_seconds",
            &[("provider", provider_label(provider)), ("app", app_id)],
            duration.as_secs_f64(),
        );
        self.add_gauge(
            "sockudo_push_dispatch_inflight",
            &[("provider", provider_label(provider)), ("app", app_id)],
            -1.0,
        );
    }

    pub fn worker_pool(&self, provider: PushProviderKind, size: usize, busy: usize) {
        self.gauge(
            "sockudo_push_worker_pool_size",
            &[("provider", provider_label(provider))],
            size as f64,
        );
        self.gauge(
            "sockudo_push_worker_pool_busy",
            &[("provider", provider_label(provider))],
            busy as f64,
        );
    }

    pub fn provider_connections(
        &self,
        provider: PushProviderKind,
        tenant: &str,
        connections: usize,
        streams_active: usize,
    ) {
        self.gauge(
            "sockudo_push_provider_connections",
            &[("provider", provider_label(provider)), ("tenant", tenant)],
            connections as f64,
        );
        self.gauge(
            "sockudo_push_provider_streams_active",
            &[("provider", provider_label(provider)), ("tenant", tenant)],
            streams_active as f64,
        );
    }

    pub fn circuit_breaker_state(&self, provider: PushProviderKind, tenant: &str, open: bool) {
        self.gauge(
            "sockudo_push_circuit_breaker_state",
            &[("provider", provider_label(provider)), ("tenant", tenant)],
            if open { 1.0 } else { 0.0 },
        );
        if open {
            self.counter(
                "sockudo_push_circuit_breaker_open_total",
                &[("provider", provider_label(provider)), ("tenant", tenant)],
                1,
            );
        }
    }

    pub fn rate_limiter_throttled(&self, provider: PushProviderKind, tenant: &str) {
        self.counter(
            "sockudo_push_rate_limiter_throttled_total",
            &[("provider", provider_label(provider)), ("tenant", tenant)],
            1,
        );
    }

    pub fn duplicate_suppressed(&self) {
        self.counter("sockudo_push_duplicate_suppressed_total", &[], 1);
    }

    pub fn status_transition_invariant_violation(&self) {
        self.counter(
            "sockudo_push_invariant_violations_total",
            &[("invariant", "status_transition")],
            1,
        );
    }

    pub fn publish_status_cas_conflict(&self, component: &'static str) {
        self.counter(
            "sockudo_push_status_cas_conflicts_total",
            &[("component", component)],
            1,
        );
    }

    pub fn publish_status_cas_exhausted(&self, component: &'static str) {
        self.counter(
            "sockudo_push_status_cas_exhausted_total",
            &[("component", component)],
            1,
        );
    }

    pub fn devices_total(&self, app_id: &str, state: DevicePushState, count: u64) {
        self.gauge(
            "sockudo_push_devices_total",
            &[("app", app_id), ("state", device_state_label(state))],
            count as f64,
        );
    }

    pub fn device_state_transition(
        &self,
        app_id: &str,
        from: DevicePushState,
        to: DevicePushState,
    ) {
        self.counter(
            "sockudo_push_device_state_transitions_total",
            &[
                ("from", device_state_label(from)),
                ("to", device_state_label(to)),
                ("app", app_id),
            ],
            1,
        );
    }

    pub fn token_invalidated(&self, provider: PushProviderKind, app_id: &str) {
        self.counter(
            "sockudo_push_token_invalidations_total",
            &[("provider", provider_label(provider)), ("app", app_id)],
            1,
        );
    }

    pub fn provider_failure_class(
        &self,
        provider: PushProviderKind,
        app_id: &str,
        failure_class: &str,
    ) {
        self.counter(
            "sockudo_push_provider_failures_total",
            &[
                ("provider", provider_label(provider)),
                ("failure_class", failure_class),
                ("app", app_id),
            ],
            1,
        );
    }

    pub fn token_invalidation_guard(&self, provider: PushProviderKind, app_id: &str) {
        self.counter(
            "sockudo_push_token_invalidation_guard_total",
            &[("provider", provider_label(provider)), ("app", app_id)],
            1,
        );
    }

    pub fn quota_acceptance_rejected(&self, app_id: &str) {
        self.counter(
            "sockudo_push_quota_acceptance_rejections_total",
            &[("app", app_id)],
            1,
        );
    }

    pub fn quota_delivery_rejected(&self, app_id: &str) {
        self.counter(
            "sockudo_push_quota_delivery_rejections_total",
            &[("app", app_id)],
            1,
        );
    }

    pub fn quota_consumed(&self, app_id: &str, acceptance: u64, delivery: u64) {
        self.gauge(
            "sockudo_push_quota_consumed_acceptance",
            &[("app", app_id)],
            acceptance as f64,
        );
        self.gauge(
            "sockudo_push_quota_consumed_delivery",
            &[("app", app_id)],
            delivery as f64,
        );
    }

    pub fn channel_publish(&self, _channel: &str) {
        self.counter("sockudo_push_channel_publish_total", &[], 1);
    }

    pub fn scheduled_jobs(&self, status: &str, count: u64) {
        self.gauge(
            "sockudo_push_scheduled_jobs_total",
            &[("status", status)],
            count as f64,
        );
    }

    pub fn scheduler_lag_seconds(&self, seconds: f64) {
        self.gauge("sockudo_push_scheduler_lag_seconds", &[], seconds);
    }

    pub fn stale_devices_removed(&self, app_id: &str, count: u64) {
        self.counter(
            "sockudo_push_stale_devices_removed_total",
            &[("app", app_id)],
            count,
        );
    }

    pub fn rate_dropped(&self, app_id: &str) {
        self.counter("sockudo_push_rate_dropped_total", &[("app", app_id)], 1);
    }

    pub fn rate_queued(&self, app_id: &str) {
        self.counter("sockudo_push_rate_queued_total", &[("app", app_id)], 1);
    }

    pub fn delivery_status(&self, app_id: &str, status: &str) {
        self.counter(
            "sockudo_push_delivery_status_total",
            &[("status", status), ("app", app_id)],
            1,
        );
    }

    pub fn wfq_dispatched(&self, provider: PushProviderKind, app_id: &str) {
        self.counter(
            "sockudo_push_wfq_dispatched_total",
            &[("provider", provider_label(provider)), ("app", app_id)],
            1,
        );
    }

    pub fn wfq_starvation_seconds(&self, provider: PushProviderKind, app_id: &str, seconds: f64) {
        self.gauge(
            "sockudo_push_wfq_starvation_seconds",
            &[("provider", provider_label(provider)), ("app", app_id)],
            seconds,
        );
    }

    pub fn retry_scheduled(&self, provider: PushProviderKind, app_id: &str) {
        self.counter(
            "sockudo_push_retry_scheduled_total",
            &[("provider", provider_label(provider)), ("app", app_id)],
            1,
        );
    }

    pub fn retry_attempted(&self, provider: PushProviderKind, app_id: &str) {
        self.counter(
            "sockudo_push_retry_attempted_total",
            &[("provider", provider_label(provider)), ("app", app_id)],
            1,
        );
    }

    pub fn retry_deferred(&self, provider: &str, app_id: &str, delay_ms: u64) {
        self.counter(
            "sockudo_push_retry_deferred_total",
            &[("provider", provider), ("app", app_id)],
            1,
        );
        self.gauge(
            "sockudo_push_retry_deferred_delay_seconds",
            &[("provider", provider), ("app", app_id)],
            delay_ms as f64 / 1000.0,
        );
    }

    pub fn retry_expired(&self, provider: &str, app_id: &str) {
        self.counter(
            "sockudo_push_retry_expired_total",
            &[("provider", provider), ("app", app_id)],
            1,
        );
    }

    pub fn retry_dead_lettered(&self, provider: &str, app_id: &str) {
        self.counter(
            "sockudo_push_retry_dead_lettered_total",
            &[("provider", provider), ("app", app_id)],
            1,
        );
    }

    pub fn retry_malformed(&self, provider: &str, reason: &str) {
        self.counter(
            "sockudo_push_retry_malformed_total",
            &[("provider", provider), ("reason", reason)],
            1,
        );
    }

    pub fn worker_exit(&self, kind: &str, worker: &str, reason: &str) {
        self.counter(
            "sockudo_push_worker_exits_total",
            &[("kind", kind), ("worker", worker), ("reason", reason)],
            1,
        );
    }

    pub fn queue_local_requeued(&self, stage: &str, reason: &str) {
        self.counter(
            "sockudo_push_queue_local_requeued_total",
            &[("stage", stage), ("reason", reason)],
            1,
        );
    }

    pub fn repair_scanned(&self, category: &str, count: u64) {
        self.counter(
            "sockudo_push_repair_scanned_total",
            &[("category", category)],
            count,
        );
    }

    pub fn repair_requeued(&self, stage: &str, reason: &str) {
        self.counter(
            "sockudo_push_repair_requeued_total",
            &[("stage", stage), ("reason", reason)],
            1,
        );
    }

    pub fn repair_skipped(&self, reason: &str) {
        self.counter(
            "sockudo_push_repair_skipped_total",
            &[("reason", reason)],
            1,
        );
    }

    pub fn cleanup_scanned(&self, category: &str, count: u64) {
        self.counter(
            "sockudo_push_cleanup_scanned_total",
            &[("category", category)],
            count,
        );
    }

    pub fn cleanup_deleted(&self, category: &str, count: u64) {
        self.counter(
            "sockudo_push_cleanup_deleted_total",
            &[("category", category)],
            count,
        );
    }

    pub fn cleanup_error(&self, backend: &str, category: &str) {
        self.counter(
            "sockudo_push_cleanup_errors_total",
            &[("backend", backend), ("category", category)],
            1,
        );
    }

    pub fn cleanup_tick_duration(&self, duration: Duration) {
        self.observe(
            "sockudo_push_cleanup_tick_duration_seconds",
            &[],
            duration.as_secs_f64(),
        );
    }

    pub fn delivery_result(&self, provider: PushProviderKind, app_id: &str, outcome: &str) {
        self.counter(
            "sockudo_push_dispatched_total",
            &[
                ("provider", provider_label(provider)),
                ("status", outcome),
                ("app", app_id),
            ],
            1,
        );
    }

    pub fn get(&self, name: &str) -> u64 {
        self.samples
            .iter()
            .filter(|entry| entry.key().name == name)
            .map(|entry| entry.value().value as u64)
            .sum()
    }

    pub fn snapshot(&self) -> PushMetricSnapshotMap {
        self.samples
            .iter()
            .map(|entry| {
                (
                    (entry.key().name, entry.key().labels.clone()),
                    entry.value().clone(),
                )
            })
            .collect()
    }

    fn with_sample(
        &self,
        name: &'static str,
        labels: &[(&'static str, &str)],
        update: impl FnOnce(&mut PushMetricSnapshot),
    ) {
        let key = MetricKey {
            name,
            labels: labels
                .iter()
                .map(|(label, value)| (*label, (*value).to_owned()))
                .collect(),
        };
        let mut sample = self.samples.entry(key).or_default();
        update(&mut sample);
    }
}

const HISTOGRAM_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

fn describe_push_metrics() {
    static DESCRIBED: OnceLock<()> = OnceLock::new();
    DESCRIBED.get_or_init(|| {
        for spec in PUSH_METRIC_SPECS {
            match spec.kind {
                PushMetricKind::Counter => {
                    describe_counter!(spec.name, "Sockudo push metric.");
                }
                PushMetricKind::Gauge => {
                    describe_gauge!(spec.name, "Sockudo push metric.");
                }
                PushMetricKind::Histogram => {
                    describe_histogram!(spec.name, "Sockudo push metric.");
                }
            }
        }
    });
}

fn metrics_labels(labels: &[(&'static str, &str)]) -> Vec<(String, String)> {
    labels
        .iter()
        .map(|(label, value)| ((*label).to_owned(), (*value).to_owned()))
        .collect()
}

pub fn provider_label(provider: PushProviderKind) -> &'static str {
    match provider {
        PushProviderKind::Fcm => "fcm",
        PushProviderKind::Apns => "apns",
        PushProviderKind::WebPush => "webpush",
        PushProviderKind::Hms => "hms",
        PushProviderKind::Wns => "wns",
    }
}

pub fn queue_stage_label(stage: PushQueueStage) -> String {
    match stage {
        PushQueueStage::PublishLog => "publish_log".to_owned(),
        PushQueueStage::ShardJobs => "shard_jobs".to_owned(),
        PushQueueStage::DeliveryJobs(provider) => {
            format!("delivery_jobs_{}", provider_label(provider))
        }
        PushQueueStage::DeliveryResults => "delivery_results".to_owned(),
        PushQueueStage::DeadLetters => "dead_letters".to_owned(),
        PushQueueStage::RetrySchedule => "retry_schedule".to_owned(),
    }
}

pub fn delivery_outcome_label(outcome: DeliveryOutcome) -> &'static str {
    match outcome {
        DeliveryOutcome::Accepted => "accepted",
        DeliveryOutcome::Rejected => "rejected",
        DeliveryOutcome::Retryable => "retryable",
        DeliveryOutcome::Expired => "expired",
        DeliveryOutcome::Cancelled => "cancelled",
    }
}

pub fn device_state_label(state: DevicePushState) -> &'static str {
    match state {
        DevicePushState::Active => "active",
        DevicePushState::Failing => "failing",
        DevicePushState::Failed => "failed",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const DASHBOARD: &str = include_str!("../../../ops/dashboards/push.json");

    #[test]
    fn metric_catalog_covers_release_4_5_observability_contract() {
        let names = PUSH_METRIC_SPECS
            .iter()
            .map(|spec| spec.name)
            .collect::<BTreeSet<_>>();
        assert!(names.contains("sockudo_push_invariant_violations_total"));
        assert!(names.contains("sockudo_push_status_cas_conflicts_total"));
        assert!(names.contains("sockudo_push_status_cas_exhausted_total"));
        let required = [
            "sockudo_push_dispatched_total",
            "sockudo_push_dispatch_duration_seconds",
            "sockudo_push_dispatch_inflight",
            "sockudo_push_publish_accepted_total",
            "sockudo_push_publish_acceptance_duration_seconds",
            "sockudo_push_publish_log_lag_seconds",
            "sockudo_push_queue_oldest_age_seconds",
            "sockudo_push_planner_duration_seconds",
            "sockudo_push_fanout_size",
            "sockudo_push_delivery_jobs_emitted_total",
            "sockudo_push_delivery_jobs_lag_seconds",
            "sockudo_push_worker_pool_size",
            "sockudo_push_worker_pool_busy",
            "sockudo_push_provider_connections",
            "sockudo_push_provider_streams_active",
            "sockudo_push_circuit_breaker_state",
            "sockudo_push_circuit_breaker_open_total",
            "sockudo_push_rate_limiter_throttled_total",
            "sockudo_push_duplicate_suppressed_total",
            "sockudo_push_devices_total",
            "sockudo_push_device_state_transitions_total",
            "sockudo_push_token_invalidations_total",
            "sockudo_push_provider_failures_total",
            "sockudo_push_token_invalidation_guard_total",
            "sockudo_push_quota_acceptance_rejections_total",
            "sockudo_push_quota_delivery_rejections_total",
            "sockudo_push_quota_consumed_acceptance",
            "sockudo_push_quota_consumed_delivery",
            "sockudo_push_channel_publish_total",
            "sockudo_push_scheduled_jobs_total",
            "sockudo_push_scheduler_lag_seconds",
            "sockudo_push_stale_devices_removed_total",
            "sockudo_push_rate_dropped_total",
            "sockudo_push_rate_queued_total",
            "sockudo_push_delivery_status_total",
            "sockudo_push_wfq_dispatched_total",
            "sockudo_push_wfq_starvation_seconds",
            "sockudo_push_retry_scheduled_total",
            "sockudo_push_retry_attempted_total",
            "sockudo_push_retry_deferred_total",
            "sockudo_push_retry_deferred_delay_seconds",
            "sockudo_push_retry_expired_total",
            "sockudo_push_retry_dead_lettered_total",
            "sockudo_push_retry_malformed_total",
            "sockudo_push_cleanup_scanned_total",
            "sockudo_push_cleanup_deleted_total",
            "sockudo_push_cleanup_errors_total",
            "sockudo_push_cleanup_tick_duration_seconds",
            "sockudo_push_repair_scanned_total",
            "sockudo_push_repair_requeued_total",
            "sockudo_push_repair_skipped_total",
        ];

        for name in required {
            assert!(names.contains(name), "missing {name}");
            assert!(DASHBOARD.contains(name), "dashboard missing {name}");
        }
    }

    #[test]
    fn push_metrics_records_labeled_samples() {
        let metrics = PushMetrics::default();
        metrics.publish_accepted("app-1", "accepted", Duration::from_millis(25));
        metrics.dispatch_finished(
            PushProviderKind::Fcm,
            "app-1",
            DeliveryOutcome::Accepted,
            Duration::from_millis(100),
        );
        metrics.duplicate_suppressed();
        metrics.status_transition_invariant_violation();
        metrics.publish_status_cas_conflict("feedback");
        metrics.publish_status_cas_exhausted("feedback");

        assert_eq!(metrics.get("sockudo_push_publish_accepted_total"), 1);
        assert_eq!(metrics.get("sockudo_push_dispatched_total"), 1);
        assert_eq!(metrics.get("sockudo_push_duplicate_suppressed_total"), 1);
        assert_eq!(metrics.get("sockudo_push_invariant_violations_total"), 1);
        assert_eq!(metrics.get("sockudo_push_status_cas_conflicts_total"), 1);
        assert_eq!(metrics.get("sockudo_push_status_cas_exhausted_total"), 1);
        let invariant_labels = metrics
            .snapshot()
            .into_iter()
            .filter_map(|((name, labels), _)| {
                (name == "sockudo_push_invariant_violations_total")
                    .then(|| labels.into_iter().find(|(key, _)| *key == "invariant"))
                    .flatten()
                    .map(|(_, value)| value)
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            invariant_labels,
            BTreeSet::from(["status_transition".to_owned()])
        );
        assert_eq!(
            metrics
                .snapshot()
                .iter()
                .filter(|((name, _), _)| *name == "sockudo_push_publish_acceptance_duration_seconds")
                .map(|(_, sample)| sample.count)
                .sum::<u64>(),
            1
        );
        assert!(metrics.snapshot().iter().any(|((name, _), sample)| *name
            == "sockudo_push_dispatch_duration_seconds"
            && sample.count == 1));
    }

    #[test]
    fn cleanup_metrics_use_bounded_labels() {
        let metrics = PushMetrics::default();
        metrics.cleanup_scanned("delivery_event", 10);
        metrics.cleanup_deleted("delivery_event", 3);
        metrics.cleanup_error("memory", "tick");
        metrics.cleanup_tick_duration(Duration::from_millis(25));

        assert_eq!(metrics.get("sockudo_push_cleanup_scanned_total"), 10);
        assert_eq!(metrics.get("sockudo_push_cleanup_deleted_total"), 3);
        assert_eq!(metrics.get("sockudo_push_cleanup_errors_total"), 1);
        assert!(metrics.snapshot().iter().all(|((name, labels), _)| {
            !name.starts_with("sockudo_push_cleanup")
                || labels
                    .iter()
                    .all(|(key, _)| !matches!(*key, "publish_id" | "device_id" | "token"))
        }));
    }
}
