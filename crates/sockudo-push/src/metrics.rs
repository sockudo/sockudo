use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::domain::{DeliveryOutcome, DevicePushState, PushProviderKind};

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
    PushMetricSpec::gauge("sockudo_push_devices_total", &["app", "state"]),
    PushMetricSpec::counter(
        "sockudo_push_device_state_transitions_total",
        &["from", "to", "app"],
    ),
    PushMetricSpec::counter(
        "sockudo_push_token_invalidations_total",
        &["provider", "app"],
    ),
    PushMetricSpec::counter("sockudo_push_quota_acceptance_rejections_total", &["app"]),
    PushMetricSpec::counter("sockudo_push_quota_delivery_rejections_total", &["app"]),
    PushMetricSpec::gauge("sockudo_push_quota_consumed_acceptance", &["app"]),
    PushMetricSpec::gauge("sockudo_push_quota_consumed_delivery", &["app"]),
    PushMetricSpec::counter("sockudo_push_channel_publish_total", &["channel"]),
    PushMetricSpec::gauge("sockudo_push_scheduled_jobs_total", &["status"]),
    PushMetricSpec::gauge("sockudo_push_scheduler_lag_seconds", &[]),
    PushMetricSpec::counter("sockudo_push_stale_devices_removed_total", &["app"]),
    PushMetricSpec::counter("sockudo_push_rate_dropped_total", &["app"]),
    PushMetricSpec::counter("sockudo_push_rate_queued_total", &["app"]),
    PushMetricSpec::counter("sockudo_push_delivery_status_total", &["status", "app"]),
    PushMetricSpec::counter("sockudo_push_wfq_dispatched_total", &["provider", "app"]),
    PushMetricSpec::gauge("sockudo_push_wfq_starvation_seconds", &["provider", "app"]),
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
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MetricKey {
    name: &'static str,
    labels: Vec<(&'static str, String)>,
}

#[derive(Clone, Default)]
pub struct PushMetrics {
    samples: Arc<Mutex<BTreeMap<MetricKey, PushMetricSnapshot>>>,
}

impl PushMetrics {
    pub fn increment(&self, name: impl Into<String>, value: u64) {
        let name = name.into();
        let name = Box::leak(name.into_boxed_str());
        self.counter(name, &[], value);
    }

    pub fn counter(&self, name: &'static str, labels: &[(&'static str, &str)], value: u64) {
        self.with_sample(name, labels, |sample| {
            sample.value += value as f64;
        });
    }

    pub fn gauge(&self, name: &'static str, labels: &[(&'static str, &str)], value: f64) {
        self.with_sample(name, labels, |sample| {
            sample.value = value;
        });
    }

    pub fn observe(&self, name: &'static str, labels: &[(&'static str, &str)], value: f64) {
        self.with_sample(name, labels, |sample| {
            sample.value = value;
            sample.count = sample.count.saturating_add(1);
            sample.sum += value;
        });
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
        self.gauge(
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
        self.gauge(
            "sockudo_push_dispatch_inflight",
            &[("provider", provider_label(provider)), ("app", app_id)],
            0.0,
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

    pub fn channel_publish(&self, channel: &str) {
        self.counter(
            "sockudo_push_channel_publish_total",
            &[("channel", channel)],
            1,
        );
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

    pub fn delivery_result(&self, provider: PushProviderKind, outcome: &str) {
        self.counter(
            "sockudo_push_dispatched_total",
            &[
                ("provider", provider_label(provider)),
                ("status", outcome),
                ("app", "unknown"),
            ],
            1,
        );
    }

    pub fn get(&self, name: &str) -> u64 {
        self.samples
            .lock()
            .ok()
            .map(|samples| {
                samples
                    .iter()
                    .filter(|(key, _)| key.name == name)
                    .map(|(_, sample)| sample.value as u64)
                    .sum()
            })
            .unwrap_or(0)
    }

    pub fn snapshot(&self) -> PushMetricSnapshotMap {
        self.samples
            .lock()
            .map(|samples| {
                samples
                    .iter()
                    .map(|(key, sample)| ((key.name, key.labels.clone()), sample.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn to_prometheus_text(&self) -> String {
        let specs = PUSH_METRIC_SPECS
            .iter()
            .map(|spec| (spec.name, spec))
            .collect::<BTreeMap<_, _>>();
        let mut out = String::new();
        for ((name, labels), sample) in self.snapshot() {
            let kind = specs
                .get(name)
                .map(|spec| spec.kind)
                .unwrap_or(PushMetricKind::Gauge);
            match kind {
                PushMetricKind::Counter | PushMetricKind::Gauge => {
                    push_metric_line(&mut out, name, &labels, sample.value);
                }
                PushMetricKind::Histogram => {
                    let mut bucket_labels = labels.clone();
                    bucket_labels.push(("le", "+Inf".to_owned()));
                    push_metric_line(
                        &mut out,
                        &format!("{name}_bucket"),
                        &bucket_labels,
                        sample.count as f64,
                    );
                    push_metric_line(&mut out, &format!("{name}_sum"), &labels, sample.sum);
                    push_metric_line(
                        &mut out,
                        &format!("{name}_count"),
                        &labels,
                        sample.count as f64,
                    );
                }
            }
        }
        out
    }

    fn with_sample(
        &self,
        name: &'static str,
        labels: &[(&'static str, &str)],
        update: impl FnOnce(&mut PushMetricSnapshot),
    ) {
        if let Ok(mut samples) = self.samples.lock() {
            let key = MetricKey {
                name,
                labels: labels
                    .iter()
                    .map(|(label, value)| (*label, (*value).to_owned()))
                    .collect(),
            };
            update(samples.entry(key).or_default());
        }
    }
}

fn push_metric_line(out: &mut String, name: &str, labels: &[(&'static str, String)], value: f64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (label, label_value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(label);
            out.push_str("=\"");
            for ch in label_value.chars() {
                match ch {
                    '\\' => out.push_str("\\\\"),
                    '"' => out.push_str("\\\""),
                    '\n' => out.push_str("\\n"),
                    _ => out.push(ch),
                }
            }
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
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
        let required = [
            "sockudo_push_dispatched_total",
            "sockudo_push_dispatch_duration_seconds",
            "sockudo_push_dispatch_inflight",
            "sockudo_push_publish_accepted_total",
            "sockudo_push_publish_acceptance_duration_seconds",
            "sockudo_push_publish_log_lag_seconds",
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

        assert_eq!(metrics.get("sockudo_push_publish_accepted_total"), 1);
        assert_eq!(metrics.get("sockudo_push_dispatched_total"), 1);
        assert_eq!(metrics.get("sockudo_push_duplicate_suppressed_total"), 1);
        assert_eq!(
            metrics
                .snapshot()
                .iter()
                .filter(|((name, _), _)| *name == "sockudo_push_publish_acceptance_duration_seconds")
                .map(|(_, sample)| sample.count)
                .sum::<u64>(),
            1
        );
        let plaintext = metrics.to_prometheus_text();
        assert!(plaintext.contains("sockudo_push_publish_accepted_total"));
        assert!(plaintext.contains("sockudo_push_dispatch_duration_seconds_count"));
    }
}
