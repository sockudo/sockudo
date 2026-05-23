#![allow(dead_code)]

use sockudo_core::error::Result;

use async_trait::async_trait;
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use metrics_exporter_tcp::TcpBuilder;
use metrics_util::layers::FanoutBuilder;
use sockudo_core::channel::ChannelType;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::utils::resolve_socket_addr;
use sockudo_core::websocket::SocketId;
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, warn};

// Histogram buckets for internal operations (in milliseconds)
// Optimized for sub-millisecond to low-millisecond measurements
const INTERNAL_LATENCY_HISTOGRAM_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

// Histogram buckets for end-to-end operations (in milliseconds)
// Covers typical response times and high-latency scenarios
const END_TO_END_LATENCY_HISTOGRAM_BUCKETS: &[f64] = &[
    0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
];

const ANNOTATION_REBUILD_HISTOGRAM_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Optional TCP metrics exporter settings.
#[derive(Debug, Clone)]
pub struct TcpExporterOptions {
    /// Host or IP address for the TCP exporter listener.
    pub host: String,
    /// TCP exporter listener port.
    pub port: u16,
    /// Internal and per-client buffer size. `None` makes the exporter unbounded.
    pub buffer_size: Option<usize>,
}

#[derive(Debug, Clone)]
struct ResolvedTcpExporterOptions {
    listen_addr: SocketAddr,
    buffer_size: Option<usize>,
}

#[derive(Debug)]
struct MetricsRegistrationError;

struct Opts {
    name: String,
    help: String,
}

impl Opts {
    fn new(name: impl Into<String>, help: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            help: help.into(),
        }
    }
}

struct HistogramOpts {
    name: String,
    help: String,
}

macro_rules! histogram_opts {
    ($name:expr, $help:expr, $buckets:expr) => {{
        let _ = $buckets;
        HistogramOpts {
            name: $name.into(),
            help: $help.into(),
        }
    }};
}

macro_rules! register_gauge {
    ($opts:expr) => {{
        let opts = $opts;
        describe_gauge!(opts.name.clone(), opts.help.clone());
        Ok::<Gauge, MetricsRegistrationError>(Gauge {
            name: opts.name,
            value: AtomicU64::new(0),
        })
    }};
}

macro_rules! register_gauge_vec {
    ($opts:expr, $labels:expr) => {{
        let opts = $opts;
        describe_gauge!(opts.name.clone(), opts.help.clone());
        Ok::<GaugeVec, MetricsRegistrationError>(GaugeVec {
            name: opts.name,
            label_names: $labels,
        })
    }};
}

macro_rules! register_counter_vec {
    ($opts:expr, $labels:expr) => {{
        let opts = $opts;
        describe_counter!(opts.name.clone(), opts.help.clone());
        Ok::<CounterVec, MetricsRegistrationError>(CounterVec {
            name: opts.name,
            label_names: $labels,
        })
    }};
}

macro_rules! register_histogram_vec {
    ($opts:expr, $labels:expr) => {{
        let opts = $opts;
        describe_histogram!(opts.name.clone(), opts.help.clone());
        Ok::<HistogramVec, MetricsRegistrationError>(HistogramVec {
            name: opts.name,
            label_names: $labels,
        })
    }};
}

fn install_prometheus_recorder(
    prefix: &str,
    tcp_exporter: Option<ResolvedTcpExporterOptions>,
) -> PrometheusHandle {
    PROMETHEUS_HANDLE
        .get_or_init(|| {
            #[allow(deprecated)]
            let mut builder = PrometheusBuilder::new()
                .set_enable_unit_suffix(false)
                .set_buckets(END_TO_END_LATENCY_HISTOGRAM_BUCKETS)
                .expect("valid Prometheus histogram buckets");

            for (metric, buckets) in [
                (
                    format!("{prefix}horizontal_adapter_resolve_time"),
                    INTERNAL_LATENCY_HISTOGRAM_BUCKETS,
                ),
                (
                    format!("{prefix}annotation_projection_rebuild_duration_seconds"),
                    ANNOTATION_REBUILD_HISTOGRAM_BUCKETS,
                ),
            ] {
                builder = builder
                    .set_buckets_for_metric(Matcher::Full(metric), buckets)
                    .expect("valid Prometheus histogram bucket override");
            }

            let prometheus_recorder = builder.build_recorder();
            let handle = prometheus_recorder.handle();

            if let Some(tcp_exporter) = tcp_exporter {
                let tcp_recorder = TcpBuilder::new()
                    .listen_address(tcp_exporter.listen_addr)
                    .buffer_size(tcp_exporter.buffer_size)
                    .build();

                match tcp_recorder {
                    Ok(tcp_recorder) => {
                        let fanout = FanoutBuilder::default()
                            .add_recorder(prometheus_recorder)
                            .add_recorder(tcp_recorder)
                            .build();
                        metrics::set_global_recorder(fanout)
                            .expect("failed to install metrics-rs fanout recorder");
                    }
                    Err(error) => {
                        warn!(
                            "Failed to start metrics-rs TCP exporter on {}: {}. Continuing with Prometheus exporter only.",
                            tcp_exporter.listen_addr, error
                        );
                        metrics::set_global_recorder(prometheus_recorder)
                            .expect("failed to install metrics-rs Prometheus recorder");
                    }
                }
            } else {
                metrics::set_global_recorder(prometheus_recorder)
                    .expect("failed to install metrics-rs Prometheus recorder");
            }

            handle
        })
        .clone()
}

trait MetricLabelValues {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)>;
}

fn labels_from_iter<'a>(
    label_names: &[&str],
    values: impl IntoIterator<Item = &'a str>,
) -> Vec<(String, String)> {
    let values = values.into_iter().collect::<Vec<_>>();
    debug_assert_eq!(
        label_names.len(),
        values.len(),
        "metric label/value count mismatch"
    );

    label_names
        .iter()
        .zip(values)
        .map(|(name, value)| ((*name).to_owned(), value.to_owned()))
        .collect()
}

impl<'a> MetricLabelValues for &'a [&'a str] {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().copied())
    }
}

impl<'a, const N: usize> MetricLabelValues for &'a [&'a str; N] {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().copied())
    }
}

impl<'a, const N: usize> MetricLabelValues for &'a [&'a String; N] {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().map(|value| value.as_str()))
    }
}

impl<'a> MetricLabelValues for &'a [&'a String] {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().map(|value| value.as_str()))
    }
}

impl MetricLabelValues for &Vec<String> {
    fn to_pairs(self, label_names: &[&str]) -> Vec<(String, String)> {
        labels_from_iter(label_names, self.iter().map(String::as_str))
    }
}

struct Gauge {
    name: String,
    value: AtomicU64,
}

impl Gauge {
    fn set(&self, value: f64) {
        self.value.store(value.to_bits(), Ordering::Relaxed);
        gauge!(self.name.clone()).set(value);
    }

    fn get(&self) -> f64 {
        f64::from_bits(self.value.load(Ordering::Relaxed))
    }
}

struct GaugeVec {
    name: String,
    label_names: &'static [&'static str],
}

impl GaugeVec {
    fn with_label_values(&self, values: impl MetricLabelValues) -> GaugeWithLabels {
        GaugeWithLabels {
            name: self.name.clone(),
            labels: values.to_pairs(self.label_names),
        }
    }

    fn reset(&self) {}
}

struct GaugeWithLabels {
    name: String,
    labels: Vec<(String, String)>,
}

impl GaugeWithLabels {
    fn inc(&self) {
        gauge!(self.name.clone(), &self.labels).increment(1.0);
    }

    fn dec(&self) {
        gauge!(self.name.clone(), &self.labels).decrement(1.0);
    }

    fn set(&self, value: f64) {
        gauge!(self.name.clone(), &self.labels).set(value);
    }
}

struct CounterVec {
    name: String,
    label_names: &'static [&'static str],
}

impl CounterVec {
    fn with_label_values(&self, values: impl MetricLabelValues) -> CounterWithLabels {
        CounterWithLabels {
            name: self.name.clone(),
            labels: values.to_pairs(self.label_names),
        }
    }
}

struct CounterWithLabels {
    name: String,
    labels: Vec<(String, String)>,
}

impl CounterWithLabels {
    fn inc(&self) {
        counter!(self.name.clone(), &self.labels).increment(1);
    }

    fn inc_by(&self, value: f64) {
        if value.is_sign_positive() {
            counter!(self.name.clone(), &self.labels).increment(value as u64);
        }
    }
}

struct HistogramVec {
    name: String,
    label_names: &'static [&'static str],
}

impl HistogramVec {
    fn with_label_values(&self, values: impl MetricLabelValues) -> HistogramWithLabels {
        HistogramWithLabels {
            name: self.name.clone(),
            labels: values.to_pairs(self.label_names),
        }
    }
}

struct HistogramWithLabels {
    name: String,
    labels: Vec<(String, String)>,
}

impl HistogramWithLabels {
    fn observe(&self, value: f64) {
        histogram!(self.name.clone(), &self.labels).record(value);
    }
}

fn prometheus_text_to_json(text: &str) -> Value {
    let mut raw = json!({});

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((metric, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };

        let (name, labels) = parse_metric_and_labels(metric);
        if labels.as_object().is_some_and(|labels| labels.is_empty()) {
            raw[name] = json!(value);
        } else {
            if !raw.as_object().unwrap().contains_key(&name) {
                raw[name] = json!([]);
            }
            let mut metric_with_labels = labels;
            metric_with_labels["value"] = json!(value);
            raw[name].as_array_mut().unwrap().push(metric_with_labels);
        }
    }

    raw
}

fn parse_metric_and_labels(metric: &str) -> (&str, Value) {
    let Some(label_start) = metric.find('{') else {
        return (metric, json!({}));
    };
    let Some(label_end) = metric.rfind('}') else {
        return (metric, json!({}));
    };

    let name = &metric[..label_start];
    let labels = &metric[label_start + 1..label_end];
    let mut label_json = json!({});

    for label in labels.split(',') {
        let Some((key, value)) = label.split_once("=\"") else {
            continue;
        };
        let value = value.strip_suffix('"').unwrap_or(value);
        label_json[key] = json!(value);
    }

    (name, label_json)
}

/// A Prometheus implementation of the metrics interface
pub struct PrometheusMetricsDriver {
    prefix: String,
    port: u16,
    handle: PrometheusHandle,

    // Process metrics (for memory leak detection)
    process_resident_memory_bytes: Gauge,
    process_virtual_memory_bytes: Gauge,
    process_cpu_seconds_total: Gauge,
    process_start_time_seconds: Gauge,
    process_open_fds: Gauge,
    process_max_fds: Gauge,

    // Tokio runtime metrics
    tokio_workers_count: Gauge,
    tokio_active_tasks: Gauge,
    tokio_injection_queue_depth: Gauge,
    tokio_worker_local_queue_depth: GaugeVec,
    tokio_worker_busy_ratio: GaugeVec,
    tokio_worker_mean_poll_duration_us: Gauge,
    tokio_budget_forced_yield_count: Gauge,

    // Metrics
    connected_sockets: GaugeVec,
    new_connections_total: CounterVec,
    new_disconnections_total: CounterVec,
    connection_errors_total: CounterVec,
    socket_bytes_received: CounterVec,
    socket_bytes_transmitted: CounterVec,
    ws_messages_received: CounterVec,
    ws_messages_sent: CounterVec,
    http_bytes_received: CounterVec,
    http_bytes_transmitted: CounterVec,
    http_calls_received: CounterVec,
    horizontal_adapter_resolve_time: HistogramVec,
    horizontal_adapter_resolved_promises: CounterVec,
    horizontal_adapter_uncomplete_promises: CounterVec,
    horizontal_adapter_sent_requests: CounterVec,
    horizontal_adapter_received_requests: CounterVec,
    horizontal_adapter_received_responses: CounterVec,
    rate_limit_checks_total: CounterVec,
    rate_limit_triggered_total: CounterVec,
    channel_subscriptions_total: CounterVec,
    channel_unsubscriptions_total: CounterVec,
    active_channels: GaugeVec,
    broadcast_latency_ms: HistogramVec,
    // Delta compression metrics
    horizontal_delta_compression_enabled: CounterVec,
    delta_compression_bandwidth_saved: CounterVec,
    delta_compression_bandwidth_original: CounterVec,
    delta_compression_full_messages: CounterVec,
    delta_compression_delta_messages: CounterVec,
    // Idempotency metrics
    idempotency_publish_total: CounterVec,
    idempotency_duplicates_total: CounterVec,
    // Ephemeral message metrics
    ephemeral_messages_total: CounterVec,
    // Event name filter metrics
    event_filter_suppressed_total: CounterVec,
    // Echo control metrics
    echo_suppressed_total: CounterVec,
    // History metrics
    history_writes_total: CounterVec,
    history_write_failures_total: CounterVec,
    history_write_latency_ms: HistogramVec,
    history_retained_messages: GaugeVec,
    history_retained_bytes: GaugeVec,
    history_evictions_total: CounterVec,
    history_evicted_bytes_total: CounterVec,
    history_queue_depth: GaugeVec,
    history_degraded_channels: GaugeVec,
    history_reset_required_channels: GaugeVec,
    history_recovery_success_total: CounterVec,
    history_recovery_failures_total: CounterVec,
    versioned_message_mutations_total: CounterVec,
    versioned_message_retrieval_total: CounterVec,
    versioned_history_substitution_total: CounterVec,
    presence_history_writes_total: CounterVec,
    presence_history_write_failures_total: CounterVec,
    presence_history_write_latency_ms: HistogramVec,
    presence_history_retained_events: GaugeVec,
    presence_history_retained_bytes: GaugeVec,
    presence_history_evictions_total: CounterVec,
    presence_history_evicted_bytes_total: CounterVec,
    presence_history_queue_depth: GaugeVec,
    presence_history_degraded_channels: GaugeVec,
    presence_history_reset_required_channels: GaugeVec,
    annotations_published_total: CounterVec,
    annotations_deleted_total: CounterVec,
    annotation_summary_deliveries_total: CounterVec,
    annotation_summary_clipped_total: CounterVec,
    annotation_projection_rebuild_total: CounterVec,
    annotation_projection_rebuild_duration_seconds: HistogramVec,
    delta_cluster_coordination_ops_total: CounterVec,
    delta_cluster_coordination_failures_total: CounterVec,
    delta_cluster_coordination_latency_ms: HistogramVec,
    delta_cluster_coordination_decisions_total: CounterVec,
    delta_cluster_coordination_backend_up: GaugeVec,
    horizontal_transport_queue_depth: GaugeVec,
    horizontal_transport_messages_dropped_total: CounterVec,
    horizontal_transport_reconnections_total: CounterVec,
}

impl PrometheusMetricsDriver {
    /// Creates a new Prometheus metrics driver
    pub async fn new(port: u16, prefix_opt: Option<&str>) -> Self {
        Self::with_tcp_exporter(port, prefix_opt, None).await
    }

    /// Creates a new Prometheus metrics driver with an optional TCP exporter fanout.
    pub async fn with_tcp_exporter(
        port: u16,
        prefix_opt: Option<&str>,
        tcp_exporter: Option<TcpExporterOptions>,
    ) -> Self {
        let prefix = prefix_opt.unwrap_or("sockudo_").to_string();
        let tcp_exporter = if let Some(options) = tcp_exporter {
            let listen_addr =
                resolve_socket_addr(&options.host, options.port, "Metrics TCP exporter").await;
            Some(ResolvedTcpExporterOptions {
                listen_addr,
                buffer_size: options.buffer_size,
            })
        } else {
            None
        };
        let handle = install_prometheus_recorder(&prefix, tcp_exporter);

        // Initialize process metrics (standard Prometheus naming, no prefix)
        let process_resident_memory_bytes = register_gauge!(Opts::new(
            "process_resident_memory_bytes",
            "Resident memory size in bytes (RSS)"
        ))
        .unwrap();

        let process_virtual_memory_bytes = register_gauge!(Opts::new(
            "process_virtual_memory_bytes",
            "Virtual memory size in bytes"
        ))
        .unwrap();

        let process_cpu_seconds_total = register_gauge!(Opts::new(
            "process_cpu_seconds_total",
            "Total user and system CPU time spent in seconds"
        ))
        .unwrap();

        let process_start_time_seconds = register_gauge!(Opts::new(
            "process_start_time_seconds",
            "Start time of the process since unix epoch in seconds"
        ))
        .unwrap();

        let process_open_fds = register_gauge!(Opts::new(
            "process_open_fds",
            "Number of open file descriptors"
        ))
        .unwrap();

        let process_max_fds = register_gauge!(Opts::new(
            "process_max_fds",
            "Maximum number of open file descriptors"
        ))
        .unwrap();

        // Tokio runtime metrics
        let tokio_workers_count = register_gauge!(Opts::new(
            format!("{prefix}tokio_workers_count"),
            "Number of Tokio runtime worker threads"
        ))
        .unwrap();

        let tokio_active_tasks = register_gauge!(Opts::new(
            format!("{prefix}tokio_active_tasks"),
            "Number of active tasks in the Tokio runtime"
        ))
        .unwrap();

        let tokio_injection_queue_depth = register_gauge!(Opts::new(
            format!("{prefix}tokio_injection_queue_depth"),
            "Depth of the Tokio runtime global injection queue"
        ))
        .unwrap();

        let tokio_worker_local_queue_depth = register_gauge_vec!(
            Opts::new(
                format!("{prefix}tokio_worker_local_queue_depth"),
                "Depth of each Tokio worker thread's local task queue"
            ),
            &["worker"]
        )
        .unwrap();

        let tokio_worker_busy_ratio = register_gauge_vec!(
            Opts::new(
                format!("{prefix}tokio_worker_busy_ratio"),
                "Ratio of time each Tokio worker thread spent executing tasks (0.0-1.0)"
            ),
            &["worker"]
        )
        .unwrap();

        let tokio_worker_mean_poll_duration_us = register_gauge!(Opts::new(
            format!("{prefix}tokio_worker_mean_poll_duration_us"),
            "Mean task poll duration across all workers in microseconds"
        ))
        .unwrap();

        let tokio_budget_forced_yield_count = register_gauge!(Opts::new(
            format!("{prefix}tokio_budget_forced_yield_count"),
            "Total number of times tasks were forced to yield by the Tokio coop budget"
        ))
        .unwrap();

        // Initialize all metrics
        let connected_sockets = register_gauge_vec!(
            Opts::new(
                format!("{prefix}connected"),
                "The number of currently connected sockets"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let new_connections_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}new_connections_total"),
                "Total amount of sockudo connection requests"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let new_disconnections_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}new_disconnections_total"),
                "Total amount of sockudo disconnections"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let connection_errors_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}connection_errors_total"),
                "Total amount of connection errors by type"
            ),
            &["app_id", "port", "error_type"]
        )
        .unwrap();

        let socket_bytes_received = register_counter_vec!(
            Opts::new(
                format!("{prefix}socket_received_bytes"),
                "Total amount of bytes that sockudo received"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let socket_bytes_transmitted = register_counter_vec!(
            Opts::new(
                format!("{prefix}socket_transmitted_bytes"),
                "Total amount of bytes that sockudo transmitted"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ws_messages_received = register_counter_vec!(
            Opts::new(
                format!("{prefix}ws_messages_received_total"),
                "The total amount of WS messages received from connections by the server"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ws_messages_sent = register_counter_vec!(
            Opts::new(
                format!("{prefix}ws_messages_sent_total"),
                "The total amount of WS messages sent to the connections from the server"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let http_bytes_received = register_counter_vec!(
            Opts::new(
                format!("{prefix}http_received_bytes"),
                "Total amount of bytes that sockudo's REST API received"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let http_bytes_transmitted = register_counter_vec!(
            Opts::new(
                format!("{prefix}http_transmitted_bytes"),
                "Total amount of bytes that sockudo's REST API sent back"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let http_calls_received = register_counter_vec!(
            Opts::new(
                format!("{prefix}http_calls_received_total"),
                "Total amount of received REST API calls"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_resolve_time = register_histogram_vec!(
            histogram_opts!(
                format!("{}horizontal_adapter_resolve_time", prefix),
                "The average resolve time for requests to other nodes",
                INTERNAL_LATENCY_HISTOGRAM_BUCKETS.to_vec()
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_resolved_promises = register_counter_vec!(
            Opts::new(
                format!("{prefix}horizontal_adapter_resolved_promises"),
                "The total amount of promises that were fulfilled by other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_uncomplete_promises = register_counter_vec!(
            Opts::new(
                format!("{prefix}horizontal_adapter_uncomplete_promises"),
                "The total amount of promises that were not fulfilled entirely by other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_sent_requests = register_counter_vec!(
            Opts::new(
                format!("{prefix}horizontal_adapter_sent_requests"),
                "The total amount of sent requests to other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_received_requests = register_counter_vec!(
            Opts::new(
                format!("{prefix}horizontal_adapter_received_requests"),
                "The total amount of received requests from other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_received_responses = register_counter_vec!(
            Opts::new(
                format!("{prefix}horizontal_adapter_received_responses"),
                "The total amount of received responses from other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let rate_limit_checks_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}rate_limit_checks_total"),
                "Total number of rate limit checks performed"
            ),
            &["app_id", "port", "limiter_type", "request_context"]
        )
        .unwrap();

        let rate_limit_triggered_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}rate_limit_triggered_total"),
                "Total number of times rate limit was triggered"
            ),
            &["app_id", "port", "limiter_type", "request_context"]
        )
        .unwrap();

        let channel_subscriptions_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}channel_subscriptions_total"),
                "Total number of channel subscriptions"
            ),
            &["app_id", "port", "channel_type"]
        )
        .unwrap();

        let channel_unsubscriptions_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}channel_unsubscriptions_total"),
                "Total number of channel unsubscriptions"
            ),
            &["app_id", "port", "channel_type"]
        )
        .unwrap();

        let active_channels = register_gauge_vec!(
            Opts::new(
                format!("{prefix}active_channels"),
                "Number of currently active channels"
            ),
            &["app_id", "port", "channel_type"]
        )
        .unwrap();

        let broadcast_latency_ms = register_histogram_vec!(
            histogram_opts!(
                format!("{prefix}broadcast_latency_ms"),
                "End-to-end latency for broadcast messages in milliseconds",
                END_TO_END_LATENCY_HISTOGRAM_BUCKETS.to_vec()
            ),
            &["app_id", "port", "channel_type", "recipient_count_bucket"]
        )
        .unwrap();

        // Delta compression metrics
        let horizontal_delta_compression_enabled = register_counter_vec!(
            Opts::new(
                format!("{prefix}horizontal_delta_compression_enabled_total"),
                "Total number of horizontal broadcasts with delta compression enabled"
            ),
            &["app_id", "port", "channel_name"]
        )
        .unwrap();

        let delta_compression_bandwidth_saved = register_counter_vec!(
            Opts::new(
                format!("{prefix}delta_compression_bandwidth_saved_bytes"),
                "Total bytes saved by delta compression"
            ),
            &["app_id", "port", "channel_name"]
        )
        .unwrap();

        let delta_compression_bandwidth_original = register_counter_vec!(
            Opts::new(
                format!("{prefix}delta_compression_bandwidth_original_bytes"),
                "Total original bytes before delta compression"
            ),
            &["app_id", "port", "channel_name"]
        )
        .unwrap();

        let delta_compression_full_messages = register_counter_vec!(
            Opts::new(
                format!("{prefix}delta_compression_full_messages_total"),
                "Total number of full messages sent (not deltas)"
            ),
            &["app_id", "port", "channel_name"]
        )
        .unwrap();

        let delta_compression_delta_messages = register_counter_vec!(
            Opts::new(
                format!("{prefix}delta_compression_delta_messages_total"),
                "Total number of delta messages sent"
            ),
            &["app_id", "port", "channel_name"]
        )
        .unwrap();

        // Idempotency metrics
        let idempotency_publish_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}idempotency_publish_total"),
                "Total number of publish requests that included an idempotency key"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let idempotency_duplicates_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}idempotency_duplicates_total"),
                "Total number of duplicate publishes caught by idempotency deduplication"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ephemeral_messages_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}ephemeral_messages_total"),
                "Total number of ephemeral messages delivered (V2 only)"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let event_filter_suppressed_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}event_filter_suppressed_total"),
                "Total number of messages suppressed by event name filtering (V2 only)"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let echo_suppressed_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}echo_suppressed_total"),
                "Total number of message deliveries skipped due to echo control (V2 only)"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_writes_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}history_writes_total"),
                "Total number of durable history writes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_write_failures_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}history_write_failures_total"),
                "Total number of durable history write failures"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_write_latency_ms = register_histogram_vec!(
            histogram_opts!(
                format!("{prefix}history_write_latency_ms"),
                "Durable history write latency in milliseconds",
                END_TO_END_LATENCY_HISTOGRAM_BUCKETS.to_vec()
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_retained_messages = register_gauge_vec!(
            Opts::new(
                format!("{prefix}history_retained_messages"),
                "Current number of retained durable history messages"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_retained_bytes = register_gauge_vec!(
            Opts::new(
                format!("{prefix}history_retained_bytes"),
                "Current number of retained durable history bytes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_evictions_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}history_evictions_total"),
                "Total number of durable history messages evicted"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_evicted_bytes_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}history_evicted_bytes_total"),
                "Total number of durable history bytes evicted"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_queue_depth = register_gauge_vec!(
            Opts::new(
                format!("{prefix}history_queue_depth"),
                "Current durable history writer queue depth"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_degraded_channels = register_gauge_vec!(
            Opts::new(
                format!("{prefix}history_degraded_channels"),
                "Current number of degraded durable history channels"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_reset_required_channels = register_gauge_vec!(
            Opts::new(
                format!("{prefix}history_reset_required_channels"),
                "Current number of reset-required durable history channels"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let history_recovery_success_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}history_recovery_success_total"),
                "Total number of successful recovery attempts"
            ),
            &["app_id", "port", "source"]
        )
        .unwrap();

        let history_recovery_failures_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}history_recovery_failures_total"),
                "Total number of failed recovery attempts"
            ),
            &["app_id", "port", "code"]
        )
        .unwrap();

        let versioned_message_mutations_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}versioned_message_mutations_total"),
                "Total number of versioned-message mutation attempts by action and result"
            ),
            &["app_id", "port", "action", "result"]
        )
        .unwrap();

        let versioned_message_retrieval_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}versioned_message_retrieval_total"),
                "Total number of versioned-message retrieval attempts by surface and result"
            ),
            &["app_id", "port", "surface", "result"]
        )
        .unwrap();

        let versioned_history_substitution_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}versioned_history_substitution_total"),
                "Total number of history substitution outcomes for versioned messages"
            ),
            &["app_id", "port", "result"]
        )
        .unwrap();

        let presence_history_writes_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}presence_history_writes_total"),
                "Total number of presence-history writes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let presence_history_write_failures_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}presence_history_write_failures_total"),
                "Total number of presence-history write failures"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let presence_history_write_latency_ms = register_histogram_vec!(
            histogram_opts!(
                format!("{prefix}presence_history_write_latency_ms"),
                "Presence-history write latency in milliseconds",
                END_TO_END_LATENCY_HISTOGRAM_BUCKETS.to_vec()
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let presence_history_retained_events = register_gauge_vec!(
            Opts::new(
                format!("{prefix}presence_history_retained_events"),
                "Current number of retained presence-history events"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let presence_history_retained_bytes = register_gauge_vec!(
            Opts::new(
                format!("{prefix}presence_history_retained_bytes"),
                "Current number of retained presence-history bytes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let presence_history_evictions_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}presence_history_evictions_total"),
                "Total number of presence-history events evicted"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let presence_history_evicted_bytes_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}presence_history_evicted_bytes_total"),
                "Total number of presence-history bytes evicted"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let presence_history_queue_depth = register_gauge_vec!(
            Opts::new(
                format!("{prefix}presence_history_queue_depth"),
                "Current presence-history writer queue depth"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let presence_history_degraded_channels = register_gauge_vec!(
            Opts::new(
                format!("{prefix}presence_history_degraded_channels"),
                "Current number of degraded presence-history channels"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let presence_history_reset_required_channels = register_gauge_vec!(
            Opts::new(
                format!("{prefix}presence_history_reset_required_channels"),
                "Current number of reset-required presence-history channels"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let annotations_published_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}annotations_published_total"),
                "Total number of annotation.create events published"
            ),
            &["channel", "type"]
        )
        .unwrap();

        let annotations_deleted_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}annotations_deleted_total"),
                "Total number of annotation.delete events published"
            ),
            &["channel", "type"]
        )
        .unwrap();

        let annotation_summary_deliveries_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}annotation_summary_deliveries_total"),
                "Total number of annotation summary messages delivered"
            ),
            &["channel"]
        )
        .unwrap();

        let annotation_summary_clipped_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}annotation_summary_clipped_total"),
                "Total number of clipped annotation summaries"
            ),
            &["channel", "type"]
        )
        .unwrap();

        let annotation_projection_rebuild_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}annotation_projection_rebuild_total"),
                "Total number of full annotation projection rebuilds"
            ),
            &["channel"]
        )
        .unwrap();

        let annotation_projection_rebuild_duration_seconds = register_histogram_vec!(
            histogram_opts!(
                format!("{prefix}annotation_projection_rebuild_duration_seconds"),
                "Full annotation projection rebuild latency in seconds",
                vec![
                    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0
                ]
            ),
            &["channel"]
        )
        .unwrap();

        let delta_cluster_coordination_ops_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}delta_cluster_coordination_ops_total"),
                "Total number of delta cluster coordination operations"
            ),
            &["backend", "op", "result"]
        )
        .unwrap();

        let delta_cluster_coordination_failures_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}delta_cluster_coordination_failures_total"),
                "Total number of delta cluster coordination failures"
            ),
            &["backend", "op", "code"]
        )
        .unwrap();

        let delta_cluster_coordination_latency_ms = register_histogram_vec!(
            histogram_opts!(
                format!("{prefix}delta_cluster_coordination_latency_ms"),
                "Delta cluster coordination latency in milliseconds",
                END_TO_END_LATENCY_HISTOGRAM_BUCKETS.to_vec()
            ),
            &["backend", "op"]
        )
        .unwrap();

        let delta_cluster_coordination_decisions_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}delta_cluster_coordination_decisions_total"),
                "Total number of delta cluster coordination decisions"
            ),
            &["backend", "decision"]
        )
        .unwrap();

        let delta_cluster_coordination_backend_up = register_gauge_vec!(
            Opts::new(
                format!("{prefix}delta_cluster_coordination_backend_up"),
                "Whether the delta cluster coordination backend is currently healthy"
            ),
            &["backend"]
        )
        .unwrap();

        let horizontal_transport_queue_depth = register_gauge_vec!(
            Opts::new(
                format!("{prefix}horizontal_transport_queue_depth"),
                "Current queue depth for a horizontal transport driver"
            ),
            &["driver"]
        )
        .unwrap();

        let horizontal_transport_messages_dropped_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}horizontal_transport_messages_dropped_total"),
                "Total number of horizontal transport messages dropped"
            ),
            &["driver"]
        )
        .unwrap();

        let horizontal_transport_reconnections_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}horizontal_transport_reconnections_total"),
                "Total number of horizontal transport reconnects"
            ),
            &["driver"]
        )
        .unwrap();

        // Reset gauge metrics to 0 on startup - they represent current state, not historical
        connected_sockets.reset();
        active_channels.reset();
        history_retained_messages.reset();
        history_retained_bytes.reset();
        history_queue_depth.reset();
        history_degraded_channels.reset();
        history_reset_required_channels.reset();
        presence_history_retained_events.reset();
        presence_history_retained_bytes.reset();
        presence_history_queue_depth.reset();
        presence_history_degraded_channels.reset();
        presence_history_reset_required_channels.reset();
        delta_cluster_coordination_backend_up.reset();
        horizontal_transport_queue_depth.reset();

        // Set process start time
        if let Ok(boot_time) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            process_start_time_seconds.set(boot_time.as_secs_f64());
        }

        Self {
            prefix,
            port,
            handle,
            process_resident_memory_bytes,
            process_virtual_memory_bytes,
            process_cpu_seconds_total,
            process_start_time_seconds,
            process_open_fds,
            process_max_fds,
            tokio_workers_count,
            tokio_active_tasks,
            tokio_injection_queue_depth,
            tokio_worker_local_queue_depth,
            tokio_worker_busy_ratio,
            tokio_worker_mean_poll_duration_us,
            tokio_budget_forced_yield_count,
            connected_sockets,
            new_connections_total,
            new_disconnections_total,
            connection_errors_total,
            socket_bytes_received,
            socket_bytes_transmitted,
            ws_messages_received,
            ws_messages_sent,
            http_bytes_received,
            http_bytes_transmitted,
            http_calls_received,
            horizontal_adapter_resolve_time,
            horizontal_adapter_resolved_promises,
            horizontal_adapter_uncomplete_promises,
            horizontal_adapter_sent_requests,
            horizontal_adapter_received_requests,
            horizontal_adapter_received_responses,
            rate_limit_checks_total,
            rate_limit_triggered_total,
            channel_subscriptions_total,
            channel_unsubscriptions_total,
            active_channels,
            broadcast_latency_ms,
            horizontal_delta_compression_enabled,
            delta_compression_bandwidth_saved,
            delta_compression_bandwidth_original,
            delta_compression_full_messages,
            delta_compression_delta_messages,
            idempotency_publish_total,
            idempotency_duplicates_total,
            ephemeral_messages_total,
            event_filter_suppressed_total,
            echo_suppressed_total,
            history_writes_total,
            history_write_failures_total,
            history_write_latency_ms,
            history_retained_messages,
            history_retained_bytes,
            history_evictions_total,
            history_evicted_bytes_total,
            history_queue_depth,
            history_degraded_channels,
            history_reset_required_channels,
            history_recovery_success_total,
            history_recovery_failures_total,
            versioned_message_mutations_total,
            versioned_message_retrieval_total,
            versioned_history_substitution_total,
            presence_history_writes_total,
            presence_history_write_failures_total,
            presence_history_write_latency_ms,
            presence_history_retained_events,
            presence_history_retained_bytes,
            presence_history_evictions_total,
            presence_history_evicted_bytes_total,
            presence_history_queue_depth,
            presence_history_degraded_channels,
            presence_history_reset_required_channels,
            annotations_published_total,
            annotations_deleted_total,
            annotation_summary_deliveries_total,
            annotation_summary_clipped_total,
            annotation_projection_rebuild_total,
            annotation_projection_rebuild_duration_seconds,
            delta_cluster_coordination_ops_total,
            delta_cluster_coordination_failures_total,
            delta_cluster_coordination_latency_ms,
            delta_cluster_coordination_decisions_total,
            delta_cluster_coordination_backend_up,
            horizontal_transport_queue_depth,
            horizontal_transport_messages_dropped_total,
            horizontal_transport_reconnections_total,
        }
    }

    /// Update Tokio runtime metrics from the current runtime handle
    pub fn update_tokio_runtime_metrics(&self) {
        let handle = tokio::runtime::Handle::current();
        let metrics = handle.metrics();

        self.tokio_workers_count.set(metrics.num_workers() as f64);
        self.tokio_active_tasks
            .set(metrics.num_alive_tasks() as f64);
        self.tokio_injection_queue_depth
            .set(metrics.global_queue_depth() as f64);

        let num_workers = metrics.num_workers();

        self.tokio_budget_forced_yield_count
            .set(metrics.budget_forced_yield_count() as f64);
        let mut total_polls: u64 = 0;
        let mut total_poll_duration_ns: u64 = 0;

        for worker in 0..num_workers {
            let worker_label = worker.to_string();

            let queue_depth = metrics.worker_local_queue_depth(worker);

            self.tokio_worker_local_queue_depth
                .with_label_values(&[&worker_label])
                .set(queue_depth as f64);

            let polls = metrics.worker_poll_count(worker);
            total_polls += polls;
            total_poll_duration_ns += metrics.worker_total_busy_duration(worker).as_nanos() as u64;
        }

        // Approximate busy ratio: busy_duration / process_uptime
        if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            let start_secs = self.process_start_time_seconds.get();
            if start_secs > 0.0 {
                let uptime_ns =
                    ((now.as_secs_f64() - start_secs) * 1_000_000_000.0).max(1.0) as u64;
                for worker in 0..num_workers {
                    let worker_label = worker.to_string();
                    let busy_ns = metrics.worker_total_busy_duration(worker).as_nanos() as u64;
                    let ratio = busy_ns as f64 / uptime_ns as f64;
                    self.tokio_worker_busy_ratio
                        .with_label_values(&[&worker_label])
                        .set(ratio.min(1.0));
                }
            }
        }

        if total_polls > 0 {
            let mean_poll_us = total_poll_duration_ns as f64 / total_polls as f64 / 1000.0;
            self.tokio_worker_mean_poll_duration_us.set(mean_poll_us);
        }
    }

    /// Update process metrics (memory, CPU, file descriptors)
    /// Called before gathering metrics to ensure fresh values
    fn update_process_metrics(&self) {
        #[cfg(target_os = "linux")]
        {
            // Read /proc/self/statm for memory info (in pages)
            if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
                let parts: Vec<&str> = statm.split_whitespace().collect();
                if parts.len() >= 2 {
                    let page_size = 4096_u64; // Standard page size on Linux
                    // First value is total program size (virtual memory) in pages
                    if let Ok(vsize_pages) = parts[0].parse::<u64>() {
                        self.process_virtual_memory_bytes
                            .set((vsize_pages * page_size) as f64);
                    }
                    // Second value is resident set size in pages
                    if let Ok(rss_pages) = parts[1].parse::<u64>() {
                        self.process_resident_memory_bytes
                            .set((rss_pages * page_size) as f64);
                    }
                }
            }

            // Read /proc/self/stat for CPU time
            if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
                let parts: Vec<&str> = stat.split_whitespace().collect();
                // utime is at index 13, stime is at index 14 (0-indexed)
                if parts.len() > 14 {
                    let ticks_per_sec = 100_f64; // Usually 100 on Linux (sysconf(_SC_CLK_TCK))
                    let utime = parts[13].parse::<u64>().unwrap_or(0);
                    let stime = parts[14].parse::<u64>().unwrap_or(0);
                    let total_ticks = utime + stime;
                    self.process_cpu_seconds_total
                        .set(total_ticks as f64 / ticks_per_sec);
                }
            }

            // Count open file descriptors
            if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
                let fd_count = entries.count();
                self.process_open_fds.set(fd_count as f64);
            }

            unsafe {
                let mut rlim: libc::rlimit = std::mem::zeroed();
                if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
                    self.process_max_fds.set(rlim.rlim_cur as f64);
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            // On macOS, use mach APIs or fall back to rusage
            unsafe {
                let mut rusage: libc::rusage = std::mem::zeroed();
                if libc::getrusage(libc::RUSAGE_SELF, &mut rusage) == 0 {
                    // maxrss is in bytes on macOS (unlike Linux where it's in KB)
                    self.process_resident_memory_bytes
                        .set(rusage.ru_maxrss as f64);
                    // CPU time
                    let utime = rusage.ru_utime.tv_sec as f64
                        + rusage.ru_utime.tv_usec as f64 / 1_000_000.0;
                    let stime = rusage.ru_stime.tv_sec as f64
                        + rusage.ru_stime.tv_usec as f64 / 1_000_000.0;
                    self.process_cpu_seconds_total.set(utime + stime);
                }

                let mut rlim: libc::rlimit = std::mem::zeroed();
                if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
                    self.process_max_fds.set(rlim.rlim_cur as f64);
                }
            }
        }

        #[cfg(target_os = "windows")]
        {
            // On Windows, use GetProcessMemoryInfo
            // For now, just set to 0 - Windows support can be added later
            // using the windows crate if needed
        }
    }

    pub fn record_horizontal_transport_queue_depth(&self, driver: &str, depth: usize) {
        self.horizontal_transport_queue_depth
            .with_label_values(&[driver])
            .set(depth as f64);
    }

    pub fn increment_horizontal_transport_messages_dropped(&self, driver: &str) {
        self.horizontal_transport_messages_dropped_total
            .with_label_values(&[driver])
            .inc();
    }

    pub fn increment_horizontal_transport_reconnections(&self, driver: &str) {
        self.horizontal_transport_reconnections_total
            .with_label_values(&[driver])
            .inc();
    }

    /// Get the tags for Prometheus
    fn get_tags(&self, app_id: &str) -> Vec<String> {
        vec![app_id.to_string(), self.port.to_string()]
    }
}

#[async_trait]
impl MetricsInterface for PrometheusMetricsDriver {
    async fn init(&self) -> Result<()> {
        // Not needed for Prometheus, metrics are registered in the constructor
        Ok(())
    }

    fn mark_new_connection(&self, app_id: &str, socket_id: &SocketId) {
        let tags = self.get_tags(app_id);
        self.connected_sockets.with_label_values(&tags).inc();
        self.new_connections_total.with_label_values(&tags).inc();

        debug!(
            "Metrics: New connection for app {}, socket {}",
            app_id, socket_id
        );
    }

    fn mark_disconnection(&self, app_id: &str, socket_id: &SocketId) {
        let tags = self.get_tags(app_id);
        self.connected_sockets.with_label_values(&tags).dec();
        self.new_disconnections_total.with_label_values(&tags).inc();

        debug!(
            "Metrics: Disconnection for app {}, socket {}",
            app_id, socket_id
        );
    }

    fn mark_connection_error(&self, app_id: &str, error_type: &str) {
        let tags = vec![
            app_id.to_string(),
            self.port.to_string(),
            error_type.to_string(),
        ];
        self.connection_errors_total.with_label_values(&tags).inc();

        debug!(
            "Metrics: Connection error for app {}, error type: {}",
            app_id, error_type
        );
    }

    fn mark_rate_limit_check(&self, app_id: &str, limiter_type: &str) {
        self.mark_rate_limit_check_with_context(app_id, limiter_type, "unknown");
    }

    fn mark_rate_limit_check_with_context(
        &self,
        app_id: &str,
        limiter_type: &str,
        request_context: &str,
    ) {
        let tags = vec![
            app_id.to_string(),
            self.port.to_string(),
            limiter_type.to_string(),
            request_context.to_string(),
        ];
        self.rate_limit_checks_total.with_label_values(&tags).inc();

        debug!(
            "Metrics: Rate limit check for app {}, limiter type: {}, context: {}",
            app_id, limiter_type, request_context
        );
    }

    fn mark_rate_limit_triggered(&self, app_id: &str, limiter_type: &str) {
        self.mark_rate_limit_triggered_with_context(app_id, limiter_type, "unknown");
    }

    fn mark_rate_limit_triggered_with_context(
        &self,
        app_id: &str,
        limiter_type: &str,
        request_context: &str,
    ) {
        let tags = vec![
            app_id.to_string(),
            self.port.to_string(),
            limiter_type.to_string(),
            request_context.to_string(),
        ];
        self.rate_limit_triggered_total
            .with_label_values(&tags)
            .inc();

        debug!(
            "Metrics: Rate limit triggered for app {}, limiter type: {}, context: {}",
            app_id, limiter_type, request_context
        );
    }

    fn mark_channel_subscription(&self, app_id: &str, channel_type: &str) {
        let tags = vec![
            app_id.to_string(),
            self.port.to_string(),
            channel_type.to_string(),
        ];
        self.channel_subscriptions_total
            .with_label_values(&tags)
            .inc();

        debug!(
            "Metrics: Channel subscription for app {}, channel type: {}",
            app_id, channel_type
        );
    }

    fn mark_channel_unsubscription(&self, app_id: &str, channel_type: &str) {
        let tags = vec![
            app_id.to_string(),
            self.port.to_string(),
            channel_type.to_string(),
        ];
        self.channel_unsubscriptions_total
            .with_label_values(&tags)
            .inc();

        debug!(
            "Metrics: Channel unsubscription for app {}, channel type: {}",
            app_id, channel_type
        );
    }

    fn update_active_channels(&self, app_id: &str, channel_type: &str, count: i64) {
        let tags = vec![
            app_id.to_string(),
            self.port.to_string(),
            channel_type.to_string(),
        ];
        self.active_channels
            .with_label_values(&tags)
            .set(count as f64);

        debug!(
            "Metrics: Active channels updated for app {}, channel type: {}, count: {}",
            app_id, channel_type, count
        );
    }

    fn mark_api_message(
        &self,
        app_id: &str,
        incoming_message_size: usize,
        sent_message_size: usize,
    ) {
        let tags = self.get_tags(app_id);
        self.http_bytes_received
            .with_label_values(&tags)
            .inc_by(incoming_message_size as f64);
        self.http_bytes_transmitted
            .with_label_values(&tags)
            .inc_by(sent_message_size as f64);
        self.http_calls_received.with_label_values(&tags).inc();

        debug!(
            "Metrics: API message for app {}, incoming size: {}, sent size: {}",
            app_id, incoming_message_size, sent_message_size
        );
    }

    fn mark_ws_message_sent(&self, app_id: &str, sent_message_size: usize) {
        let tags = self.get_tags(app_id);
        self.socket_bytes_transmitted
            .with_label_values(&tags)
            .inc_by(sent_message_size as f64);
        self.ws_messages_sent.with_label_values(&tags).inc();

        debug!(
            "Metrics: WS message sent for app {}, size: {}",
            app_id, sent_message_size
        );
    }

    fn mark_ws_messages_sent_batch(&self, app_id: &str, sent_message_size: usize, count: usize) {
        let tags = self.get_tags(app_id);
        // Batch update: total bytes = message_size * count, total messages = count
        self.socket_bytes_transmitted
            .with_label_values(&tags)
            .inc_by((sent_message_size * count) as f64);
        self.ws_messages_sent
            .with_label_values(&tags)
            .inc_by(count as f64);

        debug!(
            "Metrics: WS messages sent batch for app {}, count: {}, total size: {}",
            app_id,
            count,
            sent_message_size * count
        );
    }

    fn mark_ws_message_received(&self, app_id: &str, message_size: usize) {
        let tags = self.get_tags(app_id);
        self.socket_bytes_received
            .with_label_values(&tags)
            .inc_by(message_size as f64);
        self.ws_messages_received.with_label_values(&tags).inc();

        debug!(
            "Metrics: WS message received for app {}, size: {}",
            app_id, message_size
        );
    }

    fn track_horizontal_adapter_resolve_time(&self, app_id: &str, time_ms: f64) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_resolve_time
            .with_label_values(&tags)
            .observe(time_ms);

        debug!(
            "Metrics: Horizontal adapter resolve time for app {}, time: {} ms",
            app_id, time_ms
        );
    }

    fn track_horizontal_adapter_resolved_promises(&self, app_id: &str, resolved: bool) {
        let tags = self.get_tags(app_id);

        if resolved {
            self.horizontal_adapter_resolved_promises
                .with_label_values(&tags)
                .inc();
        } else {
            self.horizontal_adapter_uncomplete_promises
                .with_label_values(&tags)
                .inc();
        }

        debug!(
            "Metrics: Horizontal adapter promise {} for app {}",
            if resolved { "resolved" } else { "unresolved" },
            app_id
        );
    }

    fn mark_horizontal_adapter_request_sent(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_sent_requests
            .with_label_values(&tags)
            .inc();

        debug!(
            "Metrics: Horizontal adapter request sent for app {}",
            app_id
        );
    }

    fn mark_horizontal_adapter_request_received(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_received_requests
            .with_label_values(&tags)
            .inc();

        debug!(
            "Metrics: Horizontal adapter request received for app {}",
            app_id
        );
    }

    fn mark_horizontal_adapter_response_received(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_received_responses
            .with_label_values(&tags)
            .inc();

        debug!(
            "Metrics: Horizontal adapter response received for app {}",
            app_id
        );
    }

    fn track_broadcast_latency(
        &self,
        app_id: &str,
        channel_name: &str,
        recipient_count: usize,
        latency_ms: f64,
    ) {
        // Determine channel type from channel name using the ChannelType enum
        let channel_type = ChannelType::from_name(channel_name).as_str();

        if recipient_count == 0 {
            return;
        }

        // Determine recipient count bucket
        let bucket = match recipient_count {
            1..=10 => "xs",
            11..=100 => "sm",
            101..=1000 => "md",
            1001..=10000 => "lg",
            _ => "xl",
        };

        self.broadcast_latency_ms
            .with_label_values(&[app_id, &self.port.to_string(), channel_type, bucket])
            .observe(latency_ms);

        debug!(
            "Metrics: Broadcast latency for app {}, channel: {} ({}), recipients: {} ({}), latency: {} ms",
            app_id, channel_name, channel_type, recipient_count, bucket, latency_ms
        );
    }

    fn mark_idempotency_publish(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.idempotency_publish_total
            .with_label_values(&tags)
            .inc();

        debug!("Metrics: Idempotency publish for app {}", app_id);
    }

    fn mark_idempotency_duplicate(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.idempotency_duplicates_total
            .with_label_values(&tags)
            .inc();

        debug!("Metrics: Idempotency duplicate caught for app {}", app_id);
    }

    fn mark_ephemeral_message(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.ephemeral_messages_total.with_label_values(&tags).inc();
    }

    fn mark_event_filter_suppressed(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.event_filter_suppressed_total
            .with_label_values(&tags)
            .inc();
    }

    fn mark_echo_suppressed(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.echo_suppressed_total.with_label_values(&tags).inc();
    }

    fn mark_history_write(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.history_writes_total.with_label_values(&tags).inc();
    }

    fn track_history_write_latency(&self, app_id: &str, latency_ms: f64) {
        let tags = self.get_tags(app_id);
        self.history_write_latency_ms
            .with_label_values(&tags)
            .observe(latency_ms);
    }

    fn mark_history_write_failure(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.history_write_failures_total
            .with_label_values(&tags)
            .inc();
    }

    fn update_history_retained(&self, app_id: &str, messages: u64, bytes: u64) {
        let tags = self.get_tags(app_id);
        self.history_retained_messages
            .with_label_values(&tags)
            .set(messages as f64);
        self.history_retained_bytes
            .with_label_values(&tags)
            .set(bytes as f64);
    }

    fn mark_history_eviction(&self, app_id: &str, messages: u64, bytes: u64) {
        let tags = self.get_tags(app_id);
        self.history_evictions_total
            .with_label_values(&tags)
            .inc_by(messages as f64);
        self.history_evicted_bytes_total
            .with_label_values(&tags)
            .inc_by(bytes as f64);
    }

    fn update_history_queue_depth(&self, app_id: &str, depth: usize) {
        let tags = self.get_tags(app_id);
        self.history_queue_depth
            .with_label_values(&tags)
            .set(depth as f64);
    }

    fn update_history_degraded_channels(&self, app_id: &str, count: usize) {
        let tags = self.get_tags(app_id);
        self.history_degraded_channels
            .with_label_values(&tags)
            .set(count as f64);
    }

    fn update_history_reset_required_channels(&self, app_id: &str, count: usize) {
        let tags = self.get_tags(app_id);
        self.history_reset_required_channels
            .with_label_values(&tags)
            .set(count as f64);
    }

    fn mark_history_recovery_success(&self, app_id: &str, source: &str) {
        self.history_recovery_success_total
            .with_label_values(&[app_id, &self.port.to_string(), source])
            .inc();
    }

    fn mark_history_recovery_failure(&self, app_id: &str, code: &str) {
        self.history_recovery_failures_total
            .with_label_values(&[app_id, &self.port.to_string(), code])
            .inc();
    }

    fn mark_versioned_message_mutation(&self, app_id: &str, action: &str, result: &str) {
        self.versioned_message_mutations_total
            .with_label_values(&[app_id, &self.port.to_string(), action, result])
            .inc();
    }

    fn mark_versioned_message_retrieval(&self, app_id: &str, surface: &str, result: &str) {
        self.versioned_message_retrieval_total
            .with_label_values(&[app_id, &self.port.to_string(), surface, result])
            .inc();
    }

    fn mark_versioned_history_substitution(&self, app_id: &str, result: &str) {
        self.versioned_history_substitution_total
            .with_label_values(&[app_id, &self.port.to_string(), result])
            .inc();
    }

    fn mark_annotation_published(&self, channel: &str, annotation_type: &str) {
        self.annotations_published_total
            .with_label_values(&[channel, annotation_type])
            .inc();
    }

    fn mark_annotation_deleted(&self, channel: &str, annotation_type: &str) {
        self.annotations_deleted_total
            .with_label_values(&[channel, annotation_type])
            .inc();
    }

    fn mark_annotation_summary_delivery(&self, channel: &str) {
        self.annotation_summary_deliveries_total
            .with_label_values(&[channel])
            .inc();
    }

    fn mark_annotation_summary_clipped(&self, channel: &str, annotation_type: &str) {
        self.annotation_summary_clipped_total
            .with_label_values(&[channel, annotation_type])
            .inc();
    }

    fn mark_annotation_projection_rebuild(&self, channel: &str) {
        self.annotation_projection_rebuild_total
            .with_label_values(&[channel])
            .inc();
    }

    fn track_annotation_projection_rebuild_duration(&self, channel: &str, duration_seconds: f64) {
        self.annotation_projection_rebuild_duration_seconds
            .with_label_values(&[channel])
            .observe(duration_seconds);
    }

    fn mark_delta_cluster_coordination_op(&self, backend: &str, op: &str, result: &str) {
        self.delta_cluster_coordination_ops_total
            .with_label_values(&[backend, op, result])
            .inc();
    }

    fn mark_delta_cluster_coordination_failure(&self, backend: &str, op: &str, code: &str) {
        self.delta_cluster_coordination_failures_total
            .with_label_values(&[backend, op, code])
            .inc();
    }

    fn track_delta_cluster_coordination_latency(&self, backend: &str, op: &str, latency_ms: f64) {
        self.delta_cluster_coordination_latency_ms
            .with_label_values(&[backend, op])
            .observe(latency_ms);
    }

    fn mark_delta_cluster_coordination_decision(&self, backend: &str, decision: &str) {
        self.delta_cluster_coordination_decisions_total
            .with_label_values(&[backend, decision])
            .inc();
    }

    fn update_delta_cluster_coordination_backend_up(&self, backend: &str, up: bool) {
        self.delta_cluster_coordination_backend_up
            .with_label_values(&[backend])
            .set(if up { 1.0 } else { 0.0 });
    }

    fn update_horizontal_transport_queue_depth(&self, driver: &str, depth: usize) {
        self.horizontal_transport_queue_depth
            .with_label_values(&[driver])
            .set(depth as f64);
    }

    fn mark_horizontal_transport_message_dropped(&self, driver: &str) {
        self.horizontal_transport_messages_dropped_total
            .with_label_values(&[driver])
            .inc();
    }

    fn mark_horizontal_transport_reconnection(&self, driver: &str) {
        self.horizontal_transport_reconnections_total
            .with_label_values(&[driver])
            .inc();
    }

    fn mark_presence_history_write(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.presence_history_writes_total
            .with_label_values(&tags)
            .inc();
    }

    fn track_presence_history_write_latency(&self, app_id: &str, latency_ms: f64) {
        let tags = self.get_tags(app_id);
        self.presence_history_write_latency_ms
            .with_label_values(&tags)
            .observe(latency_ms);
    }

    fn mark_presence_history_write_failure(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.presence_history_write_failures_total
            .with_label_values(&tags)
            .inc();
    }

    fn update_presence_history_retained(&self, app_id: &str, events: u64, bytes: u64) {
        let tags = self.get_tags(app_id);
        self.presence_history_retained_events
            .with_label_values(&tags)
            .set(events as f64);
        self.presence_history_retained_bytes
            .with_label_values(&tags)
            .set(bytes as f64);
    }

    fn mark_presence_history_eviction(&self, app_id: &str, events: u64, bytes: u64) {
        let tags = self.get_tags(app_id);
        self.presence_history_evictions_total
            .with_label_values(&tags)
            .inc_by(events as f64);
        self.presence_history_evicted_bytes_total
            .with_label_values(&tags)
            .inc_by(bytes as f64);
    }

    fn update_presence_history_queue_depth(&self, app_id: &str, depth: usize) {
        let tags = self.get_tags(app_id);
        self.presence_history_queue_depth
            .with_label_values(&tags)
            .set(depth as f64);
    }

    fn update_presence_history_degraded_channels(&self, app_id: &str, count: usize) {
        let tags = self.get_tags(app_id);
        self.presence_history_degraded_channels
            .with_label_values(&tags)
            .set(count as f64);
    }

    fn update_presence_history_reset_required_channels(&self, app_id: &str, count: usize) {
        let tags = self.get_tags(app_id);
        self.presence_history_reset_required_channels
            .with_label_values(&tags)
            .set(count as f64);
    }

    async fn get_metrics_as_plaintext(&self) -> String {
        // Update process and runtime metrics before gathering
        self.update_process_metrics();
        self.update_tokio_runtime_metrics();

        self.handle.run_upkeep();
        self.handle.render()
    }

    /// Get metrics data as a JSON object
    async fn get_metrics_as_json(&self) -> Value {
        // Create a base JSON structure
        let metrics_json = json!({
            "connections": {
                "total": 0,
                "current": 0
            },
            "messages": {
                "received": {
                    "total": 0,
                    "bytes": 0
                },
                "transmitted": {
                    "total": 0,
                    "bytes": 0
                }
            },
            "api": {
                "requests": {
                    "total": 0,
                    "received_bytes": 0,
                    "transmitted_bytes": 0
                }
            },
            "horizontal_adapter": {
                "requests": {
                    "sent": 0,
                    "received": 0,
                    "responses_received": 0
                },
                "promises": {
                    "resolved": 0,
                    "unresolved": 0
                },
                "resolve_time_ms": {
                    "avg": 0.0,
                    "max": 0.0,
                    "min": 0.0
                }
            },
            "channels": {
                "total": 0
            },
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        let json_metrics = prometheus_text_to_json(&self.get_metrics_as_plaintext().await);

        // Return raw metrics JSON for maximum flexibility
        json!({
            "formatted": metrics_json,
            "raw": json_metrics,
            "timestamp": chrono::Utc::now().to_rfc3339()
        })
    }

    fn track_horizontal_delta_compression(&self, app_id: &str, channel_name: &str, enabled: bool) {
        if enabled {
            self.horizontal_delta_compression_enabled
                .with_label_values(&[app_id, &self.port.to_string(), channel_name])
                .inc();

            debug!(
                "Metrics: Horizontal delta compression enabled for app {}, channel: {}",
                app_id, channel_name
            );
        }
    }

    fn track_delta_compression_bandwidth(
        &self,
        app_id: &str,
        channel_name: &str,
        original_bytes: usize,
        compressed_bytes: usize,
    ) {
        let saved_bytes = original_bytes.saturating_sub(compressed_bytes);

        self.delta_compression_bandwidth_original
            .with_label_values(&[app_id, &self.port.to_string(), channel_name])
            .inc_by(original_bytes as f64);

        self.delta_compression_bandwidth_saved
            .with_label_values(&[app_id, &self.port.to_string(), channel_name])
            .inc_by(saved_bytes as f64);

        debug!(
            "Metrics: Delta compression bandwidth for app {}, channel: {}, original: {} bytes, compressed: {} bytes, saved: {} bytes ({:.1}%)",
            app_id,
            channel_name,
            original_bytes,
            compressed_bytes,
            saved_bytes,
            (saved_bytes as f64 / original_bytes as f64) * 100.0
        );
    }

    fn track_delta_compression_full_message(&self, app_id: &str, channel_name: &str) {
        self.delta_compression_full_messages
            .with_label_values(&[app_id, &self.port.to_string(), channel_name])
            .inc();

        debug!(
            "Metrics: Delta compression full message for app {}, channel: {}",
            app_id, channel_name
        );
    }

    fn track_delta_compression_delta_message(&self, app_id: &str, channel_name: &str) {
        self.delta_compression_delta_messages
            .with_label_values(&[app_id, &self.port.to_string(), channel_name])
            .inc();

        debug!(
            "Metrics: Delta compression delta message for app {}, channel: {}",
            app_id, channel_name
        );
    }

    async fn clear(&self) {
        // Reset individual metrics counters - not fully supported by Prometheus Rust client
        // So we'll just log a message
        debug!("Metrics cleared (note: Prometheus metrics can't be fully cleared)");
    }
}
