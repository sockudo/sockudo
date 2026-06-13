use super::handles::{CounterVec, Gauge, GaugeVec, HistogramVec};
use super::options::TcpExporterOptions;
use super::recorder::{
    END_TO_END_LATENCY_HISTOGRAM_BUCKETS, INTERNAL_LATENCY_HISTOGRAM_BUCKETS,
    ResolvedTcpExporterOptions, install_prometheus_recorder,
};
use metrics::{describe_counter, describe_gauge, describe_histogram};
use metrics_exporter_prometheus::PrometheusHandle;
use sockudo_core::utils::resolve_socket_addr;
use std::sync::atomic::AtomicU64;

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

/// A Prometheus implementation of the metrics interface
pub struct PrometheusMetricsDriver {
    pub(super) prefix: String,
    pub(super) port: u16,
    pub(super) handle: PrometheusHandle,

    // Process metrics (for memory leak detection)
    pub(super) process_resident_memory_bytes: Gauge,
    pub(super) process_virtual_memory_bytes: Gauge,
    pub(super) process_cpu_seconds_total: Gauge,
    pub(super) process_start_time_seconds: Gauge,
    pub(super) process_open_fds: Gauge,
    pub(super) process_max_fds: Gauge,

    // Tokio runtime metrics
    pub(super) tokio_workers_count: Gauge,
    pub(super) tokio_active_tasks: Gauge,
    pub(super) tokio_injection_queue_depth: Gauge,
    pub(super) tokio_worker_local_queue_depth: GaugeVec,
    pub(super) tokio_worker_busy_ratio: GaugeVec,
    pub(super) tokio_worker_mean_poll_duration_us: Gauge,
    pub(super) tokio_budget_forced_yield_count: Gauge,

    // Metrics
    pub(super) connected_sockets: GaugeVec,
    pub(super) new_connections_total: CounterVec,
    pub(super) new_disconnections_total: CounterVec,
    pub(super) connection_errors_total: CounterVec,
    pub(super) socket_bytes_received: CounterVec,
    pub(super) socket_bytes_transmitted: CounterVec,
    pub(super) ws_messages_received: CounterVec,
    pub(super) ws_messages_sent: CounterVec,
    pub(super) http_bytes_received: CounterVec,
    pub(super) http_bytes_transmitted: CounterVec,
    pub(super) http_calls_received: CounterVec,
    pub(super) horizontal_adapter_resolve_time: HistogramVec,
    pub(super) horizontal_adapter_resolved_promises: CounterVec,
    pub(super) horizontal_adapter_uncomplete_promises: CounterVec,
    pub(super) horizontal_adapter_sent_requests: CounterVec,
    pub(super) horizontal_adapter_received_requests: CounterVec,
    pub(super) horizontal_adapter_received_responses: CounterVec,
    pub(super) rate_limit_checks_total: CounterVec,
    pub(super) rate_limit_triggered_total: CounterVec,
    pub(super) channel_subscriptions_total: CounterVec,
    pub(super) channel_unsubscriptions_total: CounterVec,
    pub(super) active_channels: GaugeVec,
    pub(super) broadcast_latency_ms: HistogramVec,
    // Delta compression metrics
    pub(super) horizontal_delta_compression_enabled: CounterVec,
    pub(super) delta_compression_bandwidth_saved: CounterVec,
    pub(super) delta_compression_bandwidth_original: CounterVec,
    pub(super) delta_compression_full_messages: CounterVec,
    pub(super) delta_compression_delta_messages: CounterVec,
    // Idempotency metrics
    pub(super) idempotency_publish_total: CounterVec,
    pub(super) idempotency_duplicates_total: CounterVec,
    // Ephemeral message metrics
    pub(super) ephemeral_messages_total: CounterVec,
    pub(super) ai_messages_validated_total: CounterVec,
    pub(super) ai_messages_rejected_total: CounterVec,
    pub(super) ai_messages_unparseable_total: CounterVec,
    pub(super) ai_turns_started_total: CounterVec,
    pub(super) ai_turns_ended_total: CounterVec,
    pub(super) ai_cancel_signals_total: CounterVec,
    pub(super) ai_active_streams: GaugeVec,
    pub(super) ai_stream_duration_seconds: HistogramVec,
    pub(super) ai_stream_bytes_total: CounterVec,
    pub(super) appends_received_total: CounterVec,
    pub(super) appends_delivered_total: CounterVec,
    pub(super) rollup_ratio: HistogramVec,
    pub(super) active_streams: GaugeVec,
    pub(super) flush_latency: HistogramVec,
    // Event name filter metrics
    pub(super) event_filter_suppressed_total: CounterVec,
    // Echo control metrics
    pub(super) echo_suppressed_total: CounterVec,
    // History metrics
    pub(super) history_writes_total: CounterVec,
    pub(super) history_write_failures_total: CounterVec,
    pub(super) history_write_latency_ms: HistogramVec,
    pub(super) history_retained_messages: GaugeVec,
    pub(super) history_retained_bytes: GaugeVec,
    pub(super) history_evictions_total: CounterVec,
    pub(super) history_evicted_bytes_total: CounterVec,
    pub(super) history_queue_depth: GaugeVec,
    pub(super) history_degraded_channels: GaugeVec,
    pub(super) history_reset_required_channels: GaugeVec,
    pub(super) history_recovery_success_total: CounterVec,
    pub(super) history_recovery_failures_total: CounterVec,
    pub(super) versioned_message_mutations_total: CounterVec,
    pub(super) versioned_message_retrieval_total: CounterVec,
    pub(super) versioned_history_substitution_total: CounterVec,
    pub(super) presence_history_writes_total: CounterVec,
    pub(super) presence_history_write_failures_total: CounterVec,
    pub(super) presence_history_write_latency_ms: HistogramVec,
    pub(super) presence_history_retained_events: GaugeVec,
    pub(super) presence_history_retained_bytes: GaugeVec,
    pub(super) presence_history_evictions_total: CounterVec,
    pub(super) presence_history_evicted_bytes_total: CounterVec,
    pub(super) presence_history_queue_depth: GaugeVec,
    pub(super) presence_history_degraded_channels: GaugeVec,
    pub(super) presence_history_reset_required_channels: GaugeVec,
    pub(super) annotations_published_total: CounterVec,
    pub(super) annotations_deleted_total: CounterVec,
    pub(super) annotation_summary_deliveries_total: CounterVec,
    pub(super) annotation_summary_clipped_total: CounterVec,
    pub(super) annotation_projection_rebuild_total: CounterVec,
    pub(super) annotation_projection_rebuild_duration_seconds: HistogramVec,
    pub(super) delta_cluster_coordination_ops_total: CounterVec,
    pub(super) delta_cluster_coordination_failures_total: CounterVec,
    pub(super) delta_cluster_coordination_latency_ms: HistogramVec,
    pub(super) delta_cluster_coordination_decisions_total: CounterVec,
    pub(super) delta_cluster_coordination_backend_up: GaugeVec,
    pub(super) horizontal_transport_queue_depth: GaugeVec,
    pub(super) horizontal_transport_messages_dropped_total: CounterVec,
    pub(super) horizontal_transport_reconnections_total: CounterVec,
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

        let ai_messages_validated_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}ai_messages_validated_total"),
                "Total number of AI Transport messages accepted by validation"
            ),
            &["app_id", "port", "event"]
        )
        .unwrap();

        let ai_messages_rejected_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}ai_messages_rejected_total"),
                "Total number of AI Transport messages rejected by validation"
            ),
            &["app_id", "port", "code"]
        )
        .unwrap();

        let ai_messages_unparseable_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}ai_messages_unparseable_total"),
                "Total number of malformed AI Transport headers observed outside validation"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ai_turns_started_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}ai_turns_started_total"),
                "Total number of AI Transport turns started"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ai_turns_ended_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}ai_turns_ended_total"),
                "Total number of AI Transport turns ended by bounded reason"
            ),
            &["app_id", "port", "reason"]
        )
        .unwrap();

        let ai_cancel_signals_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}ai_cancel_signals_total"),
                "Total number of AI Transport cancel signals"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ai_active_streams = register_gauge_vec!(
            Opts::new(
                format!("{prefix}ai_active_streams"),
                "Number of active AI Transport streams"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ai_stream_duration_seconds = register_histogram_vec!(
            histogram_opts!(
                format!("{prefix}ai_stream_duration_seconds"),
                "AI Transport stream duration in seconds",
                vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0]
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ai_stream_bytes_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}ai_stream_bytes_total"),
                "Total AI Transport stream payload bytes observed"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let appends_received_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}appends_received_total"),
                "Total number of AI Transport message.append events received by rollup"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let appends_delivered_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}appends_delivered_total"),
                "Total number of AI Transport append deliveries emitted by rollup"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let rollup_ratio = register_histogram_vec!(
            histogram_opts!(
                format!("{prefix}rollup_ratio"),
                "Number of input appends represented by one rollup output",
                vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let active_streams = register_gauge_vec!(
            Opts::new(
                format!("{prefix}active_streams"),
                "Number of active AI Transport append rollup streams"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let flush_latency = register_histogram_vec!(
            histogram_opts!(
                format!("{prefix}flush_latency"),
                "AI Transport append rollup flush latency in milliseconds",
                vec![0.0, 5.0, 20.0, 40.0, 100.0, 500.0, 1000.0]
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
            ai_messages_validated_total,
            ai_messages_rejected_total,
            ai_messages_unparseable_total,
            ai_turns_started_total,
            ai_turns_ended_total,
            ai_cancel_signals_total,
            ai_active_streams,
            ai_stream_duration_seconds,
            ai_stream_bytes_total,
            appends_received_total,
            appends_delivered_total,
            rollup_ratio,
            active_streams,
            flush_latency,
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
    pub(super) fn update_process_metrics(&self) {
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
    pub(super) fn get_tags(&self, app_id: &str) -> Vec<String> {
        vec![app_id.to_string(), self.port.to_string()]
    }
}
