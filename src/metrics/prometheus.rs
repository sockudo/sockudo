#![allow(dead_code)]

// src/metrics/prometheus.rs

use crate::error::Result;

use super::MetricsInterface;
use crate::websocket::SocketId;
use async_trait::async_trait;
use prometheus::{
    CounterVec, Gauge, GaugeVec, HistogramVec, Opts, TextEncoder, histogram_opts,
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
};
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};
use tracing::{debug, error};

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

/// A Prometheus implementation of the metrics interface
pub struct PrometheusMetricsDriver {
    prefix: String,
    port: u16,

    // Process metrics (for memory leak detection)
    process_resident_memory_bytes: Gauge,
    process_virtual_memory_bytes: Gauge,
    process_cpu_seconds_total: Gauge,
    process_start_time_seconds: Gauge,
    process_open_fds: Gauge,

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
    // Redis Cluster transport metrics
    redis_cluster_channel_queue_size: GaugeVec,
    redis_cluster_channel_messages_dropped: CounterVec,
    redis_cluster_reconnections_total: CounterVec,
}

impl PrometheusMetricsDriver {
    /// Creates a new Prometheus metrics driver
    pub async fn new(port: u16, prefix_opt: Option<&str>) -> Self {
        let prefix = prefix_opt.unwrap_or("sockudo_").to_string();

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

        // Redis Cluster transport metrics
        let redis_cluster_channel_queue_size = register_gauge_vec!(
            Opts::new(
                format!("{prefix}redis_cluster_channel_queue_size"),
                "Current size of the Redis Cluster PubSub message queue"
            ),
            &["transport_type"]
        )
        .unwrap();

        let redis_cluster_channel_messages_dropped = register_counter_vec!(
            Opts::new(
                format!("{prefix}redis_cluster_channel_messages_dropped_total"),
                "Total number of messages dropped due to full queue"
            ),
            &["transport_type"]
        )
        .unwrap();

        let redis_cluster_reconnections_total = register_counter_vec!(
            Opts::new(
                format!("{prefix}redis_cluster_reconnections_total"),
                "Total number of Redis Cluster reconnections"
            ),
            &["transport_type"]
        )
        .unwrap();

        // Reset gauge metrics to 0 on startup - they represent current state, not historical
        connected_sockets.reset();
        active_channels.reset();
        redis_cluster_channel_queue_size.reset();

        // Set process start time
        if let Ok(boot_time) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            process_start_time_seconds.set(boot_time.as_secs_f64());
        }

        Self {
            prefix,
            port,
            process_resident_memory_bytes,
            process_virtual_memory_bytes,
            process_cpu_seconds_total,
            process_start_time_seconds,
            process_open_fds,
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
            redis_cluster_channel_queue_size,
            redis_cluster_channel_messages_dropped,
            redis_cluster_reconnections_total,
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
            }
        }

        #[cfg(target_os = "windows")]
        {
            // On Windows, use GetProcessMemoryInfo
            // For now, just set to 0 - Windows support can be added later
            // using the windows crate if needed
        }
    }

    /// Record Redis Cluster channel queue size
    pub fn record_redis_cluster_queue_size(&self, transport_type: &str, size: usize) {
        self.redis_cluster_channel_queue_size
            .with_label_values(&[transport_type])
            .set(size as f64);
    }

    /// Increment Redis Cluster messages dropped counter
    pub fn increment_redis_cluster_messages_dropped(&self, transport_type: &str) {
        self.redis_cluster_channel_messages_dropped
            .with_label_values(&[transport_type])
            .inc();
    }

    /// Increment Redis Cluster reconnections counter
    pub fn increment_redis_cluster_reconnections(&self, transport_type: &str) {
        self.redis_cluster_reconnections_total
            .with_label_values(&[transport_type])
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
        let channel_type = crate::channel::ChannelType::from_name(channel_name).as_str();

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

    async fn get_metrics_as_plaintext(&self) -> String {
        // Update process metrics before gathering
        self.update_process_metrics();

        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather(); // Gather from the default registry
        encoder
            .encode_to_string(&metric_families)
            .unwrap_or_else(|e| {
                error!("{}", format!("Failed to encode metrics to string: {}", e));
                String::from("Error encoding metrics")
            })
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

        // Gather metrics from Prometheus
        let metric_families = prometheus::gather();

        // Convert Prometheus metrics to JSON
        let mut json_metrics = json!({});

        for mf in metric_families {
            let name = mf.name();
            let metric_type = mf.type_();

            for m in mf.get_metric() {
                let labels = m.get_label();
                let mut label_json = json!({});

                // Process labels
                for label in labels {
                    label_json[label.name()] = json!(label.value());
                }

                // Get metric value based on type
                let value = match metric_type {
                    prometheus::proto::MetricType::COUNTER => {
                        json!(m.get_counter().value())
                    }
                    prometheus::proto::MetricType::GAUGE => {
                        json!(m.get_gauge().value())
                    }
                    prometheus::proto::MetricType::HISTOGRAM => {
                        let h = m.get_histogram();
                        let mut buckets = Vec::new();
                        for b in h.get_bucket() {
                            buckets.push(json!({
                                "upper_bound": b.upper_bound(),
                                "cumulative_count": b.cumulative_count()
                            }));
                        }
                        json!({
                            "sample_count": h.get_sample_count(),
                            "sample_sum": h.get_sample_sum(),
                            "buckets": buckets
                        })
                    }
                    _ => {
                        json!(null)
                    }
                };

                // Add to json_metrics
                if labels.is_empty() {
                    json_metrics[name] = value;
                } else {
                    if !json_metrics.as_object().unwrap().contains_key(&name) {
                        json_metrics[name] = json!([]);
                    }
                    let mut metric_with_labels = label_json;
                    metric_with_labels["value"] = value;
                    json_metrics[name]
                        .as_array_mut()
                        .unwrap()
                        .push(metric_with_labels);
                }
            }
        }

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
