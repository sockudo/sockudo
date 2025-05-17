// src/metrics/prometheus.rs

use crate::error::{Error, Result};

use super::MetricsInterface;
use crate::websocket::SocketId;
use async_trait::async_trait;
use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, GaugeVec,
    HistogramVec, Opts, TextEncoder,
};
use serde_json::{json, Value};
use tracing::{error, info};

/// A Prometheus implementation of the metrics interface
pub struct PrometheusMetricsDriver {
    prefix: String,
    port: u16,

    // Metrics
    connected_sockets: GaugeVec,
    new_connections_total: CounterVec,
    new_disconnections_total: CounterVec,
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
}

impl PrometheusMetricsDriver {
    /// Creates a new Prometheus metrics driver
    pub async fn new(port: u16, prefix_opt: Option<&str>) -> Self {
        let prefix = prefix_opt.unwrap_or("sockudo_").to_string();

        // Initialize all metrics
        let connected_sockets = register_gauge_vec!(
            Opts::new(
                format!("{}connected", prefix),
                "The number of currently connected sockets"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let new_connections_total = register_counter_vec!(
            Opts::new(
                format!("{}new_connections_total", prefix),
                "Total amount of sockudo connection requests"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let new_disconnections_total = register_counter_vec!(
            Opts::new(
                format!("{}new_disconnections_total", prefix),
                "Total amount of sockudo disconnections"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let socket_bytes_received = register_counter_vec!(
            Opts::new(
                format!("{}socket_received_bytes", prefix),
                "Total amount of bytes that sockudo received"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let socket_bytes_transmitted = register_counter_vec!(
            Opts::new(
                format!("{}socket_transmitted_bytes", prefix),
                "Total amount of bytes that sockudo transmitted"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ws_messages_received = register_counter_vec!(
            Opts::new(
                format!("{}ws_messages_received_total", prefix),
                "The total amount of WS messages received from connections by the server"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let ws_messages_sent = register_counter_vec!(
            Opts::new(
                format!("{}ws_messages_sent_total", prefix),
                "The total amount of WS messages sent to the connections from the server"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let http_bytes_received = register_counter_vec!(
            Opts::new(
                format!("{}http_received_bytes", prefix),
                "Total amount of bytes that sockudo's REST API received"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let http_bytes_transmitted = register_counter_vec!(
            Opts::new(
                format!("{}http_transmitted_bytes", prefix),
                "Total amount of bytes that sockudo's REST API sent back"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let http_calls_received = register_counter_vec!(
            Opts::new(
                format!("{}http_calls_received_total", prefix),
                "Total amount of received REST API calls"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_resolve_time = register_histogram_vec!(
            format!("{}horizontal_adapter_resolve_time", prefix),
            "The average resolve time for requests to other nodes",
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_resolved_promises = register_counter_vec!(
            Opts::new(
                format!("{}horizontal_adapter_resolved_promises", prefix),
                "The total amount of promises that were fulfilled by other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_uncomplete_promises = register_counter_vec!(
            Opts::new(
                format!("{}horizontal_adapter_uncomplete_promises", prefix),
                "The total amount of promises that were not fulfilled entirely by other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_sent_requests = register_counter_vec!(
            Opts::new(
                format!("{}horizontal_adapter_sent_requests", prefix),
                "The total amount of sent requests to other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_received_requests = register_counter_vec!(
            Opts::new(
                format!("{}horizontal_adapter_received_requests", prefix),
                "The total amount of received requests from other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        let horizontal_adapter_received_responses = register_counter_vec!(
            Opts::new(
                format!("{}horizontal_adapter_received_responses", prefix),
                "The total amount of received responses from other nodes"
            ),
            &["app_id", "port"]
        )
        .unwrap();

        Self {
            prefix,
            port,
            connected_sockets,
            new_connections_total,
            new_disconnections_total,
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
        }
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

        info!(
            "{}",
            format!(
                "Metrics: New connection for app {}, socket {}",
                app_id, socket_id
            )
        );
    }

    fn mark_disconnection(&self, app_id: &str, socket_id: &SocketId) {
        let tags = self.get_tags(app_id);
        self.connected_sockets.with_label_values(&tags).dec();
        self.new_disconnections_total.with_label_values(&tags).inc();

        info!(
            "{}",
            format!(
                "Metrics: Disconnection for app {}, socket {}",
                app_id, socket_id
            )
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
    }

    fn mark_ws_message_sent(&self, app_id: &str, sent_message_size: usize) {
        let tags = self.get_tags(app_id);
        self.socket_bytes_transmitted
            .with_label_values(&tags)
            .inc_by(sent_message_size as f64);
        self.ws_messages_sent.with_label_values(&tags).inc();
    }

    fn mark_ws_message_received(&self, app_id: &str, message_size: usize) {
        let tags = self.get_tags(app_id);
        self.socket_bytes_received
            .with_label_values(&tags)
            .inc_by(message_size as f64);
        self.ws_messages_received.with_label_values(&tags).inc();
    }

    fn track_horizontal_adapter_resolve_time(&self, app_id: &str, time_ms: f64) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_resolve_time
            .with_label_values(&tags)
            .observe(time_ms);
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
    }

    fn mark_horizontal_adapter_request_sent(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_sent_requests
            .with_label_values(&tags)
            .inc();
    }

    fn mark_horizontal_adapter_request_received(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_received_requests
            .with_label_values(&tags)
            .inc();
    }

    fn mark_horizontal_adapter_response_received(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_received_responses
            .with_label_values(&tags)
            .inc();
    }

    async fn get_metrics_as_plaintext(&self) -> String {
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
                    if !json_metrics.as_object().unwrap().contains_key(name) {
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

    async fn clear(&self) {
        // Reset individual metrics counters - not fully supported by Prometheus Rust client
        // So we'll just log a message
        info!(
            "{}",
            "Metrics cleared (note: Prometheus metrics can't be fully cleared)"
        );
    }
}
