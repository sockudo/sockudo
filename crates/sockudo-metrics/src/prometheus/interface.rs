use super::driver::PrometheusMetricsDriver;
use super::text::prometheus_text_to_json;
use async_trait::async_trait;
use sockudo_core::channel::ChannelType;
use sockudo_core::error::Result;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::websocket::SocketId;
use sonic_rs::{Value, json};
use tracing::debug;

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

        debug!(app_id, socket_id = %socket_id, "metrics: new connection");
    }

    fn mark_disconnection(&self, app_id: &str, socket_id: &SocketId) {
        let tags = self.get_tags(app_id);
        self.connected_sockets.with_label_values(&tags).dec();
        self.new_disconnections_total.with_label_values(&tags).inc();

        debug!(app_id, socket_id = %socket_id, "metrics: disconnection");
    }

    fn mark_connection_error(&self, app_id: &str, error_type: &str) {
        let tags = [app_id, self.port_label.as_str(), error_type];
        self.connection_errors_total.with_label_values(&tags).inc();

        debug!(app_id, error_type, "metrics: connection error");
    }

    fn mark_memory_pressure_rejection(&self) {
        self.memory_pressure_rejections_total
            .with_label_values(&[self.port_label.as_str()])
            .inc();
    }

    fn update_memory_pressure_shedding(&self, shedding: bool) {
        self.memory_pressure_shedding
            .set(if shedding { 1.0 } else { 0.0 });
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
        let tags = [
            app_id,
            self.port_label.as_str(),
            limiter_type,
            request_context,
        ];
        self.rate_limit_checks_total.with_label_values(&tags).inc();

        debug!(
            app_id,
            limiter_type, request_context, "metrics: rate limit check"
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
        let tags = [
            app_id,
            self.port_label.as_str(),
            limiter_type,
            request_context,
        ];
        self.rate_limit_triggered_total
            .with_label_values(&tags)
            .inc();

        debug!(
            app_id,
            limiter_type, request_context, "metrics: rate limit triggered"
        );
    }

    fn mark_channel_subscription(&self, app_id: &str, channel_type: &str) {
        let tags = [app_id, self.port_label.as_str(), channel_type];
        self.channel_subscriptions_total
            .with_label_values(&tags)
            .inc();

        debug!(app_id, channel_type, "metrics: channel subscription");
    }

    fn mark_channel_unsubscription(&self, app_id: &str, channel_type: &str) {
        let tags = [app_id, self.port_label.as_str(), channel_type];
        self.channel_unsubscriptions_total
            .with_label_values(&tags)
            .inc();

        debug!(app_id, channel_type, "metrics: channel unsubscription");
    }

    fn mark_channel_activated(&self, app_id: &str, channel_type: &str) {
        let port = self.port_label.as_str();
        self.active_channels
            .with_label_values(&[app_id, port, channel_type])
            .inc();
    }

    fn mark_channel_deactivated(&self, app_id: &str, channel_type: &str) {
        let port = self.port_label.as_str();
        self.active_channels
            .with_label_values(&[app_id, port, channel_type])
            .dec();
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

        debug!(app_id, "metrics: api message recorded");
    }

    fn mark_ws_message_sent(&self, app_id: &str, sent_message_size: usize) {
        let tags = self.get_tags(app_id);
        self.socket_bytes_transmitted
            .with_label_values(&tags)
            .inc_by(sent_message_size as f64);
        self.ws_messages_sent.with_label_values(&tags).inc();

        debug!(app_id, "metrics: ws message sent");
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
            app_id,
            batch_count = count,
            "metrics: ws messages sent batch"
        );
    }

    fn mark_ws_message_received(&self, app_id: &str, message_size: usize) {
        let tags = self.get_tags(app_id);
        self.socket_bytes_received
            .with_label_values(&tags)
            .inc_by(message_size as f64);
        self.ws_messages_received.with_label_values(&tags).inc();

        debug!(app_id, "metrics: ws message received");
    }

    fn track_horizontal_adapter_resolve_time(&self, app_id: &str, time_ms: f64) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_resolve_time
            .with_label_values(&tags)
            .observe(time_ms);

        debug!(app_id, "metrics: horizontal adapter resolve time recorded");
    }
    fn track_horizontal_adapter_resolved_promises(
        &self,
        app_id: &str,
        resolved: bool,
        request_type: &str,
    ) {
        let tags = self.get_tags(app_id);
        let resolved_promises = self
            .horizontal_adapter_resolved_promises
            .with_label_values(&tags);
        let uncomplete_tags = [tags[0], tags[1], request_type];
        let uncomplete_promises = self
            .horizontal_adapter_uncomplete_promises
            .with_label_values(&uncomplete_tags);

        // Register both sides of the outcome before incrementing either one so
        // alerts can compare an explicit zero against the non-zero series.
        resolved_promises.init();
        uncomplete_promises.init();

        if resolved {
            resolved_promises.inc();
        } else {
            uncomplete_promises.inc();
        }

        debug!(
            app_id,
            resolved, request_type, "metrics: horizontal adapter promise recorded"
        );
    }

    fn mark_horizontal_adapter_request_sent(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_sent_requests
            .with_label_values(&tags)
            .inc();

        debug!(app_id, "metrics: horizontal adapter request sent");
    }

    fn mark_horizontal_adapter_request_received(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_received_requests
            .with_label_values(&tags)
            .inc();

        debug!(app_id, "metrics: horizontal adapter request received");
    }

    fn mark_horizontal_adapter_response_received(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.horizontal_adapter_received_responses
            .with_label_values(&tags)
            .inc();

        debug!(app_id, "metrics: horizontal adapter response received");
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
            .with_label_values(&[app_id, self.port_label.as_str(), channel_type, bucket])
            .observe(latency_ms);

        debug!(
            app_id,
            channel_type, bucket, "metrics: broadcast latency recorded"
        );
    }

    fn mark_idempotency_publish(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.idempotency_publish_total
            .with_label_values(&tags)
            .inc();

        debug!(app_id, "metrics: idempotency publish recorded");
    }

    fn mark_idempotency_duplicate(&self, app_id: &str) {
        let tags = self.get_tags(app_id);
        self.idempotency_duplicates_total
            .with_label_values(&tags)
            .inc();

        debug!(app_id, "metrics: idempotency duplicate recorded");
    }

    fn mark_mcp_request(&self, outcome: &str) {
        self.mcp_requests_total
            .with_label_values(&[&self.port.to_string(), outcome])
            .inc();
    }

    fn mark_mcp_tool_call(&self, tool: &str, outcome: &str) {
        self.mcp_tool_calls_total
            .with_label_values(&[&self.port.to_string(), tool, outcome])
            .inc();
    }

    fn track_mcp_tool_latency(&self, tool: &str, latency_ms: f64) {
        self.mcp_tool_latency_ms
            .with_label_values(&[&self.port.to_string(), tool])
            .observe(latency_ms);
    }

    fn mark_ai_transport_validated(&self, app_id: &str, event: &str) {
        self.ai_messages_validated_total
            .with_label_values(&[app_id, self.port_label.as_str(), event])
            .inc();
    }

    fn mark_ai_transport_rejected(&self, app_id: &str, code: u32) {
        self.ai_messages_rejected_total
            .with_label_values(&[app_id, self.port_label.as_str(), &code.to_string()])
            .inc();
    }

    fn mark_ai_transport_unparseable(&self, app_id: &str) {
        self.ai_messages_unparseable_total
            .with_label_values(&[app_id, self.port_label.as_str()])
            .inc();
    }

    fn mark_ai_run_started(&self, app_id: &str) {
        self.ai_runs_started_total
            .with_label_values(&[app_id, self.port_label.as_str()])
            .inc();
        self.ai_turns_started_total
            .with_label_values(&[app_id, self.port_label.as_str()])
            .inc();
    }

    fn mark_ai_run_ended(&self, app_id: &str, reason: &str) {
        self.ai_runs_ended_total
            .with_label_values(&[app_id, self.port_label.as_str(), reason])
            .inc();
        self.ai_turns_ended_total
            .with_label_values(&[app_id, self.port_label.as_str(), reason])
            .inc();
    }

    fn mark_ai_cancel_signal(&self, app_id: &str) {
        self.ai_cancel_signals_total
            .with_label_values(&[app_id, self.port_label.as_str()])
            .inc();
    }

    fn update_ai_active_streams(&self, app_id: &str, streams: u64) {
        self.ai_active_streams
            .with_label_values(&[app_id, self.port_label.as_str()])
            .set(streams as f64);
    }

    fn add_ai_active_streams(&self, app_id: &str, delta: i64) {
        self.ai_active_streams
            .with_label_values(&[app_id, self.port_label.as_str()])
            .add(delta as f64);
    }

    fn observe_ai_stream_duration(&self, app_id: &str, duration_seconds: f64) {
        self.ai_stream_duration_seconds
            .with_label_values(&[app_id, self.port_label.as_str()])
            .observe(duration_seconds);
    }

    fn mark_ai_stream_bytes(&self, app_id: &str, bytes: usize) {
        self.ai_stream_bytes_total
            .with_label_values(&[app_id, self.port_label.as_str()])
            .inc_by(bytes as f64);
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
            .with_label_values(&[app_id, self.port_label.as_str(), source])
            .inc();
    }

    fn mark_history_recovery_failure(&self, app_id: &str, code: &str) {
        self.history_recovery_failures_total
            .with_label_values(&[app_id, self.port_label.as_str(), code])
            .inc();
    }

    fn mark_versioned_message_mutation(&self, app_id: &str, action: &str, result: &str) {
        self.versioned_message_mutations_total
            .with_label_values(&[app_id, self.port_label.as_str(), action, result])
            .inc();
    }

    fn mark_versioned_message_retrieval(&self, app_id: &str, surface: &str, result: &str) {
        self.versioned_message_retrieval_total
            .with_label_values(&[app_id, self.port_label.as_str(), surface, result])
            .inc();
    }

    fn mark_versioned_history_substitution(&self, app_id: &str, result: &str) {
        self.versioned_history_substitution_total
            .with_label_values(&[app_id, self.port_label.as_str(), result])
            .inc();
    }

    fn mark_ai_rollup_append_received(&self, app_id: &str) {
        self.appends_received_total
            .with_label_values(&[app_id, self.port_label.as_str()])
            .inc();
    }

    fn mark_ai_rollup_append_delivered(&self, app_id: &str) {
        self.appends_delivered_total
            .with_label_values(&[app_id, self.port_label.as_str()])
            .inc();
    }

    fn observe_ai_rollup_ratio(&self, app_id: &str, ratio: f64) {
        self.rollup_ratio
            .with_label_values(&[app_id, self.port_label.as_str()])
            .observe(ratio);
    }

    fn update_ai_rollup_active_streams(&self, app_id: &str, streams: u64) {
        self.active_streams
            .with_label_values(&[app_id, self.port_label.as_str()])
            .set(streams as f64);
    }

    fn add_ai_rollup_active_streams(&self, app_id: &str, delta: i64) {
        self.active_streams
            .with_label_values(&[app_id, self.port_label.as_str()])
            .add(delta as f64);
    }

    fn observe_ai_rollup_flush_latency(&self, app_id: &str, latency_ms: f64) {
        self.flush_latency
            .with_label_values(&[app_id, self.port_label.as_str()])
            .observe(latency_ms);
    }

    fn mark_annotation_published(&self, channel: &str, annotation_type: &str) {
        self.annotations_published_total
            .with_label_values(&[
                ChannelType::from_name(channel).as_str(),
                annotation_summary_type(annotation_type),
            ])
            .inc();
    }

    fn mark_annotation_deleted(&self, channel: &str, annotation_type: &str) {
        self.annotations_deleted_total
            .with_label_values(&[
                ChannelType::from_name(channel).as_str(),
                annotation_summary_type(annotation_type),
            ])
            .inc();
    }

    fn mark_annotation_summary_delivery(&self, channel: &str) {
        self.annotation_summary_deliveries_total
            .with_label_values(&[ChannelType::from_name(channel).as_str()])
            .inc();
    }

    fn mark_annotation_summary_clipped(&self, channel: &str, annotation_type: &str) {
        self.annotation_summary_clipped_total
            .with_label_values(&[
                ChannelType::from_name(channel).as_str(),
                annotation_summary_type(annotation_type),
            ])
            .inc();
    }

    fn mark_annotation_projection_rebuild(&self, channel: &str) {
        self.annotation_projection_rebuild_total
            .with_label_values(&[ChannelType::from_name(channel).as_str()])
            .inc();
    }

    fn track_annotation_projection_rebuild_duration(&self, channel: &str, duration_seconds: f64) {
        self.annotation_projection_rebuild_duration_seconds
            .with_label_values(&[ChannelType::from_name(channel).as_str()])
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

    fn mark_nats_event(&self, event: &str) {
        self.nats_events_total.with_label_values(&[event]).inc();
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
            // Preserve the historical metric label key for dashboards while
            // bounding cardinality by recording channel type instead of raw name.
            let channel_type = ChannelType::from_name(channel_name).as_str();
            self.horizontal_delta_compression_enabled
                .with_label_values(&[app_id, self.port_label.as_str(), channel_type])
                .inc();

            debug!(
                app_id,
                channel_type, "metrics: horizontal delta compression enabled"
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
            .with_label_values(&[app_id, self.port_label.as_str(), channel_name])
            .inc_by(original_bytes as f64);

        self.delta_compression_bandwidth_saved
            .with_label_values(&[app_id, self.port_label.as_str(), channel_name])
            .inc_by(saved_bytes as f64);

        debug!(app_id, "metrics: delta compression bandwidth recorded");
    }

    fn track_delta_compression_full_message(&self, app_id: &str, channel_name: &str) {
        self.delta_compression_full_messages
            .with_label_values(&[app_id, self.port_label.as_str(), channel_name])
            .inc();

        debug!(app_id, "metrics: delta compression full message recorded");
    }

    fn track_delta_compression_delta_message(&self, app_id: &str, channel_name: &str) {
        self.delta_compression_delta_messages
            .with_label_values(&[app_id, self.port_label.as_str(), channel_name])
            .inc();

        debug!(app_id, "metrics: delta compression delta message recorded");
    }

    async fn clear(&self) {
        // Reset individual metrics counters - not fully supported by Prometheus Rust client
        // So we'll just log a message
        debug!("metrics cleared: prometheus metrics cannot be fully reset");
    }
}

// Annotation names are user-defined. Keep the finite summarizer identity while
// aggregating arbitrary suffix/name churn into the same series.
fn annotation_summary_type(annotation_type: &str) -> &str {
    match annotation_type
        .split_once(':')
        .and_then(|(_, suffix)| suffix.rsplit_once(".v"))
        .map(|(summarizer, _)| summarizer)
        .unwrap_or("custom")
    {
        "count" => "count",
        "flag" => "flag",
        "distinct" => "distinct",
        "multiple" => "multiple",
        "unique" => "unique",
        "total" => "total",
        _ => "custom",
    }
}

#[cfg(test)]
mod annotation_metric_tests {
    use super::*;
    #[tokio::test]
    async fn annotation_churn_has_bounded_series_and_exact_aggregate_counts() {
        let driver = PrometheusMetricsDriver::new(6001, Some("annotations_bound_")).await;
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        metrics::with_local_recorder(&recorder, || {
            for n in 0..10_000 {
                driver.mark_annotation_published(
                    &format!("presence-room-{n}"),
                    &format!("reaction-{n}:total.v1"),
                );
                driver.track_annotation_projection_rebuild_duration(
                    &format!("presence-room-{n}"),
                    0.001,
                );
            }
        });
        let rendered = recorder.handle().render();
        let counters = rendered
            .lines()
            .filter(|line| line.starts_with("annotations_bound_annotations_published_total{"))
            .collect::<Vec<_>>();
        assert_eq!(counters.len(), 1);
        assert!(counters[0].ends_with("10000"));
        assert!(counters[0].contains("type=\"total\""));
        assert!(!rendered.contains("room-"));
        assert!(!rendered.contains("reaction-"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn promise_outcome_tracking_registers_both_counter_series() {
        let metrics = PrometheusMetricsDriver::new(0, Some("promise_test_")).await;

        metrics.track_horizontal_adapter_resolved_promises("test-app", false, "SocketsCount");
        let output = metrics.get_metrics_as_plaintext().await;

        assert!(output.contains(
            "promise_test_horizontal_adapter_resolved_promises{app_id=\"test-app\",port=\"0\"} 0"
        ));
        assert!(output.contains(
            "promise_test_horizontal_adapter_uncomplete_promises{app_id=\"test-app\",port=\"0\",request_type=\"SocketsCount\"} 1"
        ));
    }
}
