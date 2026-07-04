use crate::websocket::SocketId;
use async_trait::async_trait;
use sonic_rs::Value;

/// Metrics Interface trait that any metrics driver should implement
#[async_trait]
pub trait MetricsInterface: Send + Sync {
    /// Initialize the metrics driver
    async fn init(&self) -> crate::error::Result<()>;

    /// Handle a new connection
    fn mark_new_connection(&self, app_id: &str, socket_id: &SocketId);

    /// Handle a disconnection
    fn mark_disconnection(&self, app_id: &str, socket_id: &SocketId);

    /// Handle a connection error
    fn mark_connection_error(&self, app_id: &str, error_type: &str);

    /// Track a rate limit check
    fn mark_rate_limit_check(&self, app_id: &str, limiter_type: &str);

    /// Track a rate limit check with request context
    fn mark_rate_limit_check_with_context(
        &self,
        app_id: &str,
        limiter_type: &str,
        request_context: &str,
    );

    /// Track a rate limit trigger (when limit is exceeded)
    fn mark_rate_limit_triggered(&self, app_id: &str, limiter_type: &str);

    /// Track a rate limit trigger with request context
    fn mark_rate_limit_triggered_with_context(
        &self,
        app_id: &str,
        limiter_type: &str,
        request_context: &str,
    );

    /// Track a channel subscription
    fn mark_channel_subscription(&self, app_id: &str, channel_type: &str);

    /// Track a channel unsubscription
    fn mark_channel_unsubscription(&self, app_id: &str, channel_type: &str);

    /// Track a channel activation
    fn mark_channel_activated(&self, _app_id: &str, _channel_type: &str) {}

    /// Track a channel deactivation
    fn mark_channel_deactivated(&self, _app_id: &str, _channel_type: &str) {}

    /// Handle a new API message event being received and sent out
    fn mark_api_message(
        &self,
        app_id: &str,
        incoming_message_size: usize,
        sent_message_size: usize,
    );

    /// Handle a new WS client message event being sent
    fn mark_ws_message_sent(&self, app_id: &str, sent_message_size: usize);

    /// Handle multiple WS client messages being sent (batch update for performance)
    fn mark_ws_messages_sent_batch(&self, app_id: &str, sent_message_size: usize, count: usize);

    /// Handle a new WS client message being received
    fn mark_ws_message_received(&self, app_id: &str, message_size: usize);

    /// Track the time in which horizontal adapter resolves requests from other nodes
    fn track_horizontal_adapter_resolve_time(&self, app_id: &str, time_ms: f64);

    /// Track the fulfillings in which horizontal adapter resolves requests from other nodes
    fn track_horizontal_adapter_resolved_promises(&self, app_id: &str, resolved: bool);

    /// Handle a new horizontal adapter request sent
    fn mark_horizontal_adapter_request_sent(&self, app_id: &str);

    /// Handle a new horizontal adapter request that was marked as received
    fn mark_horizontal_adapter_request_received(&self, app_id: &str);

    /// Handle a new horizontal adapter response from other node
    fn mark_horizontal_adapter_response_received(&self, app_id: &str);

    /// Track broadcast message latency
    fn track_broadcast_latency(
        &self,
        app_id: &str,
        channel_name: &str,
        recipient_count: usize,
        latency_ms: f64,
    );

    /// Track delta compression usage in horizontal broadcasts
    fn track_horizontal_delta_compression(&self, app_id: &str, channel_name: &str, enabled: bool);

    /// Track bandwidth savings from delta compression
    fn track_delta_compression_bandwidth(
        &self,
        app_id: &str,
        channel_name: &str,
        original_bytes: usize,
        compressed_bytes: usize,
    );

    /// Track delta compression full message sends
    fn track_delta_compression_full_message(&self, app_id: &str, channel_name: &str);

    /// Track delta compression delta message sends
    fn track_delta_compression_delta_message(&self, app_id: &str, channel_name: &str);

    /// Track a publish request that includes an idempotency key
    fn mark_idempotency_publish(&self, _app_id: &str) {}

    /// Track a duplicate publish caught by idempotency deduplication
    fn mark_idempotency_duplicate(&self, _app_id: &str) {}

    /// Track an AI Transport message accepted by edge validation.
    fn mark_ai_transport_validated(&self, _app_id: &str, _event: &str) {}

    /// Track an AI Transport message rejected by edge validation.
    fn mark_ai_transport_rejected(&self, _app_id: &str, _code: u32) {}

    /// Track a malformed AI Transport header observed outside the validation path.
    fn mark_ai_transport_unparseable(&self, _app_id: &str) {}

    /// Track an AI run start event.
    fn mark_ai_run_started(&self, _app_id: &str) {}

    /// Track an AI run end event by bounded reason label.
    fn mark_ai_run_ended(&self, _app_id: &str, _reason: &str) {}

    /// Track a legacy AI turn start event.
    fn mark_ai_turn_started(&self, app_id: &str) {
        self.mark_ai_run_started(app_id);
    }

    /// Track a legacy AI turn end event by bounded reason label.
    fn mark_ai_turn_ended(&self, app_id: &str, reason: &str) {
        self.mark_ai_run_ended(app_id, reason);
    }

    /// Track an AI cancel signal.
    fn mark_ai_cancel_signal(&self, _app_id: &str) {}

    /// Update currently active AI streams.
    fn update_ai_active_streams(&self, _app_id: &str, _streams: u64) {}

    /// Observe AI stream lifetime in seconds.
    fn observe_ai_stream_duration(&self, _app_id: &str, _duration_seconds: f64) {}

    /// Track AI stream payload bytes observed at transport boundaries.
    fn mark_ai_stream_bytes(&self, _app_id: &str, _bytes: usize) {}

    /// Track a versioned AI append received by the rollup egress path.
    fn mark_ai_rollup_append_received(&self, _app_id: &str) {}

    /// Track an AI append delivery emitted by the rollup egress path.
    fn mark_ai_rollup_append_delivered(&self, _app_id: &str) {}

    /// Observe the number of input appends represented by one rollup output.
    fn observe_ai_rollup_ratio(&self, _app_id: &str, _ratio: f64) {}

    /// Update active AI append rollup streams.
    fn update_ai_rollup_active_streams(&self, _app_id: &str, _streams: u64) {}

    /// Observe latency from first pending append to flush.
    fn observe_ai_rollup_flush_latency(&self, _app_id: &str, _latency_ms: f64) {}

    /// Track an ephemeral message delivery (V2 only)
    fn mark_ephemeral_message(&self, _app_id: &str) {}

    /// Track a message suppressed by event name filtering (V2 only)
    fn mark_event_filter_suppressed(&self, _app_id: &str) {}

    /// Track a message delivery skipped due to echo control (V2 only)
    fn mark_echo_suppressed(&self, _app_id: &str) {}

    /// Track a successful durable history write.
    fn mark_history_write(&self, _app_id: &str) {}

    /// Track durable history write latency.
    fn track_history_write_latency(&self, _app_id: &str, _latency_ms: f64) {}

    /// Track a durable history write failure.
    fn mark_history_write_failure(&self, _app_id: &str) {}

    /// Update retained durable history counts for an app.
    fn update_history_retained(&self, _app_id: &str, _messages: u64, _bytes: u64) {}

    /// Track evictions performed by the durable history store.
    fn mark_history_eviction(&self, _app_id: &str, _messages: u64, _bytes: u64) {}

    /// Update the current durable history writer queue depth for an app.
    fn update_history_queue_depth(&self, _app_id: &str, _depth: usize) {}

    /// Update the number of degraded durable history channels for an app.
    fn update_history_degraded_channels(&self, _app_id: &str, _count: usize) {}

    /// Update the number of reset-required durable history channels for an app.
    fn update_history_reset_required_channels(&self, _app_id: &str, _count: usize) {}

    /// Track a recovery success sourced from hot or cold history.
    fn mark_history_recovery_success(&self, _app_id: &str, _source: &str) {}

    /// Track a recovery failure code.
    fn mark_history_recovery_failure(&self, _app_id: &str, _code: &str) {}

    /// Track a mutable-message mutation attempt/result.
    fn mark_versioned_message_mutation(&self, _app_id: &str, _action: &str, _result: &str) {}

    /// Track a latest-message or version-history retrieval result.
    fn mark_versioned_message_retrieval(&self, _app_id: &str, _surface: &str, _result: &str) {}

    /// Track history substitution and anomaly outcomes for versioned messages.
    fn mark_versioned_history_substitution(&self, _app_id: &str, _result: &str) {}

    /// Track a successful annotation publish.
    fn mark_annotation_published(&self, _channel: &str, _annotation_type: &str) {}

    /// Track a successful annotation delete.
    fn mark_annotation_deleted(&self, _channel: &str, _annotation_type: &str) {}

    /// Track a delivered annotation summary.
    fn mark_annotation_summary_delivery(&self, _channel: &str) {}

    /// Track a clipped annotation summary.
    fn mark_annotation_summary_clipped(&self, _channel: &str, _annotation_type: &str) {}

    /// Track a full annotation projection rebuild.
    fn mark_annotation_projection_rebuild(&self, _channel: &str) {}

    /// Track full annotation projection rebuild latency.
    fn track_annotation_projection_rebuild_duration(&self, _channel: &str, _duration_seconds: f64) {
    }

    /// Track delta cluster coordination operations.
    fn mark_delta_cluster_coordination_op(&self, _backend: &str, _op: &str, _result: &str) {}

    /// Track delta cluster coordination failures.
    fn mark_delta_cluster_coordination_failure(&self, _backend: &str, _op: &str, _code: &str) {}

    /// Track delta cluster coordination latency.
    fn track_delta_cluster_coordination_latency(
        &self,
        _backend: &str,
        _op: &str,
        _latency_ms: f64,
    ) {
    }

    /// Track whether coordination chose a full or delta message.
    fn mark_delta_cluster_coordination_decision(&self, _backend: &str, _decision: &str) {}

    /// Update coordination backend health.
    fn update_delta_cluster_coordination_backend_up(&self, _backend: &str, _up: bool) {}

    /// Update queue depth for a horizontal transport driver.
    fn update_horizontal_transport_queue_depth(&self, _driver: &str, _depth: usize) {}

    /// Track dropped horizontal transport messages.
    fn mark_horizontal_transport_message_dropped(&self, _driver: &str) {}

    /// Track horizontal transport reconnects.
    fn mark_horizontal_transport_reconnection(&self, _driver: &str) {}

    /// Track a successful presence-history write.
    fn mark_presence_history_write(&self, _app_id: &str) {}

    /// Track presence-history write latency.
    fn track_presence_history_write_latency(&self, _app_id: &str, _latency_ms: f64) {}

    /// Track a presence-history write failure.
    fn mark_presence_history_write_failure(&self, _app_id: &str) {}

    /// Update retained presence-history counts for an app.
    fn update_presence_history_retained(&self, _app_id: &str, _events: u64, _bytes: u64) {}

    /// Track evictions performed by the presence-history store.
    fn mark_presence_history_eviction(&self, _app_id: &str, _events: u64, _bytes: u64) {}

    /// Update the current presence-history writer queue depth for an app.
    fn update_presence_history_queue_depth(&self, _app_id: &str, _depth: usize) {}

    /// Update the number of degraded presence-history channels for an app.
    fn update_presence_history_degraded_channels(&self, _app_id: &str, _count: usize) {}

    /// Update the number of reset-required presence-history channels for an app.
    fn update_presence_history_reset_required_channels(&self, _app_id: &str, _count: usize) {}

    /// Get the stored metrics as plain text, if possible
    async fn get_metrics_as_plaintext(&self) -> String;

    /// Get the stored metrics as JSON, if possible
    async fn get_metrics_as_json(&self) -> Value;

    /// Reset the metrics at the server level
    async fn clear(&self);
}
