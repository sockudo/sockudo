use async_trait::async_trait;
use sockudo_types::socket::SocketId;
use sonic_rs::Value;

/// Metrics abstraction shared across adapters.
#[async_trait]
pub trait MetricsInterface: Send + Sync {
    async fn init(&self) -> crate::error::Result<()>;
    fn mark_new_connection(&self, app_id: &str, socket_id: &SocketId);
    fn mark_disconnection(&self, app_id: &str, socket_id: &SocketId);
    fn mark_connection_error(&self, app_id: &str, error_type: &str);
    fn mark_rate_limit_check(&self, app_id: &str, limiter_type: &str);
    fn mark_rate_limit_check_with_context(
        &self,
        app_id: &str,
        limiter_type: &str,
        request_context: &str,
    );
    fn mark_rate_limit_triggered(&self, app_id: &str, limiter_type: &str);
    fn mark_rate_limit_triggered_with_context(
        &self,
        app_id: &str,
        limiter_type: &str,
        request_context: &str,
    );
    fn mark_channel_subscription(&self, app_id: &str, channel_type: &str);
    fn mark_channel_unsubscription(&self, app_id: &str, channel_type: &str);
    fn update_active_channels(&self, app_id: &str, channel_type: &str, count: i64);
    fn mark_api_message(
        &self,
        app_id: &str,
        incoming_message_size: usize,
        sent_message_size: usize,
    );
    fn mark_ws_message_sent(&self, app_id: &str, sent_message_size: usize);
    fn mark_ws_messages_sent_batch(&self, app_id: &str, sent_message_size: usize, count: usize);
    fn mark_ws_message_received(&self, app_id: &str, message_size: usize);
    fn track_horizontal_adapter_resolve_time(&self, app_id: &str, time_ms: f64);
    fn track_horizontal_adapter_resolved_promises(&self, app_id: &str, resolved: bool);
    fn mark_horizontal_adapter_request_sent(&self, app_id: &str);
    fn mark_horizontal_adapter_request_received(&self, app_id: &str);
    fn mark_horizontal_adapter_response_received(&self, app_id: &str);
    fn track_broadcast_latency(
        &self,
        app_id: &str,
        channel_name: &str,
        recipient_count: usize,
        latency_ms: f64,
    );
    fn track_horizontal_delta_compression(&self, app_id: &str, channel_name: &str, enabled: bool);
    fn track_delta_compression_bandwidth(
        &self,
        app_id: &str,
        channel_name: &str,
        original_bytes: usize,
        compressed_bytes: usize,
    );
    fn track_delta_compression_full_message(&self, app_id: &str, channel_name: &str);
    fn track_delta_compression_delta_message(&self, app_id: &str, channel_name: &str);
    async fn get_metrics_as_plaintext(&self) -> String;
    async fn get_metrics_as_json(&self) -> Value;
    async fn clear(&self);
}
