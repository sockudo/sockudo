use async_trait::async_trait;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::delegate_connection_manager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::test_support::NoopConnectionManager;
use sockudo_core::app::App;
use sockudo_core::app::AppManager;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::Result;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::ServerOptions;
use sockudo_core::websocket::SocketId;
use sonic_rs::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub struct MockAdapter;

impl Default for MockAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl NoopConnectionManager for MockAdapter {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
delegate_connection_manager!(MockAdapter);

#[derive(Clone)]
pub struct MockAppManager {
    expected_key: Option<String>,
    expected_id: Option<String>,
    app_to_return: Option<App>,
}

impl Default for MockAppManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAppManager {
    pub fn new() -> Self {
        Self {
            expected_key: None,
            expected_id: None,
            app_to_return: None,
        }
    }

    pub fn expect_find_by_key(&mut self, key: String, app: App) {
        self.expected_key = Some(key);
        self.app_to_return = Some(app);
    }

    #[allow(dead_code)]
    pub fn expect_find_by_id(&mut self, id: String, app: App) {
        self.expected_id = Some(id);
        self.app_to_return = Some(app);
    }
}

#[async_trait]
impl AppManager for MockAppManager {
    async fn init(&self) -> Result<()> {
        Ok(())
    }
    async fn create_app(&self, _app: App) -> Result<()> {
        Ok(())
    }
    async fn update_app(&self, _app: App) -> Result<()> {
        Ok(())
    }
    async fn delete_app(&self, _app_id: &str) -> Result<()> {
        Ok(())
    }
    async fn get_apps(&self) -> Result<Vec<App>> {
        Ok(Vec::new())
    }
    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        if let Some(expected_key) = &self.expected_key {
            assert_eq!(key, expected_key, "Unexpected app key in find_by_key");
        }
        Ok(self.app_to_return.clone())
    }
    async fn find_by_id(&self, id: &str) -> Result<Option<App>> {
        if let Some(expected_id) = &self.expected_id {
            assert_eq!(id, expected_id, "Unexpected app id in find_by_id");
        }
        Ok(self.app_to_return.clone())
    }
    async fn check_health(&self) -> Result<()> {
        Ok(())
    }
}

pub struct MockCacheManager;
impl Default for MockCacheManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MockCacheManager {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl CacheManager for MockCacheManager {
    async fn has(&self, _key: &str) -> Result<bool> {
        Ok(false)
    }
    async fn get(&self, _key: &str) -> Result<Option<String>> {
        Ok(None)
    }
    async fn set(&self, _key: &str, _value: &str, _ttl_seconds: u64) -> Result<()> {
        Ok(())
    }
    async fn remove(&self, _key: &str) -> Result<()> {
        Ok(())
    }
    async fn disconnect(&self) -> Result<()> {
        Ok(())
    }
    async fn ttl(&self, _key: &str) -> Result<Option<Duration>> {
        Ok(None)
    }
    async fn check_health(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct MockMetricsInterface {
    annotation_projection_rebuilds: Arc<AtomicUsize>,
    annotation_projection_rebuild_durations: Arc<AtomicUsize>,
    horizontal_resolved_promises: Arc<AtomicUsize>,
    horizontal_uncomplete_promises: Arc<AtomicUsize>,
}
impl Default for MockMetricsInterface {
    fn default() -> Self {
        Self::new()
    }
}

impl MockMetricsInterface {
    pub fn new() -> Self {
        Self {
            annotation_projection_rebuilds: Arc::new(AtomicUsize::new(0)),
            annotation_projection_rebuild_durations: Arc::new(AtomicUsize::new(0)),
            horizontal_resolved_promises: Arc::new(AtomicUsize::new(0)),
            horizontal_uncomplete_promises: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn annotation_projection_rebuilds(&self) -> usize {
        self.annotation_projection_rebuilds.load(Ordering::Relaxed)
    }

    pub fn annotation_projection_rebuild_durations(&self) -> usize {
        self.annotation_projection_rebuild_durations
            .load(Ordering::Relaxed)
    }

    pub fn horizontal_resolved_promises(&self) -> usize {
        self.horizontal_resolved_promises.load(Ordering::Relaxed)
    }

    pub fn horizontal_uncomplete_promises(&self) -> usize {
        self.horizontal_uncomplete_promises.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl MetricsInterface for MockMetricsInterface {
    async fn init(&self) -> Result<()> {
        Ok(())
    }
    fn mark_new_connection(&self, _app_id: &str, _socket_id: &SocketId) {}
    fn mark_disconnection(&self, _app_id: &str, _socket_id: &SocketId) {}
    fn mark_connection_error(&self, _app_id: &str, _error_type: &str) {}
    fn mark_rate_limit_check(&self, _app_id: &str, _limiter_type: &str) {}
    fn mark_rate_limit_check_with_context(
        &self,
        _app_id: &str,
        _limiter_type: &str,
        _request_context: &str,
    ) {
    }
    fn mark_rate_limit_triggered(&self, _app_id: &str, _limiter_type: &str) {}
    fn mark_rate_limit_triggered_with_context(
        &self,
        _app_id: &str,
        _limiter_type: &str,
        _request_context: &str,
    ) {
    }
    fn mark_channel_subscription(&self, _app_id: &str, _channel_type: &str) {}
    fn mark_channel_unsubscription(&self, _app_id: &str, _channel_type: &str) {}

    fn mark_api_message(
        &self,
        _app_id: &str,
        _incoming_message_size: usize,
        _sent_message_size: usize,
    ) {
    }
    fn mark_ws_message_sent(&self, _app_id: &str, _sent_message_size: usize) {}
    fn mark_ws_messages_sent_batch(&self, _app_id: &str, _sent_message_size: usize, _count: usize) {
    }
    fn mark_ws_message_received(&self, _app_id: &str, _message_size: usize) {}
    fn track_horizontal_adapter_resolve_time(&self, _app_id: &str, _time_ms: f64) {}
    fn track_horizontal_adapter_resolved_promises(
        &self,
        _app_id: &str,
        resolved: bool,
        _request_type: &str,
    ) {
        if resolved {
            self.horizontal_resolved_promises
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.horizontal_uncomplete_promises
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    fn mark_horizontal_adapter_request_sent(&self, _app_id: &str) {}
    fn mark_horizontal_adapter_request_received(&self, _app_id: &str) {}
    fn mark_horizontal_adapter_response_received(&self, _app_id: &str) {}
    fn track_broadcast_latency(
        &self,
        _app_id: &str,
        _channel_name: &str,
        _recipient_count: usize,
        _latency_ms: f64,
    ) {
    }
    fn track_horizontal_delta_compression(
        &self,
        _app_id: &str,
        _channel_name: &str,
        _success: bool,
    ) {
    }
    fn track_delta_compression_bandwidth(
        &self,
        _app_id: &str,
        _channel_name: &str,
        _original_size: usize,
        _compressed_size: usize,
    ) {
    }
    fn track_delta_compression_full_message(&self, _app_id: &str, _channel_name: &str) {}
    fn track_delta_compression_delta_message(&self, _app_id: &str, _channel_name: &str) {}
    fn mark_annotation_projection_rebuild(&self, _channel: &str) {
        self.annotation_projection_rebuilds
            .fetch_add(1, Ordering::Relaxed);
    }
    fn track_annotation_projection_rebuild_duration(&self, _channel: &str, _duration_seconds: f64) {
        self.annotation_projection_rebuild_durations
            .fetch_add(1, Ordering::Relaxed);
    }
    async fn get_metrics_as_plaintext(&self) -> String {
        String::new()
    }
    async fn get_metrics_as_json(&self) -> Value {
        sonic_rs::json!({})
    }
    async fn clear(&self) {}
}

// Helper function to create a test ConnectionHandler with configurable mocks
#[allow(dead_code)]
pub fn create_test_connection_handler() -> (ConnectionHandler, MockAppManager) {
    let app_manager = MockAppManager::new();
    let builder = ConnectionHandler::builder(
        Arc::new(app_manager.clone()) as Arc<dyn AppManager + Send + Sync>,
        Arc::new(MockAdapter::new()) as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        ServerOptions::default(),
    )
    .metrics(Arc::new(MockMetricsInterface::new()));
    #[cfg(feature = "delta")]
    let builder = builder.delta_compression(Arc::new(sockudo_delta::DeltaCompressionManager::new(
        sockudo_delta::DeltaCompressionConfig::default(),
    )));
    let handler = builder.build();

    (handler, app_manager)
}

pub fn create_test_connection_handler_with_app_manager(
    app_manager: MockAppManager,
) -> ConnectionHandler {
    let builder = ConnectionHandler::builder(
        Arc::new(app_manager) as Arc<dyn AppManager + Send + Sync>,
        Arc::new(MockAdapter::new()) as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        ServerOptions::default(),
    )
    .metrics(Arc::new(MockMetricsInterface::new()));
    #[cfg(feature = "delta")]
    let builder = builder.delta_compression(Arc::new(sockudo_delta::DeltaCompressionManager::new(
        sockudo_delta::DeltaCompressionConfig::default(),
    )));
    builder.build()
}
