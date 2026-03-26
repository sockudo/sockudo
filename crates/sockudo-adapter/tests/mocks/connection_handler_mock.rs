use ahash::AHashMap;
use async_trait::async_trait;
use sockudo_adapter::connection_manager::{
    ChannelSocketCount, ConnectionManager, HorizontalAdapterInterface,
};
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::app::AppManager;
use sockudo_core::cache::CacheManager;
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::error::Result;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::namespace::Namespace;
use sockudo_core::options::ServerOptions;
use sockudo_core::websocket::{SocketId, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;
use sockudo_ws::axum_integration::WebSocketWriter;
use sonic_rs::Value;
use std::any::Any;
use std::sync::Arc;
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

#[async_trait]
impl ConnectionManager for MockAdapter {
    async fn init(&self) {}
    async fn get_namespace(&self, _app_id: &str) -> Option<Arc<Namespace>> {
        None
    }
    async fn add_socket(
        &self,
        _socket_id: SocketId,
        _socket: WebSocketWriter,
        _app_id: &str,
        _app_manager: Arc<dyn AppManager + Send + Sync>,
        _buffer_config: sockudo_core::websocket::WebSocketBufferConfig,
        _protocol_version: sockudo_protocol::ProtocolVersion,
    ) -> Result<()> {
        Ok(())
    }
    async fn get_connection(&self, _socket_id: &SocketId, _app_id: &str) -> Option<WebSocketRef> {
        None
    }
    async fn remove_connection(&self, _socket_id: &SocketId, _app_id: &str) -> Result<()> {
        Ok(())
    }
    async fn send_message(
        &self,
        _app_id: &str,
        _socket_id: &SocketId,
        _message: PusherMessage,
    ) -> Result<()> {
        Ok(())
    }
    async fn send(
        &self,
        _channel: &str,
        _message: PusherMessage,
        _except: Option<&SocketId>,
        _app_id: &str,
        _start_time_ms: Option<f64>,
    ) -> Result<()> {
        Ok(())
    }
    async fn get_channel_members(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> Result<AHashMap<String, PresenceMemberInfo>> {
        Ok(AHashMap::new())
    }
    async fn get_channel_sockets(&self, _app_id: &str, _channel: &str) -> Result<Vec<SocketId>> {
        Ok(Vec::new())
    }
    async fn remove_channel(&self, _app_id: &str, _channel: &str) {}
    async fn is_in_channel(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Result<bool> {
        Ok(false)
    }
    async fn get_user_sockets(&self, _user_id: &str, _app_id: &str) -> Result<Vec<WebSocketRef>> {
        Ok(Vec::new())
    }
    async fn cleanup_connection(&self, _app_id: &str, _ws: WebSocketRef) {}
    async fn terminate_connection(&self, _app_id: &str, _user_id: &str) -> Result<()> {
        Ok(())
    }
    async fn add_channel_to_sockets(&self, _app_id: &str, _channel: &str, _socket_id: &SocketId) {}
    async fn get_channel_socket_count_info(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> ChannelSocketCount {
        ChannelSocketCount {
            count: 0,
            complete: true,
        }
    }
    async fn get_channel_socket_count(&self, _app_id: &str, _channel: &str) -> usize {
        0
    }
    async fn add_to_channel(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Result<bool> {
        Ok(false)
    }
    async fn remove_from_channel(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Result<bool> {
        Ok(false)
    }
    async fn get_presence_member(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        None
    }
    async fn terminate_user_connections(&self, _app_id: &str, _user_id: &str) -> Result<()> {
        Ok(())
    }
    async fn add_user(&self, _ws: WebSocketRef) -> Result<()> {
        Ok(())
    }
    async fn remove_user(&self, _ws: WebSocketRef) -> Result<()> {
        Ok(())
    }
    async fn get_channels_with_socket_count(
        &self,
        _app_id: &str,
    ) -> Result<AHashMap<String, usize>> {
        Ok(AHashMap::new())
    }
    async fn get_sockets_count(&self, _app_id: &str) -> Result<usize> {
        Ok(0)
    }
    async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>> {
        Ok(Vec::new())
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    async fn remove_user_socket(
        &self,
        _user_id: &str,
        _socket_id: &SocketId,
        _app_id: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn count_user_connections_in_channel(
        &self,
        _user_id: &str,
        _app_id: &str,
        _channel: &str,
        _excluding_socket: Option<&SocketId>,
    ) -> Result<usize> {
        Ok(0)
    }

    async fn check_health(&self) -> Result<()> {
        // Mock adapter is always healthy for testing
        Ok(())
    }

    fn get_node_id(&self) -> String {
        "mock-node".to_string()
    }

    fn as_horizontal_adapter(&self) -> Option<&dyn HorizontalAdapterInterface> {
        None
    }
}

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

pub struct MockMetricsInterface;
impl Default for MockMetricsInterface {
    fn default() -> Self {
        Self::new()
    }
}

impl MockMetricsInterface {
    pub fn new() -> Self {
        Self
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
    fn update_active_channels(&self, _app_id: &str, _channel_type: &str, _count: i64) {}
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
    fn track_horizontal_adapter_resolved_promises(&self, _app_id: &str, _resolved: bool) {}
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
    let delta_manager = Arc::new(sockudo_delta::DeltaCompressionManager::new(
        sockudo_delta::DeltaCompressionConfig::default(),
    ));

    let handler = ConnectionHandler::builder(
        Arc::new(app_manager.clone()) as Arc<dyn AppManager + Send + Sync>,
        Arc::new(MockAdapter::new()) as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        ServerOptions::default(),
    )
    .metrics(Arc::new(MockMetricsInterface::new()))
    .delta_compression(delta_manager)
    .build();

    (handler, app_manager)
}

pub fn create_test_connection_handler_with_app_manager(
    app_manager: MockAppManager,
) -> ConnectionHandler {
    let delta_manager = Arc::new(sockudo_delta::DeltaCompressionManager::new(
        sockudo_delta::DeltaCompressionConfig::default(),
    ));

    ConnectionHandler::builder(
        Arc::new(app_manager.clone()) as Arc<dyn AppManager + Send + Sync>,
        Arc::new(MockAdapter::new()) as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        ServerOptions::default(),
    )
    .metrics(Arc::new(MockMetricsInterface::new()))
    .delta_compression(delta_manager)
    .build()
}
