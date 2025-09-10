use async_trait::async_trait;
use sockudo::adapter::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use sockudo::adapter::horizontal_transport::{
    HorizontalTransport, TransportConfig, TransportHandlers,
};
use sockudo::error::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Mock configuration for testing
#[derive(Clone)]
pub struct MockConfig {
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub simulate_node_count: usize,
    pub simulate_failures: bool,
    pub response_delay_ms: u64,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            prefix: "test".to_string(),
            request_timeout_ms: 1000,
            simulate_node_count: 2,
            simulate_failures: false,
            response_delay_ms: 0,
        }
    }
}

impl TransportConfig for MockConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Mock transport for testing HorizontalAdapterBase
pub struct MockTransport {
    config: MockConfig,
    handlers: Arc<Mutex<Option<TransportHandlers>>>,
    published_broadcasts: Arc<Mutex<Vec<BroadcastMessage>>>,
    published_requests: Arc<Mutex<Vec<RequestBody>>>,
    published_responses: Arc<Mutex<Vec<ResponseBody>>>,
    health_status: Arc<Mutex<bool>>,
}

impl Clone for MockTransport {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            handlers: self.handlers.clone(),
            published_broadcasts: self.published_broadcasts.clone(),
            published_requests: self.published_requests.clone(),
            published_responses: self.published_responses.clone(),
            health_status: self.health_status.clone(),
        }
    }
}

impl MockTransport {
    pub fn new(config: MockConfig) -> Self {
        Self {
            config,
            handlers: Arc::new(Mutex::new(None)),
            published_broadcasts: Arc::new(Mutex::new(Vec::new())),
            published_requests: Arc::new(Mutex::new(Vec::new())),
            published_responses: Arc::new(Mutex::new(Vec::new())),
            health_status: Arc::new(Mutex::new(true)),
        }
    }

    pub async fn get_published_broadcasts(&self) -> Vec<BroadcastMessage> {
        self.published_broadcasts.lock().await.clone()
    }

    pub async fn get_published_requests(&self) -> Vec<RequestBody> {
        self.published_requests.lock().await.clone()
    }

    pub async fn get_published_responses(&self) -> Vec<ResponseBody> {
        self.published_responses.lock().await.clone()
    }

    pub async fn clear_published_messages(&self) {
        self.published_broadcasts.lock().await.clear();
        self.published_requests.lock().await.clear();
        self.published_responses.lock().await.clear();
    }

    pub async fn set_health_status(&self, healthy: bool) {
        *self.health_status.lock().await = healthy;
    }

    pub async fn simulate_incoming_request(&self, request: RequestBody) -> Result<()> {
        let handlers = self.handlers.lock().await;
        if let Some(handlers) = handlers.as_ref() {
            // Simulate processing delay if configured
            if self.config.response_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.config.response_delay_ms)).await;
            }

            // Call the request handler
            match (handlers.on_request)(request.clone()).await {
                Ok(response) => {
                    // Auto-publish the response
                    (handlers.on_response)(response).await;
                }
                Err(e) if !self.config.simulate_failures => {
                    return Err(e);
                }
                _ => {
                    // Ignore errors if simulating failures
                }
            }
        }
        Ok(())
    }

    pub async fn simulate_incoming_response(&self, response: ResponseBody) {
        let handlers = self.handlers.lock().await;
        if let Some(handlers) = handlers.as_ref() {
            (handlers.on_response)(response).await;
        }
    }

    pub async fn simulate_incoming_broadcast(&self, broadcast: BroadcastMessage) {
        let handlers = self.handlers.lock().await;
        if let Some(handlers) = handlers.as_ref() {
            (handlers.on_broadcast)(broadcast).await;
        }
    }
}

#[async_trait]
impl HorizontalTransport for MockTransport {
    type Config = MockConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        if config.simulate_failures && config.prefix == "fail_on_new" {
            return Err(Error::Internal("Simulated connection failure".to_string()));
        }
        Ok(Self::new(config))
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        if self.config.simulate_failures && message.message.contains("simulate_error") {
            return Err(Error::Internal("Simulated broadcast failure".to_string()));
        }

        self.published_broadcasts.lock().await.push(message.clone());

        // If handlers are set, simulate receiving our own broadcast
        let handlers_guard = self.handlers.lock().await;
        if let Some(handlers) = handlers_guard.as_ref() {
            let on_broadcast = handlers.on_broadcast.clone();
            let message = message.clone();
            drop(handlers_guard); // Release the lock
            tokio::spawn(async move {
                // Simulate network delay
                if let Some(delay) = std::env::var("MOCK_NETWORK_DELAY_MS").ok() {
                    if let Ok(delay_ms) = delay.parse::<u64>() {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
                (on_broadcast)(message).await;
            });
        }

        Ok(())
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        if self.config.simulate_failures && request.app_id == "fail_request" {
            return Err(Error::Internal("Simulated request failure".to_string()));
        }

        self.published_requests.lock().await.push(request.clone());

        // Simulate responses from other nodes
        let handlers_guard = self.handlers.lock().await;
        if let Some(handlers) = handlers_guard.as_ref() {
            let on_response = handlers.on_response.clone();
            let request = request.clone();
            let node_count = self.config.simulate_node_count;
            let response_delay = self.config.response_delay_ms;
            let simulate_failures = self.config.simulate_failures;

            drop(handlers_guard); // Release the lock

            // Spawn responses from simulated nodes
            for node_idx in 1..node_count {
                let on_response = on_response.clone();
                let request = request.clone();
                tokio::spawn(async move {
                    // Simulate network/processing delay
                    if response_delay > 0 {
                        tokio::time::sleep(Duration::from_millis(response_delay)).await;
                    }

                    // Don't respond if simulating failures for this request
                    if simulate_failures && request.request_id.contains("no_response") {
                        return;
                    }

                    // Create a mock response
                    let response = ResponseBody {
                        request_id: request.request_id.clone(),
                        node_id: format!("mock-node-{}", node_idx),
                        app_id: request.app_id.clone(),
                        members: HashMap::new(),
                        socket_ids: vec![format!("socket-{}-{}", node_idx, 1)],
                        sockets_count: 1,
                        channels_with_sockets_count: {
                            let mut map = HashMap::new();
                            if let Some(channel) = &request.channel {
                                map.insert(channel.clone(), 1);
                            }
                            map
                        },
                        exists: true,
                        channels: {
                            let mut set = HashSet::new();
                            if let Some(channel) = &request.channel {
                                set.insert(channel.clone());
                            }
                            set
                        },
                        members_count: 1,
                    };

                    (on_response)(response).await;
                });
            }
        }

        Ok(())
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        if self.config.simulate_failures && response.app_id == "fail_response" {
            return Err(Error::Internal("Simulated response failure".to_string()));
        }

        self.published_responses.lock().await.push(response.clone());
        Ok(())
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        *self.handlers.lock().await = Some(handlers);
        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        Ok(self.config.simulate_node_count)
    }

    async fn check_health(&self) -> Result<()> {
        let healthy = *self.health_status.lock().await;
        if healthy {
            Ok(())
        } else {
            Err(Error::Connection("Mock transport unhealthy".to_string()))
        }
    }
}

/// Helper to create test request body
pub fn create_test_request(
    request_id: String,
    app_id: &str,
    request_type: sockudo::adapter::horizontal_adapter::RequestType,
) -> RequestBody {
    RequestBody {
        request_id,
        node_id: "test-node".to_string(),
        app_id: app_id.to_string(),
        request_type,
        channel: Some("test-channel".to_string()),
        socket_id: Some("test-socket".to_string()),
        user_id: Some("test-user".to_string()),
    }
}

/// Helper to create test broadcast message
pub fn create_test_broadcast(app_id: &str, channel: &str, message: &str) -> BroadcastMessage {
    BroadcastMessage {
        node_id: "test-node".to_string(),
        app_id: app_id.to_string(),
        channel: channel.to_string(),
        message: message.to_string(),
        except_socket_id: None,
        timestamp_ms: None,
    }
}

/// Helper to wait for a condition with timeout
pub async fn wait_for_condition<F, Fut>(condition: F, timeout_ms: u64) -> bool
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    let timeout = Duration::from_millis(timeout_ms);

    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}
