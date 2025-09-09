use sockudo::adapter::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use sockudo::adapter::transports::{NatsAdapterConfig, RedisAdapterConfig};
use sockudo::options::RedisClusterAdapterConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;
use uuid::Uuid;

/// Get Redis configuration for single instance testing (localhost:16379)
pub fn get_redis_config() -> RedisAdapterConfig {
    RedisAdapterConfig {
        url: "redis://127.0.0.1:16379/".to_string(),
        prefix: format!("test_{}", Uuid::new_v4().to_string().replace('-', "")),
        request_timeout_ms: 1000, // Reduced timeout
        cluster_mode: false,
    }
}

/// Get Redis Cluster configuration for testing (ports 17001-17003)
pub fn get_redis_cluster_config() -> RedisClusterAdapterConfig {
    RedisClusterAdapterConfig {
        nodes: vec![
            "redis://127.0.0.1:17001".to_string(),
            "redis://127.0.0.1:17002".to_string(),
            "redis://127.0.0.1:17003".to_string(),
        ],
        prefix: format!("test_{}", Uuid::new_v4().to_string().replace('-', "")),
        request_timeout_ms: 1000, // Reduced timeout
    }
}

/// Get NATS configuration for testing (ports 14222-14223)
pub fn get_nats_config() -> NatsAdapterConfig {
    NatsAdapterConfig {
        servers: "nats://127.0.0.1:14222,nats://127.0.0.1:14223".to_string(),
        prefix: format!("test_{}", Uuid::new_v4().to_string().replace('-', "")),
        request_timeout_ms: 1000, // Reduced timeout
        connection_timeout_ms: 1000, // Reduced timeout
        username: None,
        password: None,
        token: None,
    }
}

/// Create a test broadcast message
pub fn create_test_broadcast(event: &str) -> BroadcastMessage {
    BroadcastMessage {
        event: event.to_string(),
        channel: "test-channel".to_string(),
        data: serde_json::json!({"test": "data"}),
        socket: None,
        app_id: "test-app".to_string(),
        namespace: None,
    }
}

/// Create a test request message
pub fn create_test_request() -> RequestBody {
    RequestBody {
        request_id: Uuid::new_v4().to_string(),
        r#type: sockudo::adapter::horizontal_adapter::RequestType::FetchSockets,
        app_id: "test-app".to_string(),
        namespaces: None,
        rooms: None,
    }
}

/// Create a test response message
pub fn create_test_response(request_id: &str) -> ResponseBody {
    ResponseBody {
        request_id: request_id.to_string(),
        sockets: vec![],
    }
}

/// Message collector for testing listeners
#[derive(Clone)]
pub struct MessageCollector {
    broadcasts: Arc<Mutex<Vec<BroadcastMessage>>>,
    requests: Arc<Mutex<Vec<RequestBody>>>,
    responses: Arc<Mutex<Vec<ResponseBody>>>,
}

impl MessageCollector {
    pub fn new() -> Self {
        Self {
            broadcasts: Arc::new(Mutex::new(Vec::new())),
            requests: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn collect_broadcast(&self, msg: BroadcastMessage) {
        self.broadcasts.lock().await.push(msg);
    }

    pub async fn collect_request(&self, msg: RequestBody) {
        self.requests.lock().await.push(msg);
    }

    pub async fn collect_response(&self, msg: ResponseBody) {
        self.responses.lock().await.push(msg);
    }

    pub async fn get_broadcasts(&self) -> Vec<BroadcastMessage> {
        self.broadcasts.lock().await.clone()
    }

    pub async fn get_requests(&self) -> Vec<RequestBody> {
        self.requests.lock().await.clone()
    }

    pub async fn get_responses(&self) -> Vec<ResponseBody> {
        self.responses.lock().await.clone()
    }

    pub async fn wait_for_broadcast(&self, timeout_ms: u64) -> Option<BroadcastMessage> {
        let start = tokio::time::Instant::now();
        let timeout_duration = Duration::from_millis(timeout_ms);
        
        while start.elapsed() < timeout_duration {
            let broadcasts = self.broadcasts.lock().await;
            if !broadcasts.is_empty() {
                return Some(broadcasts[0].clone());
            }
            drop(broadcasts);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }

    pub async fn wait_for_request(&self, timeout_ms: u64) -> Option<RequestBody> {
        let start = tokio::time::Instant::now();
        let timeout_duration = Duration::from_millis(timeout_ms);
        
        while start.elapsed() < timeout_duration {
            let requests = self.requests.lock().await;
            if !requests.is_empty() {
                return Some(requests[0].clone());
            }
            drop(requests);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }

    pub async fn wait_for_response(&self, timeout_ms: u64) -> Option<ResponseBody> {
        let start = tokio::time::Instant::now();
        let timeout_duration = Duration::from_millis(timeout_ms);
        
        while start.elapsed() < timeout_duration {
            let responses = self.responses.lock().await;
            if !responses.is_empty() {
                return Some(responses[0].clone());
            }
            drop(responses);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }

    pub async fn clear(&self) {
        self.broadcasts.lock().await.clear();
        self.requests.lock().await.clear();
        self.responses.lock().await.clear();
    }
}

/// Wait for a condition with timeout
pub async fn wait_for_condition<F, Fut>(
    condition: F,
    timeout_ms: u64,
) -> bool
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let result = timeout(
        Duration::from_millis(timeout_ms),
        async {
            loop {
                if condition().await {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    ).await;
    
    result.unwrap_or(false)
}

/// Create transport handlers for testing
pub fn create_test_handlers(
    collector: MessageCollector,
) -> sockudo::adapter::horizontal_transport::TransportHandlers {
    use sockudo::adapter::horizontal_transport::BoxFuture;
    
    let broadcast_collector = collector.clone();
    let request_collector = collector.clone();
    let response_collector = collector.clone();

    sockudo::adapter::horizontal_transport::TransportHandlers {
        on_broadcast: Arc::new(move |msg| {
            let collector = broadcast_collector.clone();
            Box::pin(async move {
                collector.collect_broadcast(msg).await;
            }) as BoxFuture<'static, ()>
        }),
        on_request: Arc::new(move |msg| {
            let collector = request_collector.clone();
            Box::pin(async move {
                collector.collect_request(msg.clone()).await;
                // Return a dummy response for testing
                Ok(ResponseBody {
                    request_id: msg.request_id,
                    sockets: vec![],
                })
            }) as BoxFuture<'static, sockudo::error::Result<ResponseBody>>
        }),
        on_response: Arc::new(move |msg| {
            let collector = response_collector.clone();
            Box::pin(async move {
                collector.collect_response(msg).await;
            }) as BoxFuture<'static, ()>
        }),
    }
}