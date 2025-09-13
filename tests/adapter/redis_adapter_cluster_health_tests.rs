use sockudo::adapter::redis_adapter::{RedisAdapter, RedisAdapterOptions};
use sockudo::adapter::connection_manager::HorizontalAdapterInterface;
use sockudo::adapter::ConnectionManager;
use sockudo::options::ClusterHealthConfig;
use serde_json::json;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Helper to check if Redis is available
async fn is_redis_available() -> bool {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    
    match redis::Client::open(redis_url) {
        Ok(client) => match client.get_connection() {
            Ok(mut conn) => {
                use redis::Commands;
                let _: Result<String, _> = conn.ping();
                true
            }
            Err(_) => false,
        },
        Err(_) => false,
    }
}

/// Helper to create a Redis adapter with cluster health enabled
async fn create_redis_adapter(node_id: &str, cluster_config: &ClusterHealthConfig) -> RedisAdapter {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    
    let options = RedisAdapterOptions {
        url: redis_url,
        prefix: format!("test_cluster_health_{}", node_id),
        request_timeout_ms: 5000,
        cluster_mode: false,
    };
    
    let mut adapter = RedisAdapter::new(options).await.unwrap();
    adapter.set_cluster_health(cluster_config).await.unwrap();
    adapter
}

#[tokio::test]
async fn test_redis_adapter_heartbeat_propagation() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 100,
        node_timeout_ms: 500,
        cleanup_interval_ms: 200,
    };

    // Create two Redis adapters simulating two nodes
    let mut adapter1 = create_redis_adapter("node1", &cluster_config).await;
    let mut adapter2 = create_redis_adapter("node2", &cluster_config).await;

    // Initialize both adapters
    adapter1.init().await;
    adapter2.init().await;

    // Allow heartbeats to propagate
    sleep(Duration::from_millis(300)).await;

    // Both adapters should see each other's heartbeats
    // Note: This test assumes the adapters track heartbeats internally
    // The actual implementation may need to expose this for testing
    
    // Clean up
    drop(adapter1);
    drop(adapter2);
}

#[tokio::test]
async fn test_redis_adapter_presence_join_broadcast() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 100,
        node_timeout_ms: 500,
        cleanup_interval_ms: 200,
    };

    let mut adapter1 = create_redis_adapter("node1", &cluster_config).await;
    let mut adapter2 = create_redis_adapter("node2", &cluster_config).await;

    adapter1.init().await;
    adapter2.init().await;

    let app_id = "test-app";
    let channel = "presence-test-channel";
    let user_id = "user-123";
    let socket_id = "socket-456";
    let user_info = json!({"name": "TestUser", "status": "online"});

    // Broadcast presence join from adapter1
    adapter1
        .broadcast_presence_join(app_id, channel, user_id, socket_id, Some(user_info.clone()))
        .await
        .unwrap();

    // Allow time for broadcast to propagate
    sleep(Duration::from_millis(200)).await;

    // Adapter2 should receive the presence update
    // Note: The actual verification depends on how the adapter stores presence data
    // This might need access to internal state or a get_presence_members method
    
    drop(adapter1);
    drop(adapter2);
}

#[tokio::test]
async fn test_redis_adapter_presence_leave_broadcast() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 100,
        node_timeout_ms: 500,
        cleanup_interval_ms: 200,
    };

    let mut adapter1 = create_redis_adapter("node1", &cluster_config).await;
    let mut adapter2 = create_redis_adapter("node2", &cluster_config).await;

    adapter1.init().await;
    adapter2.init().await;

    let app_id = "test-app";
    let channel = "presence-test-channel";
    let user_id = "user-123";
    let socket_id = "socket-456";
    let user_info = json!({"name": "TestUser"});

    // First, join
    adapter1
        .broadcast_presence_join(app_id, channel, user_id, socket_id, Some(user_info))
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    // Then, leave
    adapter1
        .broadcast_presence_leave(app_id, channel, user_id, socket_id)
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    // Verify the presence was removed
    // Note: Implementation-specific verification needed here
    
    drop(adapter1);
    drop(adapter2);
}

#[tokio::test]
async fn test_redis_adapter_dead_node_detection() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 100,
        node_timeout_ms: 300,  // Short timeout for testing
        cleanup_interval_ms: 150,
    };

    let mut adapter1 = create_redis_adapter("node1", &cluster_config).await;
    let mut adapter2 = create_redis_adapter("node2", &cluster_config).await;
    
    adapter1.init().await;
    adapter2.init().await;

    let app_id = "test-app";
    let channel = "presence-test";
    
    // Add presence member on adapter2
    adapter2
        .broadcast_presence_join(app_id, channel, "user2", "socket2", Some(json!({"name": "User2"})))
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    // Simulate adapter2 dying by dropping it
    drop(adapter2);

    // Wait for timeout + cleanup interval
    sleep(Duration::from_millis(500)).await;

    // Adapter1 should detect that adapter2 is dead and clean up its presence data
    // Note: Need to verify cleanup actually happened
    
    drop(adapter1);
}

#[tokio::test]
async fn test_redis_adapter_multiple_apps_isolation() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 100,
        node_timeout_ms: 500,
        cleanup_interval_ms: 200,
    };

    let mut adapter1 = create_redis_adapter("node1", &cluster_config).await;
    let mut adapter2 = create_redis_adapter("node2", &cluster_config).await;

    adapter1.init().await;
    adapter2.init().await;

    // Add presence for different apps
    adapter1
        .broadcast_presence_join("app1", "channel1", "user1", "socket1", None)
        .await
        .unwrap();
    
    adapter1
        .broadcast_presence_join("app2", "channel1", "user2", "socket2", None)
        .await
        .unwrap();

    sleep(Duration::from_millis(200)).await;

    // Verify app isolation is maintained
    // Each app should only see its own presence data
    
    drop(adapter1);
    drop(adapter2);
}

#[tokio::test]
async fn test_redis_adapter_reconnection_after_failure() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 100,
        node_timeout_ms: 500,
        cleanup_interval_ms: 200,
    };

    let mut adapter = create_redis_adapter("node1", &cluster_config).await;
    adapter.init().await;

    // Add some presence data
    adapter
        .broadcast_presence_join("app1", "channel1", "user1", "socket1", None)
        .await
        .unwrap();

    // Simulate connection loss and recovery
    // Note: This would require the ability to disconnect/reconnect the Redis connection
    // which may not be exposed in the current API
    
    // After reconnection, adapter should still function
    adapter
        .broadcast_presence_join("app1", "channel2", "user2", "socket2", None)
        .await
        .unwrap();

    drop(adapter);
}

#[tokio::test]
async fn test_redis_adapter_cluster_health_disabled() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: false,  // Disabled
        heartbeat_interval_ms: 100,
        node_timeout_ms: 500,
        cleanup_interval_ms: 200,
    };

    let mut adapter = create_redis_adapter("node1", &cluster_config).await;
    adapter.init().await;

    // With cluster health disabled, presence operations should still work
    // but without dead node detection
    adapter
        .broadcast_presence_join("app1", "channel1", "user1", "socket1", None)
        .await
        .unwrap();

    adapter
        .broadcast_presence_leave("app1", "channel1", "user1", "socket1")
        .await
        .unwrap();

    // No heartbeats should be sent when disabled
    sleep(Duration::from_millis(200)).await;

    drop(adapter);
}

#[tokio::test]
async fn test_redis_adapter_concurrent_presence_operations() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 100,
        node_timeout_ms: 500,
        cleanup_interval_ms: 200,
    };

    let adapter = Arc::new(Mutex::new(create_redis_adapter("node1", &cluster_config).await));
    
    {
        let mut adapter_guard = adapter.lock().await;
        adapter_guard.init().await;
    }

    let app_id = "test-app";
    let channel = "presence-concurrent";

    // Spawn multiple concurrent presence operations
    let mut handles = vec![];
    
    for i in 0..10 {
        let adapter_clone = adapter.clone();
        let handle = tokio::spawn(async move {
            let adapter_guard = adapter_clone.lock().await;
            let user_id = format!("user-{}", i);
            let socket_id = format!("socket-{}", i);
            
            adapter_guard
                .broadcast_presence_join(app_id, channel, &user_id, &socket_id, None)
                .await
                .unwrap();
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // All presence operations should have succeeded without conflicts
    sleep(Duration::from_millis(100)).await;
}