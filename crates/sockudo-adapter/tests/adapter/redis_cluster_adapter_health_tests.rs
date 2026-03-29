#![cfg(feature = "redis-cluster")]

use sockudo_adapter::ConnectionManager;
use sockudo_adapter::connection_manager::HorizontalAdapterInterface;
use sockudo_adapter::redis_cluster_adapter::RedisClusterAdapter;
use sockudo_core::options::{ClusterHealthConfig, RedisClusterAdapterConfig};
use sonic_rs::json;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use uuid::Uuid;

/// Helper to check if Redis Cluster is available
async fn is_redis_cluster_available() -> bool {
    let nodes = env::var("REDIS_CLUSTER_NODES").unwrap_or_else(|_| {
        "redis://127.0.0.1:7001,redis://127.0.0.1:7002,redis://127.0.0.1:7003".to_string()
    });

    let node_list: Vec<String> = nodes.split(',').map(|s| s.to_string()).collect();

    if node_list.is_empty() {
        return false;
    }

    // Try to connect to first node
    match redis::Client::open(node_list[0].as_str()) {
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

/// Helper to create a RedisCluster adapter with cluster health enabled
async fn create_redis_cluster_adapter(
    test_prefix: &str,
    node_id: &str,
    cluster_config: &ClusterHealthConfig,
) -> RedisClusterAdapter {
    let nodes = env::var("REDIS_CLUSTER_NODES").unwrap_or_else(|_| {
        "redis://127.0.0.1:7001,redis://127.0.0.1:7002,redis://127.0.0.1:7003".to_string()
    });

    let node_list: Vec<String> = nodes.split(',').map(|s| s.to_string()).collect();

    let config = RedisClusterAdapterConfig {
        nodes: node_list,
        prefix: format!("{test_prefix}_{node_id}"),
        request_timeout_ms: 5000,
        use_connection_manager: true,
        use_sharded_pubsub: false,
    };

    let mut adapter = RedisClusterAdapter::new(config).await.unwrap();
    adapter.set_cluster_health(cluster_config).await.unwrap();
    adapter
}

fn unique_test_prefix(test_name: &str) -> String {
    format!("{}_{}", test_name, Uuid::new_v4().simple())
}

fn fast_cluster_health_config() -> ClusterHealthConfig {
    ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 40,
        node_timeout_ms: 180,
        cleanup_interval_ms: 60,
    }
}

async fn wait_until<F, Fut>(timeout: Duration, poll_interval: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        sleep(poll_interval).await;
    }
}

#[tokio::test]
async fn test_redis_cluster_adapter_heartbeat() {
    if !is_redis_cluster_available().await {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let cluster_config = fast_cluster_health_config();
    let test_prefix = unique_test_prefix("cluster_heartbeat");

    // Create two adapters simulating two Sockudo nodes
    let adapter1 =
        create_redis_cluster_adapter(&test_prefix, "cluster_node1", &cluster_config).await;
    let adapter2 =
        create_redis_cluster_adapter(&test_prefix, "cluster_node2", &cluster_config).await;

    adapter1.init().await;
    adapter2.init().await;

    assert!(
        wait_until(
            Duration::from_millis(500),
            Duration::from_millis(20),
            || {
                let adapter1 = &adapter1;
                let adapter2 = &adapter2;
                async move {
                    adapter1.horizontal.get_effective_node_count().await >= 2
                        && adapter2.horizontal.get_effective_node_count().await >= 2
                }
            }
        )
        .await,
        "Cluster heartbeats did not establish within the expected test window"
    );

    drop(adapter1);
    drop(adapter2);
}

#[tokio::test]
async fn test_redis_cluster_presence_synchronization() {
    if !is_redis_cluster_available().await {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let cluster_config = fast_cluster_health_config();
    let test_prefix = unique_test_prefix("cluster_presence_sync");

    let adapter1 =
        create_redis_cluster_adapter(&test_prefix, "cluster_node1", &cluster_config).await;
    let adapter2 =
        create_redis_cluster_adapter(&test_prefix, "cluster_node2", &cluster_config).await;
    let adapter3 =
        create_redis_cluster_adapter(&test_prefix, "cluster_node3", &cluster_config).await;

    adapter1.init().await;
    adapter2.init().await;
    adapter3.init().await;

    let app_id = "test-app";
    let channel = "presence-cluster-test";

    // Add presence members from different nodes
    adapter1
        .broadcast_presence_join(
            app_id,
            channel,
            "user1",
            "socket1",
            Some(json!({"node": 1})),
        )
        .await
        .unwrap();

    adapter2
        .broadcast_presence_join(
            app_id,
            channel,
            "user2",
            "socket2",
            Some(json!({"node": 2})),
        )
        .await
        .unwrap();

    adapter3
        .broadcast_presence_join(
            app_id,
            channel,
            "user3",
            "socket3",
            Some(json!({"node": 3})),
        )
        .await
        .unwrap();

    // Verify that all adapters see each other's presence entries
    let node1_id = adapter1.node_id.clone();
    let node2_id = adapter2.node_id.clone();
    let node3_id = adapter3.node_id.clone();

    assert!(
        wait_until(
            Duration::from_millis(700),
            Duration::from_millis(20),
            || {
                let adapter1 = &adapter1;
                let node1_id = node1_id.clone();
                let node2_id = node2_id.clone();
                let node3_id = node3_id.clone();
                async move {
                    let registry = adapter1.get_cluster_presence_registry().await;
                    registry
                        .get(&node1_id)
                        .and_then(|n| n.get(channel))
                        .map(|c| c.contains_key("socket1"))
                        .unwrap_or(false)
                        && registry
                            .get(&node2_id)
                            .and_then(|n| n.get(channel))
                            .map(|c| c.contains_key("socket2"))
                            .unwrap_or(false)
                        && registry
                            .get(&node3_id)
                            .and_then(|n| n.get(channel))
                            .map(|c| c.contains_key("socket3"))
                            .unwrap_or(false)
                }
            }
        )
        .await,
        "Cluster presence synchronization did not converge within the expected test window"
    );

    let registry = adapter1.get_cluster_presence_registry().await;
    assert!(
        registry
            .get(&node1_id)
            .and_then(|n| n.get(channel))
            .map(|c| c.contains_key("socket1"))
            .unwrap_or(false),
        "Adapter1 should have its own presence entry"
    );
    assert!(
        registry
            .get(&node2_id)
            .and_then(|n| n.get(channel))
            .map(|c| c.contains_key("socket2"))
            .unwrap_or(false),
        "Adapter1 should have node2's presence entry"
    );
    assert!(
        registry
            .get(&node3_id)
            .and_then(|n| n.get(channel))
            .map(|c| c.contains_key("socket3"))
            .unwrap_or(false),
        "Adapter1 should have node3's presence entry"
    );

    // Verify cluster registry has all nodes
    assert_eq!(registry.len(), 3, "Registry should track all 3 nodes");

    drop(adapter1);
    drop(adapter2);
    drop(adapter3);
}

#[tokio::test]
async fn test_redis_cluster_dead_node_cleanup() {
    if !is_redis_cluster_available().await {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 40,
        node_timeout_ms: 140,
        cleanup_interval_ms: 40,
    };
    let test_prefix = unique_test_prefix("cluster_dead_node_cleanup");

    let adapter1 =
        create_redis_cluster_adapter(&test_prefix, "cluster_node1", &cluster_config).await;
    let adapter2 =
        create_redis_cluster_adapter(&test_prefix, "cluster_node2", &cluster_config).await;

    adapter1.init().await;
    adapter2.init().await;

    let app_id = "test-app";
    let channel = "presence-cleanup-test";

    // Add presence on adapter2 (which will "die")
    adapter2
        .broadcast_presence_join(
            app_id,
            channel,
            "user_to_cleanup",
            "socket_cleanup",
            Some(json!({"status": "online"})),
        )
        .await
        .unwrap();

    assert!(
        wait_until(
            Duration::from_millis(500),
            Duration::from_millis(20),
            || {
                let adapter1 = &adapter1;
                let adapter2_node_id = adapter2.node_id.clone();
                async move {
                    adapter1
                        .get_cluster_presence_registry()
                        .await
                        .get(&adapter2_node_id)
                        .and_then(|n| n.get(channel))
                        .map(|c| c.contains_key("socket_cleanup"))
                        .unwrap_or(false)
                }
            }
        )
        .await,
        "Dead-node test setup did not replicate presence before node shutdown"
    );

    // Simulate adapter2 dying
    let dead_node_id = adapter2.node_id.clone();
    drop(adapter2);

    assert!(
        wait_until(
            Duration::from_millis(700),
            Duration::from_millis(20),
            || {
                let adapter1 = &adapter1;
                let dead_node_id = dead_node_id.clone();
                async move {
                    !adapter1
                        .get_cluster_presence_registry()
                        .await
                        .contains_key(&dead_node_id)
                }
            }
        )
        .await,
        "Dead-node cleanup did not remove registry data within the expected test window"
    );

    drop(adapter1);
}

#[tokio::test]
async fn test_redis_cluster_sharding_consistency() {
    if !is_redis_cluster_available().await {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let cluster_config = fast_cluster_health_config();
    let test_prefix = unique_test_prefix("cluster_sharding");
    let adapter =
        create_redis_cluster_adapter(&test_prefix, "cluster_node1", &cluster_config).await;
    adapter.init().await;

    // Test presence across different shards (different channel names will hash to different slots)
    let channels = [
        "presence-shard-{1}",
        "presence-shard-{2}",
        "presence-shard-{3}",
        "presence-shard-{4}",
        "presence-shard-{5}",
    ];

    for (i, channel) in channels.iter().enumerate() {
        let user_id = format!("user-{}", i);
        let socket_id = format!("socket-{}", i);

        adapter
            .broadcast_presence_join("test-app", channel, &user_id, &socket_id, None)
            .await
            .unwrap();
    }

    sleep(Duration::from_millis(60)).await;

    // All presence data should be correctly stored across shards

    drop(adapter);
}

#[tokio::test]
async fn test_redis_cluster_failover_handling() {
    if !is_redis_cluster_available().await {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let cluster_config = fast_cluster_health_config();
    let test_prefix = unique_test_prefix("cluster_failover");
    let adapter =
        create_redis_cluster_adapter(&test_prefix, "cluster_node1", &cluster_config).await;
    adapter.init().await;

    // Add presence data
    adapter
        .broadcast_presence_join(
            "app1",
            "channel1",
            "user1",
            "socket1",
            Some(json!({"test": true})),
        )
        .await
        .unwrap();

    // In a real scenario, we would simulate a Redis node failover here
    // The adapter should handle reconnection and continue working

    sleep(Duration::from_millis(40)).await;

    // Try to add more presence data after potential failover
    adapter
        .broadcast_presence_join("app1", "channel2", "user2", "socket2", None)
        .await
        .unwrap();

    drop(adapter);
}

#[tokio::test]
async fn test_redis_cluster_concurrent_multi_node_operations() {
    if !is_redis_cluster_available().await {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let cluster_config = fast_cluster_health_config();
    let test_prefix = unique_test_prefix("cluster_concurrent");

    // Create multiple adapters
    let adapters = Arc::new(Mutex::new(Vec::new()));

    for i in 0..3 {
        let adapter = create_redis_cluster_adapter(
            &test_prefix,
            &format!("cluster_node_{}", i),
            &cluster_config,
        )
        .await;
        adapter.init().await;
        adapters.lock().await.push(adapter);
    }

    let app_id = "test-app";
    let channel = "presence-concurrent";

    // Spawn concurrent operations from different adapters
    let mut handles = vec![];

    for i in 0..9 {
        let adapters_clone = adapters.clone();
        let handle = tokio::spawn(async move {
            let mut adapters_guard = adapters_clone.lock().await;
            let adapter_index = i % 3; // Round-robin across adapters
            let adapter = &mut adapters_guard[adapter_index];

            let user_id = format!("user-{}", i);
            let socket_id = format!("socket-{}", i);

            adapter
                .broadcast_presence_join(
                    app_id,
                    channel,
                    &user_id,
                    &socket_id,
                    Some(json!({"concurrent": true})),
                )
                .await
                .unwrap();
        });
        handles.push(handle);
    }

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    sleep(Duration::from_millis(80)).await;

    // All operations should have succeeded without conflicts
}

#[tokio::test]
async fn test_redis_cluster_large_presence_data() {
    if !is_redis_cluster_available().await {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: true,
        heartbeat_interval_ms: 60,
        node_timeout_ms: 240,
        cleanup_interval_ms: 80,
    };

    let test_prefix = unique_test_prefix("cluster_large_presence");
    let adapter =
        create_redis_cluster_adapter(&test_prefix, "cluster_node1", &cluster_config).await;
    adapter.init().await;

    // Create large user info object (near 2KB limit)
    let large_data = "x".repeat(1900);
    let user_info = json!({
        "data": large_data,
        "timestamp": 1234567890,
        "status": "online"
    });

    // Should handle large presence data correctly
    adapter
        .broadcast_presence_join(
            "app1",
            "channel1",
            "user_large",
            "socket_large",
            Some(user_info),
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(40)).await;

    drop(adapter);
}

#[tokio::test]
async fn test_redis_cluster_disabled_health_monitoring() {
    if !is_redis_cluster_available().await {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let cluster_config = ClusterHealthConfig {
        enabled: false,
        ..fast_cluster_health_config()
    };

    let test_prefix = unique_test_prefix("cluster_health_disabled");
    let adapter =
        create_redis_cluster_adapter(&test_prefix, "cluster_node1", &cluster_config).await;
    adapter.init().await;

    // Should still handle presence operations without health monitoring
    adapter
        .broadcast_presence_join("app1", "channel1", "user1", "socket1", None)
        .await
        .unwrap();

    adapter
        .broadcast_presence_leave("app1", "channel1", "user1", "socket1")
        .await
        .unwrap();

    // No heartbeats should be sent
    sleep(Duration::from_millis(60)).await;

    drop(adapter);
}
