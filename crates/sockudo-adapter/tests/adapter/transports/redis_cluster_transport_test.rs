use sockudo_adapter::horizontal_transport::HorizontalTransport;
use sockudo_adapter::transports::RedisClusterTransport;
use sockudo_core::error::Result;
use sockudo_core::options::RedisClusterAdapterConfig;

use super::test_helpers::*;

#[tokio::test]
async fn test_redis_cluster_transport_new() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport = RedisClusterTransport::new(config.clone()).await?;

    // Verify the transport was created successfully by checking health
    transport.check_health().await?;

    Ok(())
}

#[tokio::test]
async fn test_redis_cluster_config_edge_cases() -> Result<()> {
    // Test with empty prefix
    let config = RedisClusterAdapterConfig {
        nodes: vec![
            "redis://127.0.0.1:7003".to_string(),
            "redis://127.0.0.1:7001".to_string(),
        ],
        prefix: "".to_string(), // Empty prefix
        request_timeout_ms: 1000,
        use_connection_manager: false,
        use_sharded_pubsub: false,
        serialization: "json".to_string(),
    };

    let transport = RedisClusterTransport::new(config).await?;
    transport.check_health().await?;

    // Test with single node (minimal cluster)
    let config = RedisClusterAdapterConfig {
        nodes: vec!["redis://127.0.0.1:7003".to_string()], // Only one node
        prefix: "test_single_node".to_string(),
        request_timeout_ms: 1000,
        use_connection_manager: false,
        use_sharded_pubsub: false,
        serialization: "json".to_string(),
    };

    let transport = RedisClusterTransport::new(config).await?;
    transport.check_health().await?;

    // Test with connection manager enabled
    let config = RedisClusterAdapterConfig {
        nodes: vec![
            "redis://127.0.0.1:7003".to_string(),
            "redis://127.0.0.1:7001".to_string(),
        ],
        prefix: "test_conn_mgr".to_string(),
        request_timeout_ms: 1000,
        use_connection_manager: true, // Enable connection manager
        use_sharded_pubsub: false,
        serialization: "json".to_string(),
    };

    let transport = RedisClusterTransport::new(config).await?;
    transport.check_health().await?;

    Ok(())
}

#[tokio::test]
async fn test_redis_cluster_invalid_config() {
    // Test with empty nodes list
    let config = RedisClusterAdapterConfig {
        nodes: vec![], // Empty nodes list
        prefix: "test".to_string(),
        request_timeout_ms: 1000,
        use_connection_manager: false,
        use_sharded_pubsub: false,
        serialization: "json".to_string(),
    };

    let result = RedisClusterTransport::new(config).await;
    assert!(result.is_err(), "Expected error for empty nodes list");

    // Test with invalid node URLs
    let config = RedisClusterAdapterConfig {
        nodes: vec![
            "not-a-url".to_string(),
            "redis://".to_string(),
            "".to_string(),
        ],
        prefix: "test".to_string(),
        request_timeout_ms: 1000,
        use_connection_manager: false,
        use_sharded_pubsub: false,
        serialization: "json".to_string(),
    };

    let result = tokio::time::timeout(
        tokio::time::Duration::from_secs(3),
        RedisClusterTransport::new(config),
    )
    .await;

    assert!(
        result.is_err() || result.unwrap().is_err(),
        "Expected error for malformed cluster URLs"
    );
}

#[tokio::test]
async fn test_cluster_publish_broadcast() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport = RedisClusterTransport::new(config.clone()).await?;

    // Set up a listener first
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;

    // Give listener time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish a broadcast
    let broadcast = create_test_broadcast("cluster-test-event");
    transport.publish_broadcast(&broadcast).await?;

    // Wait for the message to be received
    let received = collector.wait_for_broadcast(500).await;
    assert!(received.is_some());

    let received_msg = received.unwrap();
    assert!(received_msg.message.contains("cluster-test-event"));
    assert_eq!(received_msg.channel, "test-channel");

    Ok(())
}

#[tokio::test]
async fn test_cluster_publish_request() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport = RedisClusterTransport::new(config.clone()).await?;

    // Set up a listener first
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;

    // Give listener time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish a request
    let request = create_test_request();
    let request_id = request.request_id.clone();
    transport.publish_request(&request).await?;

    // Wait for the request to be received
    let received = collector.wait_for_request(500).await;
    assert!(received.is_some());

    let received_req = received.unwrap();
    assert_eq!(received_req.request_id, request_id);

    Ok(())
}

#[tokio::test]
async fn test_cluster_publish_response() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport = RedisClusterTransport::new(config.clone()).await?;

    // Set up a listener first
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;

    // Give listener time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish a response
    let response = create_test_response("cluster-test-request-id");
    transport.publish_response(&response).await?;

    // Wait for the response to be received
    let received = collector.wait_for_response(500).await;
    assert!(received.is_some());

    let received_resp = received.unwrap();
    assert_eq!(received_resp.request_id, "cluster-test-request-id");

    Ok(())
}

#[tokio::test]
async fn test_cluster_cross_transport_communication() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport1 = RedisClusterTransport::new(config.clone()).await?;
    let transport2 = RedisClusterTransport::new(config.clone()).await?;

    // Set up listener on transport1
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport1.start_listeners(handlers).await?;

    // Give listener time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish from transport2
    let broadcast = create_test_broadcast("cluster-cross-transport");
    transport2.publish_broadcast(&broadcast).await?;

    // Verify transport1 received the message
    let received = collector.wait_for_broadcast(500).await;
    assert!(received.is_some());
    assert!(
        received
            .unwrap()
            .message
            .contains("cluster-cross-transport")
    );

    Ok(())
}

#[tokio::test]
async fn test_cluster_get_node_count() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport = RedisClusterTransport::new(config.clone()).await?;

    // Should detect connected Sockudo instances (at least 1 from this test)
    let count = transport.get_node_count().await?;
    assert!(
        count >= 1,
        "Expected at least 1 Sockudo instance, got {}",
        count
    );

    Ok(())
}

#[tokio::test]
async fn test_cluster_check_health() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport = RedisClusterTransport::new(config).await?;

    // Health check should succeed
    transport.check_health().await?;

    Ok(())
}

#[tokio::test]
async fn test_cluster_multiple_listeners() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport1 = RedisClusterTransport::new(config.clone()).await?;
    let transport2 = RedisClusterTransport::new(config.clone()).await?;
    let transport_publisher = RedisClusterTransport::new(config.clone()).await?;

    // Set up two listeners
    let collector1 = MessageCollector::new();
    let handlers1 = create_test_handlers(collector1.clone());
    transport1.start_listeners(handlers1).await?;

    let collector2 = MessageCollector::new();
    let handlers2 = create_test_handlers(collector2.clone());
    transport2.start_listeners(handlers2).await?;

    // Give listeners time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish a broadcast
    let broadcast = create_test_broadcast("cluster-multi-listener");
    transport_publisher.publish_broadcast(&broadcast).await?;

    // Both listeners should receive the message
    let received1 = collector1.wait_for_broadcast(500).await;
    let received2 = collector2.wait_for_broadcast(500).await;

    assert!(received1.is_some());
    assert!(received2.is_some());
    assert!(
        received1
            .unwrap()
            .message
            .contains("cluster-multi-listener")
    );
    assert!(
        received2
            .unwrap()
            .message
            .contains("cluster-multi-listener")
    );

    Ok(())
}

#[tokio::test]
async fn test_cluster_request_response_flow() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport1 = RedisClusterTransport::new(config.clone()).await?;
    let transport2 = RedisClusterTransport::new(config.clone()).await?;

    // Set up listeners on both transports
    let collector1 = MessageCollector::new();
    let handlers1 = create_test_handlers(collector1.clone());
    transport1.start_listeners(handlers1).await?;

    let collector2 = MessageCollector::new();
    let handlers2 = create_test_handlers(collector2.clone());
    transport2.start_listeners(handlers2).await?;

    // Give listeners time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Transport1 publishes a request
    let request = create_test_request();
    let request_id = request.request_id.clone();
    transport1.publish_request(&request).await?;

    // Transport2 should receive the request
    let received_request = collector2.wait_for_request(500).await;
    assert!(received_request.is_some());
    assert_eq!(received_request.unwrap().request_id, request_id);

    // The handler automatically sends a response (see test_helpers)
    // Transport1 should receive the response
    let received_response = collector1.wait_for_response(500).await;
    assert!(received_response.is_some());
    assert_eq!(received_response.unwrap().request_id, request_id);

    Ok(())
}

#[tokio::test]
async fn test_cluster_sharding() -> Result<()> {
    // This test verifies that the cluster can handle messages across different shards
    let config = get_redis_cluster_config();
    let transport = RedisClusterTransport::new(config.clone()).await?;

    // Set up a listener
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;

    // Give listener time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish multiple broadcasts with different channel names to test sharding
    let broadcasts = vec![
        create_test_broadcast("shard-test-1"),
        create_test_broadcast("shard-test-2"),
        create_test_broadcast("shard-test-3"),
    ];

    for broadcast in broadcasts {
        transport.publish_broadcast(&broadcast).await?;
    }

    // Should receive at least one broadcast (cluster sharding shouldn't prevent delivery)
    let received = collector.wait_for_broadcast(500).await;
    assert!(received.is_some());

    Ok(())
}

#[tokio::test]
async fn test_redis_cluster_new_inbox_returns_reply_channel() {
    let config = get_redis_cluster_config();
    let Ok(transport) = RedisClusterTransport::new(config).await else {
        return; // Skip if Redis Cluster not available
    };

    let inbox = transport.new_inbox();
    assert!(inbox.is_some(), "new_inbox() should return Some");
    let channel = inbox.unwrap();
    assert!(
        channel.contains(":#reply:"),
        "Channel name should contain ':#reply:', got: {channel}"
    );

    let inbox2 = transport.new_inbox();
    assert_eq!(
        inbox2,
        Some(channel),
        "new_inbox() should return the same value on repeated calls"
    );
}

#[tokio::test]
async fn test_redis_cluster_publish_request_to_node() {
    let config = get_redis_cluster_config();
    let Ok(transport) = RedisClusterTransport::new(config).await else {
        return; // Skip if Redis Cluster not available
    };

    let request = create_test_request();
    let result = transport
        .publish_request_to_node(&request, "test-target-node")
        .await;
    assert!(
        result.is_ok(),
        "publish_request_to_node should return Ok, got: {result:?}"
    );
}

#[tokio::test]
async fn test_redis_cluster_node_count_is_real_time_conditional() {
    let config_sharded = RedisClusterAdapterConfig {
        nodes: vec![
            "redis://127.0.0.1:7003".to_string(),
            "redis://127.0.0.1:7001".to_string(),
            "redis://127.0.0.1:7002".to_string(),
        ],
        prefix: "test_sharded_rt".to_string(),
        request_timeout_ms: 1000,
        use_connection_manager: false,
        use_sharded_pubsub: true,
        serialization: "json".to_string(),
    };
    let Ok(transport_sharded) = RedisClusterTransport::new(config_sharded).await else {
        return; // Skip if Redis Cluster not available
    };
    assert!(
        transport_sharded.node_count_is_real_time(),
        "node_count_is_real_time() should return true when use_sharded_pubsub is true"
    );

    let config_standard = get_redis_cluster_config();
    let Ok(transport_standard) = RedisClusterTransport::new(config_standard).await else {
        return;
    };
    assert!(
        !transport_standard.node_count_is_real_time(),
        "node_count_is_real_time() should return false when use_sharded_pubsub is false"
    );
}

#[tokio::test]
async fn test_redis_cluster_two_transports_have_different_reply_channels() {
    let config = get_redis_cluster_config();
    let Ok(transport1) = RedisClusterTransport::new(config.clone()).await else {
        return; // Skip if Redis Cluster not available
    };
    let Ok(transport2) = RedisClusterTransport::new(config).await else {
        return;
    };

    let channel1 = transport1.new_inbox();
    let channel2 = transport2.new_inbox();

    assert!(
        channel1.is_some(),
        "transport1 new_inbox() should return Some"
    );
    assert!(
        channel2.is_some(),
        "transport2 new_inbox() should return Some"
    );
    assert_ne!(
        channel1, channel2,
        "Two transport instances should have different reply channels"
    );
}

#[tokio::test]
async fn test_redis_cluster_reply_channel_format() {
    let known_prefix = "test_reply_format_prefix";
    let config = RedisClusterAdapterConfig {
        nodes: vec![
            "redis://127.0.0.1:7003".to_string(),
            "redis://127.0.0.1:7001".to_string(),
            "redis://127.0.0.1:7002".to_string(),
        ],
        prefix: known_prefix.to_string(),
        request_timeout_ms: 1000,
        use_connection_manager: false,
        use_sharded_pubsub: false,
        serialization: "json".to_string(),
    };
    let Ok(transport) = RedisClusterTransport::new(config).await else {
        return; // Skip if Redis Cluster not available
    };

    let inbox = transport.new_inbox();
    assert!(inbox.is_some(), "new_inbox() should return Some");
    let channel = inbox.unwrap();

    assert!(
        channel.starts_with(known_prefix),
        "Reply channel should start with prefix '{known_prefix}', got: {channel}"
    );
    assert!(
        channel.contains(":#reply:"),
        "Reply channel should contain ':#reply:', got: {channel}"
    );
}

#[tokio::test]
async fn test_redis_cluster_publish_request_with_reply() {
    let config = get_redis_cluster_config();
    let Ok(transport) = RedisClusterTransport::new(config).await else {
        return;
    };

    let inbox = transport.new_inbox().expect("new_inbox() must return Some");
    let request = create_test_request();
    assert!(
        request.reply_to.is_none(),
        "request must start with reply_to: None"
    );

    let result = transport.publish_request_with_reply(&request, &inbox).await;
    assert!(
        result.is_ok(),
        "publish_request_with_reply should succeed: {:?}",
        result.err()
    );
}
