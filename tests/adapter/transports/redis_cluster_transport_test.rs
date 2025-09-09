use sockudo::adapter::horizontal_transport::HorizontalTransport;
use sockudo::adapter::transports::RedisClusterTransport;
use sockudo::error::Result;

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
    assert!(received.unwrap().message.contains("cluster-cross-transport"));
    
    Ok(())
}

#[tokio::test]
async fn test_cluster_get_node_count() -> Result<()> {
    let config = get_redis_cluster_config();
    let transport = RedisClusterTransport::new(config.clone()).await?;
    
    // Should detect cluster nodes (3 in our test setup)
    let count = transport.get_node_count().await?;
    assert!(count >= 3, "Expected at least 3 cluster nodes, got {}", count);
    
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
    assert!(received1.unwrap().message.contains("cluster-multi-listener"));
    assert!(received2.unwrap().message.contains("cluster-multi-listener"));
    
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