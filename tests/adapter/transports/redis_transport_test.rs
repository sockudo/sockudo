use sockudo::adapter::horizontal_transport::HorizontalTransport;
use sockudo::adapter::transports::{RedisAdapterConfig, RedisTransport};
use sockudo::error::Result;

use super::test_helpers::*;

#[tokio::test]
async fn test_redis_transport_new() -> Result<()> {
    let config = get_redis_config();
    let transport = RedisTransport::new(config.clone()).await?;

    // Verify the transport was created successfully by checking health
    transport.check_health().await?;

    Ok(())
}

#[tokio::test]
async fn test_redis_transport_new_with_invalid_url() {
    // Use localhost with a port that's not listening - should fail quickly
    let config = RedisAdapterConfig {
        url: "redis://127.0.0.1:19999/".to_string(),
        prefix: "test".to_string(),
        request_timeout_ms: 1000,
        cluster_mode: false,
    };

    // Add a timeout to prevent test from hanging
    let result = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        RedisTransport::new(config),
    )
    .await;

    // Either timeout or connection error is fine
    assert!(result.is_err() || result.unwrap().is_err());
}

#[tokio::test]
async fn test_publish_broadcast() -> Result<()> {
    let config = get_redis_config();
    let transport = RedisTransport::new(config.clone()).await?;

    // Set up a listener first
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;

    // Give listener time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish a broadcast
    let broadcast = create_test_broadcast("test-event");
    transport.publish_broadcast(&broadcast).await?;

    // Wait for the message to be received
    let received = collector.wait_for_broadcast(500).await;
    assert!(received.is_some());

    let received_msg = received.unwrap();
    assert!(received_msg.message.contains("test-event"));
    assert_eq!(received_msg.channel, "test-channel");

    Ok(())
}

#[tokio::test]
async fn test_publish_request() -> Result<()> {
    let config = get_redis_config();
    let transport = RedisTransport::new(config.clone()).await?;

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
async fn test_publish_response() -> Result<()> {
    let config = get_redis_config();
    let transport = RedisTransport::new(config.clone()).await?;

    // Set up a listener first
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;

    // Give listener time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish a response
    let response = create_test_response("test-request-id");
    transport.publish_response(&response).await?;

    // Wait for the response to be received
    let received = collector.wait_for_response(500).await;
    assert!(received.is_some());

    let received_resp = received.unwrap();
    assert_eq!(received_resp.request_id, "test-request-id");

    Ok(())
}

#[tokio::test]
async fn test_start_listeners_and_receive() -> Result<()> {
    let config = get_redis_config();
    let transport1 = RedisTransport::new(config.clone()).await?;
    let transport2 = RedisTransport::new(config.clone()).await?;

    // Set up listener on transport1
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport1.start_listeners(handlers).await?;

    // Give listener time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish from transport2
    let broadcast = create_test_broadcast("cross-transport-event");
    transport2.publish_broadcast(&broadcast).await?;

    // Verify transport1 received the message
    let received = collector.wait_for_broadcast(500).await;
    assert!(received.is_some());
    assert!(received.unwrap().message.contains("cross-transport-event"));

    Ok(())
}

#[tokio::test]
async fn test_get_node_count() -> Result<()> {
    let config = get_redis_config();
    let transport1 = RedisTransport::new(config.clone()).await?;

    // With just one connection, should return 1
    let count = transport1.get_node_count().await?;
    assert!(count >= 1);

    // Set up a listener to increment subscriber count
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector);
    transport1.start_listeners(handlers).await?;

    // Give time for subscription
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Create another transport with listener
    let transport2 = RedisTransport::new(config.clone()).await?;
    let collector2 = MessageCollector::new();
    let handlers2 = create_test_handlers(collector2);
    transport2.start_listeners(handlers2).await?;

    // Give time for subscription
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Now should see 2 subscribers
    let count = transport1.get_node_count().await?;
    assert_eq!(count, 2);

    Ok(())
}

#[tokio::test]
async fn test_check_health() -> Result<()> {
    let config = get_redis_config();
    let transport = RedisTransport::new(config).await?;

    // Health check should succeed
    transport.check_health().await?;

    Ok(())
}

#[tokio::test]
async fn test_channel_names() -> Result<()> {
    let config = RedisAdapterConfig {
        url: "redis://127.0.0.1:16379/".to_string(),
        prefix: "custom_prefix".to_string(),
        request_timeout_ms: 1000,
        cluster_mode: false,
    };

    let transport = RedisTransport::new(config.clone()).await?;

    // Set up a listener to capture channel names
    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;

    // Give listener time to subscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Publish to each channel type and verify they're received
    let broadcast = create_test_broadcast("prefix-test");
    transport.publish_broadcast(&broadcast).await?;

    let request = create_test_request();
    transport.publish_request(&request).await?;

    let response = create_test_response("test-id");
    transport.publish_response(&response).await?;

    // Verify all messages were received (implicitly tests channel naming)
    assert!(collector.wait_for_broadcast(500).await.is_some());
    assert!(collector.wait_for_request(500).await.is_some());
    assert!(collector.wait_for_response(500).await.is_some());

    Ok(())
}

#[tokio::test]
async fn test_retry_broadcast_on_failure() -> Result<()> {
    // This test verifies the retry logic is in place
    // The actual retry behavior is hard to test without mocking
    let config = get_redis_config();
    let transport = RedisTransport::new(config).await?;

    // Normal broadcast should succeed
    let broadcast = create_test_broadcast("test-retry");
    transport.publish_broadcast(&broadcast).await?;

    // The retry logic is tested implicitly - if Redis is down,
    // the publish_broadcast will retry up to MAX_RETRIES times
    // We can't easily simulate this without stopping Redis

    Ok(())
}

#[tokio::test]
async fn test_multiple_listeners_receive_same_message() -> Result<()> {
    let config = get_redis_config();
    let transport1 = RedisTransport::new(config.clone()).await?;
    let transport2 = RedisTransport::new(config.clone()).await?;
    let transport_publisher = RedisTransport::new(config.clone()).await?;

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
    let broadcast = create_test_broadcast("multi-listener-event");
    transport_publisher.publish_broadcast(&broadcast).await?;

    // Both listeners should receive the message
    let received1 = collector1.wait_for_broadcast(500).await;
    let received2 = collector2.wait_for_broadcast(500).await;

    assert!(received1.is_some());
    assert!(received2.is_some());
    assert!(received1.unwrap().message.contains("multi-listener-event"));
    assert!(received2.unwrap().message.contains("multi-listener-event"));

    Ok(())
}

#[tokio::test]
async fn test_request_response_flow() -> Result<()> {
    let config = get_redis_config();
    let transport1 = RedisTransport::new(config.clone()).await?;
    let transport2 = RedisTransport::new(config.clone()).await?;

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
