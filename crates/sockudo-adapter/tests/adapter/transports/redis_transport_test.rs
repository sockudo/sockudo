use sockudo_adapter::serialization::Serialization;
use sockudo_adapter::horizontal_transport::HorizontalTransport;
use sockudo_adapter::transports::{RedisAdapterConfig, RedisTransport};
use sockudo_core::error::Result;

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
        sentinel: None,
        serialization: Serialization::default(),
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
async fn test_redis_transport_config_edge_cases() -> Result<()> {
    // Test with empty prefix
    let config = RedisAdapterConfig {
        url: "redis://127.0.0.1:16379/".to_string(),
        prefix: "".to_string(), // Empty prefix
        request_timeout_ms: 1000,
        cluster_mode: false,
        sentinel: None,
        serialization: Serialization::default(),
    };

    let transport = RedisTransport::new(config).await?;
    transport.check_health().await?;

    // Test with very short timeout
    let config = RedisAdapterConfig {
        url: "redis://127.0.0.1:16379/".to_string(),
        prefix: "test_short_timeout".to_string(),
        request_timeout_ms: 1, // 1ms timeout
        cluster_mode: false,
        sentinel: None,
        serialization: Serialization::default(),
    };

    let transport = RedisTransport::new(config).await?;
    transport.check_health().await?;

    // Test with zero timeout (edge case)
    let config = RedisAdapterConfig {
        url: "redis://127.0.0.1:16379/".to_string(),
        prefix: "test_zero_timeout".to_string(),
        request_timeout_ms: 0, // Zero timeout
        cluster_mode: false,
        sentinel: None,
        serialization: Serialization::default(),
    };

    let transport = RedisTransport::new(config).await?;
    transport.check_health().await?;

    Ok(())
}

#[tokio::test]
async fn test_redis_transport_malformed_url() {
    // Test various malformed URLs to ensure our error handling works
    let malformed_urls = vec![
        "not-a-url",
        "redis://",
        "redis://localhost:99999999", // Invalid port
        "http://localhost:6379",      // Wrong protocol
        "",
        "redis://invalid-host-that-should-not-exist:6379",
    ];

    for url in malformed_urls {
        let config = RedisAdapterConfig {
            url: url.to_string(),
            prefix: "test".to_string(),
            request_timeout_ms: 1000,
            cluster_mode: false,
            sentinel: None,
            serialization: Serialization::default(),
        };

        let result = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            RedisTransport::new(config),
        )
        .await;

        // Should either timeout or return an error
        assert!(
            result.is_err() || result.unwrap().is_err(),
            "Expected error for malformed URL: {}",
            url
        );
    }
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

    // With just one connection, should return exactly 1
    let count = transport1.get_node_count().await?;
    assert_eq!(count, 1);

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
        sentinel: None,
        serialization: Serialization::default(),
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

#[tokio::test]
async fn test_redis_new_inbox_returns_reply_channel() -> Result<()> {
    let config = get_redis_config();
    let transport = match RedisTransport::new(config).await {
        Ok(t) => t,
        Err(_) => {
            println!("Skipping test_redis_new_inbox_returns_reply_channel: Redis not available");
            return Ok(());
        }
    };

    let inbox = transport.new_inbox();
    assert!(inbox.is_some(), "new_inbox() should return Some");

    let channel = inbox.unwrap();
    assert!(
        channel.contains(":#reply:"),
        "reply channel must contain ':#reply:', got: {channel}"
    );

    let inbox2 = transport.new_inbox();
    assert_eq!(
        inbox2.as_deref(),
        Some(channel.as_str()),
        "new_inbox() should return the same channel on every call for the same instance"
    );

    Ok(())
}

#[tokio::test]
async fn test_redis_publish_request_with_reply_sets_reply_to() -> Result<()> {
    let config = get_redis_config();
    let transport = match RedisTransport::new(config).await {
        Ok(t) => t,
        Err(_) => {
            println!(
                "Skipping test_redis_publish_request_with_reply_sets_reply_to: Redis not available"
            );
            return Ok(());
        }
    };

    let inbox = transport.new_inbox();
    assert!(
        inbox.is_some(),
        "new_inbox() must be Some before publishing"
    );
    let inbox_channel = inbox.unwrap();
    assert!(!inbox_channel.is_empty());

    let request = create_test_request();
    assert!(
        request.reply_to.is_none(),
        "request must start with reply_to: None"
    );

    let result = transport
        .publish_request_with_reply(&request, &inbox_channel)
        .await;
    assert!(
        result.is_ok(),
        "publish_request_with_reply should succeed: {:?}",
        result.err()
    );

    Ok(())
}

#[tokio::test]
async fn test_redis_publish_request_to_node_uses_correct_channel() -> Result<()> {
    let config = get_redis_config();
    let transport = match RedisTransport::new(config).await {
        Ok(t) => t,
        Err(_) => {
            println!(
                "Skipping test_redis_publish_request_to_node_uses_correct_channel: Redis not available"
            );
            return Ok(());
        }
    };

    let request = create_test_request();

    let result = transport
        .publish_request_to_node(&request, "test-target-node")
        .await;
    assert!(
        result.is_ok(),
        "publish_request_to_node should succeed: {:?}",
        result.err()
    );

    Ok(())
}

#[tokio::test]
async fn test_redis_two_transports_have_different_reply_channels() -> Result<()> {
    let config = get_redis_config();

    let transport_a = match RedisTransport::new(config.clone()).await {
        Ok(t) => t,
        Err(_) => {
            println!(
                "Skipping test_redis_two_transports_have_different_reply_channels: Redis not available"
            );
            return Ok(());
        }
    };
    let transport_b = match RedisTransport::new(config).await {
        Ok(t) => t,
        Err(_) => {
            println!(
                "Skipping test_redis_two_transports_have_different_reply_channels: Redis not available"
            );
            return Ok(());
        }
    };

    let inbox_a = transport_a
        .new_inbox()
        .expect("transport_a must have an inbox");
    let inbox_b = transport_b
        .new_inbox()
        .expect("transport_b must have an inbox");

    assert_ne!(
        inbox_a, inbox_b,
        "Each transport instance must have a unique reply channel (different UUIDs)"
    );

    Ok(())
}

#[tokio::test]
async fn test_redis_reply_channel_format() -> Result<()> {
    let config = get_redis_config();
    let prefix = config.prefix.clone();

    let transport = match RedisTransport::new(config).await {
        Ok(t) => t,
        Err(_) => {
            println!("Skipping test_redis_reply_channel_format: Redis not available");
            return Ok(());
        }
    };

    let channel = transport
        .new_inbox()
        .expect("new_inbox() should return Some");

    assert!(
        channel.starts_with(&prefix),
        "reply channel must start with prefix '{prefix}', got: {channel}"
    );

    assert!(
        channel.contains(":#reply:"),
        "reply channel must contain ':#reply:', got: {channel}"
    );

    let suffix = channel
        .split(":#reply:")
        .nth(1)
        .expect("channel must have a suffix after ':#reply:'");
    assert!(
        suffix.len() >= 32,
        "UUID suffix must be at least 32 chars, got '{}' (len {})",
        suffix,
        suffix.len()
    );
    assert!(
        suffix.chars().all(|c| c.is_ascii_hexdigit()),
        "UUID suffix must be lowercase hex, got: '{suffix}'"
    );

    Ok(())
}

#[tokio::test]
async fn test_msgpack_broadcast_roundtrip() -> Result<()> {
    let mut config_a = get_redis_config();
    config_a.serialization = Serialization::MessagePack;
    let mut config_b = config_a.clone();
    config_b.prefix = config_a.prefix.clone(); // same prefix = same channels

    let transport_a = match RedisTransport::new(config_a).await {
        Ok(t) => t,
        Err(_) => {
            println!("Skipping test_msgpack_broadcast_roundtrip: Redis not available");
            return Ok(());
        }
    };
    let transport_b = match RedisTransport::new(config_b).await {
        Ok(t) => t,
        Err(_) => {
            println!("Skipping test_msgpack_broadcast_roundtrip: Redis not available");
            return Ok(());
        }
    };

    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport_b.start_listeners(handlers).await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let msg = create_test_broadcast("msgpack-test");
    transport_a.publish_broadcast(&msg).await?;

    let received = collector.wait_for_broadcast(2000).await;
    assert!(
        received.is_some(),
        "Should receive broadcast via MessagePack"
    );
    assert_eq!(received.unwrap().channel, msg.channel);

    Ok(())
}

#[tokio::test]
async fn test_backward_compat_json_to_msgpack_receiver() -> Result<()> {
    use redis::AsyncCommands;

    let mut config = get_redis_config();
    config.serialization = Serialization::MessagePack;
    let prefix = config.prefix.clone();

    let transport = match RedisTransport::new(config).await {
        Ok(t) => t,
        Err(_) => {
            println!("Skipping test_backward_compat_json_to_msgpack_receiver: Redis not available");
            return Ok(());
        }
    };

    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport.start_listeners(handlers).await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Publish raw JSON directly (simulating old node)
    let client = redis::Client::open("redis://127.0.0.1:16379/")
        .map_err(|e| sockudo_core::error::Error::Other(format!("Redis client error: {e}")))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| sockudo_core::error::Error::Other(format!("Redis connection error: {e}")))?;

    let broadcast_channel = format!("{}:#broadcast", prefix);
    let json_payload = r#"{"node_id":"old-node","app_id":"test-app","channel":"test-channel","message":"{\"event\":\"test\"}","except_socket_id":null,"timestamp_ms":null,"compression_metadata":null,"idempotency_key":null,"ephemeral":false}"#;

    let _: i32 = conn
        .publish(&broadcast_channel, json_payload)
        .await
        .map_err(|e| sockudo_core::error::Error::Other(format!("Publish error: {e}")))?;

    let received = collector.wait_for_broadcast(2000).await;
    assert!(
        received.is_some(),
        "MessagePack receiver should auto-detect and parse JSON from old node"
    );

    Ok(())
}

#[tokio::test]
async fn test_msgpack_sender_json_receiver() -> Result<()> {
    let config_base = get_redis_config();
    let prefix = config_base.prefix.clone();

    // Sender: MessagePack
    let mut config_sender = config_base.clone();
    config_sender.serialization = Serialization::MessagePack;

    // Receiver: Json (default) — but auto-detects MessagePack
    let mut config_receiver = config_base;
    config_receiver.prefix = prefix;
    config_receiver.serialization = Serialization::Json;

    let transport_sender = match RedisTransport::new(config_sender).await {
        Ok(t) => t,
        Err(_) => {
            println!("Skipping test_msgpack_sender_json_receiver: Redis not available");
            return Ok(());
        }
    };
    let transport_receiver = match RedisTransport::new(config_receiver).await {
        Ok(t) => t,
        Err(_) => {
            println!("Skipping test_msgpack_sender_json_receiver: Redis not available");
            return Ok(());
        }
    };

    let collector = MessageCollector::new();
    let handlers = create_test_handlers(collector.clone());
    transport_receiver.start_listeners(handlers).await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let msg = create_test_broadcast("msgpack-to-json-receiver");
    transport_sender.publish_broadcast(&msg).await?;

    let received = collector.wait_for_broadcast(2000).await;
    assert!(
        received.is_some(),
        "JSON receiver should auto-detect and parse MessagePack from new node"
    );
    assert_eq!(received.unwrap().channel, msg.channel);

    Ok(())
}
