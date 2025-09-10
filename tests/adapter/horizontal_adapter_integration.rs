use sockudo::adapter::ConnectionManager;
use sockudo::adapter::horizontal_adapter::RequestType;
use sockudo::adapter::horizontal_adapter_base::HorizontalAdapterBase;
use sockudo::adapter::transports::{NatsTransport, RedisTransport};
use sockudo::error::Result;
use sockudo::protocol::messages::{MessageData, PusherMessage};
use std::sync::Arc;
use std::time::Duration;

// Import test helpers from the transports tests
use super::transports::test_helpers::{get_nats_config, get_redis_config};

#[tokio::test]
async fn test_horizontal_adapter_with_redis_transport() -> Result<()> {
    let config = get_redis_config();
    let adapter = HorizontalAdapterBase::<RedisTransport>::new(config.clone()).await?;

    // Should be able to initialize
    let mut adapter_mut = HorizontalAdapterBase::<RedisTransport>::new(config).await?;
    adapter_mut.init().await;

    // Start listeners
    adapter.start_listeners().await?;

    // Should be able to check health
    adapter.check_health().await?;

    // Should be able to send a request (even if no other nodes respond)
    let response = adapter
        .send_request("integration-app", RequestType::Sockets, None, None, None)
        .await?;

    assert_eq!(response.app_id, "integration-app");
    assert!(!response.request_id.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_horizontal_adapter_with_nats_transport() -> Result<()> {
    let config = get_nats_config();
    let adapter = HorizontalAdapterBase::<NatsTransport>::new(config.clone()).await?;

    // Should be able to initialize
    let mut adapter_mut = HorizontalAdapterBase::<NatsTransport>::new(config).await?;
    adapter_mut.init().await;

    // Start listeners
    adapter.start_listeners().await?;

    // Should be able to check health
    adapter.check_health().await?;

    // Should be able to send a request
    let response = adapter
        .send_request("integration-app", RequestType::Sockets, None, None, None)
        .await?;

    assert_eq!(response.app_id, "integration-app");
    assert!(!response.request_id.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_horizontal_adapter_broadcast_redis() -> Result<()> {
    let config = get_redis_config();
    let mut adapter = HorizontalAdapterBase::<RedisTransport>::new(config).await?;

    adapter.init().await;
    adapter.start_listeners().await?;

    let message = PusherMessage {
        channel: None,
        name: None,
        event: Some("integration-test".to_string()),
        data: Some(MessageData::String(
            "integration test broadcast".to_string(),
        )),
        user_id: None,
    };

    // Should be able to broadcast
    adapter
        .send(
            "integration-channel",
            message,
            None,
            "integration-app",
            None,
        )
        .await?;

    // Give some time for the message to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[tokio::test]
async fn test_horizontal_adapter_broadcast_nats() -> Result<()> {
    let config = get_nats_config();
    let mut adapter = HorizontalAdapterBase::<NatsTransport>::new(config).await?;

    adapter.init().await;
    adapter.start_listeners().await?;

    let message = PusherMessage {
        channel: None,
        name: None,
        event: Some("integration-test".to_string()),
        data: Some(MessageData::String(
            "integration test broadcast".to_string(),
        )),
        user_id: None,
    };

    // Should be able to broadcast
    adapter
        .send(
            "integration-channel",
            message,
            None,
            "integration-app",
            None,
        )
        .await?;

    // Give some time for the message to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[tokio::test]
async fn test_multi_adapter_communication_redis() -> Result<()> {
    let config1 = get_redis_config();
    let config2 = get_redis_config();

    let mut adapter1 = HorizontalAdapterBase::<RedisTransport>::new(config1).await?;
    let mut adapter2 = HorizontalAdapterBase::<RedisTransport>::new(config2).await?;

    // Initialize both adapters
    adapter1.init().await;
    adapter2.init().await;

    // Start listeners on both
    adapter1.start_listeners().await?;
    adapter2.start_listeners().await?;

    // Give adapters time to establish connections
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a broadcast from adapter1
    let message = PusherMessage {
        channel: None,
        name: None,
        event: Some("multi-adapter-test".to_string()),
        data: Some(MessageData::String("multi-adapter test".to_string())),
        user_id: None,
    };

    adapter1
        .send("multi-test-channel", message, None, "multi-test-app", None)
        .await?;

    // Both adapters should be healthy
    adapter1.check_health().await?;
    adapter2.check_health().await?;

    Ok(())
}

#[tokio::test]
async fn test_multi_adapter_communication_nats() -> Result<()> {
    let config1 = get_nats_config();
    let config2 = get_nats_config();

    let mut adapter1 = HorizontalAdapterBase::<NatsTransport>::new(config1).await?;
    let mut adapter2 = HorizontalAdapterBase::<NatsTransport>::new(config2).await?;

    // Initialize both adapters
    adapter1.init().await;
    adapter2.init().await;

    // Start listeners on both
    adapter1.start_listeners().await?;
    adapter2.start_listeners().await?;

    // Give adapters time to establish connections
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a broadcast from adapter1
    let message = PusherMessage {
        channel: None,
        name: None,
        event: Some("multi-adapter-nats-test".to_string()),
        data: Some(MessageData::String("multi-adapter nats test".to_string())),
        user_id: None,
    };

    adapter1
        .send("multi-nats-channel", message, None, "multi-nats-app", None)
        .await?;

    // Both adapters should be healthy
    adapter1.check_health().await?;
    adapter2.check_health().await?;

    Ok(())
}

#[tokio::test]
async fn test_adapter_socket_count_aggregation_redis() -> Result<()> {
    let config = get_redis_config();
    let adapter = HorizontalAdapterBase::<RedisTransport>::new(config).await?;

    adapter.start_listeners().await?;

    // Test socket count aggregation across nodes
    let count = adapter.get_sockets_count("socket-count-app").await?;

    // Should return a valid count (>= 0)
    assert!(count >= 0);

    Ok(())
}

#[tokio::test]
async fn test_adapter_socket_count_aggregation_nats() -> Result<()> {
    let config = get_nats_config();
    let adapter = HorizontalAdapterBase::<NatsTransport>::new(config).await?;

    adapter.start_listeners().await?;

    // Test socket count aggregation across nodes
    let count = adapter.get_sockets_count("socket-count-app").await?;

    // Should return a valid count (>= 0)
    assert!(count >= 0);

    Ok(())
}

#[tokio::test]
async fn test_adapter_request_timeout_redis() -> Result<()> {
    let mut config = get_redis_config();
    // Set a very short timeout to test timeout behavior
    config.request_timeout_ms = 50;

    let adapter = HorizontalAdapterBase::<RedisTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let start = std::time::Instant::now();

    // This request should timeout quickly since no other nodes are responding
    let response = adapter
        .send_request("timeout-test-app", RequestType::Sockets, None, None, None)
        .await?;

    let duration = start.elapsed();

    // Should timeout relatively quickly (within 200ms to account for processing overhead)
    assert!(duration <= Duration::from_millis(200));
    assert_eq!(response.app_id, "timeout-test-app");

    Ok(())
}

#[tokio::test]
async fn test_adapter_request_timeout_nats() -> Result<()> {
    let mut config = get_nats_config();
    // Set a very short timeout to test timeout behavior
    config.request_timeout_ms = 50;

    let adapter = HorizontalAdapterBase::<NatsTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let start = std::time::Instant::now();

    // This request should timeout quickly since no other nodes are responding
    let response = adapter
        .send_request("timeout-test-app", RequestType::Sockets, None, None, None)
        .await?;

    let duration = start.elapsed();

    // Should timeout relatively quickly (within 200ms to account for processing overhead)
    assert!(duration <= Duration::from_millis(200));
    assert_eq!(response.app_id, "timeout-test-app");

    Ok(())
}

#[tokio::test]
async fn test_adapter_different_request_types_redis() -> Result<()> {
    let config = get_redis_config();
    let adapter = HorizontalAdapterBase::<RedisTransport>::new(config).await?;

    adapter.start_listeners().await?;

    // Test different request types
    let request_types = vec![
        RequestType::Sockets,
        RequestType::ChannelMembers,
        RequestType::ChannelSockets,
        RequestType::ChannelSocketsCount,
        RequestType::SocketExistsInChannel,
    ];

    for request_type in request_types {
        let response = adapter
            .send_request(
                "request-type-test-app",
                request_type,
                Some("test-channel"),
                None,
                None,
            )
            .await?;

        assert_eq!(response.app_id, "request-type-test-app");
        assert!(!response.request_id.is_empty());
    }

    Ok(())
}

#[tokio::test]
async fn test_adapter_different_request_types_nats() -> Result<()> {
    let config = get_nats_config();
    let adapter = HorizontalAdapterBase::<NatsTransport>::new(config).await?;

    adapter.start_listeners().await?;

    // Test different request types
    let request_types = vec![
        RequestType::Sockets,
        RequestType::ChannelMembers,
        RequestType::ChannelSockets,
        RequestType::ChannelSocketsCount,
        RequestType::SocketExistsInChannel,
    ];

    for request_type in request_types {
        let response = adapter
            .send_request(
                "request-type-test-app",
                request_type,
                Some("test-channel"),
                None,
                None,
            )
            .await?;

        assert_eq!(response.app_id, "request-type-test-app");
        assert!(!response.request_id.is_empty());
    }

    Ok(())
}

#[tokio::test]
async fn test_adapter_concurrent_requests_redis() -> Result<()> {
    let config = get_redis_config();
    let adapter = Arc::new(HorizontalAdapterBase::<RedisTransport>::new(config).await?);

    adapter.start_listeners().await?;

    // Send multiple concurrent requests
    let mut handles = Vec::new();

    for i in 0..5 {
        let adapter = adapter.clone();
        let handle = tokio::spawn(async move {
            adapter
                .send_request(
                    &format!("concurrent-app-{}", i),
                    RequestType::Sockets,
                    Some(&format!("channel-{}", i)),
                    None,
                    None,
                )
                .await
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    let mut responses = Vec::new();
    for handle in handles {
        let response = handle.await.unwrap()?;
        responses.push(response);
    }

    // All should succeed
    assert_eq!(responses.len(), 5);

    // Each should have unique request IDs
    let mut request_ids = std::collections::HashSet::new();
    for response in &responses {
        assert!(request_ids.insert(response.request_id.clone()));
    }

    Ok(())
}

#[tokio::test]
async fn test_adapter_concurrent_requests_nats() -> Result<()> {
    let config = get_nats_config();
    let adapter = Arc::new(HorizontalAdapterBase::<NatsTransport>::new(config).await?);

    adapter.start_listeners().await?;

    // Send multiple concurrent requests
    let mut handles = Vec::new();

    for i in 0..5 {
        let adapter = adapter.clone();
        let handle = tokio::spawn(async move {
            adapter
                .send_request(
                    &format!("concurrent-app-{}", i),
                    RequestType::Sockets,
                    Some(&format!("channel-{}", i)),
                    None,
                    None,
                )
                .await
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    let mut responses = Vec::new();
    for handle in handles {
        let response = handle.await.unwrap()?;
        responses.push(response);
    }

    // All should succeed
    assert_eq!(responses.len(), 5);

    // Each should have unique request IDs
    let mut request_ids = std::collections::HashSet::new();
    for response in &responses {
        assert!(request_ids.insert(response.request_id.clone()));
    }

    Ok(())
}
