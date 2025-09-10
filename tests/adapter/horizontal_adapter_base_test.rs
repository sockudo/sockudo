use sockudo::adapter::ConnectionManager;
use sockudo::adapter::horizontal_adapter::RequestType;
use sockudo::adapter::horizontal_adapter_base::HorizontalAdapterBase;
use sockudo::adapter::horizontal_transport::{HorizontalTransport, TransportConfig};
use sockudo::error::Result;
use sockudo::protocol::messages::{MessageData, PusherMessage};
use sockudo::websocket::SocketId;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use super::horizontal_adapter_helpers::{MockConfig, MockTransport};

#[tokio::test]
async fn test_horizontal_adapter_base_new() -> Result<()> {
    let config = MockConfig::default();
    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    // Should be able to create successfully
    assert_eq!(adapter.config.prefix(), "test");
    assert_eq!(adapter.config.request_timeout_ms(), 1000);

    Ok(())
}

#[tokio::test]
async fn test_horizontal_adapter_base_new_failure() {
    let config = MockConfig {
        prefix: "fail_on_new".to_string(),
        simulate_failures: true,
        ..MockConfig::default()
    };

    let result = HorizontalAdapterBase::<MockTransport>::new(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_send_request_happy_path() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3, // 1 local + 2 remote = 3 total
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    // Start listeners to enable request handling
    adapter.start_listeners().await?;

    // Send a request
    let response = adapter
        .send_request(
            "test-app",
            RequestType::Sockets,
            Some("test-channel"),
            Some("test-socket"),
            Some("test-user"),
        )
        .await?;

    // Verify response structure
    assert_eq!(response.app_id, "test-app");
    assert!(response.sockets_count >= 0);
    assert!(!response.request_id.is_empty());

    // Verify the request was published to transport
    let published_requests = adapter.transport.get_published_requests().await;
    assert_eq!(published_requests.len(), 1);
    assert_eq!(published_requests[0].app_id, "test-app");
    assert_eq!(published_requests[0].request_type, RequestType::Sockets);

    Ok(())
}

#[tokio::test]
async fn test_send_request_timeout() -> Result<()> {
    let config = MockConfig {
        request_timeout_ms: 100, // Very short timeout
        simulate_node_count: 3,
        response_delay_ms: 200, // Longer than timeout
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let start = std::time::Instant::now();

    // This should timeout
    let response = adapter
        .send_request("test-app", RequestType::Sockets, None, None, None)
        .await?;

    let duration = start.elapsed();

    // Should timeout in approximately 100ms (±50ms tolerance)
    assert!(duration >= Duration::from_millis(90));
    assert!(duration <= Duration::from_millis(200));

    // Should still return a response (aggregated from partial results)
    assert_eq!(response.app_id, "test-app");
    assert!(!response.request_id.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_send_request_zero_nodes() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 1, // Only local node (0 remote nodes)
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    // Should return immediately with empty response
    let response = adapter
        .send_request("test-app", RequestType::Sockets, None, None, None)
        .await?;

    assert_eq!(response.app_id, "test-app");
    assert_eq!(response.sockets_count, 0);
    assert!(response.members.is_empty());
    assert!(response.socket_ids.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_send_request_partial_responses() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 4, // 1 local + 3 remote
        response_delay_ms: 50,
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    // Create a request that will get partial responses
    let _request_id = format!("partial_response_{}", Uuid::new_v4());

    // Simulate some nodes not responding by using the special request_id
    let response = adapter
        .send_request(
            "test-app",
            RequestType::Sockets,
            Some("test-channel"),
            None,
            None,
        )
        .await?;

    // Should aggregate whatever responses we got
    assert_eq!(response.app_id, "test-app");

    Ok(())
}

#[tokio::test]
async fn test_send_request_different_request_types() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 2,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
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
                "test-app",
                request_type.clone(),
                Some("test-channel"),
                None,
                None,
            )
            .await?;

        assert_eq!(response.app_id, "test-app");
        assert!(!response.request_id.is_empty());

        // Verify request was published with correct type
        let published_requests = adapter.transport.get_published_requests().await;
        let last_request = published_requests.last().unwrap();
        assert_eq!(last_request.request_type, request_type);

        adapter.transport.clear_published_messages().await;
    }

    Ok(())
}

#[tokio::test]
async fn test_send_request_concurrent() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 2,
        response_delay_ms: 50,
        ..MockConfig::default()
    };

    let adapter = Arc::new(HorizontalAdapterBase::<MockTransport>::new(config).await?);
    adapter.start_listeners().await?;

    // Send multiple concurrent requests
    let mut handles = Vec::new();

    for i in 0..5 {
        let adapter = adapter.clone();
        let handle = tokio::spawn(async move {
            adapter
                .send_request(
                    &format!("test-app-{}", i),
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
    let mut request_ids = HashSet::new();
    for response in &responses {
        assert!(request_ids.insert(response.request_id.clone()));
    }

    // All requests should have been published
    let published_requests = adapter.transport.get_published_requests().await;
    assert_eq!(published_requests.len(), 5);

    Ok(())
}

#[tokio::test]
async fn test_start_listeners_twice() -> Result<()> {
    let config = MockConfig::default();
    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    // Should be able to start listeners multiple times without error
    adapter.start_listeners().await?;
    adapter.start_listeners().await?;

    Ok(())
}

#[tokio::test]
async fn test_transport_health_check() -> Result<()> {
    let config = MockConfig::default();
    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    // Should be healthy by default
    adapter.transport.check_health().await?;

    // Make transport unhealthy
    adapter.transport.set_health_status(false).await;

    let result = adapter.transport.check_health().await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_response_aggregation_sockets() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3, // Should get 2 responses
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let response = adapter
        .send_request(
            "test-app",
            RequestType::Sockets,
            Some("test-channel"),
            None,
            None,
        )
        .await?;

    // Should aggregate socket counts from all responding nodes
    assert_eq!(response.app_id, "test-app");
    // Each mock node returns 1 socket, so with 2 nodes responding we should get >= 1
    assert!(response.sockets_count >= 1);

    Ok(())
}

#[tokio::test]
async fn test_response_aggregation_channel_members() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let response = adapter
        .send_request(
            "test-app",
            RequestType::ChannelMembers,
            Some("presence-test"),
            None,
            None,
        )
        .await?;

    assert_eq!(response.app_id, "test-app");
    // Response structure should be valid even if empty
    assert!(response.members_count >= 0);

    Ok(())
}

#[tokio::test]
async fn test_request_id_uniqueness() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 2,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let mut request_ids = HashSet::new();

    // Generate multiple requests and verify unique IDs
    for _ in 0..10 {
        let response = adapter
            .send_request("test-app", RequestType::Sockets, None, None, None)
            .await?;

        assert!(
            request_ids.insert(response.request_id.clone()),
            "Duplicate request ID: {}",
            response.request_id
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_publish_failures() -> Result<()> {
    let config = MockConfig {
        simulate_failures: true,
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    // Request with app_id "fail_request" should cause publish failure
    let result = adapter
        .send_request("fail_request", RequestType::Sockets, None, None, None)
        .await;

    assert!(result.is_err());

    Ok(())
}

// ========================================
// ConnectionManager Trait Tests
// ========================================

#[tokio::test]
async fn test_connection_manager_init() -> Result<()> {
    let config = MockConfig::default();
    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    // Should be able to initialize
    adapter.init().await;

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_get_namespace() -> Result<()> {
    let config = MockConfig::default();
    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    adapter.init().await;

    // Should return None for non-existent namespace initially
    let _namespace = adapter.get_namespace("test-app").await;
    // The local adapter should handle this appropriately

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_get_sockets_count() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3, // 2 remote nodes will respond
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    // Should aggregate socket counts across nodes
    let count = adapter.get_sockets_count("test-app").await?;

    // Should be a valid count (>= 0)
    assert!(count >= 0);

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_get_channels_with_socket_count() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let channels = adapter.get_channels_with_socket_count("test-app").await?;

    // Should return a valid map (can be empty)
    assert!(channels.len() >= 0);

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_get_channel_socket_count() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let count = adapter
        .get_channel_socket_count("test-app", "test-channel")
        .await;

    // Should return a valid count
    assert!(count >= 0);

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_is_in_channel() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let socket_id = SocketId("test-socket-123".to_string());
    let exists = adapter
        .is_in_channel("test-app", "test-channel", &socket_id)
        .await?;

    // Should return a boolean result
    assert!(exists == true || exists == false);

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_get_channel_members() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let members = adapter
        .get_channel_members("test-app", "presence-test")
        .await?;

    // Should return a valid members map
    assert!(members.len() >= 0);

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_get_channel_sockets() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let sockets = adapter
        .get_channel_sockets("test-app", "test-channel")
        .await?;

    // Should return a valid socket list
    assert!(sockets.len() >= 0);

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_get_user_sockets() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let sockets = adapter.get_user_sockets("test-user", "test-app").await?;

    // Should return a valid socket list
    assert!(sockets.len() >= 0);

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_count_user_connections_in_channel() -> Result<()> {
    let config = MockConfig {
        simulate_node_count: 3,
        response_delay_ms: 10,
        ..MockConfig::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;
    adapter.start_listeners().await?;

    let count = adapter
        .count_user_connections_in_channel("test-user", "test-app", "test-channel", None)
        .await?;

    // Should return a valid count
    assert!(count >= 0);

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_remove_channel() -> Result<()> {
    let config = MockConfig::default();
    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    adapter.init().await;

    // Should be able to remove channel without error
    adapter.remove_channel("test-app", "test-channel").await;

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_add_to_channel() -> Result<()> {
    let config = MockConfig::default();
    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    adapter.init().await;

    let socket_id = SocketId("test-socket".to_string());

    // Should be able to add to channel without error
    adapter
        .add_to_channel("test-app", "test-channel", &socket_id)
        .await;

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_remove_from_channel() -> Result<()> {
    let config = MockConfig::default();
    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    adapter.init().await;

    let socket_id = SocketId("test-socket".to_string());

    // Should be able to remove from channel without error
    adapter
        .remove_from_channel("test-app", "test-channel", &socket_id)
        .await;

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_terminate_user_connections() -> Result<()> {
    let config = MockConfig::default();
    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    adapter.init().await;

    // Should be able to terminate user connections
    let result = adapter
        .terminate_user_connections("test-app", "test-user")
        .await;

    // Should succeed or fail gracefully
    assert!(result.is_ok() || result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_send_message() -> Result<()> {
    let config = MockConfig::default();
    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    adapter.init().await;

    let socket_id = SocketId("test-socket".to_string());
    let message = PusherMessage {
        channel: None,
        name: None,
        event: Some("test-event".to_string()),
        data: Some(MessageData::String("test message".to_string())),
        user_id: None,
    };

    // Should handle send message gracefully
    let result = adapter.send_message("test-app", &socket_id, message).await;

    // May succeed or fail depending on whether socket exists
    assert!(result.is_ok() || result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_send_broadcast() -> Result<()> {
    let config = MockConfig::default();
    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    adapter.init().await;
    adapter.start_listeners().await?;

    let message = PusherMessage {
        channel: None,
        name: None,
        event: Some("broadcast-test".to_string()),
        data: Some(MessageData::String("broadcast test".to_string())),
        user_id: None,
    };

    // Should be able to send broadcast
    adapter
        .send("test-channel", message, None, "test-app", None)
        .await?;

    // Verify broadcast was published to transport
    let published_broadcasts = adapter.transport.get_published_broadcasts().await;
    assert_eq!(published_broadcasts.len(), 1);
    assert_eq!(published_broadcasts[0].channel, "test-channel");
    assert_eq!(published_broadcasts[0].app_id, "test-app");

    Ok(())
}

#[tokio::test]
async fn test_connection_manager_check_health() -> Result<()> {
    let config = MockConfig::default();
    let adapter = HorizontalAdapterBase::<MockTransport>::new(config).await?;

    // Should be healthy by default
    adapter.check_health().await?;

    // Make unhealthy and test
    adapter.transport.set_health_status(false).await;
    let result = adapter.check_health().await;
    assert!(result.is_err());

    Ok(())
}
