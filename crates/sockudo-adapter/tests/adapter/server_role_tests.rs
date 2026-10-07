use sockudo_adapter::connection_manager::ConnectionManager;
use sockudo_adapter::horizontal_adapter::RequestType;
use sockudo_adapter::horizontal_adapter_base::HorizontalAdapterBase;
use sockudo_core::error::Result;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use std::time::{Duration, Instant};

use super::horizontal_adapter_helpers::{MockConfig, MockNodeState, MockTransport};

#[tokio::test]
async fn test_api_only_always_publishes_horizontally() -> Result<()> {
    let config = MockConfig {
        node_states: vec![MockNodeState::new("single-node")],
        ..Default::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config.clone()).await?;
    adapter.cluster_health_enabled = true;
    adapter.set_api_only(true);
    adapter.init().await;

    let message = PusherMessage {
        channel: Some("test-channel".to_string()),
        event: Some("test-event".to_string()),
        data: Some(MessageData::String("test message".to_string())),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    };

    adapter
        .send("test-channel", message, None, "test-app", None)
        .await?;

    let published_broadcasts = adapter.transport.get_published_broadcasts().await;
    assert_eq!(
        published_broadcasts.len(),
        1,
        "API-only mode must publish broadcasts even when it is the only node"
    );

    Ok(())
}

#[tokio::test]
async fn test_api_only_should_skip_horizontal_communication_is_false() -> Result<()> {
    let config = MockConfig {
        node_states: vec![MockNodeState::new("single-node")],
        ..Default::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config.clone()).await?;
    adapter.cluster_health_enabled = true;
    adapter.set_api_only(true);

    assert!(
        !adapter.should_skip_horizontal_communication().await,
        "API-only mode must never skip horizontal publishing"
    );

    Ok(())
}

#[tokio::test]
async fn test_default_should_skip_horizontal_communication_is_true() -> Result<()> {
    let config = MockConfig {
        node_states: vec![MockNodeState::new("single-node")],
        ..Default::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config.clone()).await?;
    adapter.cluster_health_enabled = true;
    adapter.settle_discovery_for_test();

    assert!(
        adapter.should_skip_horizontal_communication().await,
        "default single-node mode must keep the horizontal short-circuit"
    );

    Ok(())
}

#[tokio::test]
async fn test_api_only_request_reply_short_circuits() -> Result<()> {
    let config = MockConfig {
        node_states: vec![MockNodeState::new("single-node")],
        ..Default::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config.clone()).await?;
    adapter.cluster_health_enabled = true;
    adapter.set_api_only(true);
    adapter.init().await;

    let start = Instant::now();
    let response = adapter
        .send_request("test-app", RequestType::SocketsCount, None, None, None)
        .await?;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(100),
        "api-only request/reply must return immediately, took {:?}",
        elapsed
    );
    assert!(response.complete, "api-only response must be complete");
    assert_eq!(response.sockets_count, 0);
    assert_eq!(response.responses_received, 0);

    let published_requests = adapter.transport.get_published_requests().await;
    let app_requests: Vec<_> = published_requests
        .into_iter()
        .filter(|r| r.request_type != RequestType::Heartbeat)
        .collect();
    assert_eq!(
        app_requests.len(),
        0,
        "api-only adapter must not publish requests to the transport"
    );

    Ok(())
}

#[tokio::test]
async fn test_api_only_get_sockets_count_returns_local() -> Result<()> {
    let config = MockConfig {
        node_states: vec![MockNodeState::new("single-node")],
        ..Default::default()
    };

    let mut adapter = HorizontalAdapterBase::<MockTransport>::new(config.clone()).await?;
    adapter.cluster_health_enabled = true;
    adapter.set_api_only(true);
    adapter.init().await;

    let start = Instant::now();
    let count = adapter.get_sockets_count("test-app").await?;
    let elapsed = start.elapsed();

    assert_eq!(count, 0, "api-only pod has no local connections");
    assert!(
        elapsed < Duration::from_millis(100),
        "get_sockets_count must not hang on api-only adapter, took {:?}",
        elapsed
    );

    Ok(())
}
