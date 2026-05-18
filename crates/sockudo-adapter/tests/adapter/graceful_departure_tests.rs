use crate::adapter::horizontal_adapter_helpers::{MockConfig, MockNodeState, MockTransport};
use sockudo_adapter::connection_manager::ConnectionManager;
use sockudo_adapter::horizontal_adapter::{
    RequestBody, RequestType, current_timestamp, generate_request_id,
};
use sockudo_adapter::horizontal_adapter_base::HorizontalAdapterBase;
use sockudo_adapter::horizontal_transport::HorizontalTransport;

#[tokio::test]
async fn test_announce_node_departure_publishes_node_dead() {
    let config = MockConfig {
        node_states: vec![MockNodeState::new("single-node")],
        ..Default::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config)
        .await
        .unwrap();

    let own_node_id = adapter.get_node_id();

    adapter.announce_node_departure().await.unwrap();

    let published = adapter.transport.get_published_requests().await;
    let node_dead: Vec<_> = published
        .iter()
        .filter(|r| r.request_type == RequestType::NodeDead)
        .collect();

    assert_eq!(node_dead.len(), 1);
    assert_eq!(
        node_dead[0].dead_node_id.as_deref(),
        Some(own_node_id.as_str())
    );
    assert_eq!(node_dead[0].node_id, own_node_id);
    assert_eq!(node_dead[0].app_id, "cluster");
}

#[tokio::test]
async fn test_receiving_node_dead_prunes_peer_heartbeat() {
    let config = MockConfig {
        node_states: vec![MockNodeState::new("single-node")],
        ..Default::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config)
        .await
        .unwrap();

    adapter
        .horizontal
        .add_discovered_node_for_test("peer-node".to_string())
        .await;

    assert_eq!(adapter.horizontal.get_effective_node_count().await, 2);

    let departure_request = RequestBody {
        request_id: generate_request_id(),
        node_id: "peer-node".to_string(),
        app_id: "cluster".to_string(),
        request_type: RequestType::NodeDead,
        channel: None,
        socket_id: None,
        user_id: None,
        user_info: None,
        timestamp: Some(current_timestamp()),
        dead_node_id: Some("peer-node".to_string()),
        target_node_id: None,
        channels: None,
    };

    adapter
        .horizontal
        .process_request(departure_request)
        .await
        .expect("processing NodeDead must succeed");

    assert_eq!(adapter.horizontal.get_effective_node_count().await, 1);
}

#[tokio::test]
async fn test_announce_node_departure_ok_when_transport_unhealthy() {
    let config = MockConfig {
        node_states: vec![MockNodeState::new("single-node")],
        ..Default::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config)
        .await
        .unwrap();

    adapter.transport.set_health_status(false).await;

    let result = adapter.announce_node_departure().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_mock_transport_node_count_is_not_real_time() {
    let config = MockConfig {
        node_states: vec![MockNodeState::new("single-node")],
        ..Default::default()
    };

    let adapter = HorizontalAdapterBase::<MockTransport>::new(config)
        .await
        .unwrap();

    assert!(!adapter.transport.node_count_is_real_time());
}
