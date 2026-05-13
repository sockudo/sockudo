use crate::adapter::horizontal_adapter_helpers::{MockConfig, MockTransport};
use sockudo_adapter::horizontal_adapter::RequestType;
use sockudo_adapter::horizontal_adapter_base::HorizontalAdapterBase;
use sockudo_adapter::horizontal_transport::HorizontalTransport;

#[tokio::test]
async fn default_sync_sends_single_message() {
    let config = MockConfig::default();
    let adapter = HorizontalAdapterBase::<MockTransport>::new(config)
        .await
        .unwrap();

    let our_node_id = adapter.node_id.clone();

    for i in 0..250 {
        adapter
            .horizontal
            .add_presence_entry(
                &our_node_id,
                &format!("presence-channel-{}", i),
                &format!("socket-{}", i),
                &format!("user-{}", i),
                "app-id",
                Some(sonic_rs::json!({"name": format!("User {}", i)})),
            )
            .await;
    }

    adapter
        .transport
        .sync_presence_state_to_node(&adapter.horizontal, "target-node")
        .await
        .unwrap();

    let requests: Vec<_> = adapter
        .transport
        .get_published_requests()
        .await
        .into_iter()
        .filter(|r| r.request_type == RequestType::PresenceStateSync)
        .collect();

    assert_eq!(
        requests.len(),
        1,
        "Expected exactly one PresenceStateSync message, got {}",
        requests.len()
    );
    assert_eq!(
        requests[0].target_node_id.as_deref(),
        Some("target-node"),
        "Expected target_node_id == \"target-node\""
    );
}
