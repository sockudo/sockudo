use crate::adapter::horizontal_adapter_helpers::{MockConfig, MockTransport};
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::horizontal_adapter_base::HorizontalAdapterBase;
use sonic_rs::json;

#[tokio::test]
async fn test_local_members_merges_local_and_remote() {
    let config = MockConfig::default();
    let adapter = HorizontalAdapterBase::<MockTransport>::new(config)
        .await
        .unwrap();

    adapter
        .horizontal
        .add_presence_entry(
            "node-2",
            "presence-chat",
            "socket-a",
            "user-local",
            "app-1",
            Some(json!({"name": "Alice"})),
        )
        .await;
    adapter
        .horizontal
        .add_presence_entry(
            "node-3",
            "presence-chat",
            "socket-b",
            "user-remote",
            "app-1",
            Some(json!({"name": "Bob"})),
        )
        .await;

    let result = adapter
        .get_local_channel_members("app-1", "presence-chat")
        .await
        .unwrap();

    assert!(
        result.contains_key("user-local"),
        "user-local (node-2) should be in merged result"
    );
    assert!(
        result.contains_key("user-remote"),
        "user-remote (node-3) should be in merged result"
    );

    let requests = adapter.transport.get_published_requests().await;
    assert!(
        requests.is_empty(),
        "get_local_channel_members must issue zero NATS requests, got: {:?}",
        requests.len()
    );
}

#[tokio::test]
async fn test_local_members_filters_app_and_excludes_self_node() {
    let config = MockConfig::default();
    let adapter = HorizontalAdapterBase::<MockTransport>::new(config)
        .await
        .unwrap();
    let self_node = adapter.node_id.clone();

    adapter
        .horizontal
        .add_presence_entry(
            &self_node,
            "presence-chat",
            "socket-self",
            "user-self",
            "app-1",
            None,
        )
        .await;

    adapter
        .horizontal
        .add_presence_entry(
            "node-2",
            "presence-chat",
            "socket-a1",
            "user-app1",
            "app-1",
            None,
        )
        .await;

    adapter
        .horizontal
        .add_presence_entry(
            "node-2",
            "presence-chat",
            "socket-a2",
            "user-app2",
            "app-2",
            None,
        )
        .await;

    let result = adapter
        .get_local_channel_members("app-1", "presence-chat")
        .await
        .unwrap();

    assert!(
        result.contains_key("user-app1"),
        "user-app1 from remote node under app-1 must be included"
    );
    assert!(
        !result.contains_key("user-self"),
        "user-self registered under own node_id must be excluded"
    );
    assert!(
        !result.contains_key("user-app2"),
        "user-app2 from app-2 must be excluded when querying app-1"
    );
}

#[tokio::test]
async fn test_local_members_deduplicates_multi_socket_user() {
    let config = MockConfig::default();
    let adapter = HorizontalAdapterBase::<MockTransport>::new(config)
        .await
        .unwrap();

    adapter
        .horizontal
        .add_presence_entry(
            "node-2",
            "presence-chat",
            "socket-a",
            "user-1",
            "app-1",
            Some(json!({"name": "Eve", "socket": "a"})),
        )
        .await;
    adapter
        .horizontal
        .add_presence_entry(
            "node-3",
            "presence-chat",
            "socket-b",
            "user-1",
            "app-1",
            Some(json!({"name": "Eve", "socket": "b"})),
        )
        .await;

    let result = adapter
        .get_local_channel_members("app-1", "presence-chat")
        .await
        .unwrap();

    assert_eq!(
        result.len(),
        1,
        "user-1 must appear exactly once regardless of socket count; got {} entries",
        result.len()
    );
    assert!(
        result.contains_key("user-1"),
        "deduplicated result must contain user-1"
    );
}
