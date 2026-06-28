//! Per-pod `active_channels` gauge in a clustered (horizontal) deployment,
//! where the local node count can differ from the cluster-wide count.

use crate::adapter::horizontal_adapter_helpers::MockConfig;
use ahash::AHashMap;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::channel_manager::ChannelManager;
use sockudo_adapter::horizontal_adapter::RequestType;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::PusherMessage;
use std::sync::Arc;

const APP_ID: &str = "test-app";
// Seeded on both remote mock nodes, so its cluster-wide count exceeds the local.
const SHARED_CHANNEL: &str = "channel-1";

async fn multi_node() -> Arc<dyn ConnectionManager + Send + Sync> {
    let adapter = MockConfig::create_multi_node_adapter().await.unwrap();
    // Wire response handlers, else cross-node count queries time out to 0.
    adapter.start_listeners().await.unwrap();
    Arc::new(adapter)
}

/// First local subscriber activates the gauge even when the channel already
/// exists cluster-wide.
#[tokio::test]
async fn subscribe_activates_on_local_transition_not_cluster_count() {
    let cm = multi_node().await;
    let socket = SocketId::new();
    let msg = PusherMessage::channel_event("pusher:subscribe", SHARED_CHANNEL, sonic_rs::json!({}));

    let resp = ChannelManager::subscribe(
        &cm,
        &socket.to_string(),
        &msg,
        SHARED_CHANNEL,
        false,
        APP_ID,
    )
    .await
    .unwrap();

    let local = cm
        .get_local_channel_socket_count(APP_ID, SHARED_CHANNEL)
        .await;
    let cluster = cm.get_channel_socket_count(APP_ID, SHARED_CHANNEL).await;

    assert_eq!(local, 1);
    assert!(cluster > local, "cluster={cluster}, local={local}");
    assert!(resp.activated_locally);
}

/// A re-subscribe by an already-present socket does not activate again.
#[tokio::test]
async fn resubscribe_does_not_activate_again() {
    let cm = multi_node().await;
    let socket = SocketId::new();
    let msg = PusherMessage::channel_event("pusher:subscribe", SHARED_CHANNEL, sonic_rs::json!({}));

    let first = ChannelManager::subscribe(
        &cm,
        &socket.to_string(),
        &msg,
        SHARED_CHANNEL,
        false,
        APP_ID,
    )
    .await
    .unwrap();
    assert!(first.activated_locally);

    let second = ChannelManager::subscribe(
        &cm,
        &socket.to_string(),
        &msg,
        SHARED_CHANNEL,
        false,
        APP_ID,
    )
    .await
    .unwrap();
    assert!(!second.activated_locally);
}

/// The last local subscriber leaving deactivates locally even while remote nodes
/// still hold the channel.
#[tokio::test]
async fn unsubscribe_empties_local_while_cluster_still_holds_channel() {
    let cm = multi_node().await;
    let socket = SocketId::new();

    cm.add_to_channel(APP_ID, SHARED_CHANNEL, &socket)
        .await
        .unwrap();

    let leave = ChannelManager::unsubscribe(&cm, &socket.to_string(), SHARED_CHANNEL, APP_ID, None)
        .await
        .unwrap();

    let local = cm
        .get_local_channel_socket_count(APP_ID, SHARED_CHANNEL)
        .await;
    let cluster = cm.get_channel_socket_count(APP_ID, SHARED_CHANNEL).await;

    assert_eq!(local, 0);
    assert!(cluster > 0, "cluster={cluster}");
    assert!(leave.remaining_connections.unwrap() > 0);
}

/// Churn-path subscribe only needs the local transition. Cluster-wide counts are
/// emitted later by handler notification code, not inside ChannelManager.
#[tokio::test]
async fn subscribe_does_not_fan_out_for_cluster_count() {
    let adapter = Arc::new(MockConfig::create_multi_node_adapter().await.unwrap());
    adapter.start_listeners().await.unwrap();
    let cm: Arc<dyn ConnectionManager + Send + Sync> = adapter.clone();
    let socket = SocketId::new();
    let msg = PusherMessage::channel_event("pusher:subscribe", SHARED_CHANNEL, sonic_rs::json!({}));

    let resp = ChannelManager::subscribe_local(
        &cm,
        &socket.to_string(),
        &msg,
        SHARED_CHANNEL,
        false,
        APP_ID,
    )
    .await
    .unwrap();

    assert_eq!(resp.channel_connections, Some(1));
    assert!(resp.activated_locally);
    assert!(
        adapter.transport.get_published_requests().await.is_empty(),
        "subscribe should not publish horizontal count/existence requests"
    );
}

/// Manual unsubscribe uses the same local transition path; callers that need a
/// cluster count perform one post-ack count query.
#[tokio::test]
async fn unsubscribe_local_does_not_fan_out_for_cluster_count() {
    let adapter = Arc::new(MockConfig::create_multi_node_adapter().await.unwrap());
    adapter.start_listeners().await.unwrap();
    let cm: Arc<dyn ConnectionManager + Send + Sync> = adapter.clone();
    let socket = SocketId::new();

    cm.add_to_channel(APP_ID, SHARED_CHANNEL, &socket)
        .await
        .unwrap();

    let leave =
        ChannelManager::unsubscribe_local(&cm, &socket.to_string(), SHARED_CHANNEL, APP_ID, None)
            .await
            .unwrap();

    assert_eq!(leave.remaining_connections, Some(0));
    assert!(
        adapter
            .transport
            .get_published_requests()
            .await
            .iter()
            .all(|request| request.request_type != RequestType::ChannelSocketsCount),
        "unsubscribe_local should not publish horizontal count requests"
    );
}

#[tokio::test]
async fn batch_socket_counts_use_registry_when_aggregate_counts_enabled() {
    let mut adapter = MockConfig::create_multi_node_adapter().await.unwrap();
    adapter.set_aggregate_counts(true);

    let mut remote_counts = AHashMap::new();
    remote_counts.insert(SHARED_CHANNEL.to_string(), 7);
    remote_counts.insert("channel-2".to_string(), 3);
    adapter
        .horizontal
        .apply_remote_channel_counts(APP_ID, "node-remote", remote_counts, false);

    let socket = SocketId::new();
    adapter
        .add_to_channel(APP_ID, SHARED_CHANNEL, &socket)
        .await
        .unwrap();

    let counts = adapter
        .get_batch_channel_socket_counts(APP_ID, &[SHARED_CHANNEL, "channel-2", "channel-3"])
        .await
        .unwrap();

    assert_eq!(counts.get(SHARED_CHANNEL), Some(&8));
    assert_eq!(counts.get("channel-2"), Some(&3));
    assert!(!counts.contains_key("channel-3"));

    let published_batch_requests = adapter
        .transport
        .get_published_requests()
        .await
        .into_iter()
        .filter(|request| request.request_type == RequestType::BatchChannelSocketsCount)
        .count();
    assert_eq!(
        published_batch_requests, 0,
        "aggregate batch socket counts should issue zero NATS requests"
    );
}
