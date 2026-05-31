//! Per-pod `active_channels` gauge in a clustered (horizontal) deployment,
//! where the local node count can differ from the cluster-wide count.

use crate::adapter::horizontal_adapter_helpers::MockConfig;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::channel_manager::ChannelManager;
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
