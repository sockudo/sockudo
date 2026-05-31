use sockudo_adapter::ConnectionManager;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_core::websocket::SocketId;
use std::collections::BTreeMap;

// remove_from_channel must clean the filter index even when the socket is no
// longer in the namespace, as happens on the disconnect path.
#[tokio::test]
async fn remove_from_channel_cleans_filter_index_when_socket_already_gone() {
    let adapter = LocalAdapter::new();
    let app_id = "app-1";
    let channel = "my-channel";
    let socket_id = SocketId::from_string("1.1").unwrap();

    // A subscribe with no client filter registers the socket in no_filter
    adapter
        .get_filter_index()
        .add_socket_filter(channel, socket_id, None);

    let before = adapter.get_filter_index().lookup(channel, &BTreeMap::new());
    assert!(before.no_filter.contains(&socket_id));

    // Socket absent from the namespace, mirroring the disconnect path
    adapter
        .remove_from_channel(app_id, channel, &socket_id)
        .await
        .unwrap();

    let after = adapter.get_filter_index().lookup(channel, &BTreeMap::new());
    assert!(
        !after.no_filter.contains(&socket_id),
        "filter index leaked socket {socket_id} after disconnect"
    );
}
