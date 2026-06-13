use crate::adapter::horizontal_adapter_helpers::MockConfig;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::horizontal_adapter::RequestType;
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::error::Result;
use sockudo_core::namespace::Namespace;
use sockudo_core::websocket::{SocketId, WebSocket, WebSocketRef};
use sockudo_ws::axum_integration;
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, WebSocketStream};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

async fn create_test_writer() -> axum_integration::WebSocketWriter {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let ws = axum_integration::WebSocket::from_tcp(stream, WsConfig::default());
        let (_reader, writer) = ws.split();
        writer
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (_client_ws, _): (WebSocketStream<sockudo_ws::Stream<Http1>>, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();

    server.await.unwrap()
}

async fn seed_local_user_connection(
    adapter: &sockudo_adapter::horizontal_adapter_base::HorizontalAdapterBase<
        crate::adapter::horizontal_adapter_helpers::MockTransport,
    >,
    app_id: &str,
    channel: &str,
    user_id: &str,
    socket_id: SocketId,
) {
    let writer = create_test_writer().await;
    let mut ws = WebSocket::new(socket_id, writer);
    ws.subscribe_to_channel(channel.to_string());
    ws.state.user_id = Some(user_id.to_string());

    let ws_ref = WebSocketRef::new(ws);

    let namespace = adapter
        .local_adapter
        .namespaces
        .entry(app_id.to_string())
        .or_insert_with(|| Arc::new(Namespace::new(app_id.to_string())))
        .clone();

    namespace
        .users
        .entry(user_id.to_string())
        .or_default()
        .insert(ws_ref.clone());

    namespace
        .presence_data
        .entry(socket_id)
        .or_default()
        .insert(
            channel.to_string(),
            PresenceMemberInfo {
                user_id: user_id.to_string(),
                user_info: None,
            },
        );
}

/// Test that local connections short-circuit without a cross-cluster request
#[tokio::test]
async fn test_user_has_connections_returns_true_from_local_without_remote_request() -> Result<()> {
    let adapter = MockConfig::create_multi_node_adapter().await?;

    let socket_id = SocketId::from_string("local-socket-1").unwrap();
    seed_local_user_connection(&adapter, "app-1", "presence-chat", "local-user", socket_id).await;

    let result = adapter
        .user_has_connections_in_channel("local-user", "app-1", "presence-chat", None)
        .await?;

    assert!(result, "should be true when user has a local connection");

    let requests = adapter.transport.get_published_requests().await;
    let remote_count_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.request_type == RequestType::CountUserConnectionsInChannel)
        .collect();
    assert!(
        remote_count_requests.is_empty(),
        "expected zero remote requests when local count > 0, got {}",
        remote_count_requests.len()
    );

    Ok(())
}

/// Test that absent local connections trigger a cross-cluster request
#[tokio::test]
async fn test_user_has_connections_falls_back_to_remote_when_no_local_connections() -> Result<()> {
    let adapter = MockConfig::create_multi_node_adapter().await?;

    let result = adapter
        .user_has_connections_in_channel("absent-user", "app-1", "presence-chat", None)
        .await?;

    assert!(
        !result,
        "should be false when neither local nor remote nodes have the user"
    );

    let requests = adapter.transport.get_published_requests().await;
    let count_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.request_type == RequestType::CountUserConnectionsInChannel)
        .collect();

    assert_eq!(
        count_requests.len(),
        1,
        "exactly one remote request expected"
    );
    assert_eq!(
        count_requests[0].user_id,
        Some("absent-user".to_string()),
        "request must carry the queried user_id"
    );
    assert_eq!(
        count_requests[0].channel,
        Some("presence-chat".to_string()),
        "request must carry the queried channel"
    );

    Ok(())
}
