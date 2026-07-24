use crate::adapter::horizontal_adapter_helpers::MockConfig;
use crate::mocks::connection_handler_mock::{MockAppManager, MockCacheManager};
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::horizontal_adapter::RequestType;
use sockudo_core::app::{App, AppLimitsPolicy, AppManager, AppPolicy};
use sockudo_core::cache::CacheManager;
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::error::Result;
use sockudo_core::namespace::Namespace;
use sockudo_core::options::ServerOptions;
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
        .insert(socket_id);

    namespace.sockets.insert(socket_id, ws_ref.clone());

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

/// By default, presence transition checks keep strict request/reply semantics
/// even if the replicated registry already has a matching remote entry.
#[tokio::test]
async fn test_user_has_connections_uses_remote_request_for_presence_by_default() -> Result<()> {
    let adapter = MockConfig::create_multi_node_adapter().await?;

    adapter
        .horizontal
        .add_presence_entry(
            "remote-node",
            "presence-chat",
            "remote-socket-1",
            "remote-user",
            "app-1",
            None,
        )
        .await;

    let result = adapter
        .user_has_connections_in_channel("remote-user", "app-1", "presence-chat", None)
        .await?;

    assert!(
        !result,
        "strict mode should ignore the eventually consistent presence registry"
    );

    let requests = adapter.transport.get_published_requests().await;
    let count_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.request_type == RequestType::CountUserConnectionsInChannel)
        .collect();

    assert_eq!(
        count_requests.len(),
        1,
        "strict presence transition check should issue one remote count request"
    );
    assert_eq!(
        count_requests[0].user_id,
        Some("remote-user".to_string()),
        "request must carry the queried user_id"
    );
    assert_eq!(
        count_requests[0].channel,
        Some("presence-chat".to_string()),
        "request must carry the queried channel"
    );

    Ok(())
}

#[tokio::test]
async fn test_user_has_connections_uses_presence_registry_when_fast_transitions_enabled()
-> Result<()> {
    let mut adapter = MockConfig::create_multi_node_adapter().await?;
    adapter.set_fast_presence_transitions(true);

    adapter
        .horizontal
        .add_presence_entry(
            "remote-node",
            "presence-chat",
            "remote-socket-1",
            "remote-user",
            "app-1",
            None,
        )
        .await;

    let result = adapter
        .user_has_connections_in_channel("remote-user", "app-1", "presence-chat", None)
        .await?;

    assert!(
        result,
        "fast mode should use the replicated presence registry"
    );

    let requests = adapter.transport.get_published_requests().await;
    let count_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.request_type == RequestType::CountUserConnectionsInChannel)
        .collect();

    assert!(
        count_requests.is_empty(),
        "fast presence transition check should issue zero remote count requests, got {}",
        count_requests.len()
    );

    Ok(())
}

/// Non-presence user-connection calls keep the existing request/reply behavior.
#[tokio::test]
async fn test_user_has_connections_falls_back_to_remote_for_non_presence_channel() -> Result<()> {
    let adapter = MockConfig::create_multi_node_adapter().await?;

    let result = adapter
        .user_has_connections_in_channel("absent-user", "app-1", "private-chat", None)
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
        Some("private-chat".to_string()),
        "request must carry the queried channel"
    );

    Ok(())
}

#[tokio::test]
async fn test_count_user_connections_in_channel_uses_remote_request_for_presence_by_default()
-> Result<()> {
    let adapter = MockConfig::create_multi_node_adapter().await?;

    adapter
        .horizontal
        .add_presence_entry(
            "remote-node-a",
            "presence-chat",
            "remote-socket-a",
            "remote-user",
            "app-1",
            None,
        )
        .await;
    adapter
        .horizontal
        .add_presence_entry(
            "remote-node-b",
            "presence-chat",
            "remote-socket-b",
            "remote-user",
            "app-1",
            None,
        )
        .await;

    let count = adapter
        .count_user_connections_in_channel("remote-user", "app-1", "presence-chat", None)
        .await?;

    assert_eq!(
        count, 0,
        "strict mode should use request/reply instead of the replicated presence registry"
    );

    let requests = adapter.transport.get_published_requests().await;
    let count_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.request_type == RequestType::CountUserConnectionsInChannel)
        .collect();

    assert_eq!(
        count_requests.len(),
        1,
        "strict presence count should issue one remote count request"
    );
    assert_eq!(
        count_requests[0].user_id,
        Some("remote-user".to_string()),
        "request must carry the queried user_id"
    );
    assert_eq!(
        count_requests[0].channel,
        Some("presence-chat".to_string()),
        "request must carry the queried channel"
    );

    Ok(())
}

#[tokio::test]
async fn test_count_user_connections_in_channel_uses_registry_when_fast_transitions_enabled()
-> Result<()> {
    let mut adapter = MockConfig::create_multi_node_adapter().await?;
    adapter.set_fast_presence_transitions(true);

    adapter
        .horizontal
        .add_presence_entry(
            "remote-node-a",
            "presence-chat",
            "remote-socket-a",
            "remote-user",
            "app-1",
            None,
        )
        .await;
    adapter
        .horizontal
        .add_presence_entry(
            "remote-node-b",
            "presence-chat",
            "remote-socket-b",
            "remote-user",
            "app-1",
            None,
        )
        .await;

    let count = adapter
        .count_user_connections_in_channel("remote-user", "app-1", "presence-chat", None)
        .await?;

    assert_eq!(count, 2);

    let requests = adapter.transport.get_published_requests().await;
    let count_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.request_type == RequestType::CountUserConnectionsInChannel)
        .collect();

    assert!(
        count_requests.is_empty(),
        "fast presence count should issue zero remote count requests, got {}",
        count_requests.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_presence_member_count_uses_registry_without_channel_members_request() -> Result<()> {
    let adapter = Arc::new(MockConfig::create_multi_node_adapter().await?);

    adapter
        .horizontal
        .add_presence_entry(
            "remote-node",
            "presence-chat",
            "remote-socket",
            "remote-user",
            "app-1",
            None,
        )
        .await;

    let handler = ConnectionHandler::builder(
        Arc::new(MockAppManager::new()) as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()) as Arc<dyn CacheManager + Send + Sync>,
        ServerOptions::default(),
    )
    .build();
    let app = App::from_policy(
        "app-1".to_string(),
        "key".to_string(),
        "secret".to_string(),
        true,
        AppPolicy {
            limits: AppLimitsPolicy {
                max_presence_members_per_channel: Some(100),
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let count = handler
        .get_channel_member_count(&app, "presence-chat")
        .await?;

    assert_eq!(count, 1);

    let requests = adapter.transport.get_published_requests().await;
    let channel_member_requests: Vec<_> = requests
        .iter()
        .filter(|r| r.request_type == RequestType::ChannelMembers)
        .collect();

    assert!(
        channel_member_requests.is_empty(),
        "presence member count should issue zero ChannelMembers requests, got {}",
        channel_member_requests.len()
    );

    Ok(())
}
