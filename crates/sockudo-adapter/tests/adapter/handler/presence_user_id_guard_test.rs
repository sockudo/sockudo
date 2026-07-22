use crate::mocks::connection_handler_mock::MockCacheManager;
use futures_util::StreamExt;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::handler::types::SubscriptionRequest;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::options::ServerOptions;
use sockudo_core::token::Token;
use sockudo_core::websocket::{SocketId, UserInfo, WebSocket, WebSocketBufferConfig, WebSocketRef};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration;
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Message as WsMessage, WebSocketStream};
use sonic_rs::JsonValueTrait;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

type TestClient = WebSocketStream<sockudo_ws::Stream<Http1>>;

async fn create_test_pair() -> (axum_integration::WebSocketWriter, TestClient) {
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
    let (client_ws, _): (TestClient, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();

    (server.await.unwrap(), client_ws)
}

async fn create_test_writer() -> axum_integration::WebSocketWriter {
    create_test_pair().await.0
}

async fn make_ws_ref(user_id: Option<&str>) -> WebSocketRef {
    let writer = create_test_writer().await;
    let mut ws = WebSocket::new(SocketId::new(), writer);
    if let Some(uid) = user_id {
        ws.state.user_id = Some(uid.to_string());
    }
    WebSocketRef::new(ws)
}

struct SigninHarness {
    handler: ConnectionHandler,
    adapter: Arc<LocalAdapter>,
    app: App,
    socket_id: SocketId,
    ws_ref: WebSocketRef,
}

struct PresenceHarness {
    handler: ConnectionHandler,
    adapter: Arc<LocalAdapter>,
    app_manager: Arc<MemoryAppManager>,
    app: App,
}

async fn make_presence_harness() -> PresenceHarness {
    let app = App::from_policy(
        "presence-lifecycle-app".to_string(),
        "presence-key".to_string(),
        "presence-secret".to_string(),
        true,
        AppPolicy::default(),
    );
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app.clone()).await.unwrap();
    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        ServerOptions::default(),
    )
    .local_adapter(adapter.clone())
    .build();

    PresenceHarness {
        handler,
        adapter,
        app_manager,
        app,
    }
}

async fn add_presence_socket(harness: &PresenceHarness) -> (SocketId, TestClient) {
    let socket_id = SocketId::new();
    let (writer, client) = create_test_pair().await;
    harness
        .adapter
        .add_socket(
            socket_id,
            writer,
            &harness.app.id,
            harness.app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V1,
            WireFormat::Json,
            true,
            sockudo_protocol::AppendMode::Delta,
        )
        .await
        .unwrap();
    (socket_id, client)
}

fn presence_request(
    harness: &PresenceHarness,
    socket_id: &SocketId,
    channel: &str,
    user_id: &str,
) -> SubscriptionRequest {
    let channel_data = sonic_rs::json!({
        "user_id": user_id,
        "user_info": {"name": user_id}
    })
    .to_string();
    let signature = Token::new(harness.app.key.clone(), harness.app.secret.clone())
        .sign(&format!("{socket_id}:{channel}:{channel_data}"));

    SubscriptionRequest {
        channel: channel.to_string(),
        auth: Some(format!("{}:{signature}", harness.app.key)),
        channel_data: Some(channel_data),
        #[cfg(feature = "tag-filtering")]
        tags_filter: None,
        #[cfg(feature = "delta")]
        delta: None,
        rewind: None,
        event_name_filter: None,
        annotation_subscribe: false,
    }
}

async fn recv_event(
    client: &mut TestClient,
    event: &str,
) -> sockudo_protocol::messages::PusherMessage {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let frame = client
                .next()
                .await
                .expect("websocket stream ended")
                .expect("websocket receive failed");
            let message: sockudo_protocol::messages::PusherMessage = match frame {
                WsMessage::Text(bytes) => sonic_rs::from_slice(bytes.as_ref()).unwrap(),
                WsMessage::Binary(bytes) => sonic_rs::from_slice(&bytes).unwrap(),
                other => panic!("unexpected websocket frame: {other:?}"),
            };
            if message.event.as_deref() == Some(event) {
                return message;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {event}"))
}

async fn make_signin_harness(initial_user_id: Option<&str>) -> SigninHarness {
    let app = App::from_policy(
        "signin-migration-app".to_string(),
        "signin-key".to_string(),
        "signin-secret".to_string(),
        true,
        AppPolicy::default(),
    );
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app.clone()).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        ServerOptions::default(),
    )
    .local_adapter(adapter.clone())
    .build();

    let socket_id = SocketId::new();
    adapter
        .add_socket(
            socket_id,
            create_test_writer().await,
            &app.id,
            app_manager as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V1,
            WireFormat::Json,
            true,
            sockudo_protocol::AppendMode::Delta,
        )
        .await
        .unwrap();
    let ws_ref = adapter.get_connection(&socket_id, &app.id).await.unwrap();
    if let Some(user_id) = initial_user_id {
        ws_ref.inner.lock().await.state.user_id = Some(user_id.to_string());
        adapter.add_user(ws_ref.clone()).await.unwrap();
    }

    SigninHarness {
        handler,
        adapter,
        app,
        socket_id,
        ws_ref,
    }
}

#[tokio::test]
async fn test_presence_subscription_guard_preserves_signin_user_id() {
    let ws_ref = make_ws_ref(Some("real-user-42")).await;

    let member_user_id = "real-user-42:123.456".to_string();
    {
        let mut conn_locked = ws_ref.inner.lock().await;
        if conn_locked.state.user_id.is_none() {
            conn_locked.state.user_id = Some(member_user_id.clone());
        }
    }

    let conn_locked = ws_ref.inner.lock().await;
    assert_eq!(
        conn_locked.state.user_id.as_deref(),
        Some("real-user-42"),
        "user_id set by signin must not be overwritten by presence subscription"
    );
}

#[tokio::test]
async fn test_presence_subscription_guard_sets_user_id_when_none() {
    let ws_ref = make_ws_ref(None).await;

    let member_user_id = "presence-user-99".to_string();
    {
        let mut conn_locked = ws_ref.inner.lock().await;
        if conn_locked.state.user_id.is_none() {
            conn_locked.state.user_id = Some(member_user_id.clone());
        }
    }

    let conn_locked = ws_ref.inner.lock().await;
    assert_eq!(
        conn_locked.state.user_id.as_deref(),
        Some("presence-user-99"),
        "user_id should be set from presence member when no signin occurred"
    );
}

#[tokio::test]
async fn production_signin_migrates_composite_routing_identity() {
    let harness = make_signin_harness(Some("alice:presence-socket")).await;

    harness
        .handler
        .update_connection_with_user_info(
            &harness.socket_id,
            &harness.app,
            &UserInfo {
                id: "alice".to_string(),
                watchlist: None,
                info: Some(sonic_rs::json!({"name": "Alice"})),
                capabilities: None,
                meta: None,
            },
        )
        .await
        .unwrap();

    assert!(
        harness
            .adapter
            .get_user_sockets("alice:presence-socket", &harness.app.id)
            .await
            .unwrap()
            .is_empty(),
        "production signin must remove the prior presence-derived route"
    );
    let current = harness
        .adapter
        .get_user_sockets("alice", &harness.app.id)
        .await
        .unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(*current[0].get_socket_id_sync(), harness.socket_id);
    assert_eq!(
        harness
            .ws_ref
            .inner
            .lock()
            .await
            .state
            .user_info
            .as_ref()
            .and_then(|info| info.info.as_ref())
            .and_then(|info| info.get("name"))
            .and_then(|name| name.as_str()),
        Some("Alice")
    );
}

#[tokio::test]
async fn production_signin_rejects_disconnecting_socket_without_reindexing() {
    let harness = make_signin_harness(Some("alice:presence-socket")).await;
    harness.ws_ref.inner.lock().await.state.disconnecting = true;

    let result = harness
        .handler
        .update_connection_with_user_info(
            &harness.socket_id,
            &harness.app,
            &UserInfo {
                id: "alice".to_string(),
                watchlist: None,
                info: None,
                capabilities: None,
                meta: None,
            },
        )
        .await;

    assert!(
        matches!(result, Err(sockudo_core::error::Error::ConnectionClosed(_))),
        "signin must return ConnectionClosed once teardown has started: {result:?}"
    );
    assert!(
        harness
            .adapter
            .get_user_sockets("alice", &harness.app.id)
            .await
            .unwrap()
            .is_empty(),
        "rejected signin must not create a new route"
    );
    assert_eq!(
        harness
            .adapter
            .get_user_sockets("alice:presence-socket", &harness.app.id)
            .await
            .unwrap()
            .len(),
        1,
        "rejected signin must leave existing metadata for teardown to remove"
    );
}

#[tokio::test]
async fn production_same_id_signin_updates_user_info_without_duplicate_route() {
    let harness = make_signin_harness(Some("alice")).await;

    harness
        .handler
        .update_connection_with_user_info(
            &harness.socket_id,
            &harness.app,
            &UserInfo {
                id: "alice".to_string(),
                watchlist: None,
                info: Some(sonic_rs::json!({"role": "operator"})),
                capabilities: None,
                meta: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        harness
            .adapter
            .get_user_sockets("alice", &harness.app.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        harness
            .ws_ref
            .inner
            .lock()
            .await
            .state
            .user_info
            .as_ref()
            .and_then(|info| info.info.as_ref())
            .and_then(|info| info.get("role"))
            .and_then(|role| role.as_str()),
        Some("operator")
    );
}

#[tokio::test]
async fn production_v1_presence_lifecycle_preserves_member_events_and_single_user_route() {
    let harness = make_presence_harness().await;
    let (alice_socket, mut alice_client) = add_presence_socket(&harness).await;
    let (bob_socket, mut bob_client) = add_presence_socket(&harness).await;

    harness
        .handler
        .handle_subscribe_request(
            &alice_socket,
            &harness.app,
            presence_request(&harness, &alice_socket, "presence-room", "alice"),
        )
        .await
        .unwrap();
    let alice_ack = recv_event(&mut alice_client, "pusher_internal:subscription_succeeded").await;
    assert_eq!(alice_ack.channel.as_deref(), Some("presence-room"));

    harness
        .handler
        .handle_subscribe_request(
            &alice_socket,
            &harness.app,
            presence_request(&harness, &alice_socket, "presence-room-two", "alice"),
        )
        .await
        .unwrap();
    recv_event(&mut alice_client, "pusher_internal:subscription_succeeded").await;
    assert_eq!(
        harness
            .adapter
            .get_user_sockets("alice", &harness.app.id)
            .await
            .unwrap()
            .len(),
        1,
        "multiple presence channels must retain one routing entry per socket"
    );

    harness
        .handler
        .handle_subscribe_request(
            &bob_socket,
            &harness.app,
            presence_request(&harness, &bob_socket, "presence-room", "bob"),
        )
        .await
        .unwrap();
    recv_event(&mut bob_client, "pusher_internal:subscription_succeeded").await;
    let added = recv_event(&mut alice_client, "pusher_internal:member_added").await;
    assert_eq!(added.channel.as_deref(), Some("presence-room"));
    assert!(
        added
            .data
            .as_ref()
            .is_some_and(|data| sonic_rs::to_string(data).unwrap().contains("bob")),
        "member_added must identify bob: {:?}",
        added.data
    );

    harness
        .handler
        .handle_disconnect(&harness.app.id, &bob_socket)
        .await
        .unwrap();
    let removed = recv_event(&mut alice_client, "pusher_internal:member_removed").await;
    assert_eq!(removed.channel.as_deref(), Some("presence-room"));
    assert!(
        removed
            .data
            .as_ref()
            .is_some_and(|data| sonic_rs::to_string(data).unwrap().contains("bob")),
        "member_removed must identify bob: {:?}",
        removed.data
    );

    harness
        .handler
        .handle_disconnect(&harness.app.id, &alice_socket)
        .await
        .unwrap();
    assert!(
        harness
            .adapter
            .get_user_sockets("alice", &harness.app.id)
            .await
            .unwrap()
            .is_empty(),
        "last disconnect must remove the presence user's routing entry"
    );
}
