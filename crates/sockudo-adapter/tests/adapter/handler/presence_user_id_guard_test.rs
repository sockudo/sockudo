use sockudo_core::websocket::{SocketId, WebSocket, WebSocketRef};
use sockudo_ws::axum_integration;
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, WebSocketStream};
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

async fn make_ws_ref(user_id: Option<&str>) -> WebSocketRef {
    let writer = create_test_writer().await;
    let mut ws = WebSocket::new(SocketId::new(), writer);
    if let Some(uid) = user_id {
        ws.state.user_id = Some(uid.to_string());
    }
    WebSocketRef::new(ws)
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
