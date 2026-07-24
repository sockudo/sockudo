use bytes::Bytes;
use futures_util::StreamExt;
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::namespace::Namespace;
use sockudo_core::websocket::{SocketId, WebSocket, WebSocketRef};
use sockudo_ws::axum_integration;
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Stream as WsStream, WebSocketStream};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

type ClientWs = WebSocketStream<WsStream<Http1>>;

async fn create_ws_pair() -> (axum_integration::WebSocketWriter, ClientWs) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let ws = axum_integration::WebSocket::from_tcp(stream, WsConfig::default());
        let (_reader, writer) = ws.split();
        writer
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (client_ws, _): (ClientWs, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();

    let writer = server_task.await.unwrap();
    (writer, client_ws)
}

fn insert_live_socket(
    namespace: &Namespace,
    socket_id: SocketId,
    writer: axum_integration::WebSocketWriter,
    user_id: &str,
) -> WebSocketRef {
    let mut ws = WebSocket::new(socket_id, writer);
    ws.state.user_id = Some(user_id.to_string());
    let ws_ref = WebSocketRef::new(ws);

    namespace.sockets.insert(socket_id, ws_ref.clone());
    namespace
        .users
        .entry(user_id.to_string())
        .or_default()
        .insert(socket_id);

    ws_ref
}

fn insert_presence(namespace: &Namespace, socket_id: SocketId, channel: &str, user_id: &str) {
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

async fn wait_for_close_frame(client: &mut ClientWs) -> Option<(u16, String)> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match client.next().await {
                Some(Ok(sockudo_ws::Message::Close(Some(reason)))) => {
                    return Some((reason.code, reason.reason.to_string()));
                }
                Some(Ok(sockudo_ws::Message::Close(None))) => return None,
                Some(Ok(_)) => continue,
                Some(Err(e)) => panic!("client read error: {e}"),
                None => return None,
            }
        }
    })
    .await
    .expect("timed out waiting for close frame")
}

#[tokio::test]
async fn server_to_user_fanout_reaches_all_live_sockets() {
    let namespace = Namespace::new("test-app".to_string());

    let (writer_a1, mut client_a1) = create_ws_pair().await;
    let (writer_a2, mut client_a2) = create_ws_pair().await;
    let (writer_b, mut client_b) = create_ws_pair().await;

    let sid_a1 = SocketId::new();
    let sid_a2 = SocketId::new();
    let sid_b = SocketId::new();

    insert_live_socket(&namespace, sid_a1, writer_a1, "alice");
    insert_live_socket(&namespace, sid_a2, writer_a2, "alice");
    insert_live_socket(&namespace, sid_b, writer_b, "bob");

    let alice_sockets = namespace.get_user_sockets("alice").unwrap();
    assert_eq!(
        alice_sockets.len(),
        2,
        "alice should have exactly 2 live sockets"
    );

    let payload = Bytes::from_static(b"{\"event\":\"test:fanout\",\"data\":{}}");
    for ws_ref in &alice_sockets {
        ws_ref.send_broadcast(payload.clone()).unwrap();
    }

    let frame_a1 = tokio::time::timeout(Duration::from_secs(2), client_a1.next())
        .await
        .expect("timed out waiting for alice socket 1")
        .expect("alice socket 1 stream ended")
        .expect("alice socket 1 read error");
    assert!(
        matches!(frame_a1, sockudo_ws::Message::Text(_)),
        "alice socket 1 should receive a text frame, got: {frame_a1:?}"
    );

    let frame_a2 = tokio::time::timeout(Duration::from_secs(2), client_a2.next())
        .await
        .expect("timed out waiting for alice socket 2")
        .expect("alice socket 2 stream ended")
        .expect("alice socket 2 read error");
    assert!(
        matches!(frame_a2, sockudo_ws::Message::Text(_)),
        "alice socket 2 should receive a text frame, got: {frame_a2:?}"
    );

    let bob_result = tokio::time::timeout(Duration::from_millis(80), client_b.next()).await;
    assert!(
        bob_result.is_err(),
        "bob should not receive any frame from the alice-targeted fanout"
    );
}

#[tokio::test]
async fn stale_id_is_ignored_by_routing_and_counting() {
    let namespace = Namespace::new("test-app".to_string());

    let (writer, mut client) = create_ws_pair().await;
    let live_sid = SocketId::new();
    let stale_sid = SocketId::new();

    let _ws_ref = insert_live_socket(&namespace, live_sid, writer, "alice");

    namespace
        .users
        .entry("alice".to_string())
        .or_default()
        .insert(stale_sid);

    assert_eq!(
        namespace.users.get("alice").unwrap().len(),
        2,
        "users[alice] should contain 2 IDs (1 live + 1 stale)"
    );

    let resolved = namespace.get_user_sockets("alice").unwrap();
    assert_eq!(
        resolved.len(),
        1,
        "get_user_sockets must skip stale IDs; expected 1 live socket"
    );
    assert_eq!(
        *resolved[0].get_socket_id_sync(),
        live_sid,
        "resolved socket must be the live one"
    );

    insert_presence(&namespace, live_sid, "presence-chat", "alice");

    let count = namespace
        .count_user_connections_in_channel("alice", "presence-chat", None)
        .unwrap();
    assert_eq!(
        count, 1,
        "count_user_connections_in_channel must return 1; stale ID has no presence_data"
    );

    namespace.terminate_user_connections("alice").await.unwrap();

    let close = wait_for_close_frame(&mut client).await;
    assert_eq!(
        close,
        Some((4009, "You got disconnected by the app.".to_string())),
        "live socket must receive the exact terminate close frame"
    );
}

#[tokio::test]
async fn terminate_user_connections_closes_live_socket() {
    let namespace = Namespace::new("test-app".to_string());

    let (writer, mut client) = create_ws_pair().await;
    let socket_id = SocketId::new();
    insert_live_socket(&namespace, socket_id, writer, "alice");

    namespace.terminate_user_connections("alice").await.unwrap();

    let close = wait_for_close_frame(&mut client).await;
    assert_eq!(
        close,
        Some((4009, "You got disconnected by the app.".to_string())),
        "terminate_user_connections must preserve its exact close payload"
    );
}

#[tokio::test]
async fn force_reconnect_closes_live_socket() {
    let namespace = Namespace::new("test-app".to_string());

    let (writer, mut client) = create_ws_pair().await;
    let socket_id = SocketId::new();
    insert_live_socket(&namespace, socket_id, writer, "alice");

    namespace
        .force_reconnect_user_connections("alice")
        .await
        .unwrap();

    let close = wait_for_close_frame(&mut client).await;
    assert_eq!(
        close,
        Some((4200, "Force reconnect requested by the app.".to_string())),
        "force_reconnect_user_connections must preserve its exact close payload"
    );
}
