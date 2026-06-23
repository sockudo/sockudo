use sockudo_adapter::ConnectionManager;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Stream as WsStream, WebSocketStream};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

const APP_ID: &str = "gauge-app";

async fn make_writer() -> WebSocketWriter {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let ws = WebSocket::from_tcp(stream, WsConfig::default());
        let (_reader, writer) = ws.split();
        writer
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (_client_ws, _): (WebSocketStream<WsStream<Http1>>, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();

    server.await.unwrap()
}

async fn register_socket(adapter: &LocalAdapter, app_manager: Arc<MemoryAppManager>) -> SocketId {
    let socket_id = SocketId::new();
    adapter
        .add_socket(
            socket_id,
            make_writer().await,
            APP_ID,
            app_manager as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V2,
            WireFormat::Json,
            true,
        )
        .await
        .unwrap();
    socket_id
}

/// join_channel_fast flags the join as new only the first time. A re-subscribe
/// by the same socket reports false.
#[tokio::test]
async fn join_channel_fast_reports_transition_only_on_first_join() {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager
        .create_app(App::from_policy(
            APP_ID.to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            AppPolicy::default(),
        ))
        .await
        .unwrap();

    let adapter = LocalAdapter::new();
    adapter.init().await;
    let socket_id = register_socket(&adapter, app_manager).await;

    let channel = "public-room";

    let first = adapter.join_channel_fast(APP_ID, channel, &socket_id);
    assert_eq!(
        first,
        Some((1, true, true)),
        "first join is a genuine 0->1 transition"
    );

    let second = adapter.join_channel_fast(APP_ID, channel, &socket_id);
    assert_eq!(
        second,
        Some((1, false, false)),
        "re-subscribe must not report a new transition"
    );

    let third = adapter.join_channel_fast(APP_ID, channel, &socket_id);
    assert_eq!(
        third,
        Some((1, false, false)),
        "repeated re-subscribes stay non-transitional"
    );
}
