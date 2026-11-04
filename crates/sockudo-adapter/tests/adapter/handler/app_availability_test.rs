use crate::mocks::connection_handler_mock::MockCacheManager;
use futures_util::StreamExt;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::handler::app_availability::AppAvailabilitySweep;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::options::ServerOptions;
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
use sockudo_protocol::{AppendMode, ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Message, Stream as WsStream, WebSocketStream};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout};

type TestClient = WebSocketStream<WsStream<Http1>>;

struct Harness {
    handler: ConnectionHandler,
    adapter: Arc<LocalAdapter>,
    app_manager: Arc<MemoryAppManager>,
}

fn app(id: &str) -> App {
    App::from_policy(
        id.to_string(),
        format!("{id}-key"),
        format!("{id}-secret"),
        true,
        AppPolicy::default(),
    )
}

async fn build_harness(apps: &[&App]) -> Harness {
    let app_manager = Arc::new(MemoryAppManager::new());
    for app in apps {
        app_manager.create_app((*app).clone()).await.unwrap();
    }
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
    Harness {
        handler,
        adapter,
        app_manager,
    }
}

async fn create_test_pair() -> (WebSocketWriter, TestClient) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let socket = WebSocket::from_tcp(stream, WsConfig::default());
        let (mut reader, writer) = socket.split();
        tokio::spawn(async move {
            while let Some(message) = reader.next().await {
                if message.is_err() {
                    break;
                }
            }
        });
        writer
    });

    let stream = TcpStream::connect(address).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (client, _): (TestClient, _) = client
        .connect(stream, &address.to_string(), "/", None)
        .await
        .unwrap();

    (server.await.unwrap(), client)
}

async fn add_socket(harness: &Harness, app_id: &str) -> TestClient {
    let (writer, client) = create_test_pair().await;
    harness
        .adapter
        .add_socket(
            SocketId::new(),
            writer,
            app_id,
            harness.app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V2,
            WireFormat::Json,
            true,
            AppendMode::Delta,
        )
        .await
        .unwrap();
    client
}

/// Reads frames until the close frame and returns its code.
async fn expect_close_code(client: &mut TestClient) -> u16 {
    timeout(Duration::from_secs(2), async {
        while let Some(frame) = client.next().await {
            if let Message::Close(Some(close)) = frame.expect("websocket receive failed") {
                return close.code;
            }
        }
        panic!("stream ended without a close frame");
    })
    .await
    .expect("timed out waiting for close frame")
}

async fn assert_still_open(client: &mut TestClient) {
    assert!(
        timeout(Duration::from_millis(200), client.next())
            .await
            .is_err(),
        "connection of an available app received a frame"
    );
}

#[tokio::test]
async fn sweep_closes_live_connections_of_disabled_app_only() {
    let disabled = app("availability-disabled");
    let healthy = app("availability-healthy");
    let harness = build_harness(&[&disabled, &healthy]).await;
    let mut disabled_client = add_socket(&harness, &disabled.id).await;
    let mut healthy_client = add_socket(&harness, &healthy.id).await;

    let sweep = harness
        .handler
        .close_connections_of_unavailable_apps()
        .await;
    assert_eq!(
        sweep,
        AppAvailabilitySweep {
            apps_checked: 2,
            apps_unavailable: 0,
            connections_closed: 0,
        }
    );

    let mut now_disabled = disabled.clone();
    now_disabled.enabled = false;
    harness.app_manager.update_app(now_disabled).await.unwrap();

    let sweep = harness
        .handler
        .close_connections_of_unavailable_apps()
        .await;
    assert_eq!(sweep.apps_unavailable, 1);
    assert_eq!(sweep.connections_closed, 1);
    assert_eq!(expect_close_code(&mut disabled_client).await, 4003);
    assert_still_open(&mut healthy_client).await;
}

#[tokio::test]
async fn sweep_closes_live_connections_of_deleted_app() {
    let deleted = app("availability-deleted");
    let harness = build_harness(&[&deleted]).await;
    let mut first = add_socket(&harness, &deleted.id).await;
    let mut second = add_socket(&harness, &deleted.id).await;

    harness.app_manager.delete_app(&deleted.id).await.unwrap();

    let sweep = harness
        .handler
        .close_connections_of_unavailable_apps()
        .await;
    assert_eq!(sweep.apps_unavailable, 1);
    assert_eq!(sweep.connections_closed, 2);
    assert_eq!(expect_close_code(&mut first).await, 4001);
    assert_eq!(expect_close_code(&mut second).await, 4001);
}
