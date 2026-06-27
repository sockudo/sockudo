use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::namespace::Namespace;
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, WebSocketStream};
use std::hint::black_box;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

struct BenchAppManager {
    app: App,
}

#[async_trait]
impl AppManager for BenchAppManager {
    async fn init(&self) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn create_app(&self, _config: App) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn update_app(&self, _config: App) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn delete_app(&self, _app_id: &str) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn get_apps(&self) -> sockudo_core::error::Result<Vec<App>> {
        Ok(vec![self.app.clone()])
    }

    async fn find_by_id(&self, app_id: &str) -> sockudo_core::error::Result<Option<App>> {
        Ok((app_id == self.app.id).then(|| self.app.clone()))
    }

    async fn find_by_key(&self, key: &str) -> sockudo_core::error::Result<Option<App>> {
        Ok((key == self.app.key).then(|| self.app.clone()))
    }

    async fn check_health(&self) -> sockudo_core::error::Result<()> {
        Ok(())
    }
}

fn seed_namespace(
    exact_channel_count: usize,
    wildcard_channel_count: usize,
    sockets_per_channel: usize,
) -> Namespace {
    let namespace = Namespace::new("bench-app".to_string());

    for channel_idx in 0..exact_channel_count {
        let channel = format!("room-{channel_idx}");
        for _ in 0..sockets_per_channel {
            namespace.add_channel_to_socket(&channel, &SocketId::new());
        }
    }

    for channel_idx in 0..wildcard_channel_count {
        let channel = format!("room-{channel_idx}-*");
        for _ in 0..sockets_per_channel {
            namespace.add_channel_to_socket(&channel, &SocketId::new());
        }
    }

    namespace
}

async fn create_server_writer(addr: std::net::SocketAddr) -> WebSocketWriter {
    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let ws = WebSocket::from_tcp(stream, WsConfig::default());
        let (_reader, writer) = ws.split();
        writer
    });

    let client_stream = TcpStream::connect(local_addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (_client_ws, _): (WebSocketStream<sockudo_ws::Stream<Http1>>, _) = client
        .connect(client_stream, &local_addr.to_string(), "/", None)
        .await
        .unwrap();

    server_task.await.unwrap()
}

async fn seed_namespace_with_refs(
    exact_channel_count: usize,
    wildcard_channel_count: usize,
    sockets_per_channel: usize,
) -> Namespace {
    let namespace = Namespace::new("bench-app".to_string());
    let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(BenchAppManager {
        app: App::from_policy(
            "bench-app".to_string(),
            "bench-key".to_string(),
            "bench-secret".to_string(),
            true,
            AppPolicy::default(),
        ),
    });

    for channel_idx in 0..exact_channel_count {
        let channel = format!("room-{channel_idx}");
        for socket_idx in 0..sockets_per_channel {
            let writer =
                create_server_writer("127.0.0.1:0".parse().unwrap_or_else(|_| unreachable!()))
                    .await;
            let socket_id = SocketId::new();
            namespace
                .add_socket(
                    socket_id,
                    writer,
                    Arc::clone(&app_manager),
                    sockudo_core::namespace::SocketInitOptions {
                        buffer_config: WebSocketBufferConfig::default(),
                        protocol_version: ProtocolVersion::V2,
                        wire_format: WireFormat::Json,
                        append_mode: sockudo_protocol::AppendMode::Delta,
                        echo_messages: true,
                    },
                )
                .await
                .unwrap();
            namespace.add_channel_to_socket(&channel, &socket_id);

            black_box(socket_idx);
        }
    }

    for channel_idx in 0..wildcard_channel_count {
        let channel = format!("room-{channel_idx}-*");
        for socket_idx in 0..sockets_per_channel {
            let writer =
                create_server_writer("127.0.0.1:0".parse().unwrap_or_else(|_| unreachable!()))
                    .await;
            let socket_id = SocketId::new();
            namespace
                .add_socket(
                    socket_id,
                    writer,
                    Arc::clone(&app_manager),
                    sockudo_core::namespace::SocketInitOptions {
                        buffer_config: WebSocketBufferConfig::default(),
                        protocol_version: ProtocolVersion::V2,
                        wire_format: WireFormat::Json,
                        append_mode: sockudo_protocol::AppendMode::Delta,
                        echo_messages: true,
                    },
                )
                .await
                .unwrap();
            namespace.add_channel_to_socket(&channel, &socket_id);

            black_box(socket_idx);
        }
    }

    namespace
}

fn bench_namespace_matching(c: &mut Criterion) {
    let namespace = seed_namespace(20_000, 500, 4);
    let rt = Runtime::new().expect("runtime");
    let namespace_refs = rt.block_on(seed_namespace_with_refs(1_000, 100, 1));
    let namespace_refs_no_wildcards = rt.block_on(seed_namespace_with_refs(1_000, 0, 1));

    let mut group = c.benchmark_group("namespace_matching");

    group.bench_function("exact_match_many_channels", |b| {
        b.iter(|| {
            let matches = namespace.get_matching_channel_socket_ids_except("room-42", None);
            black_box(matches.len());
        });
    });

    group.bench_function("wildcard_match_many_channels", |b| {
        b.iter(|| {
            let matches = namespace.get_matching_channel_socket_ids_except("room-42-suffix", None);
            black_box(matches.len());
        });
    });

    group.bench_function("exact_match_socket_refs_many_channels", |b| {
        b.iter(|| {
            let matches = namespace_refs.get_matching_channel_socket_refs_except("room-42", None);
            black_box(matches.len());
        });
    });

    group.bench_function("exact_match_socket_refs_no_wildcards", |b| {
        b.iter(|| {
            let matches = namespace_refs_no_wildcards
                .get_matching_channel_socket_refs_except("room-42", None);
            black_box(matches.len());
        });
    });

    group.bench_function("exact_match_socket_refs_partitioned_no_wildcards", |b| {
        b.iter(|| {
            let (v1, v2) = namespace_refs_no_wildcards
                .get_matching_channel_socket_refs_partitioned_except("room-42", None);
            black_box((v1.len(), v2.len()));
        });
    });

    group.bench_function("exact_match_socket_refs_partitioned_many_channels", |b| {
        b.iter(|| {
            let (v1, v2) =
                namespace_refs.get_matching_channel_socket_refs_partitioned_except("room-42", None);
            black_box((v1.len(), v2.len()));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_namespace_matching);
criterion_main!(benches);
