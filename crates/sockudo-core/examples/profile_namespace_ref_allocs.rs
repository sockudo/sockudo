use async_trait::async_trait;
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::namespace::Namespace;
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, WebSocketStream};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

struct CountingAlloc;

static ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAlloc = CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

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

async fn create_server_writer() -> WebSocketWriter {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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

async fn seed_namespace_with_refs() -> Namespace {
    let namespace = Namespace::new("profile-app".to_string());
    let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(BenchAppManager {
        app: App::from_policy(
            "profile-app".to_string(),
            "profile-key".to_string(),
            "profile-secret".to_string(),
            true,
            AppPolicy::default(),
        ),
    });

    for channel_idx in 0..1_000 {
        let channel = format!("room-{channel_idx}");
        let writer = create_server_writer().await;
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
                    echo_messages: true,
                },
            )
            .await
            .unwrap();
        namespace.add_channel_to_socket(&channel, &socket_id);
    }

    for channel_idx in 0..100 {
        let channel = format!("room-{channel_idx}-*");
        let writer = create_server_writer().await;
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
                    echo_messages: true,
                },
            )
            .await
            .unwrap();
        namespace.add_channel_to_socket(&channel, &socket_id);
    }

    namespace
}

fn reset_alloc_stats() {
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    DEALLOC_CALLS.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    DEALLOC_BYTES.store(0, Ordering::Relaxed);
}

fn main() {
    let rt = Runtime::new().unwrap();
    let namespace = rt.block_on(seed_namespace_with_refs());

    reset_alloc_stats();

    for _ in 0..100_000 {
        let (v1, v2) =
            namespace.get_matching_channel_socket_refs_partitioned_except("room-42", None);
        black_box((v1.len(), v2.len()));
    }

    println!(
        "alloc_calls={} dealloc_calls={} alloc_bytes={} dealloc_bytes={}",
        ALLOC_CALLS.load(Ordering::Relaxed),
        DEALLOC_CALLS.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
        DEALLOC_BYTES.load(Ordering::Relaxed)
    );
}
