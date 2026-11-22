//! Per-message cost of delivering through a live split WebSocket.
//!
//! `hot_path_replacements` in the adapter crate benchmarks fanout fan-out
//! bookkeeping only; nothing exercised the writer loop or the per-message
//! enqueue path. This drives a real loopback socket so a regression in
//! `WebSocketRef::send_message`, the outbound queue accounting, or the writer
//! task's dispatch shows up as a throughput change.
//!
//! Each batch owns its own socket, so a slow batch cannot back up the next
//! one, and the socket teardown is excluded from the measurement.

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use sockudo_core::websocket::{SocketId, WebSocket, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;
use std::hint::black_box;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

/// Messages enqueued per measured iteration. Large enough that Criterion needs
/// few socket setups, small enough to keep a sample well under the measurement
/// window.
const BATCH: usize = 16_384;

/// Queue headroom so the writer, not the buffer limit, is the only backpressure.
/// Must exceed `BATCH`: the batch is enqueued without waiting for the writer.
const QUEUE_CAPACITY: usize = BATCH * 2;

struct LiveSocket {
    socket: WebSocketRef,
    /// Kept alive: dropping either half cancels the split driver. The peer is
    /// owned by the drain task spawned in `live_socket`.
    _reader: sockudo_ws::axum_integration::WebSocketReader,
}

fn live_socket(rt: &Runtime) -> LiveSocket {
    let (socket, reader) = rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (accepted, peer) = tokio::join!(listener.accept(), TcpStream::connect(addr));
        let (accepted, _) = accepted.unwrap();

        // Drain everything the writer produces so it never blocks on the peer.
        let mut peer = peer.unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match peer.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });

        let (reader, writer) =
            sockudo_ws::axum_integration::WebSocket::from_tcp(accepted, Default::default()).split();
        let socket = WebSocketRef::new(WebSocket::with_buffer_config(
            SocketId::new(),
            writer,
            sockudo_core::websocket::WebSocketBufferConfig::with_message_limit(
                QUEUE_CAPACITY,
                true,
            ),
        ));
        (socket, reader)
    });

    LiveSocket {
        socket,
        _reader: reader,
    }
}

fn bench_send_message(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let message = PusherMessage::pong();

    let mut group = c.benchmark_group("websocket_send_message");
    group.sample_size(20);
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function("loopback_live_writer", |b| {
        b.iter_batched(
            || live_socket(&rt),
            |live| {
                for _ in 0..BATCH {
                    black_box(live.socket.send_message(black_box(&message))).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_send_message);
criterion_main!(benches);
