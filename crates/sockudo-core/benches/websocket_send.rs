//! Per-message cost of delivering through a live split WebSocket.
//!
//! `hot_path_replacements` in the adapter crate benchmarks fanout bookkeeping
//! only; nothing exercised the writer loop or the per-message enqueue path.
//! This drives a real loopback socket and then waits until the peer has
//! stopped receiving, so a slower writer shows up in the measured time
//! instead of leaving messages unwritten.
//!
//! Each batch owns its own socket, so a slow batch cannot back up the next
//! one, and the socket setup is excluded from the measurement.

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use sockudo_core::websocket::{SocketId, WebSocket, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

/// Messages delivered per measured iteration.
const BATCH: usize = 16_384;

/// Queue headroom so the writer, not the buffer limit, is the only backpressure.
/// Must exceed `BATCH`: the batch is enqueued before the writer is waited on.
const QUEUE_CAPACITY: usize = BATCH * 2;

/// Upper bound on the drain wait, so a broken socket fails the bench loudly
/// instead of hanging.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

struct LiveSocket {
    socket: WebSocketRef,
    /// Bytes the peer has read. The peer task owns the stream.
    delivered: Arc<AtomicUsize>,
    /// Kept alive: dropping either half cancels the split driver.
    _reader: sockudo_ws::axum_integration::WebSocketReader,
}

fn live_socket(rt: &Runtime) -> LiveSocket {
    let delivered = Arc::new(AtomicUsize::new(0));
    let peer_delivered = Arc::clone(&delivered);

    let (socket, reader) = rt.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (accepted, peer) = tokio::join!(listener.accept(), TcpStream::connect(addr));
        let (accepted, _) = accepted.unwrap();

        // Drain everything the writer produces so it never blocks on the peer,
        // and count it so the benchmark can wait for delivery.
        let mut peer = peer.unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match peer.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => {
                        peer_delivered.fetch_add(read, Ordering::Relaxed);
                    }
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
        delivered,
        _reader: reader,
    }
}

/// Block until the peer stops receiving. The whole batch is queued, so the
/// writer has no reason to idle before it is drained; two equal samples mean
/// everything written so far has arrived.
fn wait_for_delivery(rt: &Runtime, delivered: &AtomicUsize) {
    rt.block_on(async {
        let drained = async {
            let mut last = delivered.load(Ordering::Relaxed);
            loop {
                tokio::time::sleep(Duration::from_micros(200)).await;
                let now = delivered.load(Ordering::Relaxed);
                if now > 0 && now == last {
                    return now;
                }
                last = now;
            }
        };
        tokio::time::timeout(DRAIN_TIMEOUT, drained)
            .await
            .expect("the peer must receive the batch within the drain timeout")
    });
}

fn bench_send_message(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let message = PusherMessage::pong();

    let mut group = c.benchmark_group("websocket_send_message");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function("loopback_live_writer", |b| {
        b.iter_batched(
            || live_socket(&rt),
            |live| {
                for _ in 0..BATCH {
                    match live.socket.send_message(black_box(&message)) {
                        Ok(()) => {}
                        // The queue is twice the batch, so this means the writer
                        // died rather than that the batch outran it.
                        Err(error) => panic!("send failed mid-batch: {error}"),
                    }
                }
                wait_for_delivery(&rt, &live.delivered);
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_send_message);
criterion_main!(benches);
