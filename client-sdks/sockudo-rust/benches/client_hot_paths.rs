//! Hot-path benchmarks: `cargo bench --bench client_hot_paths`.
#![allow(clippy::unwrap_used, missing_docs)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use sockudo_client::__private as client;
use sockudo_client::{SockudoClient, Transport, WireFormat};

const EVENT: &str = r#"{"event":"price.updated","channel":"market:btc","data":"{\"symbol\":\"BTC-USD\",\"price\":101234.56,\"size\":0.25,\"side\":\"buy\",\"ts\":1731000000123}","message_id":"0b3f6e2c-6a7f-4a0e-9d3c-2f7c1d2b9a11","stream_id":"s-1","serial":184467}"#;

fn decode(c: &mut Criterion) {
    let raw = Bytes::from_static(EVENT.as_bytes());
    let mut group = c.benchmark_group("decode");
    group.throughput(Throughput::Bytes(raw.len() as u64));
    group.bench_function("json_event", |b| {
        b.iter(|| black_box(client::decode_json(raw.clone()).unwrap()));
    });
    group.bench_function("json_event_and_typed_payload", |b| {
        #[derive(serde::Deserialize)]
        #[allow(dead_code)]
        struct Tick {
            symbol: String,
            price: f64,
            size: f64,
            side: String,
            ts: u64,
        }
        b.iter(|| {
            let event = client::decode_json(raw.clone()).unwrap();
            black_box(event.json::<Tick>().unwrap())
        });
    });
    for format in [
        #[cfg(feature = "msgpack")]
        WireFormat::MessagePack,
        #[cfg(feature = "protobuf")]
        WireFormat::Protobuf,
    ] {
        let encoded = Bytes::from(
            client::encode(
                format,
                "price.updated",
                Some("market:btc"),
                br#"{"price":1}"#,
            )
            .unwrap(),
        );
        group.bench_function(format!("{format:?}_event"), |b| {
            b.iter(|| black_box(client::decode(format, encoded.clone(), false).unwrap()));
        });
    }
    group.finish();
}

fn encode(c: &mut Criterion) {
    let data = br#"{"x":12,"y":-4,"dir":"north"}"#;
    let mut group = c.benchmark_group("encode");
    group.bench_function("json_client_event", |b| {
        b.iter(|| {
            black_box(
                client::encode(WireFormat::Json, "client-move", Some("private-game"), data)
                    .unwrap(),
            )
        });
    });
    group.finish();
}

fn dedup(c: &mut Criterion) {
    let ids: Vec<String> = (0..4096)
        .map(|i| format!("0b3f6e2c-6a7f-4a0e-9d3c-{i:012}"))
        .collect();
    c.bench_function("dedup/check_and_track_1000_window", |b| {
        let mut dedup = client::Dedup::new(1000);
        let mut index = 0;
        b.iter(|| {
            index = (index + 1) & 4095;
            black_box(dedup.check(&ids[index]))
        });
    });
}

fn delta(c: &mut Criterion) {
    let old = br#"{"event":"book","channel":"ob:btc","data":"{\"bids\":[[101234.5,1.2],[101234.0,0.5],[101233.5,3.1]],\"asks\":[[101235.0,0.7],[101235.5,2.2]],\"seq\":1}"}"#;
    let new = br#"{"event":"book","channel":"ob:btc","data":"{\"bids\":[[101234.5,1.3],[101234.0,0.5],[101233.5,3.1]],\"asks\":[[101235.0,0.6],[101235.5,2.2]],\"seq\":2}"}"#;
    let patch = fossil_delta::delta(new, old);
    let mut full = old.to_vec();
    full.truncate(full.len() - 1);
    full.extend_from_slice(br#","__delta_seq":12,"__conflation_key":"BTC"}"#);
    let mut group = c.benchmark_group("delta");
    group.bench_function("fossil_apply", |b| {
        b.iter(|| black_box(client::fossil_apply(old, &patch).unwrap()));
    });
    group.bench_function("strip_markers", |b| {
        b.iter(|| black_box(client::strip_delta_markers(&full)));
    });
    group.finish();
}

/// End-to-end: a loopback server streams `N` events through the real client
/// (socket read → frame parse → decode → dedup → channel dispatch → callback).
fn end_to_end(c: &mut Criterion) {
    const N: u64 = 50_000;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("end_to_end");
    group.throughput(Throughput::Elements(N));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));
    group.bench_function("loopback_events", |b| {
        b.iter_custom(|iterations| {
            runtime.block_on(async {
                let mut total = Duration::ZERO;
                for _ in 0..iterations {
                    total += run_loopback(N).await;
                }
                total
            })
        });
    });
    group.finish();
}

async fn run_loopback(count: u64) -> Duration {
    use futures_util::{SinkExt, StreamExt};
    use sockudo_ws::Message;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        tcp.set_nodelay(true).unwrap();
        let ws_server = sockudo_ws::server::WebSocketServer::<sockudo_ws::Http1>::new(
            sockudo_ws::Config::default(),
        );
        let (mut ws, _) = ws_server.accept(tcp).await.unwrap();
        ws.send(Message::Text(Bytes::from_static(
            br#"{"event":"sockudo:connection_established","data":"{\"socket_id\":\"1.1\",\"activity_timeout\":120}"}"#,
        )))
        .await
        .unwrap();
        // Wait for the subscribe.
        while let Some(Ok(message)) = ws.next().await {
            if matches!(message, Message::Text(ref t) if t.windows(9).any(|w| w == b"subscribe")) {
                break;
            }
        }
        ready_rx.await.unwrap();
        for index in 0..count {
            let frame = format!(
                r#"{{"event":"tick","channel":"bench","data":"{{\"price\":101234.56,\"i\":{index}}}","message_id":"m-{index}"}}"#
            );
            ws.feed(Message::Text(Bytes::from(frame))).await.unwrap();
        }
        ws.flush().await.unwrap();
        // Keep the socket open until the client is done.
        let _ = ws.next().await;
    });

    let client = SockudoClient::builder("bench")
        .ws_host("127.0.0.1")
        .ws_port(port)
        .force_tls(false)
        .disabled_transports([Transport::Wss])
        .build()
        .unwrap();
    let received = Arc::new(AtomicU64::new(0));
    let done = Arc::new(tokio::sync::Notify::new());
    let channel = client.subscribe("bench");
    {
        let received = Arc::clone(&received);
        let done = Arc::clone(&done);
        channel.bind("tick", move |event| {
            black_box(event.data.as_bytes().len());
            if received.fetch_add(1, Ordering::Relaxed) + 1 == count {
                done.notify_one();
            }
        });
    }
    client.connect().unwrap();
    client.connected().await.unwrap();
    // Let the subscribe flush before timing.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let started = Instant::now();
    ready_tx.send(()).unwrap();
    done.notified().await;
    let elapsed = started.elapsed();
    client.disconnect().await;
    let _ = server.await;
    elapsed
}

criterion_group!(benches, decode, encode, dedup, delta, end_to_end);
criterion_main!(benches);
