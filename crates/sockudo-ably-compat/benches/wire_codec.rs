//! Ably codec throughput benchmarks.
#![allow(dead_code, unused_imports)]

#[path = "../src/codec.rs"]
mod codec;
#[path = "../src/protocol.rs"]
mod protocol;

use codec::{decode_protocol_bytes, encode_protocol_bytes};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use protocol::{ACTION_MESSAGE, AblyFormat};
use serde::Serialize;
use std::{hint::black_box, time::Duration};

#[derive(Serialize)]
struct Frame<'a> {
    action: u8,
    messages: Vec<Message<'a>>,
}

#[derive(Serialize)]
struct Message<'a> {
    name: &'static str,
    data: &'a [u8],
    encoding: &'static str,
}

fn msgpack_frame(payload: &[u8]) -> Vec<u8> {
    let frame = Frame {
        action: ACTION_MESSAGE,
        messages: vec![Message {
            name: "binary",
            data: payload,
            encoding: "cipher+aes-256-cbc",
        }],
    };
    let mut encoded = Vec::with_capacity(payload.len() + 128);
    let mut serializer = rmp_serde::Serializer::new(&mut encoded)
        .with_struct_map()
        .with_bytes(rmp_serde::config::BytesMode::ForceAll);
    frame.serialize(&mut serializer).unwrap();
    encoded
}

fn benchmark_wire_codec(criterion: &mut Criterion) {
    for size in [64_usize, 1024, 64 * 1024] {
        let payload = vec![0xa5; size];
        let encoded = msgpack_frame(&payload);
        let decoded = decode_protocol_bytes(&encoded, AblyFormat::MsgPack).unwrap();

        let mut decode = criterion.benchmark_group("ably_wire_decode");
        decode.warm_up_time(Duration::from_millis(200));
        decode.measurement_time(Duration::from_millis(500));
        decode.sample_size(20);
        decode.throughput(Throughput::Bytes(size as u64));
        decode.bench_with_input(
            BenchmarkId::from_parameter(size),
            &encoded,
            |bencher, bytes| {
                bencher
                    .iter(|| decode_protocol_bytes(black_box(bytes), AblyFormat::MsgPack).unwrap());
            },
        );
        decode.finish();

        let mut projection = criterion.benchmark_group("ably_wire_projection");
        projection.warm_up_time(Duration::from_millis(200));
        projection.measurement_time(Duration::from_millis(500));
        projection.sample_size(20);
        projection.throughput(Throughput::Bytes(size as u64));
        projection.bench_with_input(
            BenchmarkId::from_parameter(size),
            &encoded,
            |bencher, bytes| {
                bencher.iter(|| {
                    let frame =
                        decode_protocol_bytes(black_box(bytes), AblyFormat::MsgPack).unwrap();
                    black_box(frame.messages.unwrap().remove(0).data)
                });
            },
        );
        projection.finish();

        let mut encode = criterion.benchmark_group("ably_wire_encode");
        encode.warm_up_time(Duration::from_millis(200));
        encode.measurement_time(Duration::from_millis(500));
        encode.sample_size(20);
        encode.throughput(Throughput::Bytes(size as u64));
        for format in [AblyFormat::Json, AblyFormat::MsgPack] {
            encode.bench_with_input(
                BenchmarkId::new(
                    match format {
                        AblyFormat::Json => "json",
                        AblyFormat::MsgPack => "msgpack",
                    },
                    size,
                ),
                &decoded,
                |bencher, message| {
                    bencher.iter(|| encode_protocol_bytes(black_box(message), format).unwrap());
                },
            );
        }
        encode.finish();
    }
}

criterion_group!(benches, benchmark_wire_codec);
criterion_main!(benches);
