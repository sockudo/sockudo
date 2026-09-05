//! Isolated performance diagnostic; includes current production FilterIndex source.
//! Build using /tmp/sockudo-build-filter-index-probe.py; no production edits.
#![allow(dead_code)]
#[path = "../../../../../crates/sockudo-adapter/src/filter_index.rs"]
mod filter_index;
use filter_index::FilterIndex;
use sockudo_core::websocket::SocketId;
use sockudo_filter::node::FilterNodeBuilder;
use std::hint::black_box;
use std::time::Instant;

fn main() {
    println!(
        "probe=current_source_filter_index; profile=rustc-opt3; subscribers_per_channel=1; tracing=no_subscriber"
    );
    for keys in [100usize, 1_000, 10_000] {
        let index = FilterIndex::new();
        let socket = SocketId { high: 1, low: 1 };
        for value in 0..keys {
            let filter = FilterNodeBuilder::eq("key", &value.to_string());
            index.add_socket_filter("room", socket, Some(&filter));
            // Normal unregister-by-known-filter; leaves empty equality buckets.
            index.remove_socket_filter("room", socket, Some(&filter));
        }
        let stats = index.stats("room");
        assert_eq!(stats.eq_entries, keys);
        assert_eq!(stats.eq_sockets, 0);
        let mut ns = Vec::new();
        for _ in 0..9 {
            let start = Instant::now();
            for _ in 0..100 {
                black_box(&index).remove_socket_all_filters(black_box("room"), black_box(socket));
            }
            ns.push(start.elapsed().as_nanos() as f64 / 100.0);
        }
        ns.sort_by(f64::total_cmp);
        println!(
            "historical_values={keys} retained_eq_entries={} retained_sockets={} removal_ns_min={:.0} median={:.0} max={:.0}",
            stats.eq_entries, stats.eq_sockets, ns[0], ns[4], ns[8]
        );
    }
}
