//! Component diagnostic: exact production conflation extraction implementation.
//! Minimal manager storage isolates extraction from unused runtime state.
#![allow(dead_code)]
mod manager {
    pub struct DeltaCompressionManager {
        pub config: sockudo_core::delta_types::DeltaCompressionConfig,
    }
}
#[path = "../../../../../crates/sockudo-delta/src/conflation.rs"]
mod conflation;
use manager::DeltaCompressionManager;
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let manager = DeltaCompressionManager {
        config: Default::default(),
    };
    println!(
        "probe=current_source_conflation_extraction_component; profile=rustc-opt3; samples=9; iterations=20; no_network"
    );
    for payload_bytes in [1_024, 16_384, 65_536] {
        let payload = format!(
            "{{\"data\":{{\"symbol\":\"BTC\",\"content\":\"{}\"}}}}",
            "x".repeat(payload_bytes)
        );
        for subscribers in [1, 100, 1_000] {
            for variant in ["repeated", "once"] {
                let mut samples = Vec::new();
                for _ in 0..9 {
                    let started = Instant::now();
                    for _ in 0..20 {
                        if variant == "repeated" {
                            for _ in 0..subscribers {
                                black_box(manager.extract_conflation_key_from_path(
                                    black_box(payload.as_bytes()),
                                    black_box("data.symbol"),
                                ));
                            }
                        } else {
                            let key = manager.extract_conflation_key_from_path(
                                black_box(payload.as_bytes()),
                                black_box("data.symbol"),
                            );
                            for _ in 0..subscribers {
                                black_box(key.as_str());
                            }
                        }
                    }
                    samples.push(started.elapsed().as_nanos() as f64 / 20.0);
                }
                samples.sort_by(f64::total_cmp);
                println!(
                    "payload_bytes={} subscribers={subscribers} variant={variant} ns_min={:.0} median={:.0} max={:.0}",
                    payload.len(),
                    samples[0],
                    samples[4],
                    samples[8]
                );
            }
        }
    }
}
