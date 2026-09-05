#[path = "../../../baseline/crates/sockudo-delta/src/coordination/redis_coordinator.rs"]
mod coordinator;
use coordinator::RedisClusterCoordinator;
use sockudo_core::delta_types::ClusterCoordinator;
use std::{sync::Arc, time::Instant};
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let manager = Arc::new(RedisClusterCoordinator::new("redis://127.0.0.1:16391/", Some(&format!("sockudo-perf-fanout-{}", uuid::Uuid::new_v4()))).await.unwrap());
    for independent in [false, true] {
        for sample in 0..9 {
            let start = Instant::now();
            let mut jobs = Vec::new();
            for publisher in 0..32 {
                let manager = Arc::clone(&manager);
                jobs.push(tokio::spawn(async move {
                    let channel = format!("sample-{sample}-channel-{}", if independent { publisher } else { 0 });
                    let mut full = 0usize;
                    for _ in 0..64 { let outcome = manager.increment_and_check("app", &channel, if independent {"independent"} else {"shared"}, 16).await.unwrap(); if outcome.0 { full += 1; } }
                    full
                }));
            }
            let mut full = 0;
            for job in jobs { full += job.await.unwrap(); }
            assert_eq!(full, 128);
            println!("F3 independent={independent} sample={sample} completed=2048 full={full} ns={}", start.elapsed().as_nanos());
        }
    }
}
