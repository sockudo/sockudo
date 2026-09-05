#![allow(dead_code)]
extern crate self as sockudo_core;
pub mod error { #[derive(Debug, thiserror::Error)] pub enum Error { #[error("{0}")] Cache(String) } pub type Result<T> = std::result::Result<T, Error>; }
include!("redis_support.rs");
mod redis_cache_manager;
mod counted_cache;
use counted_cache::{CACHE_CALLS,READ_BYTES};
pub mod cache;
mod stats;
use stats::*;
use std::{alloc::{GlobalAlloc, Layout, System}, sync::atomic::{AtomicU64, Ordering}, time::{Duration, Instant}};
use chrono::{TimeZone, Utc};
struct CountingAllocator;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
#[global_allocator] static ALLOCATOR: CountingAllocator = CountingAllocator;
unsafe impl GlobalAlloc for CountingAllocator {
 unsafe fn alloc(&self,l:Layout)->*mut u8 { ALLOCS.fetch_add(1,Ordering::Relaxed);BYTES.fetch_add(l.size() as u64,Ordering::Relaxed);unsafe{System.alloc(l)} }
 unsafe fn dealloc(&self,p:*mut u8,l:Layout){unsafe{System.dealloc(p,l)}}
 unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8{ALLOCS.fetch_add(1,Ordering::Relaxed);BYTES.fetch_add(n as u64,Ordering::Relaxed);unsafe{System.realloc(p,l,n)}}
}
fn config() -> StatsRuntimeConfig { StatsRuntimeConfig { queue_capacity: 16_384, flush_interval: Duration::from_millis(10), retention_seconds: 3_600, max_scan_entries: 20_000, cas_retries: 8 } }
#[tokio::main(flavor="current_thread")]
async fn main() {
 let mode=std::env::args().nth(1).unwrap_or_else(||"a2".to_owned());
 let at=1_770_134_580_000;
 let fixture = if let Ok(url) = std::env::var("REDIS_URL") {
  Some(std::sync::Arc::new(counted_cache::CountedCache(redis_cache_manager::RedisCacheManager::with_url(&url, Some(&format!("ably-audit-{}-{}",std::process::id(),chrono::Utc::now().timestamp_nanos_opt().unwrap()))).await.unwrap())) as std::sync::Arc<dyn cache::CacheManager>)
 } else {None};
 if mode=="a2" {
  let aggregator=StatsAggregator::new(fixture.clone(),config());
  for rep in 0..7 {
   let mut latencies=Vec::with_capacity(100);
   ALLOCS.store(0,Ordering::Relaxed);BYTES.store(0,Ordering::Relaxed);CACHE_CALLS.store(0,Ordering::Relaxed);READ_BYTES.store(0,Ordering::Relaxed);
   let start=Instant::now();
   for _ in 0..100 {let one=Instant::now();aggregator.record(StatsObservation::messages("audit",at,"inbound","realtime",1,256).unwrap()).await.unwrap();latencies.push(one.elapsed().as_micros());}
   let us=start.elapsed().as_micros();let allocations=ALLOCS.load(Ordering::Relaxed);let bytes=BYTES.load(Ordering::Relaxed);let cache_calls=CACHE_CALLS.load(Ordering::Relaxed);let read_bytes=READ_BYTES.load(Ordering::Relaxed);
   latencies.sort_unstable();
   let page=aggregator.query("audit",&StatsQuery::parse("minute",None,None,None,Some(1),None).unwrap(),at).await.unwrap();
   assert_eq!(page.items[0].entries["messages.inbound.all.messages.count"],100*(rep+1));
   println!("a2,rep={rep},count=100,us={us},p50_us={},p95_us={},p99_us={},allocations={allocations},allocated_bytes={bytes},cache_calls={cache_calls},read_bytes={read_bytes}",latencies[50],latencies[95],latencies[99]);
  }
 } else {
  for n in [1_000,10_000] {
   let app=format!("audit-{n}");
   let aggregator=StatsAggregator::new(fixture.clone(),config());
   let fixtures=(0..n).map(|minute|serde_json::json!({"intervalId":Utc.timestamp_millis_opt(at+minute*60_000).unwrap().format("%Y-%m-%d:%H:%M").to_string(),"inbound":{"realtime":{"messages":{"count":1}}}})).collect();
   aggregator.ingest_fixtures(&app,fixtures).await.unwrap();
   let end=Utc.timestamp_millis_opt(at+(n-1)*60_000).unwrap().format("%Y-%m-%d:%H:%M").to_string();
   let start=Utc.timestamp_millis_opt(at+(n-2)*60_000).unwrap().format("%Y-%m-%d:%H:%M").to_string();
   let query=StatsQuery::parse("minute",Some("backwards"),Some(&start),Some(&end),Some(2),None).unwrap();
   // Complete automatic legacy-index migration before steady-state measurements.
   // The same warmup query is issued to each implementation; incomplete pages never count.
   for warmup in 0..256 {
    match aggregator.query(&app,&query,at).await {
     Ok(page) => {
      assert_eq!(page.items.len(),2);
      if let Some(cache)=&fixture {
       use base64::Engine;
       let key=format!("stats-index:v1:{}:root",base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(app.as_bytes()));
       if let Some(root)=cache.get(&key).await.unwrap() {
        if !serde_json::from_str::<serde_json::Value>(&root).unwrap()["ready"].as_bool().unwrap() {continue;}
       }
      }
      break;
     },
     Err(error) => {assert!(warmup < 255,"index warmup failed: {error}");}
    }
   }
   for rep in 0..7 {
    ALLOCS.store(0,Ordering::Relaxed);BYTES.store(0,Ordering::Relaxed);CACHE_CALLS.store(0,Ordering::Relaxed);READ_BYTES.store(0,Ordering::Relaxed);
    let started=Instant::now();let page=aggregator.query(&app,&query,at).await.unwrap();
    let us=started.elapsed().as_micros();let allocations=ALLOCS.load(Ordering::Relaxed);let bytes=BYTES.load(Ordering::Relaxed);let cache_calls=CACHE_CALLS.load(Ordering::Relaxed);let read_bytes=READ_BYTES.load(Ordering::Relaxed);
    assert_eq!(page.items.len(),2);assert_eq!(page.items[0].interval_id,end);assert_eq!(page.items[1].interval_id,start);
    assert!(page.items.iter().all(|i|i.entries["messages.inbound.all.messages.count"]==1));
    println!("a3,n={n},rep={rep},returned=2,us={us},allocations={allocations},allocated_bytes={bytes},cache_calls={cache_calls},read_bytes={read_bytes}");
   }
  }
 }
}
