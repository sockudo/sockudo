#![allow(dead_code)]
extern crate self as sockudo_core;
pub mod error { #[derive(Debug,thiserror::Error)]pub enum Error {#[error("{0}")]Cache(String)}pub type Result<T>=std::result::Result<T,Error>;}
pub mod cache;
include!("redis_support.rs");
mod redis_cache_manager;
mod counted_cache;
use cache::CacheManager;
use counted_cache::{CACHE_CALLS,READ_BYTES};
use std::{collections::HashMap,sync::{Arc,Mutex,OnceLock,atomic::{AtomicU64,Ordering}},alloc::{GlobalAlloc,Layout,System},time::{Duration,Instant}};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant as TokioInstant;
use serde::{Serialize,Deserialize};
use tracing::warn;
use sha2::{Digest,Sha256};
use base64::Engine;
const ABLY_REVOCATION_MAX_ENTRIES:usize=100_000;
const ABLY_REVOCATION_MAX_BYTES:usize=64*1024*1024;
struct CountingAllocator;
static ALLOCS:AtomicU64=AtomicU64::new(0);static BYTES:AtomicU64=AtomicU64::new(0);
#[global_allocator]static ALLOCATOR:CountingAllocator=CountingAllocator;
unsafe impl GlobalAlloc for CountingAllocator {
 unsafe fn alloc(&self,l:Layout)->*mut u8 {ALLOCS.fetch_add(1,Ordering::Relaxed);BYTES.fetch_add(l.size() as u64,Ordering::Relaxed);unsafe{System.alloc(l)}}
 unsafe fn dealloc(&self,p:*mut u8,l:Layout){unsafe{System.dealloc(p,l)}}
 unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8{ALLOCS.fetch_add(1,Ordering::Relaxed);BYTES.fetch_add(n as u64,Ordering::Relaxed);unsafe{System.realloc(p,l,n)}}
}
#[derive(Clone,Debug)]struct AblyAuthError {message:String}
impl AblyAuthError {fn internal()->Self{Self{message:"internal".into()}}fn backend_unavailable()->Self{Self{message:"unavailable".into()}}fn revocation_capacity()->Self{Self{message:"capacity".into()}}}
fn auth_backend_failure(operation: &'static str)->AblyAuthError{eprintln!("revocation probe backend failure: {operation}");AblyAuthError::backend_unavailable()}
#[derive(Default)]struct Metrics(AtomicU64);
impl Metrics {fn mark_backend_failure(&self){self.0.fetch_add(1,Ordering::Relaxed);}}
#[derive(Clone)]struct ConnectionAuthorization{revocable:bool,issued_ms:i64,client_id:Option<String>,revocation_key:Option<String>}
struct AblyConnectionAttachment;
// This probe uses only clientId records. Capability intersection has independent regression tests;
// no channel projection or capability parsing is included in its performance claim.
fn channel_revocation_applies(record:&AblyRevocationRecord,_:&ConnectionAuthorization,_:i64)->bool{assert_ne!(record.target_type,"channel");false}
fn now_ms()->i64{chrono::Utc::now().timestamp_millis()}
include!("records.rs");
include!("snapshot.rs");
#[derive(Default)]struct Hub {cache:Option<Arc<dyn CacheManager>>,metrics:Metrics,revocations:Mutex<AblyRevocationStore>,revocation_snapshots:Mutex<HashMap<String,RevocationSnapshotSlot>>}
include!("checks.rs");
#[tokio::main(flavor="current_thread")]
async fn main(){
 let url=std::env::var("REDIS_URL").unwrap();
 let prefix=format!("a1-audit-{}-{}",std::process::id(),now_ms());
 let owned=Arc::new(counted_cache::CountedCache(redis_cache_manager::RedisCacheManager::with_url(&url,Some(&prefix)).await.unwrap()));
 let cache:Arc<dyn CacheManager>=owned.clone();
 for records in [128,1024] {for sessions in [100,1000] {
  if std::env::var_os("DIAGNOSTIC").is_some() && (records!=1024||sessions!=100) {continue;}
  let app=format!("app-{records}-{sessions}");let issued=now_ms()-1000;
  for n in 0..records {let record=AblyRevocationRecord{target_type:"clientId".into(),target_value:format!("other-{n}"),issued_before:issued+1,applies_at:issued};cache.set(&revocation_cache_key(&app,"clientId",&record.target_value),&serde_json::to_string(&record).unwrap(),3600).await.unwrap();}
  let authorization=Arc::new(ConnectionAuthorization{revocable:true,issued_ms:issued,client_id:Some("victim".into()),revocation_key:None});
  for rep in 0..7 {
   let hub=Arc::new(Hub{cache:Some(cache.clone()),..Default::default()});let channels=Arc::new(HashMap::new());
   ALLOCS.store(0,Ordering::Relaxed);BYTES.store(0,Ordering::Relaxed);CACHE_CALLS.store(0,Ordering::Relaxed);READ_BYTES.store(0,Ordering::Relaxed);
   let start=Instant::now();let mut tasks=tokio::task::JoinSet::new();
   for _ in 0..sessions {let hub=hub.clone();let authorization=authorization.clone();let channels=channels.clone();let app=app.clone();tasks.spawn(async move {
    let start=Instant::now();
    #[cfg(feature="current")]let revoked=hub.authorization_is_revoked_from_snapshot(&app,&authorization,&channels).await;
    #[cfg(not(feature="current"))]let revoked=hub.authorization_is_revoked(&app,&authorization,&channels).await;
    assert!(!revoked);start.elapsed().as_micros()
   });}
   let mut latency=Vec::new();while let Some(task)=tasks.join_next().await {latency.push(task.unwrap());}
   let us=start.elapsed().as_micros();let allocations=ALLOCS.load(Ordering::Relaxed);let allocated_bytes=BYTES.load(Ordering::Relaxed);let calls=CACHE_CALLS.load(Ordering::Relaxed);let read_bytes=READ_BYTES.load(Ordering::Relaxed);latency.sort_unstable();assert_eq!(latency.len(),sessions);assert_eq!(hub.metrics.0.load(Ordering::Relaxed),0);
   println!("a1,records={records},sessions={sessions},rep={rep},allowed={sessions},us={us},p50_us={},p95_us={},p99_us={},allocations={allocations},allocated_bytes={allocated_bytes},cache_calls={calls},read_bytes={read_bytes}",latency[sessions/2],latency[sessions*95/100],latency[sessions*99/100]);
  }
 }}
 #[cfg(feature="current")]
 {
  let app="remote-freshness";
  let authorization=ConnectionAuthorization{revocable:true,issued_ms:now_ms()-1000,client_id:Some("victim".into()),revocation_key:None};
  let hub=Hub{cache:Some(cache.clone()),..Default::default()};let channels=HashMap::new();
  assert!(!hub.authorization_is_revoked_from_snapshot(app,&authorization,&channels).await);
  let record=AblyRevocationRecord{target_type:"clientId".into(),target_value:"victim".into(),issued_before:authorization.issued_ms+1,applies_at:authorization.issued_ms};
  cache.set(&revocation_cache_key(app,"clientId","victim"),&serde_json::to_string(&record).unwrap(),3600).await.unwrap();
  let start=Instant::now();assert!(hub.authorization_is_revoked(app,&authorization,&channels).await);let fresh_us=start.elapsed().as_micros();
  while !hub.authorization_is_revoked_from_snapshot(app,&authorization,&channels).await {assert!(start.elapsed()<Duration::from_millis(500));tokio::time::sleep(Duration::from_millis(10)).await;}
  assert!(start.elapsed()<Duration::from_millis(500));
  println!("a1fresh,fresh_rejected=1,active_rejected=1,fresh_us={fresh_us},active_us={}",start.elapsed().as_micros());
 }
 owned.0.clear_prefix().await.unwrap();
}
