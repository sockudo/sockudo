use std::{alloc::{GlobalAlloc,Layout,System},sync::{Arc,atomic::{AtomicU64,AtomicUsize,Ordering}},time::{Duration,Instant}};
use async_trait::async_trait;
use sockudo_core::{app::{App,AppManager,AppPolicy}, cache::CacheManager, metrics::MetricsInterface, error::Result};
use sockudo_app::memory_app_manager::MemoryAppManager;
use sonic_rs::json;
static ALLOCS:AtomicU64=AtomicU64::new(0); static BYTES:AtomicU64=AtomicU64::new(0);
struct Alloc;
unsafe impl GlobalAlloc for Alloc {
 unsafe fn alloc(&self,l:Layout)->*mut u8 { ALLOCS.fetch_add(1,Ordering::Relaxed); BYTES.fetch_add(l.size() as u64,Ordering::Relaxed); unsafe{System.alloc(l)} }
 unsafe fn dealloc(&self,p:*mut u8,l:Layout){unsafe{System.dealloc(p,l)}}
 unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8{ALLOCS.fetch_add(1,Ordering::Relaxed);BYTES.fetch_add(n as u64,Ordering::Relaxed);unsafe{System.realloc(p,l,n)}}
}
#[global_allocator] static A:Alloc=Alloc;
fn app(id:&str)->App {App::from_policy(id.into(),format!("{id}-key"),"synthetic-secret".into(),true,AppPolicy::default())}
fn snapshot()->(Instant,u64,u64){(Instant::now(),ALLOCS.load(Ordering::Relaxed),BYTES.load(Ordering::Relaxed))}
fn report(label:&str,s:(Instant,u64,u64),extra:sonic_rs::Value){println!("{}",json!({"case":label,"elapsed_us":s.0.elapsed().as_micros(),"allocations":ALLOCS.load(Ordering::Relaxed)-s.1,"allocated_bytes":BYTES.load(Ordering::Relaxed)-s.2,"verified":extra}));}
async fn metrics(){
 let driver=sockudo_metrics::PrometheusMetricsDriver::new(6001,Some("perf_")).await;
 driver.mark_ws_message_received("app",64);
 for repeat in 0..5 {let start=snapshot();for _ in 0..100_000{driver.mark_ws_message_received("app",64);}report("S6_ws_100k",start,json!({"repeat":repeat}));}
 let text=driver.get_metrics_as_plaintext().await;assert!(text.lines().any(|line|line.starts_with("perf_ws_messages_received_total{")&&line.ends_with("500001")),"missing exact received count");
 for count in [1000,10_000,100_000] {
  let start=snapshot();for n in 0..count {driver.mark_annotation_published(&format!("presence-{count}-{n}"),&format!("custom-{n}:total.v1"));driver.mark_annotation_projection_rebuild(&format!("presence-{count}-{n}"));driver.track_annotation_projection_rebuild_duration(&format!("presence-{count}-{n}"),0.001);}
  report("S5_churn",start,json!({"count":count}));
  for repeat in 0..3{let start=snapshot();let rendered=driver.get_metrics_as_plaintext().await;let series=rendered.lines().filter(|line|!line.starts_with('#')).count();let total:f64=rendered.lines().filter(|line|line.starts_with("perf_annotations_published_total{")).map(|line|line.rsplit(' ').next().unwrap().parse::<f64>().unwrap()).sum();report("S5_scrape",start,json!({"repeat":repeat,"count":count,"series":series,"bytes":rendered.len(),"published":total}));assert!(total>=count as f64);}
 }
}
#[derive(Default)]struct CountingCache{sets:AtomicUsize}
#[async_trait]impl CacheManager for CountingCache{
 async fn has(&self,_:&str)->Result<bool>{Ok(false)}async fn get(&self,_:&str)->Result<Option<String>>{Ok(None)}
 async fn set(&self,_:&str,_:&str,_:u64)->Result<()>{self.sets.fetch_add(1,Ordering::Relaxed);Ok(())}
 async fn remove(&self,_:&str)->Result<()>{Ok(())}async fn disconnect(&self)->Result<()>{Ok(())}async fn ttl(&self,_:&str)->Result<Option<Duration>>{Ok(None)}
 async fn scan_prefix(&self,_:&str,_:usize)->Result<Vec<(String,String)>>{Ok(vec![])}
 async fn scan_prefix_page(&self,_:&str,_:Option<String>,_:usize)->Result<sockudo_core::cache::CacheScanPage>{Ok(Default::default())}
}
async fn readiness(){
 for count in [1,100,1000]{let inner=Arc::new(MemoryAppManager::new());for n in 0..count{inner.create_app(app(&n.to_string())).await.unwrap();}let cache=Arc::new(CountingCache::default());let manager=sockudo_app::cached_app_manager::CachedAppManager::new(inner,cache.clone(),sockudo_core::options::CacheSettings{enabled:true,ttl:60});
 for repeat in 0..5{let before=cache.sets.load(Ordering::Relaxed);let start=snapshot();let apps=manager.get_apps().await.unwrap();assert_eq!(apps.len(),count);report("S2_enumeration",start,json!({"repeat":repeat,"count":count,"sets":cache.sets.load(Ordering::Relaxed)-before}));}}
}

#[path="../../../diagnostics/src/bin/services.rs"] mod diagnostic;
#[tokio::main(flavor="multi_thread",worker_threads=4)]async fn main(){match std::env::args().nth(1).as_deref(){Some("metrics")=>metrics().await,Some("readiness")=>readiness().await,Some("apps")=>diagnostic::run_services_diagnostics().await,_=>panic!("mode required")}}
