mod recovery;
use std::{alloc::{GlobalAlloc,Layout,System},sync::{Arc,atomic::{AtomicU64,AtomicUsize,Ordering}},time::{Duration,Instant}};
use async_trait::async_trait;
use sockudo_core::{app::{App,AppManager,AppPolicy}, cache::CacheManager, metrics::MetricsInterface, options::{QueueReliabilityConfig,WebhookRetryConfig}, queue::QueueInterface, webhook_types::{JobData,JobPayload,JobProcessorFnAsync,Webhook}, error::Result};
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
fn job(id:&str)->JobData {JobData{job_id:Some(id.into()),app_key:format!("{id}-key"),app_id:id.into(),app_secret:"synthetic-secret".into(),payload:JobPayload{time_ms:1,events:vec![json!({"name":"member_added","channel":"presence-perf","user_id":id})]},original_signature:String::new()}}
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
#[derive(Clone)] struct BlockQueue { open:Arc<AtomicUsize>,accepted:Arc<AtomicUsize> }
#[async_trait] impl QueueInterface for BlockQueue {
 async fn add_to_queue(&self,_:&str,data:JobData)->Result<()> {self.add_batch_to_queue("",vec![data]).await}
 async fn add_batch_to_queue(&self,_:&str,data:Vec<JobData>)->Result<()> {while self.open.load(Ordering::Relaxed)==0{tokio::time::sleep(Duration::from_millis(1)).await;}self.accepted.fetch_add(data.iter().map(|j|j.payload.events.len()).sum::<usize>(),Ordering::Relaxed);Ok(())}
 async fn process_queue(&self,_:&str,_:JobProcessorFnAsync)->Result<()>{Ok(())} async fn disconnect(&self)->Result<()>{Ok(())} async fn check_health(&self)->Result<()>{Ok(())}
}
async fn batch(){
 use sockudo_webhook::integration::{WebhookIntegration,WebhookConfig,BatchingConfig,QueueManager};
 let q=BlockQueue{open:Arc::new(AtomicUsize::new(0)),accepted:Arc::new(AtomicUsize::new(0))};
 let mut a=app("batch");a.policy.webhooks=Some(vec![Webhook{event_types:vec!["member_added".into()],..Default::default()}]);
 let integration=WebhookIntegration::new(WebhookConfig{batching:BatchingConfig{enabled:true,duration:5,size:100},..Default::default()},Arc::new(MemoryAppManager::new()),Some(Arc::new(QueueManager::new(Box::new(q.clone()))))).await.unwrap();
 let start=snapshot();let mut accepted=0;let mut rejected=0;
 for n in 0..100_000{match integration.send_member_added(&a,"presence-perf",&n.to_string()).await{Ok(())=>accepted+=1,Err(_)=>rejected+=1}if n%1000==0{tokio::task::yield_now().await;}}
 report("S7_blocked_admission",start,json!({"accepted":accepted,"rejected":rejected,"delivered":q.accepted.load(Ordering::Relaxed)}));
 q.open.store(1,Ordering::Relaxed);
 tokio::time::timeout(Duration::from_secs(10),async{while q.accepted.load(Ordering::Relaxed)<accepted{tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
 assert_eq!(accepted,q.accepted.load(Ordering::Relaxed));println!("{}",json!({"case":"S7_recovered","accepted":accepted,"delivered":q.accepted.load(Ordering::Relaxed)}));
}
async fn redis_cache(){
 use sockudo_cache::redis_cache_manager::{RedisCacheManager,RedisCacheConfig};
 let cache=RedisCacheManager::new(RedisCacheConfig{url:std::env::var("REDIS_URL").unwrap(),prefix:format!("services-bench-{}",std::process::id()),..Default::default()}).await.unwrap();
 let payload="x".repeat(4096);for n in 0..10_000{cache.set(&format!("item-{n:05}"),&payload,120).await.unwrap();}
 for repeat in 0..5{let start=snapshot();let mut cursor=None;let mut count=0;loop{let page=cache.scan_prefix_page("item-",cursor,256).await.unwrap();for (_,v) in page.entries{assert_eq!(v,payload);count+=1;}cursor=page.next_cursor;if cursor.is_none(){break;}}assert_eq!(count,10_000);report("S4_redis_sweep",start,json!({"repeat":repeat,"count":count}));}
 for n in 0..10_000{cache.remove(&format!("item-{n:05}")).await.unwrap();}
}
async fn redis_queue(){
 use sockudo_queue::RedisQueueManager;
 let manager=RedisQueueManager::new_with_config(&std::env::var("REDIS_URL").unwrap(),None,&format!("services-skew-{}",std::process::id()),4,5000,QueueReliabilityConfig{worker_prefetch:1,worker_poll_interval_ms:5,completed_retention:0,event_retention:0,..Default::default()}).await.unwrap();
 let completed=Arc::new(AtomicUsize::new(0));let slow=Arc::new(AtomicUsize::new(0));let mut jobs=Vec::new();for n in 0..1000{jobs.push(job(&n.to_string()));}
 manager.add_batch_to_queue("skew",jobs).await.unwrap();let start=snapshot();let c=completed.clone();let slow_c=slow.clone();
 manager.process_queue("skew",Box::new(move|job|{let c=c.clone();let slow=slow_c.clone();Box::pin(async move{let n:usize=job.app_id.parse().unwrap();if n%100==0{slow.fetch_add(1,Ordering::Relaxed);tokio::time::sleep(Duration::from_millis(100)).await;}c.fetch_add(1,Ordering::Relaxed);Ok(())})})).await.unwrap();
 tokio::time::timeout(Duration::from_secs(30),async{while completed.load(Ordering::Relaxed)<1000{tokio::time::sleep(Duration::from_millis(1)).await;}}).await.unwrap();report("S8_redis_skew",start,json!({"delivered":completed.load(Ordering::Relaxed),"slow":slow.load(Ordering::Relaxed)}));assert_eq!(slow.load(Ordering::Relaxed),10);manager.disconnect().await.unwrap();
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
#[tokio::main(flavor="multi_thread",worker_threads=4)]async fn main(){match std::env::args().nth(1).as_deref(){Some("feedback")=>recovery::feedback().await,Some("limiter")=>recovery::limiter().await,Some("lambda")=>recovery::lambda().await,Some("http")=>recovery::http().await,Some("readiness")=>readiness().await,Some("metrics")=>metrics().await,Some("batch")=>batch().await,Some("redis-cache")=>redis_cache().await,Some("redis-queue")=>redis_queue().await,_=>panic!("mode required")}}
