use sockudo_push::{domain::*,feedback::PushFeedbackProcessor,pipeline::{MemoryPushQueue,PushQueue,PushQueueStage,PushQueuePayload},sql::PostgresPushStore,storage::PushPublishStatusStore};
use std::{sync::Arc,time::Instant};
use sonic_rs::json;
#[tokio::main(flavor="multi_thread",worker_threads=4)]
async fn main(){
 let pool=sqlx::PgPool::connect(&std::env::var("SOCKUDO_FEEDBACK_BENCH_POSTGRES_URL").unwrap()).await.unwrap();
 for group in [1,64]{for repeat in 0..3{
  let app=format!("feedback-bench-{}-{group}-{repeat}",std::process::id());
  let store=Arc::new(PostgresPushStore::new(pool.clone()));let queue=Arc::new(MemoryPushQueue::new());let processor=PushFeedbackProcessor::new(store.clone(),queue.clone());let count=256;let batch=if group==1{64}else{4};
  for campaign in 0..count/group {store.put_publish_status(PublishStatus{app_id:app.clone(),publish_id:format!("publish-{campaign}"),state:PublishLifecycleState::Dispatching,counters:PublishCounters{planned:group as u64,..Default::default()},fanout_regime:None,retry_after_ms:None,error_reason:None}).await.unwrap();}
  let mut elapsed=0;let mut processed=0;let mut run_calls=0;
  // Equal batches on both sides:64 singleton campaigns or4 same-campaign outcomes.
  // The baseline loses counters under64 concurrent same-campaign CAS writes; that
  // failed correctness workload is retained separately, not compared as throughput.
  for chunk in 0..count/batch {
   for i in 0..batch {let n=chunk*batch+i;let result=DeliveryResult{app_id:app.clone(),publish_id:format!("publish-{}",n/group),provider:PushProviderKind::Fcm,batch_id:format!("batch-{n}"),device_id:None,outcome:DeliveryOutcome::Accepted,provider_message_id:Some(format!("provider-{n}")),error:None,attempt:1};queue.produce(PushQueueStage::DeliveryResults,format!("result-{n}"),PushQueuePayload::DeliveryResult(Box::new(result))).await.unwrap();}
   let started=Instant::now();let mut completed=0;while completed<batch{let done=processor.run_once("feedback-perf").await.unwrap();run_calls+=1;completed+=done;assert!(started.elapsed().as_secs()<30,"feedback did not drain safely");if completed<batch{tokio::time::sleep(std::time::Duration::from_millis(1)).await;}}elapsed+=started.elapsed().as_micros();assert_eq!(completed,batch);processed+=completed;
  }
  let mut revisions=0;for campaign in 0..count/group{let value=store.get_versioned_publish_status(&app,&format!("publish-{campaign}")).await.unwrap().unwrap();assert_eq!(value.status.counters.succeeded,group as u64);assert_eq!(value.status.counters.dispatched,group as u64);revisions+=value.revision-1;}
  assert_eq!(queue.lag(PushQueueStage::DeliveryResults).await.unwrap().inflight_depth,0);
  println!("{}",json!({"case":"P5_P6_postgres_feedback","group":group,"repeat":repeat,"processed":processed,"elapsed_us":elapsed,"status_revision_increases":revisions,"queue_batch_limit":batch,"run_calls":run_calls}));
 }}
 pool.close().await;
}
